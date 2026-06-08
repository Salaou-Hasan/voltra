// ============================================================================
// NeonDB throughput benchmarks
//
// Scenario 1 — Pure engine (reducer hot-path)
//   increment_1x       : single-thread, full commit cycle
//   increment_no_commit: single-thread, no commit (reducer logic only)
//   parallel_Nx        : N threads, 5K iters each (measures shard contention)
//
// Scenario 2 — Subscription fan-out
//   fan_out_0_subs     : baseline (publish with no subscribers)
//   fan_out_1_sub      : 1 subscriber
//   fan_out_10_subs    : 10 subscribers (was 22× regression, now ~flat)
//   fan_out_50_subs    : 50 subscribers
//   fan_out_100_subs   : 100 subscribers (was 193× regression, now ~flat)
//   fan_out_cross_table: 100 subs on table_B, publish to table_A (reverse-index
//                        fast-path — should be near-zero marginal cost)
//
// Scenario 3 — Game genre workloads
//   fps_tick           : 0 subs, 2 writes/call (position ticks, no broadcast)
//   idle_clicker       : 0 subs, 1 write/call  (simplest case)
//   moba_tick          : 16 subs, 4 writes/call (MOBA state broadcast)
//   racing_tick        : 12 subs, 3 writes/call (racing position broadcast)
// ============================================================================

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use neondb::{
    reducer::{increment_reducer, ReducerContext},
    subscriptions::{OutboundFrames, SubscriptionManager},
    table::{RowDelta, TableStore},
};
use std::sync::Arc;
use tokio::sync::mpsc;

// ── Helpers ────────────────────────────────────────────────────────────────────

fn make_delta(table: &str, key: &str, v: i64) -> RowDelta {
    RowDelta {
        table_name: table.to_string(),
        operation: "update".to_string(),
        row_key: key.to_string(),
        row_id: 1,
        shard_id: 0,
        payload_arc: None,
        row_data: Some(serde_json::json!({"value": v})),
        counter_add_amount: 0,
        counter_add_timestamp: 0,
    }
}

/// Build a SubscriptionManager with `n` subscribers all watching `table`.
/// Returns the manager and the receivers (held alive to keep channels open).
fn build_sub_manager(
    n: usize,
    table: &str,
) -> (
    Arc<SubscriptionManager>,
    Vec<mpsc::Receiver<OutboundFrames>>,
) {
    let mgr = Arc::new(SubscriptionManager::new());
    let mut rxs = Vec::with_capacity(n);
    for i in 0..n {
        let (tx, rx) = mpsc::channel::<OutboundFrames>(4096);
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, format!("sub_{}", i), table.to_string())
            .unwrap();
        rxs.push(rx);
    }
    (mgr, rxs)
}

// ── Scenario 1: Pure engine ───────────────────────────────────────────────────

fn bench_scenario1_pure_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("scenario1_pure_engine");
    group.throughput(Throughput::Elements(1));

    // 1a: Full cycle (read + write + commit)
    group.bench_function("increment_full_cycle", |b| {
        let tables = Arc::new(TableStore::new());
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1000);
            increment_reducer(
                black_box(&mut ctx),
                black_box("counter".to_string()),
                black_box(1),
            )
            .unwrap();
            ctx.commit().unwrap();
        });
    });

    // 1b: No commit (reducer logic + DashMap write, no apply_delta)
    group.bench_function("increment_no_commit", |b| {
        let tables = Arc::new(TableStore::new());
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1000);
            increment_reducer(
                black_box(&mut ctx),
                black_box("counter".to_string()),
                black_box(1),
            )
            .unwrap();
        });
    });

    // 1c: Parallel scaling — 1 / 2 / 4 / 8 / 16 threads, 5K iters each
    for threads in [1usize, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("parallel_threads", threads),
            &threads,
            |b, &t| {
                let tables = Arc::new(TableStore::new());
                b.iter(|| {
                    let handles: Vec<_> = (0..t)
                        .map(|i| {
                            let tbl = tables.clone();
                            std::thread::spawn(move || {
                                for _ in 0..5_000 {
                                    let mut ctx = ReducerContext::new(tbl.clone(), 1000);
                                    increment_reducer(&mut ctx, format!("counter_{}", i % 256), 1)
                                        .unwrap();
                                    ctx.commit().unwrap();
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ── Scenario 2: Subscription fan-out ─────────────────────────────────────────

fn bench_scenario2_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("scenario2_fan_out");
    group.throughput(Throughput::Elements(1));

    let deltas = vec![make_delta("players", "hero_1", 42)];

    // Sub counts to benchmark
    for n_subs in [0usize, 1, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("fan_out_subs", n_subs),
            &n_subs,
            |b, &n| {
                let (mgr, _rxs) = build_sub_manager(n, "players");
                b.iter(|| {
                    mgr.publish_deltas(black_box(&deltas));
                });
            },
        );
    }

    // Cross-table fast path: 100 subscribers on table_B, publish to table_A.
    // With the reverse index this should be O(1) — just one failed DashMap
    // lookup — regardless of how many subscribers table_B has.
    group.bench_function("cross_table_fast_path_100_subs", |b| {
        let (mgr, _rxs) = build_sub_manager(100, "table_b");
        let deltas_a = vec![make_delta("table_a", "k1", 1)];
        b.iter(|| {
            mgr.publish_deltas(black_box(&deltas_a));
        });
    });

    group.finish();
}

// ── Scenario 3: Game genre workloads ─────────────────────────────────────────
//
// Each game genre has a characteristic (subscriber_count, writes_per_call).
// We simulate one "server tick" = one reducer call:
//   - produce N deltas (one per write)
//   - publish to M subscribers
//
// These numbers come from the benchmark analysis in the images:
//   FPS       :  0 subs, 2 writes  → ~345K writes/sec baseline
//   Idle      :  0 subs, 1 write   → ~189K TPS
//   MOBA      : 16 subs, 4 writes  → target ~17K TPS
//   Racing    : 12 subs, 3 writes  → target ~17K TPS
// ─────────────────────────────────────────────────────────────────────────────

fn bench_scenario3_game_genres(c: &mut Criterion) {
    let mut group = c.benchmark_group("scenario3_game_genres");

    struct Genre {
        name: &'static str,
        subscribers: usize,
        writes_per_call: usize,
    }

    let genres = [
        Genre {
            name: "fps_position_tick",
            subscribers: 0,
            writes_per_call: 2,
        },
        Genre {
            name: "idle_clicker",
            subscribers: 0,
            writes_per_call: 1,
        },
        Genre {
            name: "moba_state_tick",
            subscribers: 16,
            writes_per_call: 4,
        },
        Genre {
            name: "racing_tick",
            subscribers: 12,
            writes_per_call: 3,
        },
    ];

    for genre in &genres {
        let n_writes = genre.writes_per_call;
        let n_subs = genre.subscribers;
        let genre_name = genre.name;

        group.throughput(Throughput::Elements(n_writes as u64));

        group.bench_function(genre_name, |b| {
            let tables = Arc::new(TableStore::new());
            let (mgr, _rxs) = build_sub_manager(n_subs, "players");

            // Pre-build a vec of table/key strings so alloc is outside the hot loop
            let keys: Vec<String> = (0..n_writes).map(|i| format!("entity_{}", i)).collect();

            b.iter(|| {
                // Simulate one reducer call: N staged writes + commit + publish
                let mut ctx = ReducerContext::new(tables.clone(), 1000);
                for key in &keys {
                    ctx.set_row(
                        "players".to_string(),
                        key.clone(),
                        serde_json::json!({"x": 1.0, "y": 2.0, "hp": 100}),
                    )
                    .unwrap();
                }
                let deltas = ctx.commit().unwrap();
                mgr.publish_deltas(black_box(&deltas));
            });
        });
    }

    group.finish();
}

// ── Wire up ───────────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_scenario1_pure_engine,
    bench_scenario2_fan_out,
    bench_scenario3_game_genres,
);
criterion_main!(benches);
