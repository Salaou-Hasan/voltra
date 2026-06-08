// ============================================================================
// Scenario 2 — Full Engine Benchmark with Subscriptions + All Subsystems Live
//
// Tests the complete in-process pipeline simultaneously:
//
//   ┌─────────────────────────────────────────────────────────────────┐
//   │  Tokio runtime  →  kanal channel  →  N reducer workers         │
//   │       ↓                                    ↓                   │
//   │  ReducerContext                      ctx.commit()              │
//   │       ↓                                    ↓                   │
//   │  TableStore (DashMap)            BatchedWalWriter (async)      │
//   │       ↓                                    ↓                   │
//   │  SubscriptionManager.publish_deltas()   Arc<Bytes> fan-out     │
//   └─────────────────────────────────────────────────────────────────┘
//
// Scenarios measured:
//
//  A. Zero subscribers           — baseline, confirms no fan-out overhead
//  B. 1 subscriber (no filter)   — one sub_rx channel write per delta
//  C. 10 subscribers             — hot fan-out path
//  D. 100 subscribers            — stress fan-out (MMO-style broadcast)
//  E. 10 subs + WAL enabled      — full pipeline with disk write batching
//  F. Concurrent: 8 producer tasks + 10 subscribers simultaneously
//
// Run with:
//   cargo bench --bench scenario2_full_pipeline
// ============================================================================

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use neondb::{
    reducer::{increment_reducer, ReducerContext},
    subscriptions::{OutboundFrames, SubscriptionManager},
    table::TableStore,
    wal::BatchedWalWriter,
};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// ── helpers ───────────────────────────────────────────────────────────────────

fn new_rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get().max(2))
        .enable_all()
        .build()
        .unwrap()
}

fn new_store() -> Arc<TableStore> {
    Arc::new(TableStore::new())
}

/// Register `n` subscribers all watching the "counters" table (no predicate).
/// Returns the receivers so they stay alive (if dropped, sends are silently ignored).
fn register_n_subscribers(
    mgr: &Arc<SubscriptionManager>,
    n: usize,
) -> Vec<tokio::sync::mpsc::Receiver<OutboundFrames>> {
    let mut rxs = Vec::with_capacity(n);
    for i in 0..n {
        let (tx, rx) = mpsc::channel::<OutboundFrames>(4096);
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, format!("sub_{}", i), "counters".to_string())
            .unwrap();
        rxs.push(rx);
    }
    rxs
}

// ── Bench A/B/C/D: fan-out at varying subscriber counts ──────────────────────

fn bench_fanout_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_pipeline/fanout_scaling");
    group.throughput(Throughput::Elements(1));

    for n_subs in [0usize, 1, 10, 100] {
        let tables = new_store();
        let mgr = Arc::new(SubscriptionManager::new());
        let _rxs = register_n_subscribers(&mgr, n_subs);

        group.bench_with_input(
            BenchmarkId::new("subscribers", n_subs),
            &n_subs,
            |b, _| {
                b.iter(|| {
                    let mut ctx = ReducerContext::new(tables.clone(), 1_000);
                    let _ = increment_reducer(
                        black_box(&mut ctx),
                        "hp".to_string(),
                        black_box(1),
                    )
                    .unwrap();
                    let deltas = ctx.commit().unwrap();
                    mgr.publish_deltas(black_box(&deltas));
                });
            },
        );
    }

    group.finish();
}

// ── Bench E: full pipeline — reducer + WAL (no-fsync) + subscriptions ────────

fn bench_full_pipeline_with_wal(c: &mut Criterion) {
    let _rt = new_rt();
    let mut group = c.benchmark_group("full_pipeline/with_wal");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);

    for n_subs in [0usize, 10, 50] {
        let tables = new_store();
        let mgr = Arc::new(SubscriptionManager::new());
        let _rxs = register_n_subscribers(&mgr, n_subs);

        // WAL with no-fsync so we measure batching overhead, not SSD latency.
        let wal_path = std::env::temp_dir()
            .join(format!("neondb_bench_s2_{}.wal", n_subs));
        let _ = std::fs::remove_file(&wal_path);
        let wal = Arc::new(
            BatchedWalWriter::open(&wal_path, 10, 512, /*unsafe_no_fsync=*/true).unwrap(),
        );
        let seq = Arc::new(std::sync::atomic::AtomicU64::new(0));

        group.bench_with_input(
            BenchmarkId::new("subs_plus_wal", n_subs),
            &n_subs,
            |b, _| {
                b.iter(|| {
                    let mut ctx = ReducerContext::new(tables.clone(), 1_000);
                    let _ = increment_reducer(
                        black_box(&mut ctx),
                        "hp".to_string(),
                        black_box(1),
                    )
                    .unwrap();
                    let deltas = ctx.commit().unwrap();

                    let s = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = neondb::wal::WalEntry::new(
                        1_000,
                        s,
                        "increment".to_string(),
                        vec![],
                        deltas.clone(),
                    );
                    wal.append(&entry, s).unwrap();
                    mgr.publish_deltas(black_box(&deltas));
                });
            },
        );
    }

    group.finish();
}

// ── Bench F: concurrent producers — N async tasks + M subscribers ────────────
//
// Closest to production: kanal channel + tokio spawn_blocking workers
// + subscription fan-out all running simultaneously inside one Tokio runtime.
fn bench_concurrent_producers(c: &mut Criterion) {
    let rt = new_rt();
    let mut group = c.benchmark_group("full_pipeline/concurrent_producers");
    group.sample_size(10);

    for (producers, subs) in [(1, 0), (4, 0), (4, 10), (8, 10), (8, 50)] {
        let total_calls: u64 = producers as u64 * 2_000;
        group.throughput(Throughput::Elements(total_calls));

        let tables = Arc::new(TableStore::new());
        let mgr = Arc::new(SubscriptionManager::new());
        let _rxs = register_n_subscribers(&mgr, subs);

        group.bench_with_input(
            BenchmarkId::new(format!("{}prod_{}subs", producers, subs), total_calls),
            &(producers, subs),
            |b, &(p, _)| {
                b.iter(|| {
                    rt.block_on(async {
                        let tables = tables.clone();
                        let mgr = mgr.clone();
                        let barrier = Arc::new(tokio::sync::Barrier::new(p));
                        let mut handles = Vec::with_capacity(p);

                        for tid in 0..p {
                            let tbl = tables.clone();
                            let m = mgr.clone();
                            let bar = barrier.clone();
                            handles.push(tokio::spawn(async move {
                                bar.wait().await;
                                for i in 0u64..2_000 {
                                    let key = format!("c_{}", (tid * 16 + i as usize) % 128);
                                    let tbl2 = tbl.clone();
                                    let (deltas, _) = tokio::task::spawn_blocking(move || {
                                        let mut ctx =
                                            ReducerContext::new(tbl2, 1_000);
                                        let _ = increment_reducer(&mut ctx, key, 1)
                                            .unwrap();
                                        let deltas = ctx.commit().unwrap();
                                        (deltas, ())
                                    })
                                    .await
                                    .unwrap();
                                    m.publish_deltas(&deltas);
                                }
                            }));
                        }

                        for h in handles {
                            h.await.unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

// ── Bench G: subscription predicate evaluation cost ──────────────────────────
//
// With a WHERE filter (e.g. "counters WHERE value >= 100"), every delta must
// be evaluated against the predicate.  Measures filter overhead at scale.
fn bench_predicate_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_pipeline/predicate_filter");
    group.throughput(Throughput::Elements(1));

    let tables = new_store();
    let mgr = Arc::new(SubscriptionManager::new());

    // Half subs match (value >= 0 always true for positive increments),
    // half don't (value >= 999999).
    let mut rxs = Vec::new();
    for i in 0..20usize {
        let (tx, rx) = mpsc::channel::<OutboundFrames>(4096);
        let cid = mgr.register_client(tx);
        let query = if i < 10 {
            "counters WHERE value >= 0".to_string()
        } else {
            "counters WHERE value >= 999999".to_string()
        };
        mgr.subscribe(cid, format!("sub_{}", i), query).unwrap();
        rxs.push(rx);
    }

    group.bench_function("20_subs_mixed_predicates", |b| {
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1_000);
            let _ =
                increment_reducer(black_box(&mut ctx), "score".to_string(), black_box(1))
                    .unwrap();
            let deltas = ctx.commit().unwrap();
            mgr.publish_deltas(black_box(&deltas));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_fanout_scaling,
    bench_full_pipeline_with_wal,
    bench_concurrent_producers,
    bench_predicate_fanout,
);
criterion_main!(benches);
