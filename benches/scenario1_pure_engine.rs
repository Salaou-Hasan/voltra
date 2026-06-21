// ============================================================================
// Scenario 1 — Pure Engine Benchmark (no subscriptions, no WAL, no network)
//
// Measures the raw throughput of the table engine + reducer pipeline in
// complete isolation.  Every subsystem that adds latency outside the core
// read/write path is bypassed:
//
//   • No WAL writes
//   • No subscription fan-out
//   • No Tokio runtime / async overhead
//   • No WebSocket framing
//
// What IS exercised:
//   • ReducerContext construction
//   • DashMap read-your-writes + apply_delta
//   • serde_json serialisation / deserialisation (Arc<Bytes> path)
//   • increment_reducer logic
//   • ctx.commit() → TableStore.apply_delta()
//
// Run with:
//   cargo bench --bench scenario1_pure_engine
//
// All groups use Criterion throughput reporting — output shows both ns/iter
// and millions-of-ops/sec.
// ============================================================================

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use voltra::{
    reducer::{increment_reducer, ReducerContext},
    table::TableStore,
};
use std::sync::Arc;

// ── helpers ───────────────────────────────────────────────────────────────────

fn new_store() -> Arc<TableStore> {
    Arc::new(TableStore::new())
}

// ── Bench A: single-threaded increment — full cycle ───────────────────────────
//
// Absolute baseline: one increment call per iteration including
// ReducerContext allocation and commit.  Comparable to the existing
// throughput bench so you can see if anything regressed.
fn bench_single_increment(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_engine/single_thread");
    group.throughput(Throughput::Elements(1));

    let tables = new_store();

    // Full cycle: construct ctx → reduce → commit
    group.bench_function("increment_full_cycle", |b| {
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1_000);
            let _ = increment_reducer(
                black_box(&mut ctx),
                black_box("hp".to_string()),
                black_box(1),
            )
            .unwrap();
            ctx.commit().unwrap();
        });
    });

    // Reduce only — skips commit (measures just the reducer + staged write)
    group.bench_function("increment_no_commit", |b| {
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1_000);
            let _ = increment_reducer(
                black_box(&mut ctx),
                black_box("hp".to_string()),
                black_box(1),
            )
            .unwrap();
        });
    });

    group.finish();
}

// ── Bench B: batched writes — N increments per ctx.commit() ──────────────────
//
// Games batch many writes per server tick.  Shows how commit overhead scales
// as the pending_deltas Vec grows (each element is an Arc<Bytes> clone —
// expected to be near-flat up to cache pressure).
fn bench_batch_increment(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_engine/batch");

    for batch_size in [1u64, 4, 16, 64, 256] {
        group.throughput(Throughput::Elements(batch_size));
        let tables = new_store();

        group.bench_with_input(
            BenchmarkId::new("N_increments_per_commit", batch_size),
            &batch_size,
            |b, &n| {
                b.iter(|| {
                    let mut ctx = ReducerContext::new(tables.clone(), 1_000);
                    for i in 0..n {
                        // 32 distinct counter names — realistic hot-key spread
                        let key = format!("counter_{}", i % 32);
                        let _ =
                            increment_reducer(&mut ctx, black_box(key), black_box(1)).unwrap();
                    }
                    ctx.commit().unwrap();
                });
            },
        );
    }

    group.finish();
}

// ── Bench C: multi-table write per reducer ────────────────────────────────────
//
// Most game reducers touch > 1 table (player row + event log + counter).
// Measures 3 staged writes + 1 commit.
fn bench_multi_table_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_engine/multi_table");
    group.throughput(Throughput::Elements(3)); // 3 table writes per call

    let tables = new_store();

    group.bench_function("3_tables_per_reducer", |b| {
        b.iter(|| {
            let mut ctx = ReducerContext::new(tables.clone(), 1_000);

            // Simulate: update player row
            ctx.set_row(
                "players".to_string(),
                "player_1".to_string(),
                serde_json::json!({ "id": 1, "hp": 90, "mana": 50 }),
            )
            .unwrap();

            // Log a damage event
            ctx.set_row(
                "events".to_string(),
                "evt_1".to_string(),
                serde_json::json!({ "type": "damage", "amount": 10 }),
            )
            .unwrap();

            // Bump a global counter
            let _ = increment_reducer(&mut ctx, "damage_dealt".to_string(), 10).unwrap();

            ctx.commit().unwrap();
        });
    });

    group.finish();
}

// ── Bench D: parallel multi-threaded throughput ──────────────────────────────
//
// N std::threads all hitting the same Arc<TableStore>.
// Measures aggregate committed TPS and DashMap contention under real
// multi-core pressure — no Tokio overhead, pure engine.
fn bench_parallel_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_engine/parallel");
    // Use shorter sample time for the expensive parallel bench
    group.sample_size(10);

    let cpus = num_cpus::get();
    let thread_counts: Vec<usize> = {
        let mut v = vec![1usize];
        if cpus >= 2 { v.push(2); }
        if cpus >= 4 { v.push(4); }
        if cpus > 4  { v.push(cpus); }
        v
    };

    for threads in thread_counts {
        let iters_per_thread: u64 = 5_000;
        let total = iters_per_thread * threads as u64;
        group.throughput(Throughput::Elements(total));

        let tables = Arc::new(TableStore::new());

        group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &t| {
                b.iter(|| {
                    let barrier = Arc::new(std::sync::Barrier::new(t + 1));
                    let mut handles = Vec::with_capacity(t);

                    for tid in 0..t {
                        let tbl = tables.clone();
                        let bar = barrier.clone();
                        handles.push(std::thread::spawn(move || {
                            bar.wait(); // all threads start simultaneously
                            for i in 0..iters_per_thread {
                                // 256 distinct keys → realistic key spread
                                let key =
                                    format!("c_{}", (tid * 16 + i as usize) % 256);
                                let mut ctx = ReducerContext::new(tbl.clone(), 1_000);
                                let _ = increment_reducer(&mut ctx, key, 1).unwrap();
                                ctx.commit().unwrap();
                            }
                        }));
                    }
                    barrier.wait(); // start line
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ── Bench E: read-dominant workload ──────────────────────────────────────────
//
// Leaderboards, inventory checks, stat lookups — all read-only paths.
// Tests DashMap scan throughput at realistic row counts.
fn bench_read_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_engine/reads");

    for row_count in [100u64, 1_000, 10_000] {
        // Seed the store
        let tables = Arc::new(TableStore::new());
        for i in 0..row_count {
            tables
                .set_counter(format!("counter_{}", i), i as i32, 0)
                .unwrap();
        }

        group.throughput(Throughput::Elements(row_count));
        group.bench_with_input(
            BenchmarkId::new("list_counters_rows", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(tables.list_counters().unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get_single_row_from", row_count),
            &row_count,
            |b, &n| {
                b.iter(|| {
                    let key = format!("counter_{}", n / 2); // median key
                    let _ = black_box(tables.get_counter(&key).unwrap());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_increment,
    bench_batch_increment,
    bench_multi_table_write,
    bench_parallel_throughput,
    bench_read_scan,
);
criterion_main!(benches);
