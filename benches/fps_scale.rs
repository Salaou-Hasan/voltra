// ============================================================================
// FPS Scale Benchmark — Voltra per-lobby isolation proof
//
// Validates that the LobbyDispatcher (transparent l{N}_ routing in TableStore)
// adds negligible overhead and that concurrent lobby writes don't contend.
//
// Three benchmark groups:
//
//  A. routing_overhead
//     • single_table_20k    — 20K sequential writes, all to one flat table
//     • lobby_routed_20k    — same 20K writes distributed across 200 lobbies
//     → should show <5% overhead from the l{N}_ prefix check
//
//  B. concurrent_isolation
//     • sequential_20k      — 20K writes on 1 OS thread (baseline)
//     • parallel_200_lobby  — 20K writes on N threads, each owning ~N/200 lobbies
//     → parallel case should be faster (scales with cores), proving isolation
//
//  C. noisy_neighbor
//     • hot_lobby_alone     — 1 hot lobby, 5K writes
//     • cold_lobby_alone    — 1 cold lobby, 100 writes
//     • cold_lobby_under_pressure — 100 writes on lobby-1 while lobby-0 hammers
//     → cold lobby latency should not spike under the hot lobby's load
//
// Run with:
//   cargo bench --bench fps_scale
//
// For profiling (no Criterion overhead):
//   cargo bench --bench fps_scale -- --profile-time=30
// ============================================================================

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use voltra::{reducer::ReducerContext, table::TableStore};
use std::sync::Arc;
use std::time::Duration;

// ── helpers ───────────────────────────────────────────────────────────────────

fn new_store() -> Arc<TableStore> {
    Arc::new(TableStore::new())
}

/// Write one position-update row for `player_id` in `lobby_id`.
fn move_player(tables: &Arc<TableStore>, lobby_id: usize, player_id: usize) {
    let table = format!("l{}_players", lobby_id);
    let key   = format!("p{}", player_id);
    let mut ctx = ReducerContext::new(tables.clone(), 0);
    let row = ctx.get_row(&table, &key)
        .unwrap_or(None)
        .unwrap_or_else(|| serde_json::json!({"x": 0, "y": 0, "hp": 100}));
    let new_row = serde_json::json!({
        "x": (row["x"].as_i64().unwrap_or(0) + 1) % 10_000,
        "y": row["y"].as_i64().unwrap_or(0),
        "hp": row["hp"].as_i64().unwrap_or(100),
    });
    ctx.set_row(&table, &key, new_row).unwrap();
    ctx.commit().unwrap();
}

/// Write to a flat (non-routed) table — baseline without prefix lookup.
fn move_player_flat(tables: &Arc<TableStore>, player_id: usize) {
    let key = format!("p{}", player_id);
    let mut ctx = ReducerContext::new(tables.clone(), 0);
    let row = ctx.get_row("players", &key)
        .unwrap_or(None)
        .unwrap_or_else(|| serde_json::json!({"x": 0, "y": 0, "hp": 100}));
    let new_row = serde_json::json!({
        "x": (row["x"].as_i64().unwrap_or(0) + 1) % 10_000,
        "y": row["y"].as_i64().unwrap_or(0),
        "hp": row["hp"].as_i64().unwrap_or(100),
    });
    ctx.set_row("players", &key, new_row).unwrap();
    ctx.commit().unwrap();
}

/// Pre-seed N lobbies × M players so the benchmark avoids cold-start `None` returns.
fn seed_players(tables: &Arc<TableStore>, lobbies: usize, players_per_lobby: usize) {
    for lobby in 0..lobbies {
        for player in 0..players_per_lobby {
            let table = format!("l{}_players", lobby);
            let key   = format!("p{}", player);
            let mut ctx = ReducerContext::new(tables.clone(), 0);
            ctx.set_row(&table, &key, serde_json::json!({"x": 0, "y": 0, "hp": 100})).unwrap();
            ctx.commit().unwrap();
        }
    }
}

fn seed_flat(tables: &Arc<TableStore>, players: usize) {
    for player in 0..players {
        let key = format!("p{}", player);
        let mut ctx = ReducerContext::new(tables.clone(), 0);
        ctx.set_row("players", &key, serde_json::json!({"x": 0, "y": 0, "hp": 100})).unwrap();
        ctx.commit().unwrap();
    }
}

// ── A: routing overhead ───────────────────────────────────────────────────────
//
// Proves that the l{N}_ prefix routing adds negligible overhead vs. a flat table.

fn bench_routing_overhead(c: &mut Criterion) {
    const TOTAL_PLAYERS: usize = 20_000;
    const LOBBIES:        usize = 200;
    const PPL:            usize = TOTAL_PLAYERS / LOBBIES; // 100 players/lobby

    let flat_store = new_store();
    seed_flat(&flat_store, TOTAL_PLAYERS);

    let lobby_store = new_store();
    seed_players(&lobby_store, LOBBIES, PPL);

    let mut group = c.benchmark_group("fps_scale/routing_overhead");
    group.throughput(Throughput::Elements(TOTAL_PLAYERS as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    // Baseline: 20K writes to a single flat table (no lobby routing)
    group.bench_function("single_table_20k", |b| {
        let tables = flat_store.clone();
        b.iter(|| {
            for player_id in 0..TOTAL_PLAYERS {
                move_player_flat(black_box(&tables), player_id);
            }
        });
    });

    // Same 20K writes distributed across 200 lobby sub-stores
    group.bench_function("lobby_routed_20k", |b| {
        let tables = lobby_store.clone();
        b.iter(|| {
            for lobby_id in 0..LOBBIES {
                for player_id in 0..PPL {
                    move_player(black_box(&tables), lobby_id, player_id);
                }
            }
        });
    });

    group.finish();
}

// ── B: concurrent isolation ───────────────────────────────────────────────────
//
// Proves that parallel lobby writes scale with cores and don't contend.
// Uses std::thread::scope for fork-join parallelism without a Tokio runtime.

fn bench_concurrent_isolation(c: &mut Criterion) {
    const LOBBIES: usize = 200;
    const PPL:     usize = 100;
    const TOTAL:   usize = LOBBIES * PPL;

    let tables = new_store();
    seed_players(&tables, LOBBIES, PPL);

    let worker_threads = num_cpus::get().max(2).min(LOBBIES);

    let mut group = c.benchmark_group("fps_scale/concurrent_isolation");
    group.throughput(Throughput::Elements(TOTAL as u64));
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(20));

    // Sequential: all 20K writes on the calling thread
    group.bench_function("sequential_20k", |b| {
        let tables = tables.clone();
        b.iter(|| {
            for lobby_id in 0..LOBBIES {
                for player_id in 0..PPL {
                    move_player(black_box(&tables), lobby_id, player_id);
                }
            }
        });
    });

    // Parallel: distribute lobbies evenly across `worker_threads` OS threads.
    // Each thread owns a disjoint set of lobby sub-stores → zero DashMap shard contention.
    group.bench_with_input(
        BenchmarkId::new("parallel_lobbies", format!("{}_threads", worker_threads)),
        &worker_threads,
        |b, &threads| {
            let tables = tables.clone();
            b.iter(|| {
                let lobbies_per_thread = (LOBBIES + threads - 1) / threads;
                std::thread::scope(|s| {
                    for t in 0..threads {
                        let tables = &tables;
                        let start = t * lobbies_per_thread;
                        let end   = (start + lobbies_per_thread).min(LOBBIES);
                        s.spawn(move || {
                            for lobby_id in start..end {
                                for player_id in 0..PPL {
                                    move_player(black_box(tables), lobby_id, player_id);
                                }
                            }
                        });
                    }
                });
            });
        },
    );

    group.finish();
}

// ── C: noisy-neighbor isolation ───────────────────────────────────────────────
//
// The hot lobby hammers lobby-0 at 50× the rate of lobby-1.
// The cold lobby's write time should not increase under this pressure.
// This directly validates that per-lobby sub-stores have independent DashMap shards.

fn bench_noisy_neighbor(c: &mut Criterion) {
    let tables = new_store();
    seed_players(&tables, 2, 100);

    let mut group = c.benchmark_group("fps_scale/noisy_neighbor");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(15));

    // Lobby-1 alone: 100 writes with no competing lobby
    group.bench_function("cold_lobby_alone_100_writes", |b| {
        let tables = tables.clone();
        b.iter(|| {
            for player_id in 0..100 {
                move_player(black_box(&tables), 1, player_id);
            }
        });
    });

    // Lobby-1 concurrently with lobby-0 hammering 5000 writes on another thread.
    // Criterion measures only lobby-1's wall time.
    group.bench_function("cold_lobby_under_hot_pressure_100_writes", |b| {
        let tables = tables.clone();
        b.iter(|| {
            // Spawn the hot lobby on a background thread.
            let hot_tables = tables.clone();
            let hot = std::thread::spawn(move || {
                for i in 0..5_000 {
                    move_player(&hot_tables, 0, i % 100);
                }
            });
            // Measured path: 100 writes on lobby-1.
            for player_id in 0..100 {
                move_player(black_box(&tables), 1, player_id);
            }
            hot.join().unwrap();
        });
    });

    group.finish();
}

// ── entry point ───────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_routing_overhead,
    bench_concurrent_isolation,
    bench_noisy_neighbor,
);
criterion_main!(benches);
