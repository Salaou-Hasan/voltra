// ============================================================================
// Scenario 3 — Real-World Game Genre Benchmarks
//
// Each benchmark simulates the actual reducer patterns of a specific game
// genre.  All run with the full pipeline (subscriptions + WAL batching) so
// numbers reflect real production throughput, not just engine micro-perf.
//
// Genres covered:
//
//  1. MMORPG   — many players, shared world, heavy write bursts (combat, XP)
//  2. FPS/BR   — high-frequency position + health updates, large player count
//  3. RTS      — resource tick every N ms, unit orders, fog-of-war queries
//  4. Card Game — turn-based, complex multi-table transactions per action
//  5. Idle/Clicker — single counter hot-path, millions of increments/sec
//  6. Racing   — ordered leaderboard updates, tight update windows
//  7. MOBA     — 10-player match, ability cooldowns, gold/kill tracking
//
// Run with:
//   cargo bench --bench scenario3_game_genres
//
// The WAL uses unsafe_no_fsync=true throughout so disk latency doesn't
// dominate — set to false if you want to see real durable-write numbers.
// ============================================================================

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use neondb::{
    reducer::{increment_reducer, ReducerContext},
    subscriptions::{OutboundFrames, SubscriptionManager},
    table::TableStore,
    wal::{BatchedWalWriter, WalEntry},
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc::unbounded_channel;

// ── shared test infrastructure ────────────────────────────────────────────────

struct GameServer {
    tables: Arc<TableStore>,
    mgr: Arc<SubscriptionManager>,
    wal: Arc<BatchedWalWriter>,
    seq: Arc<AtomicU64>,
    /// Keep subscription receivers alive so channels stay open.
    _rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<OutboundFrames>>,
}

impl GameServer {
    fn new(wal_tag: &str, n_subs: usize, sub_table: &str) -> Self {
        let tables = Arc::new(TableStore::new());
        let mgr = Arc::new(SubscriptionManager::new());

        let mut rxs = Vec::with_capacity(n_subs);
        for i in 0..n_subs {
            let (tx, rx) = unbounded_channel::<OutboundFrames>();
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("sub_{}", i), sub_table.to_string())
                .unwrap();
            rxs.push(rx);
        }

        let wal_path =
            std::env::temp_dir().join(format!("neondb_bench_s3_{}.wal", wal_tag));
        let _ = std::fs::remove_file(&wal_path);
        let wal = Arc::new(
            BatchedWalWriter::open(&wal_path, 10, 512, /*unsafe_no_fsync=*/true).unwrap(),
        );

        GameServer {
            tables,
            mgr,
            wal,
            seq: Arc::new(AtomicU64::new(0)),
            _rxs: rxs,
        }
    }

    /// Run a reducer closure and commit it through the full pipeline.
    fn run<F>(&self, reducer_name: &str, f: F)
    where
        F: FnOnce(&mut ReducerContext),
    {
        let mut ctx = ReducerContext::new(self.tables.clone(), 1_000);
        f(&mut ctx);
        let deltas = ctx.commit().unwrap();
        let s = self.seq.fetch_add(1, Ordering::Relaxed);
        let entry = WalEntry::new(
            1_000, s,
            reducer_name.to_string(),
            vec![],
            deltas.clone(),
        );
        self.wal.append(&entry, s).unwrap();
        self.mgr.publish_deltas(&deltas);
    }
}

// ── 1. MMORPG ─────────────────────────────────────────────────────────────────
//
// Patterns: 100 concurrent players, each call = hit on enemy (damage player HP,
// award XP, log combat event, increment server-wide kill counter).
// 4 table writes per call.  10 clients subscribed to "players" table.
fn bench_mmorpg(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/mmorpg");
    group.throughput(Throughput::Elements(4)); // 4 writes per combat hit
    group.sample_size(50);

    let srv = GameServer::new("mmorpg", 10, "players");

    // Seed 100 players
    for i in 0u32..100 {
        srv.tables
            .set_row(
                "players".to_string(),
                format!("player_{}", i),
                serde_json::json!({ "id": i, "hp": 100, "xp": 0, "level": 1 }),
            )
            .unwrap();
    }

    let player_idx = std::sync::atomic::AtomicU64::new(0);

    group.bench_function("combat_hit_100_players", |b| {
        b.iter(|| {
            let pid = player_idx.fetch_add(1, Ordering::Relaxed) % 100;
            srv.run("combat_hit", |ctx| {
                // 1. Update attacker XP
                let _ = increment_reducer(ctx, format!("xp_player_{}", pid), 50).unwrap();
                // 2. Update target HP
                let hp_key = format!("player_{}", (pid + 1) % 100);
                let hp_val = serde_json::json!({
                    "id": (pid + 1) % 100,
                    "hp": black_box(90i32),
                    "xp": 0,
                    "level": 1,
                });
                ctx.set_row("players".to_string(), hp_key, hp_val).unwrap();
                // 3. Log combat event
                ctx.set_row(
                    "combat_log".to_string(),
                    format!("evt_{}", pid),
                    serde_json::json!({ "attacker": pid, "damage": 10 }),
                )
                .unwrap();
                // 4. Global kill counter
                let _ = increment_reducer(ctx, "total_kills".to_string(), 0).unwrap();
            });
        });
    });

    group.finish();
}

// ── 2. FPS / Battle Royale ────────────────────────────────────────────────────
//
// Patterns: 64 players, position update every 50ms tick (2 writes: position +
// zone), plus occasional kill events.  High frequency — tests pure write TPS.
// 0 subscribers (BR servers rarely push every position to all clients).
fn bench_fps_br(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/fps_battleroyale");
    group.sample_size(100);

    let srv = GameServer::new("fps", 0, "positions");

    // Bench A: position tick (2 writes per player)
    {
        group.throughput(Throughput::Elements(2));
        let pid = std::sync::atomic::AtomicU64::new(0);

        group.bench_function("position_tick_64_players", |b| {
            b.iter(|| {
                let p = pid.fetch_add(1, Ordering::Relaxed) % 64;
                srv.run("position_update", |ctx| {
                    ctx.set_row(
                        "positions".to_string(),
                        format!("player_{}", p),
                        serde_json::json!({ "x": black_box(1234.5f32), "y": 567.8, "z": 9.0 }),
                    )
                    .unwrap();
                    ctx.set_row(
                        "zones".to_string(),
                        format!("player_{}", p),
                        serde_json::json!({ "zone": "circle_3" }),
                    )
                    .unwrap();
                });
            });
        });
    }

    // Bench B: kill event (3 writes: kill log + stat update + alive count)
    {
        group.throughput(Throughput::Elements(3));
        group.bench_function("kill_event", |b| {
            b.iter(|| {
                srv.run("kill_event", |ctx| {
                    ctx.set_row(
                        "kill_log".to_string(),
                        "kill_1".to_string(),
                        serde_json::json!({ "killer": 5, "victim": 12, "weapon": "rifle" }),
                    )
                    .unwrap();
                    let _ = increment_reducer(ctx, "kills_player_5".to_string(), 1).unwrap();
                    let _ = increment_reducer(ctx, "alive_count".to_string(), -1).unwrap();
                });
            });
        });
    }

    group.finish();
}

// ── 3. RTS (Real-Time Strategy) ──────────────────────────────────────────────
//
// Patterns: resource tick every 100ms (gold, wood, food per player),
// unit order dispatch (set row per unit).  10 players, 50 units each.
// 5 clients subscribed to "resources".
fn bench_rts(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/rts");
    group.sample_size(50);

    let srv = GameServer::new("rts", 5, "resources");

    // Seed 10 players with resources
    for p in 0u32..10 {
        srv.tables
            .set_counter(format!("gold_p{}", p), 500, 0)
            .unwrap();
        srv.tables
            .set_counter(format!("wood_p{}", p), 200, 0)
            .unwrap();
    }

    // Resource tick: +10 gold, +5 wood for all 10 players (20 increments)
    group.throughput(Throughput::Elements(20));
    group.bench_function("resource_tick_10_players", |b| {
        b.iter(|| {
            srv.run("resource_tick", |ctx| {
                for p in 0..10 {
                    let _ = increment_reducer(ctx, format!("gold_p{}", p), 10).unwrap();
                    let _ = increment_reducer(ctx, format!("wood_p{}", p), 5).unwrap();
                }
            });
        });
    });

    // Unit order: move order for one unit (1 write)
    group.throughput(Throughput::Elements(1));
    let uid = std::sync::atomic::AtomicU64::new(0);
    group.bench_function("unit_order", |b| {
        b.iter(|| {
            let u = uid.fetch_add(1, Ordering::Relaxed) % 500;
            srv.run("unit_order", |ctx| {
                ctx.set_row(
                    "units".to_string(),
                    format!("unit_{}", u),
                    serde_json::json!({ "target_x": black_box(42), "target_y": 37, "order": "move" }),
                )
                .unwrap();
            });
        });
    });

    group.finish();
}

// ── 4. Card Game (turn-based) ─────────────────────────────────────────────────
//
// Patterns: complex transaction per card play — update game state, move card
// from hand to board, deduct mana, log history.  5 writes per action.
// 2 subscribers (the two players in the match).
fn bench_card_game(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/card_game");
    group.throughput(Throughput::Elements(5)); // 5 writes per card play
    group.sample_size(100);

    let srv = GameServer::new("card", 2, "game_state");

    group.bench_function("play_card_action", |b| {
        let turn = std::sync::atomic::AtomicU64::new(0);
        b.iter(|| {
            let t = turn.fetch_add(1, Ordering::Relaxed);
            let player = t % 2;
            srv.run("play_card", |ctx| {
                // 1. Deduct mana
                let _ =
                    increment_reducer(ctx, format!("mana_p{}", player), -3).unwrap();
                // 2. Move card to board
                ctx.set_row(
                    "board".to_string(),
                    format!("card_{}", t % 30),
                    serde_json::json!({ "owner": player, "type": "creature", "attack": 3, "defense": 2 }),
                )
                .unwrap();
                // 3. Remove from hand
                ctx.delete_row(
                    "hand".to_string(),
                    format!("hand_card_{}", t % 7),
                )
                .unwrap();
                // 4. Update game state
                ctx.set_row(
                    "game_state".to_string(),
                    "match_1".to_string(),
                    serde_json::json!({ "turn": t, "active_player": player }),
                )
                .unwrap();
                // 5. Log action
                ctx.set_row(
                    "action_log".to_string(),
                    format!("action_{}", t),
                    serde_json::json!({ "type": "play_card", "turn": t }),
                )
                .unwrap();
            });
        });
    });

    group.finish();
}

// ── 5. Idle / Clicker ─────────────────────────────────────────────────────────
//
// Patterns: millions of increment calls, single hot counter, zero subscribers.
// This is the highest theoretical TPS scenario — measures the speed ceiling.
fn bench_idle_clicker(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/idle_clicker");
    group.throughput(Throughput::Elements(1));
    group.sample_size(500);

    let srv = GameServer::new("idle", 0, "cookies");

    // Hot single counter — absolutely minimal work per call
    group.bench_function("click_hot_counter", |b| {
        b.iter(|| {
            srv.run("click", |ctx| {
                let _ =
                    increment_reducer(ctx, black_box("cookies".to_string()), black_box(1))
                        .unwrap();
            });
        });
    });

    // Prestige event — 10 resets at once (bulk write)
    group.throughput(Throughput::Elements(10));
    group.bench_function("prestige_reset_10_counters", |b| {
        b.iter(|| {
            srv.run("prestige", |ctx| {
                for i in 0..10 {
                    let _ = increment_reducer(ctx, format!("resource_{}", i), 0).unwrap();
                }
            });
        });
    });

    group.finish();
}

// ── 6. Racing ─────────────────────────────────────────────────────────────────
//
// Patterns: 16 cars, lap completed → update leaderboard position + best lap
// + race stats.  3 writes per lap.  16 subs (each spectator watching all).
fn bench_racing(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/racing");
    group.throughput(Throughput::Elements(3));
    group.sample_size(100);

    let srv = GameServer::new("racing", 16, "leaderboard");

    let car = std::sync::atomic::AtomicU64::new(0);

    group.bench_function("lap_completed_16_cars", |b| {
        b.iter(|| {
            let c_id = car.fetch_add(1, Ordering::Relaxed) % 16;
            srv.run("lap_completed", |ctx| {
                // Update leaderboard position
                ctx.set_row(
                    "leaderboard".to_string(),
                    format!("car_{}", c_id),
                    serde_json::json!({ "position": (c_id + 1) % 16, "laps": 3 }),
                )
                .unwrap();
                // Best lap time
                ctx.set_row(
                    "lap_times".to_string(),
                    format!("car_{}_lap", c_id),
                    serde_json::json!({ "time_ms": black_box(87_432u32) }),
                )
                .unwrap();
                // Race stat counter
                let _ = increment_reducer(ctx, "total_laps".to_string(), 1).unwrap();
            });
        });
    });

    group.finish();
}

// ── 7. MOBA (Multiplayer Online Battle Arena) ─────────────────────────────────
//
// 10-player match (5v5), very tight update window.
// Ability cast: cooldown set, damage dealt, gold awarded, kill check.
// 4 writes per ability.  10 subscribers (all players watching all events).
fn bench_moba(c: &mut Criterion) {
    let mut group = c.benchmark_group("genres/moba");
    group.throughput(Throughput::Elements(4));
    group.sample_size(100);

    let srv = GameServer::new("moba", 10, "players");

    // Seed 10 heroes
    for i in 0u32..10 {
        srv.tables
            .set_row(
                "players".to_string(),
                format!("hero_{}", i),
                serde_json::json!({ "id": i, "hp": 600, "gold": 500, "kills": 0 }),
            )
            .unwrap();
    }

    let tick = std::sync::atomic::AtomicU64::new(0);

    group.bench_function("ability_cast_5v5", |b| {
        b.iter(|| {
            let t = tick.fetch_add(1, Ordering::Relaxed);
            let caster = t % 10;
            let target = (caster + 1) % 10;
            srv.run("ability_cast", |ctx| {
                // 1. Set cooldown
                ctx.set_row(
                    "cooldowns".to_string(),
                    format!("hero_{}_q", caster),
                    serde_json::json!({ "ready_at": 1000 + t }),
                )
                .unwrap();
                // 2. Deal damage (update target row)
                ctx.set_row(
                    "players".to_string(),
                    format!("hero_{}", target),
                    serde_json::json!({ "id": target, "hp": black_box(540i32), "gold": 500, "kills": 0 }),
                )
                .unwrap();
                // 3. Award gold for ability hit
                let _ = increment_reducer(ctx, format!("gold_hero_{}", caster), 25).unwrap();
                // 4. Global damage meter
                let _ = increment_reducer(ctx, "match_damage_total".to_string(), 60).unwrap();
            });
        });
    });

    // Kill confirm — 5 writes (more expensive)
    group.throughput(Throughput::Elements(5));
    group.bench_function("kill_confirm", |b| {
        let t = tick.load(Ordering::Relaxed);
        b.iter(|| {
            let killer = t % 10;
            let victim = (killer + 5) % 10;
            srv.run("kill_confirm", |ctx| {
                // respawn timer
                ctx.set_row(
                    "respawns".to_string(),
                    format!("hero_{}", victim),
                    serde_json::json!({ "respawn_at": 1000 + t }),
                )
                .unwrap();
                // killer's kill count
                let _ =
                    increment_reducer(ctx, format!("kills_hero_{}", killer), 1).unwrap();
                // bounty gold
                let _ =
                    increment_reducer(ctx, format!("gold_hero_{}", killer), 300).unwrap();
                // global kill counter
                let _ = increment_reducer(ctx, "match_kill_total".to_string(), 1).unwrap();
                // update scoreboard
                ctx.set_row(
                    "scoreboard".to_string(),
                    "match_1".to_string(),
                    serde_json::json!({ "team_a_kills": 3, "team_b_kills": 2 }),
                )
                .unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mmorpg,
    bench_fps_br,
    bench_rts,
    bench_card_game,
    bench_idle_clicker,
    bench_racing,
    bench_moba,
);
criterion_main!(benches);
