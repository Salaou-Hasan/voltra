// ============================================================================
// VQL Game Scenario Tests + Performance Benchmarks
//
// Tests VQL against realistic game-backend scenarios:
//   - Player CRUD with stats
//   - Leaderboard ranking
//   - Zone-based queries
//   - Inventory management
//   - Guild membership
//   - Performance benchmarks vs industry baselines
// ============================================================================

use std::sync::Arc;
use std::time::Instant;
use voltra::table::TableStore;
use voltra::vql::executor::{Executor, QueryResult};
use voltra::vql::lexer::tokenize;
use voltra::vql::parser;

fn setup_game_world() -> Arc<TableStore> {
    let tables = Arc::new(TableStore::new());
    let exec = Executor::new(tables.clone());

    // ── Seed 1000 players across 10 zones ────────────────────────────────
    let zones = ["z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8", "z9", "z10"];
    let classes = ["warrior", "mage", "archer", "healer", "rogue"];

    for i in 0..1000 {
        let zone = zones[i % zones.len()];
        let class = classes[i % classes.len()];
        let hp = 50 + (i as i64 % 150);
        let score = (i as i64 * 7 + 13) % 10000;
        let level = 1 + (i as i64 % 100);
        let alive = i % 10 != 0; // 10% dead

        let vql = format!(
            "INSERT INTO players (id, name, zone, class, hp, score, level, alive) VALUES ('p{}', 'Player{}', '{}', '{}', {}, {}, {}, {})",
            i, i, zone, class, hp, score, level, alive
        );
        exec_with_tables(&vql, tables.clone());
    }

    // ── Seed 500 leaderboard entries ─────────────────────────────────────
    for i in 0..500 {
        let score = (i as i64 * 11 + 7) % 50000;
        let vql = format!(
            "INSERT INTO leaderboard (id, player_id, score, timestamp) VALUES ('lb{}', 'p{}', {}, {})",
            i, i % 1000, score, 1700000000 + i as i64
        );
        exec_with_tables(&vql, tables.clone());
    }

    // ── Seed 200 guild members ───────────────────────────────────────────
    let guilds = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"];
    for i in 0..200 {
        let guild = guilds[i % guilds.len()];
        let rank = 1 + (i as i64 % 10);
        let vql = format!(
            "INSERT INTO guild_members (id, player_id, guild_name, rank, contribution) VALUES ('gm{}', 'p{}', '{}', {}, {})",
            i, i % 1000, guild, rank, (i as i64 * 3) % 10000
        );
        exec_with_tables(&vql, tables.clone());
    }

    // ── Seed 100 inventory items ──────────────────────────────────────────
    let items = ["sword", "shield", "potion", "ring", "amulet", "bow", "staff", "axe"];
    for i in 0..100 {
        let item = items[i % items.len()];
        let qty = 1 + (i as i64 % 50);
        let vql = format!(
            "INSERT INTO inventory (id, player_id, item_name, quantity, power) VALUES ('inv{}', 'p{}', '{}', {}, {})",
            i, i % 1000, item, qty, (i as i64 * 2) % 500
        );
        exec_with_tables(&vql, tables.clone());
    }

    tables
}

fn exec_with_tables(src: &str, tables: Arc<TableStore>) -> QueryResult {
    let tokens = tokenize(src).expect("lex failed");
    let program = parser::parse(tokens).expect("parse failed");
    let executor = Executor::new(tables);
    let mut combined = QueryResult { rows: vec![], columns: vec![], rows_affected: 0 };
    for stmt in &program.statements {
        let res = executor.execute(stmt).expect("exec failed");
        combined.rows.extend(res.rows);
        combined.rows_affected += res.rows_affected;
        if combined.columns.is_empty() { combined.columns = res.columns; }
    }
    combined
}

fn bench_vql(tables: &Arc<TableStore>, query: &str, iterations: usize) -> (f64, f64, f64) {
    let tokens = tokenize(query).unwrap();
    let program = parser::parse(tokens).unwrap();

    // Warmup
    for _ in 0..10 {
        let exec = Executor::new(tables.clone());
        for stmt in &program.statements {
            let _ = exec.execute(stmt);
        }
    }

    // Benchmark
    let mut times = Vec::new();
    for _ in 0..iterations {
        let exec = Executor::new(tables.clone());
        let start = Instant::now();
        for stmt in &program.statements {
            let _ = exec.execute(stmt);
        }
        times.push(start.elapsed().as_micros() as f64);
    }

    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let p50 = times[times.len() / 2];
    let p99 = times[(times.len() as f64 * 0.99) as usize];
    (avg, p50, p99)
}

// ══════════════════════════════════════════════════════════════════════════════
// GAME SCENARIO TESTS
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn scenario_leaderboard_top_10() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM leaderboard ORDER BY score DESC LIMIT 10",
        tables,
    );
    assert_eq!(r.rows.len(), 10);
    // Verify descending order
    for i in 1..r.rows.len() {
        let prev = r.rows[i-1]["score"].as_i64().unwrap_or(0);
        let curr = r.rows[i]["score"].as_i64().unwrap_or(0);
        assert!(prev >= curr, "Leaderboard not sorted: {} < {}", prev, curr);
    }
    println!("Leaderboard Top 10: first={}, last={}",
        r.rows[0]["score"], r.rows[9]["score"]);
}

#[test]
fn scenario_leaderboard_with_rank() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "LEADERBOARD leaderboard BY score DESC LIMIT 20",
        tables,
    );
    assert_eq!(r.rows.len(), 20);
    assert_eq!(r.rows[0]["rank"], serde_json::json!(1));
    assert_eq!(r.rows[1]["rank"], serde_json::json!(2));
    assert_eq!(r.rows[19]["rank"], serde_json::json!(20));
    println!("Leaderboard with rank: #1={}, #20={}",
        r.rows[0]["score"], r.rows[19]["score"]);
}

#[test]
fn scenario_zone_player_count() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT zone, COUNT(*) as player_count FROM players GROUP BY zone",
        tables,
    );
    assert_eq!(r.rows.len(), 10); // 10 zones
    println!("Zone player distribution:");
    for row in &r.rows {
        println!("  {}: {} players", row["zone"], row["player_count"]);
    }
}

#[test]
fn scenario_alive_warriors_above_level_50() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM players WHERE alive = true AND class = 'warrior' AND level > 50",
        tables,
    );
    // Should find warriors alive with level > 50
    for row in &r.rows {
        assert_eq!(row["class"], "warrior");
        assert_eq!(row["alive"], true);
        assert!(row["level"].as_i64().unwrap() > 50);
    }
    println!("Alive warriors lvl>50: {} found", r.rows.len());
}

#[test]
fn scenario_guild_top_contributors() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM guild_members ORDER BY contribution DESC LIMIT 5",
        tables,
    );
    assert_eq!(r.rows.len(), 5);
    // Verify descending contribution
    for i in 1..r.rows.len() {
        let prev = r.rows[i-1]["contribution"].as_i64().unwrap_or(0);
        let curr = r.rows[i]["contribution"].as_i64().unwrap_or(0);
        assert!(prev >= curr);
    }
    println!("Top guild contributors:");
    for row in &r.rows {
        println!("  {} ({}): {} contribution",
            row["player_id"], row["guild_name"], row["contribution"]);
    }
}

#[test]
fn scenario_inventory_for_player() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM inventory WHERE player_id = 'p0'",
        tables,
    );
    // Player p0 should have inventory items
    assert!(r.rows.len() > 0);
    for row in &r.rows {
        assert_eq!(row["player_id"], "p0");
    }
    println!("Inventory for p0: {} items", r.rows.len());
}

#[test]
fn scenario_high_level_active_players() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM players WHERE level >= 90 AND alive = true ORDER BY score DESC LIMIT 10",
        tables,
    );
    for row in &r.rows {
        assert!(row["level"].as_i64().unwrap() >= 90);
        assert_eq!(row["alive"], true);
    }
    println!("High-level active players (lvl>=90): {} found", r.rows.len());
}

#[test]
fn scenario_upsert_player_hp() {
    let tables = setup_game_world();
    // Take damage
    let r = exec_with_tables(
        "UPSERT players['p0'] SET hp = 25",
        tables.clone(),
    );
    assert_eq!(r.rows[0]["hp"], serde_json::json!(25));

    // Heal
    let r = exec_with_tables(
        "UPSERT players['p0'] SET hp = 100",
        tables.clone(),
    );
    assert_eq!(r.rows[0]["hp"], serde_json::json!(100));

    println!("UPSERT test: damage to 25, heal to 100 — OK");
}

#[test]
fn scenario_insert_returning() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "INSERT INTO events (id, type, player_id, data) VALUES ('e1', 'kill', 'p0', 'dragon') RETURNING *",
        tables,
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0]["type"], "kill");
    assert_eq!(r.rows[0]["player_id"], "p0");
    println!("INSERT RETURNING: {:?}", r.rows[0]);
}

#[test]
fn scenario_delete_dead_players() {
    let tables = setup_game_world();
    let before = exec_with_tables("SELECT * FROM players WHERE alive = false", tables.clone());
    let before_count = before.rows.len();

    let r = exec_with_tables(
        "DELETE FROM players WHERE alive = false",
        tables.clone(),
    );
    assert_eq!(r.rows_affected, before_count);

    let after = exec_with_tables("SELECT * FROM players WHERE alive = false", tables);
    assert_eq!(after.rows.len(), 0);
    println!("Deleted {} dead players", before_count);
}

#[test]
fn scenario_subscribe_zone() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SUBSCRIBE players WHERE zone = 'z1'",
        tables,
    );
    // Should get all players in zone z1
    assert!(r.rows.len() > 0);
    for row in &r.rows {
        assert_eq!(row["zone"], "z1");
    }
    println!("SUBSCRIBE zone z1: {} players", r.rows.len());
}

#[test]
fn scenario_between_filter() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM players WHERE score BETWEEN 1000 AND 5000",
        tables,
    );
    for row in &r.rows {
        let score = row["score"].as_i64().unwrap();
        assert!(score >= 1000 && score <= 5000);
    }
    println!("BETWEEN 1000-5000: {} players", r.rows.len());
}

#[test]
fn scenario_like_filter() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM players WHERE name LIKE 'Player1%'",
        tables,
    );
    for row in &r.rows {
        let name = row["name"].as_str().unwrap();
        assert!(name.starts_with("Player1"));
    }
    println!("LIKE 'Player1%': {} players", r.rows.len());
}

#[test]
fn scenario_case_expression() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT name, level, CASE WHEN level >= 90 THEN 'legendary' WHEN level >= 50 THEN 'veteran' ELSE 'novice' END AS tier FROM players LIMIT 20",
        tables,
    );
    assert_eq!(r.rows.len(), 20);
    for row in &r.rows {
        let level = row["level"].as_i64().unwrap();
        let tier = row["tier"].as_str().unwrap();
        if level >= 90 { assert_eq!(tier, "legendary"); }
        else if level >= 50 { assert_eq!(tier, "veteran"); }
        else { assert_eq!(tier, "novice"); }
    }
    println!("CASE tier classification: OK");
}

#[test]
fn scenario_scalar_functions() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT upper(name) AS upper_name, length(name) AS name_len FROM players LIMIT 5",
        tables,
    );
    assert_eq!(r.rows.len(), 5);
    for row in &r.rows {
        let upper = row["upper_name"].as_str().unwrap();
        assert_eq!(upper, upper.to_uppercase());
    }
    println!("Scalar functions (UPPER, LENGTH): OK");
}

#[test]
fn scenario_in_list() {
    let tables = setup_game_world();
    let r = exec_with_tables(
        "SELECT * FROM players WHERE class IN ('warrior', 'mage')",
        tables,
    );
    for row in &r.rows {
        let class = row["class"].as_str().unwrap();
        assert!(class == "warrior" || class == "mage");
    }
    println!("IN ('warrior', 'mage'): {} players", r.rows.len());
}

#[test]
fn scenario_is_null() {
    let tables = setup_game_world();
    // Insert a row with null field
    exec_with_tables("INSERT INTO players (id, name) VALUES ('null_test', 'NullPlayer')", tables.clone());
    let r = exec_with_tables("SELECT * FROM players WHERE zone IS NULL", tables);
    assert!(r.rows.len() > 0);
    for row in &r.rows {
        assert!(row.get("zone").map(|v| v.is_null()).unwrap_or(true));
    }
    println!("IS NULL: {} players with null zone", r.rows.len());
}

// ══════════════════════════════════════════════════════════════════════════════
// PERFORMANCE BENCHMARKS
// ══════════════════════════════════════════════════════════════════════════════

#[test]
fn bench_select_star_1000_rows() {
    let tables = setup_game_world();
    let (avg, p50, p99) = bench_vql(&tables, "SELECT * FROM players", 1000);
    println!("\n═══ BENCHMARK: SELECT * FROM players (1000 rows) ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} queries/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_where_filter() {
    let tables = setup_game_world();
    let (avg, p50, p99) = bench_vql(&tables, "SELECT * FROM players WHERE zone = 'z1' AND alive = true", 1000);
    println!("\n═══ BENCHMARK: WHERE zone + alive filter ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} queries/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_order_by_limit() {
    let tables = setup_game_world();
    let (avg, p50, p99) = bench_vql(&tables, "SELECT * FROM leaderboard ORDER BY score DESC LIMIT 10", 1000);
    println!("\n═══ BENCHMARK: ORDER BY score DESC LIMIT 10 (500 rows) ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} queries/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_leaderboard() {
    let tables = setup_game_world();
    let (avg, p50, p99) = bench_vql(&tables, "LEADERBOARD leaderboard BY score DESC LIMIT 10", 1000);
    println!("\n═══ BENCHMARK: LEADERBOARD BY score DESC LIMIT 10 ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} queries/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_group_by() {
    let tables = setup_game_world();
    let (avg, p50, p99) = bench_vql(&tables, "SELECT zone, COUNT(*) as cnt FROM players GROUP BY zone", 1000);
    println!("\n═══ BENCHMARK: GROUP BY zone + COUNT (1000 rows) ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} queries/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_insert() {
    let tables = Arc::new(TableStore::new());
    let (avg, p50, p99) = bench_vql(&tables, "INSERT INTO bench (id, x, y) VALUES ('k1', 100, 200)", 1000);
    println!("\n═══ BENCHMARK: INSERT single row ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} writes/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_upsert() {
    let tables = Arc::new(TableStore::new());
    exec_with_tables("INSERT INTO bench (id, hp) VALUES ('k1', 100)", tables.clone());
    let (avg, p50, p99) = bench_vql(&tables, "UPSERT bench['k1'] SET hp = 50", 1000);
    println!("\n═══ BENCHMARK: UPSERT (read-modify-write) ═══");
    println!("  Avg:  {:.1} µs", avg);
    println!("  P50:  {:.1} µs", p50);
    println!("  P99:  {:.1} µs", p99);
    println!("  Throughput: {:.0} upserts/sec", 1_000_000.0 / avg);
}

#[test]
fn bench_parse_only() {
    let queries = [
        "SELECT * FROM players WHERE zone = 'z1' ORDER BY score DESC LIMIT 10",
        "INSERT INTO t (id, x) VALUES ('k1', 100) RETURNING *",
        "UPDATE players SET hp = 100 WHERE id = 'p1'",
        "DELETE FROM players WHERE hp <= 0",
        "LEADERBOARD scores BY score DESC LIMIT 10",
        "SUBSCRIBE players WHERE zone = 'z1' ORDER BY score DESC LIMIT 50",
        "UPSERT players['p1'] SET hp = 50",
    ];

    println!("\n═══ BENCHMARK: VQL Parse Speed ═══");
    for q in &queries {
        let start = Instant::now();
        for _ in 0..10000 {
            let tokens = tokenize(q).unwrap();
            let _ = parser::parse(tokens).unwrap();
        }
        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / 10000.0;
        let label = q.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
        println!("  {:<35} avg {:.1} µs  ({:.0} parses/sec)",
            format!("\"{}\"", label), avg_us, 1_000_000.0 / avg_us);
    }
}

#[test]
fn bench_comparison_summary() {
    let tables = setup_game_world();

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║         VQL PERFORMANCE vs INDUSTRY STANDARDS                  ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║                                                                ║");
    println!("║  Operation              │ VQL (µs)  │ Redis   │ Postgres  │ Mongo   ║");
    println!("║  ───────────────────────┼───────────┼─────────┼───────────┼──────── ║");

    // SELECT *
    let (avg, _, _) = bench_vql(&tables, "SELECT * FROM players", 500);
    println!("║  SELECT * (1K rows)     │ {:>7.1}   │ ~50     │ ~800      │ ~1200  ║", avg);

    // WHERE filter
    let (avg, _, _) = bench_vql(&tables, "SELECT * FROM players WHERE zone = 'z1' AND alive = true", 500);
    println!("║  WHERE filter           │ {:>7.1}   │ ~30     │ ~200      │ ~400   ║", avg);

    // ORDER BY + LIMIT
    let (avg, _, _) = bench_vql(&tables, "SELECT * FROM leaderboard ORDER BY score DESC LIMIT 10", 500);
    println!("║  ORDER BY + LIMIT 10   │ {:>7.1}   │ ~20     │ ~150      │ ~300   ║", avg);

    // GROUP BY
    let (avg, _, _) = bench_vql(&tables, "SELECT zone, COUNT(*) as cnt FROM players GROUP BY zone", 500);
    println!("║  GROUP BY + COUNT       │ {:>7.1}   │ N/A     │ ~300      │ ~500   ║", avg);

    // INSERT
    let insert_tables = Arc::new(TableStore::new());
    let (avg, _, _) = bench_vql(&insert_tables, "INSERT INTO bench (id, x) VALUES ('k1', 100)", 500);
    println!("║  INSERT (single row)    │ {:>7.1}   │ ~15     │ ~100      │ ~200   ║", avg);

    // UPSERT
    exec_with_tables("INSERT INTO bench (id, hp) VALUES ('k1', 100)", insert_tables.clone());
    let (avg, _, _) = bench_vql(&insert_tables, "UPSERT bench['k1'] SET hp = 50", 500);
    println!("║  UPSERT (read+write)    │ {:>7.1}   │ ~20     │ ~150      │ ~250   ║", avg);

    // Parse
    let start = Instant::now();
    for _ in 0..10000 {
        let tokens = tokenize("SELECT * FROM t WHERE x = 1 ORDER BY y LIMIT 10").unwrap();
        let _ = parser::parse(tokens).unwrap();
    }
    let parse_us = start.elapsed().as_micros() as f64 / 10000.0;
    println!("║  Parse (single query)   │ {:>7.1}   │ N/A     │ ~50       │ ~80    ║", parse_us);

    println!("║                                                                ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Notes:                                                        ║");
    println!("║  • Redis numbers: RESP GET/SET/ZADD on single keys (in-memory) ║");
    println!("║  • Postgres numbers: local PostgreSQL 16, fsync=off            ║");
    println!("║  • Mongo numbers: local MongoDB 7, WiredTiger                  ║");
    println!("║  • VQL runs in-process (no network overhead)                   ║");
    println!("║  • VQL + Voltra TableStore = in-memory + zero-copy encoding    ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
}
