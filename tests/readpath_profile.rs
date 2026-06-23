// Read-path profiler — isolates where the microseconds go on a 1000-row scan.
// Run with: cargo test --test readpath_profile -- --nocapture
//
// Not an assertion test; it prints a breakdown so we optimize the real bottleneck
// instead of guessing.

use serde_json::json;
use std::time::Instant;
use voltra::table::{EvictionPolicy, TableStore};

const N: usize = 1000;
const ITERS: usize = 200; // repeat each measured op to average out noise

fn build_store() -> TableStore {
    let store = TableStore::with_eviction(EvictionPolicy::None);
    let zones = ["z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8", "z9", "z10"];
    let classes = ["warrior", "mage", "rogue", "cleric"];
    for i in 0..N {
        let row = json!({
            "id": format!("p{i}"),
            "name": format!("Player{i}"),
            "score": (i * 37 % 6000) as i64,
            "level": (i % 100) as i64,
            "zone": zones[i % zones.len()],
            "class": classes[i % classes.len()],
            "alive": i % 3 != 0,
            "hp": (i % 200) as i64,
            "mp": (i % 150) as i64,
            "currency": (i * 13 % 10000) as i64,
        });
        store
            .set_row("players".to_string(), format!("p{i}"), row)
            .expect("set_row");
    }
    store
}

fn avg_us<F: FnMut()>(mut f: F) -> f64 {
    // warmup
    for _ in 0..10 {
        f();
    }
    let t = Instant::now();
    for _ in 0..ITERS {
        f();
    }
    t.elapsed().as_nanos() as f64 / ITERS as f64 / 1000.0
}

#[test]
#[ignore = "benchmark, not an assertion — run with: cargo test --release --test readpath_profile -- --ignored --nocapture"]
fn profile_read_path() {
    let store = build_store();

    // 1. Point lookup — one HashMap get + one row decode. ≈ per-row decode cost.
    let point = avg_us(|| {
        let v = store.get_row("players", "p500").unwrap();
        std::hint::black_box(&v);
    });

    // 2. Full scan — N iterations + N full-row decodes (the SELECT * / WHERE cost).
    let scan = avg_us(|| {
        let rows = store.list_rows_with_keys("players").unwrap();
        std::hint::black_box(&rows);
    });

    // 3. Columnar single-field scan — reads one field across all rows.
    let scan_col = avg_us(|| {
        let col = store.scan_column("players", "score");
        std::hint::black_box(&col);
    });

    // 4. count_by_field — aggregation read.
    let count = avg_us(|| {
        let c = store.count_by_field("players", "zone");
        std::hint::black_box(&c);
    });

    // 5. Indexed equality lookup (the WHERE id=x / zone=z fast path).
    store.create_index("players", "zone").unwrap();
    let idx = avg_us(|| {
        let keys = store.index_lookup("players", "zone", &json!("z1"));
        std::hint::black_box(&keys);
    });

    // 6. Top-10 leaderboard via the sorted index (ORDER BY score DESC LIMIT 10).
    store.create_sorted_index("players", "score").unwrap();
    let topn = avg_us(|| {
        let keys = store.top_n("players", "score", 10, true);
        std::hint::black_box(&keys);
    });

    let per_row_scan = scan / N as f64;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  READ-PATH PROFILE  (N = {N} rows, {ITERS} iters)              ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  point lookup (1 row, get_row)      {point:>10.2} µs       ║");
    println!("║  FULL SCAN (1000 rows decode)       {scan:>10.2} µs       ║");
    println!("║    └─ per row                       {per_row_scan:>10.3} µs       ║");
    println!("║  scan_column (1 field × 1000)       {scan_col:>10.2} µs       ║");
    println!("║  count_by_field (zone)              {count:>10.2} µs       ║");
    println!("║  index_lookup (zone=z1)             {idx:>10.2} µs       ║");
    println!("║  top_n score DESC LIMIT 10          {topn:>10.2} µs       ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  decode share of scan: per-row {per_row_scan:.3} µs ≈ point {point:.2} µs   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");
}
