/// engine_bench — Raw Voltra engine benchmark (no network, no WAL, no templates)
///
/// Measures:
///   • Write TPS  — set_row + commit
///   • Read TPS   — get_row (lock-free DashMap read)
///   • Full TPS   — get_row + modify + set_row + commit (read-modify-write cycle)
///
/// Modes:
///   1. Single thread
///   2. 24 threads in parallel (aggregate)
///   3. Hybrid (50% reads, 50% full RMW, 24 threads)
///
/// Each test runs for 3 seconds then prints results.

use voltra::{reducer::ReducerContext, table::TableStore};
use std::{
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

const DURATION: Duration = Duration::from_secs(3);
const THREAD_COUNT: usize = 24;
const KEY_SPACE: usize = 1024; // distinct keys → realistic contention spread

fn write_tps(tables: Arc<TableStore>, threads: usize) -> u64 {
    // Each thread owns its own slab of keys — no OCC conflicts, pure throughput.
    let slab = KEY_SPACE / threads.max(1);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counts: Arc<Vec<std::sync::atomic::AtomicU64>> = Arc::new(
        (0..threads)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect(),
    );

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let tbl = tables.clone();
            let bar = barrier.clone();
            let stp = stop.clone();
            let cnt = counts.clone();
            let base = tid * slab;
            let slab_sz = slab.max(1);
            thread::spawn(move || {
                bar.wait();
                let mut local = 0u64;
                let mut i = 0usize;
                while !stp.load(std::sync::atomic::Ordering::Relaxed) {
                    let key = format!("row_{}", base + (i % slab_sz));
                    let val = serde_json::json!({ "v": i });
                    let mut ctx = ReducerContext::new(tbl.clone(), i as u64);
                    ctx.set_row("bench".to_string(), key, val).unwrap();
                    ctx.commit().unwrap();
                    local += 1;
                    i += 1;
                }
                cnt[tid].store(local, std::sync::atomic::Ordering::Relaxed);
            })
        })
        .collect();

    barrier.wait();
    thread::sleep(DURATION);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    counts
        .iter()
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .sum()
}

fn read_tps(tables: Arc<TableStore>, threads: usize) -> u64 {
    // Pre-seed some rows so reads aren't all misses
    for i in 0..KEY_SPACE {
        tables
            .set_row(
                "bench".to_string(),
                format!("row_{}", i),
                serde_json::json!({ "v": i }),
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counts: Arc<Vec<std::sync::atomic::AtomicU64>> = Arc::new(
        (0..threads)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect(),
    );

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let tbl = tables.clone();
            let bar = barrier.clone();
            let stp = stop.clone();
            let cnt = counts.clone();
            thread::spawn(move || {
                bar.wait();
                let mut local = 0u64;
                let mut i = 0usize;
                while !stp.load(std::sync::atomic::Ordering::Relaxed) {
                    let key = format!("row_{}", (tid * 64 + i) % KEY_SPACE);
                    let _ = tbl.get_row("bench", &key).unwrap();
                    local += 1;
                    i += 1;
                }
                cnt[tid].store(local, std::sync::atomic::Ordering::Relaxed);
            })
        })
        .collect();

    barrier.wait();
    thread::sleep(DURATION);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    counts
        .iter()
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .sum()
}

fn rmw_tps(tables: Arc<TableStore>, threads: usize) -> u64 {
    // Each thread owns its own slab — no OCC conflicts.
    let slab = KEY_SPACE / threads.max(1);
    for i in 0..KEY_SPACE {
        tables
            .set_row(
                "bench".to_string(),
                format!("row_{}", i),
                serde_json::json!({ "v": 0 }),
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counts: Arc<Vec<std::sync::atomic::AtomicU64>> = Arc::new(
        (0..threads)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect(),
    );

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let tbl = tables.clone();
            let bar = barrier.clone();
            let stp = stop.clone();
            let cnt = counts.clone();
            let base = tid * slab;
            let slab_sz = slab.max(1);
            thread::spawn(move || {
                bar.wait();
                let mut local = 0u64;
                let mut i = 0usize;
                while !stp.load(std::sync::atomic::Ordering::Relaxed) {
                    let key = format!("row_{}", base + (i % slab_sz));
                    let mut ctx = ReducerContext::new(tbl.clone(), i as u64);
                    let cur = ctx.get_row("bench", &key).unwrap()
                        .and_then(|v| v["v"].as_i64())
                        .unwrap_or(0);
                    ctx.set_row(
                        "bench".to_string(),
                        key,
                        serde_json::json!({ "v": cur + 1 }),
                    )
                    .unwrap();
                    ctx.commit().unwrap();
                    local += 1;
                    i += 1;
                }
                cnt[tid].store(local, std::sync::atomic::Ordering::Relaxed);
            })
        })
        .collect();

    barrier.wait();
    thread::sleep(DURATION);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    counts
        .iter()
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .sum()
}

fn hybrid_tps(tables: Arc<TableStore>, threads: usize) -> (u64, u64) {
    // Half threads read, half do full RMW on their own slab (no conflicts)
    let read_threads = threads / 2;
    let write_threads = threads - read_threads;
    let slab = KEY_SPACE / write_threads.max(1);

    for i in 0..KEY_SPACE {
        tables
            .set_row(
                "bench".to_string(),
                format!("row_{}", i),
                serde_json::json!({ "v": 0 }),
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let write_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();

    // Read threads — scan the full key space freely
    for tid in 0..read_threads {
        let tbl = tables.clone();
        let bar = barrier.clone();
        let stp = stop.clone();
        let rc = read_count.clone();
        handles.push(thread::spawn(move || {
            bar.wait();
            let mut local = 0u64;
            let mut i = 0usize;
            while !stp.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("row_{}", (tid * 43 + i) % KEY_SPACE);
                let _ = tbl.get_row("bench", &key).unwrap();
                local += 1;
                i += 1;
            }
            rc.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    // Write threads — each on its own slab to avoid OCC conflict
    for wid in 0..write_threads {
        let tbl = tables.clone();
        let bar = barrier.clone();
        let stp = stop.clone();
        let wc = write_count.clone();
        let base = wid * slab;
        let slab_sz = slab.max(1);
        handles.push(thread::spawn(move || {
            bar.wait();
            let mut local = 0u64;
            let mut i = 0usize;
            while !stp.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("row_{}", base + (i % slab_sz));
                let mut ctx = ReducerContext::new(tbl.clone(), i as u64);
                let cur = ctx.get_row("bench", &key).unwrap()
                    .and_then(|v| v["v"].as_i64())
                    .unwrap_or(0);
                ctx.set_row(
                    "bench".to_string(),
                    key,
                    serde_json::json!({ "v": cur + 1 }),
                )
                .unwrap();
                ctx.commit().unwrap();
                local += 1;
                i += 1;
            }
            wc.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
        }));
    }

    barrier.wait();
    thread::sleep(DURATION);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    (
        read_count.load(std::sync::atomic::Ordering::Relaxed),
        write_count.load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn fmt(n: u64) -> String {
    let n = n / DURATION.as_secs();
    if n >= 1_000_000 {
        format!("{:.2}M/s", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K/s", n as f64 / 1_000.0)
    } else {
        format!("{}/s", n)
    }
}

fn header(title: &str) {
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  {}", title);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

fn row(label: &str, val: &str) {
    println!("  {:<30} {}", label, val);
}

fn main() {
    let cpus = num_cpus::get();
    println!();
    println!("  Voltra Raw Engine Benchmark");
    println!("  {} logical CPUs | {} threads for parallel tests | {} sec per test",
        cpus, THREAD_COUNT, DURATION.as_secs());
    println!("  Key space: {} distinct rows | No WAL | No network | No templates", KEY_SPACE);

    // ── 1. SINGLE THREAD ──────────────────────────────────────────────────────
    header("MODE 1 — Single Thread");

    print!("  Writes    ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let st_write = write_tps(Arc::new(TableStore::new()), 1);
    println!("{}", fmt(st_write));

    print!("  Reads     ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let st_read = read_tps(Arc::new(TableStore::new()), 1);
    println!("{}", fmt(st_read));

    print!("  RMW (TPS) ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let st_rmw = rmw_tps(Arc::new(TableStore::new()), 1);
    println!("{}", fmt(st_rmw));

    println!();
    row("Write TPS (1 thread):", &fmt(st_write));
    row("Read TPS  (1 thread):", &fmt(st_read));
    row("Full TPS  (1 thread):", &fmt(st_rmw));

    // ── 2. 24 THREADS PARALLEL ───────────────────────────────────────────────
    header(&format!("MODE 2 — {} Threads Parallel (Aggregate)", THREAD_COUNT));

    print!("  Writes    ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let mt_write = write_tps(Arc::new(TableStore::new()), THREAD_COUNT);
    println!("{}", fmt(mt_write));

    print!("  Reads     ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let mt_read = read_tps(Arc::new(TableStore::new()), THREAD_COUNT);
    println!("{}", fmt(mt_read));

    print!("  RMW (TPS) ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let mt_rmw = rmw_tps(Arc::new(TableStore::new()), THREAD_COUNT);
    println!("{}", fmt(mt_rmw));

    println!();
    row("Write TPS (aggregate):", &fmt(mt_write));
    row("Read TPS  (aggregate):", &fmt(mt_read));
    row("Full TPS  (aggregate):", &fmt(mt_rmw));
    row("Write scale vs 1-thread:", &format!("{:.1}x", mt_write as f64 / st_write as f64));
    row("Read  scale vs 1-thread:", &format!("{:.1}x", mt_read  as f64 / st_read  as f64));

    // ── 3. HYBRID (12 reader + 12 writer threads) ────────────────────────────
    header(&format!("MODE 3 — Hybrid ({} reader + {} writer threads)", THREAD_COUNT/2, THREAD_COUNT - THREAD_COUNT/2));

    print!("  Running   ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let (hy_read, hy_write) = hybrid_tps(Arc::new(TableStore::new()), THREAD_COUNT);
    println!("done");

    let hy_total = hy_read + hy_write;
    println!();
    row("Read TPS  (hybrid):", &fmt(hy_read));
    row("Write TPS (hybrid):", &fmt(hy_write));
    row("Total TPS (hybrid):", &fmt(hy_total));

    // ── SUMMARY ───────────────────────────────────────────────────────────────
    header("SUMMARY");
    println!("  {:^18}  {:>12}  {:>12}  {:>12}", "Mode", "Writes/s", "Reads/s", "Full TPS");
    println!("  {:─<18}  {:─>12}  {:─>12}  {:─>12}", "", "", "", "");
    println!("  {:18}  {:>12}  {:>12}  {:>12}", "Single thread", fmt(st_write), fmt(st_read), fmt(st_rmw));
    println!("  {:18}  {:>12}  {:>12}  {:>12}", "24 threads", fmt(mt_write), fmt(mt_read), fmt(mt_rmw));
    println!("  {:18}  {:>12}  {:>12}  {:>12}", "Hybrid (12+12)", fmt(hy_write), fmt(hy_read), fmt(hy_total));
    println!();
}
