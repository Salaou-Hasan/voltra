//! End-to-end WebSocket benchmark.
//!
//! Starts the Voltra server binary automatically, runs concurrent WebSocket
//! clients, and reports throughput + latency.
//!
//! Usage:
//!   cargo bench --bench end_to_end
//!
//! Or to run against an already-running server:
//!   WS_URL=ws://your-host:3000 cargo bench --bench end_to_end

use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use voltra::network::message::{
    ClientMessage, ReducerCall, ReducerResponse, ServerMessage, SqlQuery,
};
use serde::Serialize;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

// ── Server lifecycle ──────────────────────────────────────────────────────────

fn server_binary_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let exe = if cfg!(windows) {
        "voltra.exe"
    } else {
        "voltra"
    };
    manifest_dir.join("target").join("release").join(exe)
}

fn ensure_server_built() {
    let binary = server_binary_path();
    if binary.exists() {
        return;
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    println!("Building Voltra release binary (first run only)…");
    let status = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&manifest_dir)
        .status()
        .expect("cargo build failed");
    assert!(status.success(), "cargo build --release failed");
}

fn spawn_server(port: u16, wal_path: PathBuf) -> Child {
    ensure_server_built();
    // Derive a unique metrics port so the bench server doesn't collide with
    // the voltra.toml default (3001) that `Config::from_env()` would pick up.
    let metrics_port = port + 1000;
    Command::new(server_binary_path())
        .arg("start")
        .env("VOLTRA_HOST", "127.0.0.1")
        .env("VOLTRA_PORT", port.to_string())
        .env("VOLTRA_METRICS_PORT", metrics_port.to_string())
        .env("VOLTRA_WAL_PATH", &wal_path)
        .env("VOLTRA_UNSAFE_NO_FSYNC", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn Voltra server")
}

async fn wait_for_server(url: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if tokio_tungstenite::connect_async(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Server did not become ready within 10 seconds");
}

// ── Client workload ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct IncrementArgs {
    name: String,
    delta: i32,
}


async fn client_workload_write(
    client_id: usize,
    num_calls: usize,
    url: String,
    latencies: Arc<Mutex<Histogram<u64>>>,
) -> usize {
    let (mut ws, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("  client-{} connect failed (write): {}", client_id, e);
            return 0;
        }
    };

    let args_bytes = rmp_serde::to_vec(&IncrementArgs {
        name: "bench_counter".to_string(),
        delta: 1,
    })
    .unwrap();

    let mut success = 0usize;
    for i in 0..num_calls {
        let call_id = (client_id as u64) * 1_000_000 + i as u64;
        let call = ReducerCall {
            call_id,
            reducer_name: "increment".to_string(),
            args: args_bytes.clone(),
        };
        let frame = rmp_serde::to_vec(&call).unwrap();

        let t0 = Instant::now();
        if ws.send(Message::Binary(frame)).await.is_err() {
            break;
        }

        if let Ok(Some(Ok(Message::Binary(bytes)))) =
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            if rmp_serde::from_slice::<ReducerResponse>(&bytes).is_ok() {
                let us = t0.elapsed().as_micros() as u64;
                if let Ok(mut h) = latencies.lock() {
                    let _ = h.record(us);
                }
                success += 1;
            }
        }
    }
    let _ = ws.close(None).await;
    success
}

async fn client_workload_read(
    client_id: usize,
    num_calls: usize,
    url: String,
    latencies: Arc<Mutex<Histogram<u64>>>,
) -> usize {
    let (mut ws, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("  client-{} connect failed (read): {}", client_id, e);
            return 0;
        }
    };

    let mut success = 0usize;
    for i in 0..num_calls {
        let query_id = (client_id as u64) * 1_000_000 + i as u64;

        let sql = "SELECT * FROM players WHERE zone = 'north' LIMIT 1".to_string();
        let msg = ClientMessage::SqlQuery(SqlQuery { query_id, sql });
        let frame = rmp_serde::to_vec(&msg).unwrap();

        let t0 = Instant::now();
        if ws.send(Message::Binary(frame)).await.is_err() {
            break;
        }

        if let Ok(Some(Ok(Message::Binary(bytes)))) =
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            if let Ok(ServerMessage::SqlResult(r)) = rmp_serde::from_slice::<ServerMessage>(&bytes) {
                if r.success {
                    let us = t0.elapsed().as_micros() as u64;
                    if let Ok(mut h) = latencies.lock() {
                        let _ = h.record(us);
                    }
                    success += 1;
                }
            }
        }
    }

    let _ = ws.close(None).await;
    success
}

async fn client_workload_broadcast(
    client_id: usize,
    subscription_id: String,
    url: String,
    stop_at: Instant,
    notifications: Arc<Mutex<u64>>,
) -> usize {
    let (mut ws, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("  client-{} connect failed (broadcast): {}", client_id, e);
            return 0;
        }
    };

    // Subscribe to the counters table — this is what the write workload
    // increments via the `increment` reducer, so every write triggers
    // a subscription notification here.  (The old "players WHERE zone='north'"
    // query never received notifications because no writes targeted that table.)
    let query = "counters".to_string();
    let subscribe = ClientMessage::Subscribe { subscription_id, query };
    let subscribe_frame = rmp_serde::to_vec(&subscribe).unwrap();
    if ws.send(Message::Binary(subscribe_frame)).await.is_err() {
        let _ = ws.close(None).await;
        return 0;
    }

    // Drain messages until stop_at; count pushed notifications.
    let mut delivered = 0usize;
    while Instant::now() < stop_at {
        let remaining = stop_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        let next_msg = tokio::time::timeout(
            remaining.min(Duration::from_millis(250)),
            ws.next(),
        )
        .await;

        let Some(Ok(Message::Binary(bytes))) = next_msg.ok().flatten() else {
            continue;
        };

        if let Ok(server_msg) = rmp_serde::from_slice::<ServerMessage>(&bytes) {
            match server_msg {
                ServerMessage::SubscriptionDiff(d) => {
                    // Legacy one-frame mode: treat all diffs as notifications; initial_snapshot will also
                    // show up as a diff; filter it out by operation field.
                    if d.operation != "initial_snapshot" {
                        delivered += 1;
                        if let Ok(mut n) = notifications.lock() {
                            *n += 1;
                        }
                    }
                }
                ServerMessage::SubscriptionBody(b) => {
                    if b.operation != "initial_snapshot" {
                        delivered += 1;
                        if let Ok(mut n) = notifications.lock() {
                            *n += 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let _ = ws.close(None).await;
    delivered
}

// ── Benchmark entry point ─────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("=== Voltra End-to-End WebSocket Benchmark ===\n");

    let ws_url = std::env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:19000".to_string());
    let calls_per_client: usize = std::env::var("BENCH_CALLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    // Determine whether we need to start our own server
    let use_external = std::env::var("WS_URL").is_ok();
    let wal_path = std::env::temp_dir().join("voltra_e2e_bench.wal");

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let sample_period = Duration::from_millis(250);
    let bench_total_window = Duration::from_secs(15);

    // Accept "1", "true", "yes" (case-insensitive) for BENCH_SCALE_MODE.
    let scale_mode = std::env::var("BENCH_SCALE_MODE")
        .ok()
        .map(|v| {
            let v = v.trim().to_lowercase();
            v == "1" || v == "true" || v == "yes"
        })
        .unwrap_or(false);

    let default_counts: Vec<usize> = vec![10, 25, 50, 100, 200, 500, 1000];
    let client_counts: Vec<usize> = if scale_mode {
        std::env::var("BENCH_CLIENT_COUNTS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|x| x.trim().parse::<usize>().ok())
                    .filter(|&n| n > 0)
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(default_counts)
    } else {
        vec![std::env::var("BENCH_CLIENTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10)]
    };

    println!(
        "Config: calls/client={} | cores={} | scale_mode={} | client_counts={:?}\n",
        calls_per_client, cores, scale_mode, client_counts
    );

    let mut first_run = true;

    for &num_clients in &client_counts {
        println!("\n==============================");
        println!("Benchmark run: {} clients", num_clients);
        println!("==============================");

        let mut child: Option<Child> = None;
        let mut server_pid: Option<u32> = None;

        if !use_external {
            let port = 19000u16;
            println!("Starting Voltra server on port {}…", port);
            let _ = std::fs::remove_file(&wal_path);
            let c = spawn_server(port, wal_path.clone());
            server_pid = Some(c.id());
            child = Some(c);
            wait_for_server(&ws_url).await;
            println!("Server ready.\n");
        } else if first_run {
            println!("Using external server at {}\n", ws_url);
        }

        first_run = false;

        // CPU/memory sampler (Windows). Best-effort; if it fails, we still run benchmarks.
        let stats = Arc::new(Mutex::new(CpuMemStats::default()));
        let sampler_done = Arc::new(Mutex::new(false));

        let sampler_handle = if server_pid.is_some() {
            let stats = stats.clone();
            let done_flag = sampler_done.clone();
            let pid = server_pid.unwrap();
            let cores_for_norm = cores;

            Some(tokio::spawn(async move {
                let mut last_kernel_ms: Option<f64> = None;
                let mut last_user_ms: Option<f64> = None;
                let mut last_wall: Option<Instant> = None;

                loop {
                    {
                        let done = *done_flag.lock().unwrap();
                        if done {
                            break;
                        }
                    }

                    if let Ok((kern_ms, user_ms, ws_kb)) = sample_proc_windows(pid) {
                        stats.lock().unwrap().update_mem(ws_kb);

                        if let (Some(prev_k), Some(prev_u), Some(prev_t)) =
                            (last_kernel_ms, last_user_ms, last_wall)
                        {
                            let wall = prev_t.elapsed().as_secs_f64();
                            if wall > 0.0 {
                                let delta_cpu_ms = (kern_ms - prev_k) + (user_ms - prev_u);
                                let cpu_percent = (delta_cpu_ms / 1000.0) / wall * 100.0 / (cores_for_norm as f64);
                                stats.lock().unwrap().update_cpu(cpu_percent);
                            }
                        }

                        last_kernel_ms = Some(kern_ms);
                        last_user_ms = Some(user_ms);
                        last_wall = Some(Instant::now());
                    }

                    tokio::time::sleep(sample_period).await;
                }
            }))
        } else {
            None
        };

        // Seed players for READ/BROADCAST (best-effort; only when we own the server)
        if server_pid.is_some() && !use_external {
            seed_players_over_ws(&ws_url).await;
        }

        // Warmup (write only, keep quick)
        {
            let warmup_calls = (calls_per_client / 10).max(10);
            println!("Warmup (write): {} calls/client (clients capped at 4)…", warmup_calls);

            let warm_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
            let mut handles = Vec::new();
            for id in 0..num_clients.min(4) {
                let url = ws_url.clone();
                let hist = warm_hist.clone();
                handles.push(tokio::spawn(client_workload_write(id, warmup_calls, url, hist)));
            }
            for h in handles {
                let _ = h.await;
            }
            println!("Warmup complete.\n");
        }

        // READ phase
        println!("=== READ workload ===");
        let read_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
        let read_start = Instant::now();

        let mut handles = Vec::new();
        for client_id in 0..num_clients {
            let url = ws_url.clone();
            let hist = read_hist.clone();
            handles.push(tokio::spawn(client_workload_read(
                client_id,
                calls_per_client,
                url,
                hist,
            )));
        }
        let mut read_success = 0usize;
        for h in handles {
            if let Ok(n) = h.await {
                read_success += n;
            }
        }
        let read_elapsed = read_start.elapsed();
        let read_tps = read_success as f64 / read_elapsed.as_secs_f64();

        // WRITE phase
        println!("=== WRITE workload ===");
        let write_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
        let write_start = Instant::now();

        let mut handles = Vec::new();
        for client_id in 0..num_clients {
            let url = ws_url.clone();
            let hist = write_hist.clone();
            handles.push(tokio::spawn(client_workload_write(
                client_id,
                calls_per_client,
                url,
                hist,
            )));
        }
        let mut write_success = 0usize;
        for h in handles {
            if let Ok(n) = h.await {
                write_success += n;
            }
        }
        let write_elapsed = write_start.elapsed();
        let write_tps = write_success as f64 / write_elapsed.as_secs_f64();

        // BROADCAST phase
        println!("=== BROADCAST workload ===");
        let broadcast_notifications = Arc::new(Mutex::new(0u64));
        let broadcast_duration = bench_total_window;
        let stop_at = Instant::now() + broadcast_duration;

        let mut sub_handles = Vec::new();
        for client_id in 0..num_clients {
            let url = ws_url.clone();
            let notif = broadcast_notifications.clone();
            let sub_id = format!("bench_sub_{}_north", client_id);
            sub_handles.push(tokio::spawn(client_workload_broadcast(
                client_id,
                sub_id,
                url,
                stop_at,
                notif,
            )));
        }

        let writer_handle = tokio::spawn(broadcast_writer_loop(ws_url.clone(), stop_at));

        for h in sub_handles {
            let _ = h.await;
        }
        let _ = writer_handle.await;

        let broadcast_elapsed = broadcast_duration.as_secs_f64();
        let pushed = *broadcast_notifications.lock().unwrap();
        let broadcast_tps = pushed as f64 / broadcast_elapsed;

        // Stop sampler
        *sampler_done.lock().unwrap() = true;
        if let Some(h) = sampler_handle {
            let _ = h.await;
        }

        // Results (per concurrency level)
        let s = stats.lock().unwrap();

        println!("\n--- Summary (clients={}) ---", num_clients);
        println!("Number of cores used: {}", cores);
        println!(
            "CPU usage during the benchmark: avg(normalized/core)={:.2}%, peak={:.2}% (best-effort @{:?})",
            s.cpu_avg, s.cpu_peak, sample_period
        );
        println!(
            "Memory usage (WorkingSet): avg={:.0}KB, peak={:.0}KB (best-effort @{:?})",
            s.mem_avg_kb, s.mem_peak_kb, sample_period
        );

        println!(
            "Read benchmark TPS: {:.0} (success={} in {:.3}s)",
            read_tps, read_success, read_elapsed.as_secs_f64()
        );
        println!(
            "Write benchmark TPS: {:.0} (success={} in {:.3}s)",
            write_tps, write_success, write_elapsed.as_secs_f64()
        );
        println!(
            "Broadcast benchmark TPS: {:.0} (pushed={} in {:.0}s)",
            broadcast_tps, pushed, broadcast_duration.as_secs_f64()
        );

        // Latency detail for first run only (keeps output readable during scaling)
        let first_client = client_counts.first().copied().unwrap_or(num_clients);
        if num_clients == first_client {
            println!("\n--- Latency detail (first run) ---");
            if let Ok(hist) = read_hist.lock() {
                print_latency_hist("READ latency (µs)", &hist);
            }
            if let Ok(hist) = write_hist.lock() {
                print_latency_hist("WRITE latency (µs)", &hist);
            }
        }

        if let Some(mut c) = child {
            let _ = c.kill();
            let _ = c.wait();
            let _ = std::fs::remove_file(&wal_path);
        }
    }

    println!("\n✓ Benchmark complete (scaling results printed above)");
}

#[derive(Default)]
struct CpuMemStats {
    cpu_avg: f64,
    cpu_peak: f64,
    mem_avg_kb: f64,
    mem_peak_kb: f64,
    mem_sum_kb: f64,
    mem_samples: u64,
}

impl CpuMemStats {
    fn update_cpu(&mut self, cpu_percent: f64) {
        if cpu_percent.is_finite() {
            // Maintain avg via incremental mean is out of scope; track last+peak.
            self.cpu_avg = if self.cpu_avg == 0.0 { cpu_percent } else { (self.cpu_avg + cpu_percent) / 2.0 };
            self.cpu_peak = self.cpu_peak.max(cpu_percent);
        }
    }

    fn update_mem(&mut self, ws_kb: f64) {
        self.mem_peak_kb = self.mem_peak_kb.max(ws_kb);
        self.mem_sum_kb += ws_kb;
        self.mem_samples += 1;
        self.mem_avg_kb = self.mem_sum_kb / (self.mem_samples as f64).max(1.0);
    }
}

// Windows sampling helper via wmic.
// Returns: (kernel_ms, user_ms, working_set_kb)
fn sample_proc_windows(pid: u32) -> std::result::Result<(f64, f64, f64), ()> {
    // NOTE: we run this in a best-effort way and ignore failures.
    // KernelModeTime/UserModeTime are in milliseconds for wmic on Windows.
    let pid_s = pid.to_string();

    let ws_out = Command::new("wmic")
        .args(["process", "where", &format!("ProcessId={}", pid_s), "get", "WorkingSetSize", "/value"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();

    let cpu_out = Command::new("wmic")
        .args([
            "process",
            "where",
            &format!("ProcessId={}", pid_s),
            "get",
            "KernelModeTime,UserModeTime",
            "/value",
        ])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();

    if ws_out.is_empty() || cpu_out.is_empty() {
        return Err(());
    }

    let ws_kb = parse_wmic_value_kb(&ws_out, "WorkingSetSize").ok_or(())?;
    let kern_ms = parse_wmic_value_f64(&cpu_out, "KernelModeTime").ok_or(())?;
    let user_ms = parse_wmic_value_f64(&cpu_out, "UserModeTime").ok_or(())?;
    Ok((kern_ms, user_ms, ws_kb))
}

fn parse_wmic_value_f64(s: &str, key: &str) -> Option<f64> {
    for line in s.lines() {
        let line = line.trim();
        if line.starts_with(key) && line.contains('=') {
            if let Some(v) = line.split('=').nth(1) {
                if let Ok(n) = v.trim().parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }
    None
}

fn parse_wmic_value_kb(s: &str, key: &str) -> Option<f64> {
    // WorkingSetSize is bytes
    let bytes = parse_wmic_value_f64(s, key)?;
    Some(bytes / 1024.0)
}

fn print_latency_hist(label: &str, hist: &Histogram<u64>) {
    println!("\n{}:", label);
    for pct in &[50.0f64, 90.0, 95.0, 99.0, 99.9] {
        let us = hist.value_at_percentile(*pct);
        println!("  p{:<5}: {:>6} µs ({:.2} ms)", pct, us, us as f64 / 1000.0);
    }
    println!("  max:    {:>6} µs ({:.2} ms)", hist.max(), hist.max() as f64 / 1000.0);
}

async fn seed_players_over_ws(ws_url: &str) {
    // Best-effort seeding. Uses SQL via ClientMessage::SqlQuery.
    // Ensure we don't overwhelm the server; small fixed number of inserts.
    let (mut ws, _) = match tokio_tungstenite::connect_async(ws_url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("seed connect failed: {}", e);
            return;
        }
    };

    // Insert 50 rows in zone='north'
    for i in 0..50u32 {
        let id = format!("bench_player_{}", i);
        let zone = "north";
        let score = (i % 10) as i64;

        let sql = format!(
            "INSERT INTO players (id, zone, score, active) VALUES ('{}', '{}', {}, true)",
            id, zone, score
        );
        let query_id = 10_000 + i as u64;
        let msg = ClientMessage::SqlQuery(SqlQuery { query_id, sql });
        let frame = rmp_serde::to_vec(&msg).unwrap();
        let _ = ws.send(Message::Binary(frame)).await;

        // Read one response (ignore content)
        if let Ok(Some(Ok(Message::Binary(bytes)))) = 
            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        {
            let _ = rmp_serde::from_slice::<ServerMessage>(&bytes);
        }
    }

    let _ = ws.close(None).await;
}

async fn broadcast_writer_loop(ws_url: String, stop_at: Instant) {
    // Best-effort loop that triggers subscription diffs by updating a small ring of north players.
    // We use SQL UPDATE in a tight loop until stop_at.
    let (mut ws, _) = match tokio_tungstenite::connect_async(&ws_url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("broadcast writer connect failed: {}", e);
            return;
        }
    };

    let mut i: u32 = 0;
    while Instant::now() < stop_at {
        let target = i % 50;
        let id = format!("bench_player_{}", target);
        let sql = format!(
            "UPDATE players SET score = score + 1 WHERE id = '{}' ",
            id
        );
        let query_id = 20_000_000 + i as u64;
        let msg = ClientMessage::SqlQuery(SqlQuery { query_id, sql });
        let frame = rmp_serde::to_vec(&msg).unwrap();
        if ws.send(Message::Binary(frame)).await.is_err() {
            break;
        }

        // Drain response quickly (don’t block too long)
        if let Ok(Some(Ok(Message::Binary(_bytes)))) = 
            tokio::time::timeout(Duration::from_millis(100), ws.next()).await
        {
            // ignore payload
        }

        i = i.wrapping_add(1);
    }

    let _ = ws.close(None).await;
}
