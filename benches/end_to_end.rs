//! End-to-end WebSocket benchmark.
//!
//! Starts the NeonDB server binary automatically, runs concurrent WebSocket
//! clients, and reports throughput + latency.
//!
//! Usage:
//!   cargo bench --bench end_to_end
//!
//! Or to run against an already-running server:
//!   WS_URL=ws://your-host:3000 cargo bench --bench end_to_end

use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use neondb::network::message::{ReducerCall, ReducerResponse};
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
        "neondb.exe"
    } else {
        "neondb"
    };
    manifest_dir.join("target").join("release").join(exe)
}

fn ensure_server_built() {
    let binary = server_binary_path();
    if binary.exists() {
        return;
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    println!("Building NeonDB release binary (first run only)…");
    let status = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&manifest_dir)
        .status()
        .expect("cargo build failed");
    assert!(status.success(), "cargo build --release failed");
}

fn spawn_server(port: u16, wal_path: PathBuf) -> Child {
    ensure_server_built();
    Command::new(server_binary_path())
        .arg("start")
        .env("NEONDB_HOST", "127.0.0.1")
        .env("NEONDB_PORT", port.to_string())
        .env("NEONDB_WAL_PATH", &wal_path)
        .env("NEONDB_UNSAFE_NO_FSYNC", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn NeonDB server")
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

async fn client_workload(
    client_id: usize,
    num_calls: usize,
    url: String,
    latencies: Arc<Mutex<Histogram<u64>>>,
) -> usize {
    let (mut ws, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("  client-{} connect failed: {}", client_id, e);
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

// ── Benchmark entry point ─────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("=== NeonDB End-to-End WebSocket Benchmark ===\n");

    let ws_url = std::env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:19000".to_string());
    let num_clients: usize = std::env::var("BENCH_CLIENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let calls_per_client: usize = std::env::var("BENCH_CALLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    // Determine whether we need to start our own server
    let use_external = std::env::var("WS_URL").is_ok();
    let wal_path = std::env::temp_dir().join("neondb_e2e_bench.wal");

    let mut child: Option<Child> = None;
    if !use_external {
        let port = 19000u16;
        println!("Starting NeonDB server on port {}…", port);
        let _ = std::fs::remove_file(&wal_path);
        child = Some(spawn_server(port, wal_path.clone()));
        wait_for_server(&ws_url).await;
        println!("Server ready.\n");
    } else {
        println!("Using external server at {}\n", ws_url);
    }

    println!(
        "Config: {} clients × {} calls = {} total\n",
        num_clients,
        calls_per_client,
        num_clients * calls_per_client
    );

    // Warmup
    {
        let warmup_calls = (calls_per_client / 10).max(10);
        println!("Warming up ({} calls per client)…", warmup_calls);
        let warm_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
        let mut handles = Vec::new();
        for id in 0..num_clients.min(4) {
            let url = ws_url.clone();
            let hist = warm_hist.clone();
            handles.push(tokio::spawn(client_workload(id, warmup_calls, url, hist)));
        }
        for h in handles {
            let _ = h.await;
        }
        println!("Warmup complete.\n");
    }

    // Benchmark
    let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let mut handles = Vec::new();
    let bench_start = Instant::now();

    for client_id in 0..num_clients {
        let url = ws_url.clone();
        let hist = latencies.clone();
        handles.push(tokio::spawn(client_workload(
            client_id,
            calls_per_client,
            url,
            hist,
        )));
    }

    let mut total_success = 0usize;
    for h in handles {
        if let Ok(n) = h.await {
            total_success += n;
        }
    }

    let elapsed = bench_start.elapsed();
    let tps = total_success as f64 / elapsed.as_secs_f64();

    println!("=== Results ===");
    println!("Total time:    {:.3}s", elapsed.as_secs_f64());
    println!(
        "Success:       {}/{}",
        total_success,
        num_clients * calls_per_client
    );
    println!("Throughput:    {:.0} TPS", tps);

    if let Ok(hist) = latencies.lock() {
        println!("\nLatency (round-trip, µs):");
        for pct in &[50.0f64, 90.0, 95.0, 99.0, 99.9] {
            let us = hist.value_at_percentile(*pct);
            println!("  p{:<5}: {:>6} µs ({:.2} ms)", pct, us, us as f64 / 1000.0);
        }
        println!(
            "  max:    {:>6} µs ({:.2} ms)",
            hist.max(),
            hist.max() as f64 / 1000.0
        );
    }

    if let Some(mut c) = child {
        let _ = c.kill();
        let _ = c.wait();
        let _ = std::fs::remove_file(&wal_path);
    }

    println!("\n✓ Benchmark complete");
}
