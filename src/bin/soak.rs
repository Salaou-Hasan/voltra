//! voltra-soak — Sustained-load soak test for Voltra
//!
//! Runs N WebSocket clients against a live server for a configurable DURATION
//! (minutes to days), at a target per-client call rate.  While loading, it
//! samples the server's /healthz endpoint to track memory growth, queue depth,
//! and WAL size over time — the signals that expose leaks and degradation
//! which short benchmarks never catch.
//!
//! Usage:
//!   cargo run --release --bin voltra-soak -- --duration-secs 3600 --clients 50
//!
//!   # Week-long soak with periodic CSV samples:
//!   cargo run --release --bin voltra-soak -- \
//!       --duration-secs 604800 --clients 100 --rate-per-client 10 \
//!       --csv soak_samples.csv
//!
//! Exit code 0 = healthy; 1 = failure thresholds exceeded (error rate > 1%,
//! or memory grew monotonically by more than --max-memory-growth-pct).

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use voltra::network::message::{ClientMessage, ReducerCall};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser, Debug, Clone)]
#[command(name = "voltra-soak", about = "Voltra sustained-load soak test")]
struct Args {
    /// WebSocket URL of the server under test
    #[arg(long, default_value = "ws://127.0.0.1:3000")]
    url: String,

    /// Metrics/admin URL of the server under test (for /healthz sampling)
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    metrics_url: String,

    /// Total soak duration in seconds (3600 = 1h, 86400 = 1 day, 604800 = 1 week)
    #[arg(long, default_value = "600")]
    duration_secs: u64,

    /// Number of concurrent WebSocket clients
    #[arg(long, short = 'c', default_value = "20")]
    clients: usize,

    /// Target calls per second PER CLIENT (0 = as fast as possible)
    #[arg(long, default_value = "20")]
    rate_per_client: u64,

    /// Reducer to call
    #[arg(long, default_value = "increment")]
    reducer: String,

    /// Seconds between /healthz samples + progress reports
    #[arg(long, default_value = "30")]
    sample_interval_secs: u64,

    /// Optional CSV file for time-series samples
    #[arg(long)]
    csv: Option<String>,

    /// Optional API key
    #[arg(long)]
    api_key: Option<String>,

    /// Fail the soak if memory grows more than this percent from the first
    /// to the last sample (sustained-leak detector)
    #[arg(long, default_value = "200.0")]
    max_memory_growth_pct: f64,

    /// Fail the soak if the error rate exceeds this percent
    #[arg(long, default_value = "1.0")]
    max_error_pct: f64,
}

#[derive(Serialize)]
struct IncArgs { name: String, delta: i32 }

#[derive(Debug, Clone)]
struct HealthSample {
    memory_bytes: u64,
    queue_depth: u64,
    wal_bytes: u64,
    total_rows: u64,
    connections: u64,
}

async fn sample_health(metrics_url: &str) -> Option<HealthSample> {
    let resp = reqwest::Client::new()
        .get(format!("{}/healthz", metrics_url))
        .timeout(Duration::from_secs(5))
        .send().await.ok()?;
    let v: serde_json::Value = resp.json().await.ok()?;
    Some(HealthSample {
        memory_bytes: v["memory_usage_bytes"].as_u64().unwrap_or(0),
        queue_depth:  v["reducer_queue_depth"].as_u64().unwrap_or(0),
        wal_bytes:    v["wal_file_size_bytes"].as_u64().unwrap_or(0),
        total_rows:   v["total_rows"].as_u64().unwrap_or(0),
        connections:  v["active_connections"].as_u64().unwrap_or(0),
    })
}

async fn soak_client(
    client_id: usize,
    args: Args,
    success: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    deadline: Instant,
    reconnects: Arc<AtomicUsize>,
) {
    let inc_args = rmp_serde::to_vec(&IncArgs {
        name: format!("soak_counter_{}", client_id % 16),
        delta: 1,
    }).unwrap_or_default();

    let tick = if args.rate_per_client > 0 {
        Some(Duration::from_micros(1_000_000 / args.rate_per_client))
    } else {
        None
    };

    let mut call_seq = 0u64;

    // Outer loop: reconnect on connection loss (a soak test must survive
    // transient disconnects rather than silently going idle).
    'reconnect: while Instant::now() < deadline {
        let request = {
            let mut req = match args.url.as_str().into_client_request() {
                Ok(r) => r,
                Err(_) => return,
            };
            if let Some(key) = &args.api_key {
                if let Ok(hv) = format!("Bearer {}", key).parse() {
                    req.headers_mut().insert("authorization", hv);
                }
            }
            req
        };

        let (mut ws, _) = match tokio_tungstenite::connect_async(request).await {
            Ok(pair) => pair,
            Err(e) => {
                log::debug!("soak connect error (client {}): {}", client_id, e);
                errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
                reconnects.fetch_add(1, Ordering::Relaxed);
                continue 'reconnect;
            }
        };

        while Instant::now() < deadline {
            call_seq += 1;

            // Wrap in ClientMessage envelope — required by the server's wire protocol.
            // The server's primary decode path expects ClientMessage::ReducerCall(...)
            // not a bare ReducerCall struct.
            let msg = ClientMessage::ReducerCall(ReducerCall {
                call_id: (client_id as u64) << 40 | call_seq,
                reducer_name: args.reducer.clone(),
                args: inc_args.clone(),
            });
            let frame = match rmp_serde::to_vec(&msg) {
                Ok(b) => b,
                Err(_) => { errors.fetch_add(1, Ordering::Relaxed); continue; }
            };

            let t0 = Instant::now();
            if ws.send(Message::Binary(frame)).await.is_err() {
                errors.fetch_add(1, Ordering::Relaxed);
                reconnects.fetch_add(1, Ordering::Relaxed);
                continue 'reconnect;
            }
            match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) => {
                    success.fetch_add(1, Ordering::Relaxed);
                    let us = t0.elapsed().as_micros() as u64;
                    if let Ok(mut h) = hist.lock() { let _ = h.record(us); }
                }
                Ok(Some(Ok(_))) => {} // ping/pong/close frames — ignore
                _ => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    reconnects.fetch_add(1, Ordering::Relaxed);
                    continue 'reconnect;
                }
            }

            if let Some(t) = tick {
                let spent = t0.elapsed();
                if spent < t { tokio::time::sleep(t - spent).await; }
            }
        }
        let _ = ws.close(None).await;
        break;
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let start = Instant::now();

    println!("voltra-soak: {} clients × {} calls/s for {}s against {}",
        args.clients, args.rate_per_client, args.duration_secs, args.url);

    let success = Arc::new(AtomicU64::new(0));
    let errors  = Arc::new(AtomicU64::new(0));
    let reconnects = Arc::new(AtomicUsize::new(0));
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()));

    let mut handles = Vec::new();
    for cid in 0..args.clients {
        handles.push(tokio::spawn(soak_client(
            cid, args.clone(), success.clone(), errors.clone(), hist.clone(), deadline, reconnects.clone(),
        )));
        // Stagger connections to avoid a thundering herd on connect.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ── Sampling loop ─────────────────────────────────────────────────────────
    let mut samples: Vec<HealthSample> = Vec::new();
    let mut csv_lines = vec!["elapsed_secs,success,errors,tps_window,memory_bytes,queue_depth,wal_bytes,total_rows,connections".to_string()];
    let mut last_success = 0u64;
    let mut last_t = Instant::now();

    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(args.sample_interval_secs.max(5))).await;
        let elapsed = start.elapsed().as_secs();
        let s_now = success.load(Ordering::Relaxed);
        let e_now = errors.load(Ordering::Relaxed);
        let window_tps = (s_now - last_success) as f64 / last_t.elapsed().as_secs_f64();
        last_success = s_now; last_t = Instant::now();

        let health = sample_health(&args.metrics_url).await;
        match &health {
            Some(h) => {
                println!("[{:>6}s] ok={:>10} err={:>6} tps={:>8.0} mem={:>6.1}MB queue={:>4} wal={:>6.1}MB rows={}",
                    elapsed, s_now, e_now, window_tps,
                    h.memory_bytes as f64 / 1e6, h.queue_depth,
                    h.wal_bytes as f64 / 1e6, h.total_rows);
                csv_lines.push(format!("{},{},{},{:.0},{},{},{},{},{}",
                    elapsed, s_now, e_now, window_tps,
                    h.memory_bytes, h.queue_depth, h.wal_bytes, h.total_rows, h.connections));
                samples.push(h.clone());
            }
            None => {
                println!("[{:>6}s] ok={:>10} err={:>6} tps={:>8.0} (healthz UNREACHABLE)",
                    elapsed, s_now, e_now, window_tps);
            }
        }
    }

    for h in handles { let _ = h.await; }

    // ── Final report ──────────────────────────────────────────────────────────
    let total_s = success.load(Ordering::Relaxed);
    let total_e = errors.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let error_pct = if total_s + total_e > 0 {
        100.0 * total_e as f64 / (total_s + total_e) as f64
    } else { 100.0 };

    println!("\n══════════════ SOAK REPORT ══════════════");
    println!("Duration:        {:.0}s", elapsed.as_secs_f64());
    println!("Total calls:     {} ok, {} errors ({:.3}% error rate)", total_s, total_e, error_pct);
    println!("Average TPS:     {:.0}", total_s as f64 / elapsed.as_secs_f64());
    println!("Reconnects:      {}", reconnects.load(Ordering::Relaxed));
    if let Ok(h) = hist.lock() {
        if h.len() > 0 {
            println!("Latency p50:     {:.2} ms", h.value_at_quantile(0.50) as f64 / 1000.0);
            println!("Latency p99:     {:.2} ms", h.value_at_quantile(0.99) as f64 / 1000.0);
            println!("Latency p99.9:   {:.2} ms", h.value_at_quantile(0.999) as f64 / 1000.0);
            println!("Latency max:     {:.2} ms", h.max() as f64 / 1000.0);
        }
    }

    let mut failed = false;

    if let (Some(first), Some(last)) = (samples.first(), samples.last()) {
        if first.memory_bytes > 0 {
            let growth_pct = 100.0 * (last.memory_bytes as f64 - first.memory_bytes as f64)
                / first.memory_bytes as f64;
            println!("Memory first→last: {:.1}MB → {:.1}MB ({:+.1}%)",
                first.memory_bytes as f64 / 1e6, last.memory_bytes as f64 / 1e6, growth_pct);
            if growth_pct > args.max_memory_growth_pct {
                println!("FAIL: memory grew {:.1}% (threshold {:.1}%) — possible leak",
                    growth_pct, args.max_memory_growth_pct);
                failed = true;
            }
        }
    }

    if error_pct > args.max_error_pct {
        println!("FAIL: error rate {:.3}% exceeds threshold {:.1}%", error_pct, args.max_error_pct);
        failed = true;
    }

    if let Some(csv_path) = &args.csv {
        if let Err(e) = std::fs::write(csv_path, csv_lines.join("\n")) {
            eprintln!("Could not write CSV {}: {}", csv_path, e);
        } else {
            println!("Samples written to {}", csv_path);
        }
    }

    println!("Verdict: {}", if failed { "FAIL" } else { "PASS" });
    std::process::exit(if failed { 1 } else { 0 });
}
