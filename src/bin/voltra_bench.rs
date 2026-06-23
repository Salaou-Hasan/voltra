//! voltra-bench — Standalone WebSocket benchmark tool for Voltra
//!
//! Usage:
//!   voltra-bench [OPTIONS]
//!
//! Spawns N concurrent WebSocket clients, each sending M reducer calls.
//! Records per-call round-trip latency with HDR histogram and prints a
//! rich Markdown report to stdout (and optionally to a file).
//!
//! Example:
//!   cargo run --release --bin voltra-bench -- \
//!       --url ws://127.0.0.1:3000 \
//!       --clients 20 \
//!       --calls 500 \
//!       --warmup 50 \
//!       --output report.md

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "voltra-bench",
    about = "Voltra WebSocket benchmark tool — measures round-trip TPS and latency percentiles",
    long_about = None,
)]
struct Args {
    /// WebSocket URL of the Voltra server
    #[arg(long, default_value = "ws://127.0.0.1:3000")]
    url: String,

    /// Number of concurrent WebSocket clients
    #[arg(long, short = 'c', default_value = "10")]
    clients: usize,

    /// Number of reducer calls per client (benchmark phase, after warmup)
    #[arg(long, short = 'n', default_value = "500")]
    calls: usize,

    /// Warmup calls per client (not counted in metrics)
    #[arg(long, default_value = "50")]
    warmup: usize,

    /// Reducer to call
    #[arg(long, default_value = "increment")]
    reducer: String,

    /// Counter name to increment (used with the built-in `increment` reducer)
    #[arg(long, default_value = "bench_counter")]
    counter: String,

    /// Delta to increment by
    #[arg(long, default_value = "1")]
    delta: i32,

    /// Optional API key (sent as `Authorization: Bearer <key>`)
    #[arg(long)]
    api_key: Option<String>,

    /// Write a Markdown report to this file path in addition to stdout
    #[arg(long)]
    output: Option<String>,

    /// Per-call timeout in milliseconds
    #[arg(long, default_value = "5000")]
    timeout_ms: u64,
}

// ── Reducer args / result ─────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct IncrementArgs {
    name: String,
    delta: i32,
}

// ── Per-client benchmark ──────────────────────────────────────────────────────

#[derive(Debug)]
struct ClientResult {
    success: usize,
    errors: usize,
    /// Round-trip latencies in microseconds (benchmark phase only)
    latencies_us: Vec<u64>,
}

async fn run_client(
    client_id: usize,
    url: String,
    api_key: Option<String>,
    reducer: String,
    counter: String,
    delta: i32,
    warmup: usize,
    calls: usize,
    timeout_ms: u64,
) -> ClientResult {
    // Build the WebSocket request (add auth header if needed)
    let request = {
        let mut req = url
            .as_str()
            .into_client_request()
            .expect("invalid WebSocket URL");
        if let Some(key) = &api_key {
            req.headers_mut().insert(
                "authorization",
                format!("Bearer {}", key)
                    .parse()
                    .expect("valid header value"),
            );
        }
        req
    };

    let (mut ws, _) = match tokio_tungstenite::connect_async(request).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("  client-{}: connect failed: {}", client_id, e);
            return ClientResult {
                success: 0,
                errors: calls,
                latencies_us: vec![],
            };
        }
    };

    // Pre-encode the increment args (MessagePack array format)
    let args_bytes: Vec<u8> = rmp_serde::to_vec(&IncrementArgs {
        name: counter.clone(),
        delta,
    })
    .unwrap_or_default();

    let mut success = 0usize;
    let mut errors = 0usize;
    let mut latencies_us = Vec::with_capacity(calls);

    let total_iterations = warmup + calls;

    for i in 0..total_iterations {
        let call_id = (client_id as u64) * 1_000_000 + i as u64;
        let is_warmup = i < warmup;

        // Encode ReducerCall as MessagePack
        let call = voltra::ReducerCall {
            call_id,
            reducer_name: reducer.clone(),
            args: args_bytes.clone(),
        };
        let frame = match rmp_serde::to_vec(&call) {
            Ok(b) => b,
            Err(_) => {
                errors += 1;
                continue;
            }
        };

        let t0 = Instant::now();

        if ws.send(Message::Binary(frame)).await.is_err() {
            errors += 1;
            break;
        }

        let got_response = tokio::time::timeout(Duration::from_millis(timeout_ms), ws.next()).await;

        let elapsed_us = t0.elapsed().as_micros() as u64;

        match got_response {
            Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) => {
                if !is_warmup {
                    success += 1;
                    latencies_us.push(elapsed_us);
                }
            }
            Ok(Some(Ok(_))) => {} // pong / etc — ignore
            _ => {
                if !is_warmup {
                    errors += 1;
                }
            }
        }
    }

    let _ = ws.close(None).await;

    ClientResult {
        success,
        errors,
        latencies_us,
    }
}

// ── Report ────────────────────────────────────────────────────────────────────

struct Report {
    args: Args,
    total_success: usize,
    total_errors: usize,
    elapsed: Duration,
    /// All latency samples merged into one HDR histogram
    hist: Histogram<u64>,
}

impl Report {
    fn tps(&self) -> f64 {
        self.total_success as f64 / self.elapsed.as_secs_f64()
    }

    fn success_pct(&self) -> f64 {
        let total = self.total_success + self.total_errors;
        if total == 0 {
            0.0
        } else {
            self.total_success as f64 / total as f64 * 100.0
        }
    }

    fn render(&self) -> String {
        let mut out = String::new();

        out.push_str("# Voltra Benchmark Report\n\n");

        out.push_str(&format!("**Date**: {}  \n", chrono_or_now()));
        out.push_str(&format!("**Server**: {}  \n", self.args.url));
        out.push_str(&format!("**Reducer**: {}  \n\n", self.args.reducer));

        out.push_str("## Configuration\n\n");
        out.push_str("| Parameter | Value |\n|---|---|\n");
        out.push_str(&format!("| Concurrent clients | {} |\n", self.args.clients));
        out.push_str(&format!("| Calls per client | {} |\n", self.args.calls));
        out.push_str(&format!("| Warmup per client | {} |\n", self.args.warmup));
        out.push_str(&format!(
            "| Total benchmark calls | {} |\n\n",
            self.args.clients * self.args.calls
        ));

        out.push_str("## Results\n\n");
        out.push_str("| Metric | Value |\n|---|---|\n");
        out.push_str(&format!(
            "| Total time | {:.3}s |\n",
            self.elapsed.as_secs_f64()
        ));
        out.push_str(&format!("| **Throughput** | **{:.0} TPS** |\n", self.tps()));
        out.push_str(&format!("| Successful calls | {} |\n", self.total_success));
        out.push_str(&format!("| Failed calls | {} |\n", self.total_errors));
        out.push_str(&format!(
            "| Success rate | {:.1}% |\n\n",
            self.success_pct()
        ));

        if self.hist.len() > 0 {
            out.push_str("## Latency Distribution\n\n");
            out.push_str("| Percentile | μs | ms |\n|---|---|---|\n");

            for pct in &[50.0f64, 75.0, 90.0, 95.0, 99.0, 99.9, 100.0] {
                let us = self.hist.value_at_percentile(*pct);
                let label = if *pct == 100.0 {
                    "max".to_string()
                } else {
                    format!("p{}", pct)
                };
                out.push_str(&format!(
                    "| {} | {} | {:.3} |\n",
                    label,
                    us,
                    us as f64 / 1000.0
                ));
            }
            out.push('\n');
        }

        out
    }
}

fn chrono_or_now() -> String {
    // Simple UTC timestamp without an extra crate dependency
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("Unix timestamp {}", secs)
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("=== Voltra Benchmark ===");
    println!("Server : {}", args.url);
    println!("Reducer: {}", args.reducer);
    println!(
        "Clients: {}  |  Calls/client: {}  |  Warmup/client: {}",
        args.clients, args.calls, args.warmup
    );
    if args.api_key.is_some() {
        println!("Auth   : Bearer token set");
    }
    println!();

    // ── Verify the server is reachable ────────────────────────────────────────
    {
        let mut probe = args
            .url
            .as_str()
            .into_client_request()
            .expect("invalid URL");
        if let Some(key) = &args.api_key {
            probe.headers_mut().insert(
                "authorization",
                format!("Bearer {}", key)
                    .parse()
                    .expect("valid header value"),
            );
        }
        match tokio_tungstenite::connect_async(probe).await {
            Ok(_) => println!("✓ Server is reachable\n"),
            Err(e) => {
                eprintln!("✗ Cannot connect to {}: {}", args.url, e);
                eprintln!("  Start the server first: cargo run --release -- start");
                std::process::exit(1);
            }
        }
    }

    // ── Spawn clients ─────────────────────────────────────────────────────────
    println!(
        "Running {} warmup + {} benchmark calls per client ({} clients)…",
        args.warmup, args.calls, args.clients
    );

    let latencies_shared: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut join_set = JoinSet::new();

    let bench_start = Instant::now();

    for client_id in 0..args.clients {
        let url = args.url.clone();
        let api_key = args.api_key.clone();
        let reducer = args.reducer.clone();
        let counter = args.counter.clone();
        let delta = args.delta;
        let warmup = args.warmup;
        let calls = args.calls;
        let timeout_ms = args.timeout_ms;

        join_set.spawn(run_client(
            client_id, url, api_key, reducer, counter, delta, warmup, calls, timeout_ms,
        ));
    }

    let mut total_success = 0usize;
    let mut total_errors = 0usize;

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(cr) => {
                total_success += cr.success;
                total_errors += cr.errors;
                if let Ok(mut lats) = latencies_shared.lock() {
                    lats.extend(cr.latencies_us);
                }
            }
            Err(e) => eprintln!("Client task panicked: {}", e),
        }
    }

    let elapsed = bench_start.elapsed();

    // ── Build histogram ───────────────────────────────────────────────────────
    let mut hist = Histogram::<u64>::new(3).expect("histogram alloc");
    if let Ok(lats) = latencies_shared.lock() {
        for &us in lats.iter() {
            let _ = hist.record(us);
        }
    }

    // ── Build and print report ────────────────────────────────────────────────
    let report = Report {
        args,
        total_success,
        total_errors,
        elapsed,
        hist,
    };

    let md = report.render();
    print!("{}", md);

    if let Some(path) = &report.args.output {
        match std::fs::write(path, &md) {
            Ok(_) => println!("Report written to: {}", path),
            Err(e) => eprintln!("Failed to write report to {}: {}", path, e),
        }
    }
}
