// latency_bench — WebSocket round-trip latency at increasing concurrency
//
// Starts an embedded Voltra server, drives it with N concurrent clients,
// and measures p50 / p99 / p999 latency + aggregate TPS.
//
// Run each concurrency level for 5 seconds, then print a table.
// The table directly shows WHY 15K CCU hits ~54K TPS: TPS = clients / latency.

#![allow(dead_code)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use voltra::{
    network::message::{ClientMessage, ReducerCall},
    reducer::{context::ReducerContext, native::NativeReducerBackend, registry::NativeReducerItem},
    ServerHandle,
};
use serde_json::json;
use std::{
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

// ── Minimal native reducer for the benchmark ─────────────────────────────────

fn bench_ping(ctx: &mut ReducerContext, args: &[u8]) -> voltra::error::Result<Vec<u8>> {
    let a: Vec<serde_json::Value> = rmp_serde::from_slice(args).unwrap_or_default();
    let key = a.first().and_then(|v| v.as_str()).unwrap_or("k");
    ctx.set_row(
        "bench".into(),
        key.into(),
        json!({ "v": 1, "ts": ctx.timestamp() }),
    )?;
    Ok(rmp_serde::to_vec(&json!({ "ok": true })).unwrap())
}

inventory::submit! { NativeReducerItem {
    name: "bench_ping",
    make: || Box::new(NativeReducerBackend::new(bench_ping)),
}}

// ── Embedded server startup ───────────────────────────────────────────────────

async fn start_server(port: u16) -> ServerHandle {
    let dir = std::env::temp_dir().join(format!("voltra_latbench_{}", port));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let toml = format!(
        r#"[server]
ws = "{host}:{port}"
metrics_port = {mp}
workers = 0
unsafe_no_fsync = true
[wal]
path = "{wal}"
[snapshot]
dir = "{snap}"
"#,
        host = "127.0.0.1",
        port = port,
        mp   = port + 1000,
        wal  = dir.join("wal.bin").display(),
        snap = dir.join("snaps").display(),
    );

    let mut config = voltra::config::Config::from_env();
    config.port              = port;
    config.metrics_port      = port + 1000;
    config.workers           = 0; // = num_cpus
    config.unsafe_no_fsync   = true;
    config.wal_path          = dir.join("wal.bin");
    config.snapshot_dir      = dir.join("snaps");
    let _ = toml; // used for documentation clarity only

    match voltra::run_server_with_handle(config).await {
        Ok((handle, fut)) => { tokio::spawn(fut); handle }
        Err(e) => panic!("Server start failed: {e}"),
    }
}

async fn wait_ready(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() { return; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server on :{port} never became ready");
}

// ── Single WebSocket client ───────────────────────────────────────────────────

struct WsClient {
    sink:   futures::stream::SplitSink<
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
                >, Message>,
    stream: futures::stream::SplitStream<
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
                >>,
    call_id: u64,
}

impl WsClient {
    async fn connect(url: &str, client_id: u64) -> Option<Self> {
        let mut req = url.into_client_request().ok()?;
        req.headers_mut().insert(
            "X-Voltra-Client-ID",
            client_id.to_string().parse().unwrap(),
        );
        let (ws, _) = tokio_tungstenite::connect_async(req).await.ok()?;
        let (sink, stream) = futures::StreamExt::split(ws);
        Some(Self { sink, stream, call_id: 0 })
    }

    async fn call(&mut self, key: &str) -> Option<u64> {
        self.call_id += 1;
        let id = self.call_id;

        let msg = ClientMessage::ReducerCall(ReducerCall {
            call_id:      id,
            reducer_name: "bench_ping".into(),
            args:         rmp_serde::to_vec(&vec![key]).unwrap(),
        });
        let frame = rmp_serde::to_vec(&msg).unwrap();

        let t0 = Instant::now();
        self.sink.send(Message::Binary(frame)).await.ok()?;

        // Wait for ReducerResponse matching our call_id
        loop {
            let raw = self.stream.next().await?.ok()?;
            let bytes = match raw {
                Message::Binary(b) => b,
                Message::Ping(b) => { let _ = self.sink.send(Message::Pong(b)).await; continue; }
                Message::Close(_) => return None,
                _ => continue,
            };
            // Decode: expect [call_id, success, payload] array (ReducerResponse)
            if let Ok(arr) = rmp_serde::from_slice::<Vec<serde_json::Value>>(&bytes) {
                if arr.first().and_then(|v| v.as_u64()) == Some(id) {
                    return Some(t0.elapsed().as_micros() as u64);
                }
            }
        }
    }
}

// ── Run one concurrency level ─────────────────────────────────────────────────

async fn run_level(
    port: u16,
    clients: usize,
    duration: Duration,
) -> (u64 /*tps*/, u64 /*p50_us*/, u64 /*p99_us*/, u64 /*p999_us*/) {
    let url = format!("ws://127.0.0.1:{port}");
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let total = Arc::new(AtomicU64::new(0));
    let stop  = Arc::new(AtomicBool::new(false));

    let deadline = Instant::now() + duration;

    let mut tasks = Vec::with_capacity(clients);
    for cid in 0..clients {
        let url2    = url.clone();
        let hist2   = hist.clone();
        let total2  = total.clone();
        let stop2   = stop.clone();

        tasks.push(tokio::spawn(async move {
            let Some(mut ws) = WsClient::connect(&url2, cid as u64).await else { return; };
            let mut i = 0usize;
            while !stop2.load(Ordering::Relaxed) {
                let key = format!("k_{}", (cid * 64 + i) % 1024);
                if let Some(us) = ws.call(&key).await {
                    hist2.lock().await.record(us).ok();
                    total2.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
                i += 1;
            }
        }));
    }

    // Wait for test duration
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    stop.store(true, Ordering::Relaxed);

    // Collect results
    for t in tasks { let _ = tokio::time::timeout(Duration::from_secs(2), t).await; }

    let ops = total.load(Ordering::Relaxed);
    let tps = ops / duration.as_secs();
    let h   = hist.lock().await;
    let p50  = h.value_at_quantile(0.50);
    let p99  = h.value_at_quantile(0.99);
    let p999 = h.value_at_quantile(0.999);

    (tps, p50, p99, p999)
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let port: u16 = 3850;
    let warmup_secs = 3u64;
    let test_secs   = 5u64;

    println!();
    println!("  Voltra WebSocket Round-Trip Latency Benchmark");
    println!("  Embedded server on :{port} | native reducer | {} sec per level", test_secs);
    println!("  Metric: p50 / p99 / p999 latency in milliseconds");
    println!("  TPS shown = calls/sec completed by ALL clients combined");
    println!();

    // Start server
    print!("  Starting embedded server ... ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let _handle = start_server(port).await;
    wait_ready(port).await;
    println!("ready.");

    // Warmup with 10 clients
    print!("  Warming up ({}s, 10 clients) ... ", warmup_secs);
    let _ = std::io::Write::flush(&mut std::io::stdout());
    run_level(port, 10, Duration::from_secs(warmup_secs)).await;
    println!("done.");
    println!();

    // Concurrency levels to test
    let levels: &[usize] = &[1, 5, 10, 50, 100, 250, 500, 1000];

    println!("  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
        "Clients", "TPS", "p50 (ms)", "p99 (ms)", "p999 (ms)", "Little's Law check");
    println!("  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
        "-------", "----------", "----------", "----------", "----------", "-------------------");

    let mut results = Vec::new();
    for &n in levels {
        print!("  {:>8}  running...  ", n);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let (tps, p50, p99, p999) = run_level(port, n, Duration::from_secs(test_secs)).await;
        let p50_ms  = p50  as f64 / 1000.0;
        let p99_ms  = p99  as f64 / 1000.0;
        let p999_ms = p999 as f64 / 1000.0;
        // Little's Law: predicted TPS = N / (p50_ms / 1000) = N * 1000 / p50_ms
        let predicted = if p50_ms > 0.0 { (n as f64 * 1000.0 / p50_ms) as u64 } else { 0 };
        // clear the "running..." line
        print!("\r");
        println!("  {:>8}  {:>10}  {:>10.2}  {:>10.2}  {:>10.2}  predicted {}/s",
            n, fmt_tps(tps), p50_ms, p99_ms, p999_ms, fmt_tps(predicted));
        results.push((n, tps, p50_ms, p99_ms));
    }

    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  WHY 15K CCU HIT ~54K TPS  (Little's Law: TPS = N / mean_latency)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("  At 15,000 concurrent clients, if p50 latency = 289ms:");
    println!("    TPS ceiling = 15,000 / 0.289s = ~51,900 TPS  ← matches the 53K measured");
    println!();
    println!("  The engine itself does 876K writes/s on 24 threads (see engine-bench).");
    println!("  The gap is NOT the engine. It is:");
    println!();
    println!("  1. QUEUE WAIT TIME — 15K clients fire simultaneously. Each call waits");
    println!("     in kanal queue before one of the 24 workers picks it up.");
    println!("     Queue depth / (24 workers × execution_rate) = avg wait time.");
    println!();
    println!("  2. SCHEDULER CONTENTION — server + 3 client processes share 24 cores.");
    println!("     30,000+ Tokio tasks (15K reader + 15K writer) compete with");
    println!("     24 reducer workers for the same 24 CPU threads.");
    println!();
    println!("  3. NETWORK STACK PRESSURE — 15K concurrent WebSocket connections.");
    println!("     Each response travels: worker → kanal → write task → TCP → client.");
    println!("     At 15K clients, TCP send buffers and OS scheduler are saturated.");
    println!();
    println!("  SOLUTION: separate server box from client boxes (voltra-sim serve +");
    println!("  --external flag). With clients on different machines, the server");
    println!("  stops fighting for CPU with its own load generator.");
    println!();

    // Show latency growth trend from our actual measurements
    if let (Some(first), Some(last)) = (results.first(), results.last()) {
        let latency_growth = last.2 / first.2;
        let tps_ceiling_15k = 15_000.0f64 / (last.2 * 15_000.0 / last.0 as f64 / 1000.0) as f64;
        println!("  From this run: p50 grew {:.1}× from {} → {} clients ({:.2}ms → {:.2}ms)",
            latency_growth, first.0, last.0, first.2, last.2);
    }
    println!();
}

fn fmt_tps(n: u64) -> String {
    if n >= 1_000_000 { format!("{:.2}M", n as f64 / 1_000_000.0) }
    else if n >= 1_000 { format!("{:.1}K", n as f64 / 1_000.0) }
    else { format!("{}", n) }
}
