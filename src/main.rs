// ============================================================================
// NeonDB main.rs — high-throughput rewrite
//
// Session 8 fix:
//  - Pass Arc<TableStore> into start_listener so WebSocket handler can
//    deliver initial_snapshot frames on subscribe (TODO-003 wiring).
//
// Previous sessions:
//  1. TableStore is now Arc<TableStore> (no Mutex) — DashMap handles concurrency.
//  2. SegQueue + sleep(50ms) poll loop replaced by kanal async channel —
//     zero-sleep receive, true async wakeup.
//  3. N parallel reducer worker tasks (num_cpus) — CPU-bound reducers no
//     longer serialise on a single task.
//  4. reducer_timeout_ms wired up via tokio::time::timeout.
//  5. SubscriptionManager wrapped in Arc (no Mutex) — DashMap inside.
//  6. WAL recovery uses Arc<TableStore> directly.
//  7. TODO-003: tables passed to start_listener for initial-snapshot delivery.
// ============================================================================

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use neondb::{
    config::{Config, ScheduledReducerConfig},
    error::Result,
    network::{start_listener, PendingCall, ReducerResponse},
    reducer::{ReducerContext, ReducerRegistry},
    subscriptions::SubscriptionManager,
    table::TableStore,
    wal::{
        snapshot::{find_latest_snapshot, load_snapshot, save_snapshot},
        BatchedWalWriter, WalEntry, WalReader,
    },
};
use rmp_serde;
use tokio::sync::watch;

#[derive(Parser, Debug)]
#[command(name = "neondb")]
#[command(author, version, about = "NeonDB — self-hosted real-time game backend")]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    // ── Server ───────────────────────────────────────────────────────────────
    /// Scaffold a new NeonDB project in PATH
    Init {
        #[arg(value_name = "PATH", default_value = ".")]
        path: PathBuf,
    },
    /// Compile JS reducers in modules/ to WASM (requires `javy`)
    Build {
        #[arg(
            short = 'm',
            long,
            default_value = "modules",
            help = "Directory containing .js reducers to compile"
        )]
        modules_dir: Option<PathBuf>,
    },
    /// Start the NeonDB server
    Start {
        #[arg(short = 'a', long, help = "Listen address (default 127.0.0.1)")]
        host: Option<String>,
        #[arg(short = 'p', long, help = "WebSocket port (default 3000)")]
        port: Option<u16>,
        #[arg(short = 'd', long, help = "Data directory (sets WAL path)")]
        data_dir: Option<PathBuf>,
        #[arg(long = "wal-path", help = "Explicit WAL file path")]
        wal_path: Option<PathBuf>,
        #[arg(short = 'f', long, help = "WAL fsync interval ms")]
        fsync_interval_ms: Option<u32>,
    },

    // ── Inspect (read-only, hits the admin HTTP port) ─────────────────────
    /// Show server status and metrics
    Status {
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics HTTP URL"
        )]
        metrics_url: String,
    },
    /// List all tables and their row counts
    Tables {
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics HTTP URL"
        )]
        metrics_url: String,
    },
    /// Read rows from a table  (optionally filter to a single row_key)
    Get {
        /// Table name (e.g. `players`, `counters`)
        table: String,
        /// Optional row_key to return just one row
        key: Option<String>,
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics HTTP URL"
        )]
        metrics_url: String,
    },

    // ── Interactive (WebSocket) ───────────────────────────────────────────
    /// Call a reducer once and print the result
    Call {
        /// Reducer name (e.g. `increment`)
        reducer: String,
        /// JSON-encoded args.  For the built-in `increment` use '["counter", 1]'.
        #[arg(help = "JSON args, e.g. '[\"my_counter\", 5]'")]
        args: Option<String>,
        #[arg(
            long,
            default_value = "ws://127.0.0.1:3000",
            help = "WebSocket URL of the server"
        )]
        url: String,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
    /// Subscribe to a table and stream live updates (Ctrl-C to stop)
    Watch {
        /// Subscription query, e.g. `counters` or `players WHERE level > 5`
        query: String,
        #[arg(
            long,
            default_value = "ws://127.0.0.1:3000",
            help = "WebSocket URL of the server"
        )]
        url: String,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
    /// Run a WebSocket throughput benchmark against a running server
    Bench {
        #[arg(
            long,
            default_value = "ws://127.0.0.1:3000",
            help = "WebSocket URL of the server"
        )]
        url: String,
        #[arg(
            short = 'c',
            long,
            default_value = "10",
            help = "Number of concurrent clients"
        )]
        clients: usize,
        #[arg(short = 'n', long, default_value = "500", help = "Calls per client")]
        calls: usize,
        #[arg(long, default_value = "50", help = "Warmup calls per client")]
        warmup: usize,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // ── Server commands ───────────────────────────────────────────────
        Commands::Init { path } => {
            init_project(path)?;
            Ok(())
        }
        Commands::Build { modules_dir } => {
            build_wasm_modules(modules_dir.as_deref().unwrap_or(Path::new("modules")))
        }
        Commands::Start {
            host,
            port,
            data_dir,
            wal_path,
            fsync_interval_ms,
        } => {
            let mut config = Config::from_env();
            if let Some(h) = host {
                config.host = h;
            }
            if let Some(p) = port {
                config.port = p;
            }
            if let Some(d) = data_dir {
                config.wal_path = d.join("neondb.wal");
            }
            if let Some(w) = wal_path {
                config.wal_path = w;
            }
            if let Some(f) = fsync_interval_ms {
                config.fsync_interval_ms = f;
            }
            run_server(config).await
        }

        // ── Inspect commands (HTTP) ───────────────────────────────────
        Commands::Status { metrics_url } => neondb::cli::cmd_status(&metrics_url).await,
        Commands::Tables { metrics_url } => neondb::cli::cmd_tables(&metrics_url).await,
        Commands::Get {
            table,
            key,
            metrics_url,
        } => neondb::cli::cmd_get(&metrics_url, &table, key.as_deref()).await,

        // ── Interactive commands (WebSocket) ───────────────────────────
        Commands::Call {
            reducer,
            args,
            url,
            api_key,
        } => neondb::cli::cmd_call(&url, &reducer, args.as_deref(), api_key.as_deref()).await,
        Commands::Watch {
            query,
            url,
            api_key,
        } => neondb::cli::cmd_watch(&url, &query, api_key.as_deref()).await,
        Commands::Bench {
            url,
            clients,
            calls,
            warmup,
            api_key,
        } => run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await,
    }
}

/// Run a quick inline WebSocket benchmark from the CLI (`neondb bench`).
async fn run_cli_bench(
    ws_url: &str,
    num_clients: usize,
    calls_per_client: usize,
    warmup_per_client: usize,
    api_key: Option<&str>,
) -> Result<()> {
    use futures::{SinkExt, StreamExt};
    use hdrhistogram::Histogram;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use tokio::task::JoinSet;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    #[derive(serde::Serialize)]
    struct IncrArgs {
        name: String,
        delta: i32,
    }
    #[derive(serde::Serialize)]
    struct CallW {
        #[serde(rename = "ReducerCall")]
        rc: (u64, String, Vec<u8>),
    }

    println!("=== NeonDB Bench ===");
    println!("  Server  : {}", ws_url);
    println!(
        "  Clients : {}  Calls/client: {}  Warmup: {}",
        num_clients, calls_per_client, warmup_per_client
    );

    let args_bytes = rmp_serde::to_vec(&IncrArgs {
        name: "bench".to_string(),
        delta: 1,
    })
    .unwrap();
    let latencies: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(Histogram::new(3).unwrap()));
    let mut join_set = JoinSet::new();
    let start = Instant::now();

    for cid in 0..num_clients {
        let url = ws_url.to_string();
        let api = api_key.map(String::from);
        let args = args_bytes.clone();
        let lat = latencies.clone();
        let warmup = warmup_per_client;
        let calls = calls_per_client;

        join_set.spawn(async move {
            let mut req = url.as_str().into_client_request().unwrap();
            if let Some(k) = &api {
                req.headers_mut()
                    .insert("authorization", format!("Bearer {}", k).parse().unwrap());
            }
            let Ok((mut ws, _)) = tokio_tungstenite::connect_async(req).await else {
                return 0usize;
            };
            let total = warmup + calls;
            let mut ok = 0usize;
            for i in 0..total {
                let cw = rmp_serde::to_vec(&CallW {
                    rc: (
                        (cid as u64) * 1_000_000 + i as u64,
                        "increment".to_string(),
                        args.clone(),
                    ),
                })
                .unwrap();
                let t0 = Instant::now();
                if ws.send(Message::Binary(cw)).await.is_err() {
                    break;
                }
                if let Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) =
                    tokio::time::timeout(Duration::from_secs(10), ws.next()).await
                {
                    if i >= warmup {
                        let us = t0.elapsed().as_micros() as u64;
                        if let Ok(mut h) = lat.lock() {
                            let _ = h.record(us);
                        }
                        ok += 1;
                    }
                }
            }
            let _ = ws.close(None).await;
            ok
        });
    }

    let mut total = 0usize;
    while let Some(r) = join_set.join_next().await {
        if let Ok(n) = r {
            total += n;
        }
    }
    let elapsed = start.elapsed();
    let tps = total as f64 / elapsed.as_secs_f64();

    println!("\nResults:");
    println!("  Time       : {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput : {:.0} TPS", tps);
    println!(
        "  Success    : {}/{}",
        total,
        num_clients * calls_per_client
    );
    if let Ok(h) = latencies.lock() {
        println!(
            "  Latency (µs): p50={} p95={} p99={} max={}",
            h.value_at_percentile(50.0),
            h.value_at_percentile(95.0),
            h.value_at_percentile(99.0),
            h.max()
        );
    }
    Ok(())
}

/// Compile every `.js` module in `modules_dir` to `.wasm` using the `javy` compiler.
///
/// `javy` embeds QuickJS into a WASM module, giving JS reducers near-native
/// performance via the existing Wasmtime runtime — no V8 required.
///
/// Install javy: <https://github.com/bytecodealliance/javy/releases>
///   Or via cargo: `cargo install javy`
fn build_wasm_modules(modules_dir: &Path) -> Result<()> {
    if !modules_dir.is_dir() {
        println!(
            "No '{}' directory found. Create one and add your .js reducers.",
            modules_dir.display()
        );
        return Ok(());
    }

    // Check that javy is available.
    let javy_ok = std::process::Command::new("javy")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !javy_ok {
        eprintln!("Error: 'javy' compiler not found.");
        eprintln!();
        eprintln!("Install javy to compile JS reducers to WASM:");
        eprintln!("  cargo install javy");
        eprintln!("  or download from https://github.com/bytecodealliance/javy/releases");
        eprintln!();
        eprintln!("Why WASM? Compiled reducers run via Wasmtime (Cranelift JIT) and are");
        eprintln!("10-50x faster than the Boa interpreter used for raw .js files.");
        return Err(neondb::error::NeonDBError::internal("javy not installed"));
    }

    let entries: Vec<_> = std::fs::read_dir(modules_dir)
        .map_err(|e| {
            neondb::error::NeonDBError::internal(format!(
                "Cannot read {}: {}",
                modules_dir.display(),
                e
            ))
        })?
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("js"))
                .unwrap_or(false)
        })
        .collect();

    if entries.is_empty() {
        println!(
            "No .js files found in {}. Nothing to build.",
            modules_dir.display()
        );
        return Ok(());
    }

    let mut compiled = 0usize;
    let mut failed = 0usize;

    for entry in &entries {
        let js_path = entry.path();
        let wasm_path = js_path.with_extension("wasm");

        print!(
            "Compiling {} -> {} ... ",
            js_path.file_name().unwrap_or_default().to_string_lossy(),
            wasm_path.file_name().unwrap_or_default().to_string_lossy(),
        );

        let status = std::process::Command::new("javy")
            .arg("compile")
            .arg(&js_path)
            .arg("-o")
            .arg(&wasm_path)
            .status();

        match status {
            Ok(s) if s.success() => {
                println!("OK");
                compiled += 1;
            }
            Ok(s) => {
                println!("FAILED (exit code {})", s.code().unwrap_or(-1));
                failed += 1;
            }
            Err(e) => {
                println!("FAILED ({})", e);
                failed += 1;
            }
        }
    }

    println!();
    println!("Build complete: {} compiled, {} failed", compiled, failed);
    if compiled > 0 {
        println!("Compiled WASM modules will be loaded automatically on next 'neondb start'.");
    }
    if failed > 0 {
        return Err(neondb::error::NeonDBError::internal(format!(
            "{} module(s) failed to compile",
            failed
        )));
    }
    Ok(())
}

fn init_project(path: PathBuf) -> Result<()> {
    let project_path = fs::canonicalize(&path).unwrap_or(path);
    fs::create_dir_all(&project_path)?;
    let toml = r#"[project]
name = "neondb-sample"
version = "0.1.0"

[server]
host = "127.0.0.1"
port = 3000
"#;
    fs::write(project_path.join("neondb.toml"), toml)?;
    fs::write(
        project_path.join("README_INIT.md"),
        "Run `neondb start` from the project root to start the native NeonDB server.",
    )?;
    println!("Initialized NeonDB project at {}", project_path.display());
    Ok(())
}

async fn run_server(config: Config) -> Result<()> {
    let mut logger = env_logger::Builder::from_default_env();
    logger.filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info));
    let _ = logger.try_init();

    log::info!("Starting NeonDB Server");
    log::info!("Config: {:?}", config);

    // ── Table store (lock-free, DashMap-based) ────────────────────────────────
    let mut ts = TableStore::new();
    ts.set_shard(config.shard_id, config.shard_count);
    let tables = Arc::new(ts);

    // ── Reducer registry ──────────────────────────────────────────────────────
    let registry = Arc::new(ReducerRegistry::new()?);
    log::info!(
        "Reducer registry initialized. Available reducers: {:?}",
        registry.list_reducers()
    );

    // ── Snapshot + WAL recovery ─────────────────────────────────────────────
    //
    // Recovery order:
    //   1. Find the most-recent snapshot file in snapshot_dir.
    //   2. If found, load it (restores all rows + next_row_id) and record
    //      last_sequence so we can skip WAL entries already covered.
    //   3. Replay only WAL entries with sequence_number > snapshot.last_sequence.
    //   4. Initialise global_seq to (max_replayed_seq + 1) to prevent
    //      duplicate sequence numbers across restarts.
    let mut min_wal_seq: u64 = 0;
    let mut initial_seq: u64 = 0;

    let snap_dir = config.snapshot_dir.clone();
    if let Some((snap_path, snap_seq)) = find_latest_snapshot(&snap_dir) {
        log::info!("Loading snapshot: {:?} (seq {})", snap_path, snap_seq);
        match load_snapshot(&snap_path, &tables) {
            Ok(meta) => {
                min_wal_seq = meta.last_sequence;
                initial_seq = meta.last_sequence.saturating_add(1);
                log::info!(
                    "Snapshot loaded: {} rows, replaying WAL from seq > {}",
                    meta.row_count,
                    meta.last_sequence
                );
            }
            Err(e) => log::warn!("Failed to load snapshot: {} — replaying full WAL", e),
        }
    }

    log::info!("Recovering from WAL: {:?}", config.wal_path);
    if config.wal_path.exists() {
        match recover_from_wal(&config.wal_path, &tables, min_wal_seq) {
            Ok((n, max_seq)) => {
                log::info!("Recovered {} entries from WAL (last seq={})", n, max_seq);
                initial_seq = initial_seq.max(max_seq.saturating_add(1));
            }
            Err(e) => log::warn!("Failed to recover from WAL: {}", e),
        }
    } else {
        log::info!("WAL file does not exist, starting fresh");
    }

    // ── Schema migrations ─────────────────────────────────────────────────────
    let migrations_dir = std::path::PathBuf::from("migrations");
    match neondb::migrations::apply_migrations(&migrations_dir, &tables) {
        Ok(0) => log::debug!("No migrations to apply"),
        Ok(n) => log::info!("Applied {} migration file(s)", n),
        Err(e) => log::warn!("Migration error: {}", e),
    }

    // ── kanal async channel — replaces SegQueue + sleep(50ms) ────────────────
    let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();

    // ── Subscription manager (Arc, no Mutex — uses DashMap internally) ────────
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(
        config.two_frame_protocol,
    ));
    log::info!(
        "Subscription fan-out mode: {}",
        if config.two_frame_protocol {
            "two-frame (O(1) encode)"
        } else {
            "legacy (one encode per subscriber)"
        }
    );

    let active_connections = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // ── WebSocket listener ────────────────────────────────────────────────────
    // NOTE: tables is passed so the subscribe handler can deliver
    // initial_snapshot frames for all existing matching rows (TODO-003).
    let listener_handle = {
        let config_c = config.clone();
        let tx_c = reducer_tx.clone();
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone(); // <── TODO-003 fix
        let conns_c = active_connections.clone();
        let rx_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host,
                config_c.port,
                tx_c,
                subs_c,
                tables_c, // <── TODO-003 fix
                config_c.max_connections,
                config_c.api_key.clone(),
                conns_c,
                rx_shutdown,
            )
            .await
            {
                log::error!("Listener error: {}", e);
            }
        })
    };

    // ── Metrics server ────────────────────────────────────────────────────────
    let metrics_handle = {
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone();
        let mport = config.metrics_port;
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, rx_shutdown).await
            {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    // ── WAL writer ────────────────────────────────────────────────────────────
    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path,
        config.wal_batch_interval_ms,
        config.wal_batch_size,
        config.unsafe_no_fsync,
    )?);

    // ── Parallel reducer workers (one per logical CPU) ────────────────────────
    // Each worker owns a clone of the kanal receiver (MPMC), the Arc<TableStore>,
    // Arc<ReducerRegistry>, Arc<SubscriptionManager>, and Arc<BatchedWalWriter>.
    // They race to pull the next call; no coordination needed between workers.
    let worker_count = num_cpus::get().max(1);
    log::info!("Starting {} parallel reducer workers", worker_count);

    let timeout_ms = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w = config.snapshot_dir.clone();
    // global_seq starts after the last replayed WAL entry so new entries
    // never duplicate sequence numbers from a previous run.
    let global_seq = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone();
        let tables_w = tables.clone();
        let registry_w = registry.clone();
        let subs_w = subscription_manager.clone();
        let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone();
        let snap_interval_w = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone();

        let handle = tokio::spawn(async move {
            log::debug!("Reducer worker {} started", worker_id);
            loop {
                let call = match rx.recv().await {
                    Ok(c) => c,
                    Err(_) => break, // channel closed — graceful shutdown
                };

                let call_id = call.call_id;
                let tables_blk = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args = call.args.clone();
                let timestamp = current_timestamp_nanos();
                let call_caller_id = call.caller_id.clone();

                // Run the (potentially CPU-heavy) reducer on the blocking thread
                // pool so it doesn't starve the Tokio async runtime.
                let blk_result = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, timestamp);
                        ctx.caller_id = call_caller_id.clone();
                        let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            registry_blk.execute(&reducer_name, &mut ctx, &args)
                        }));
                        (exec, ctx)
                    }),
                )
                .await;

                let response = match blk_result {
                    Err(_timeout) => {
                        log::warn!("call_id={} timed out after {}ms", call_id, timeout_ms);
                        ReducerResponse::error(call_id, "Reducer timed out".to_string())
                    }
                    Ok(Err(join_err)) => {
                        log::error!("Blocking task join error: {}", join_err);
                        ReducerResponse::error(call_id, "Internal task error".to_string())
                    }
                    Ok(Ok((exec_result, mut ctx))) => match exec_result {
                        Ok(Ok(result_bytes)) => match ctx.commit() {
                            Ok(deltas) => {
                                let seq_num =
                                    seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let entry = WalEntry::new(
                                    timestamp,
                                    seq_num,
                                    call.reducer_name.clone(),
                                    call.args.clone(),
                                    deltas.clone(),
                                );
                                match wal_w.append(&entry, seq_num) {
                                    Err(e) => {
                                        log::error!("WAL append failed: {}", e);
                                        ReducerResponse::error(call_id, e.to_string())
                                    }
                                    Ok(_) => {
                                        subs_w.publish_deltas(&deltas);

                                        // Trigger a background snapshot every
                                        // `snapshot_interval` committed transactions.
                                        // fetch_add returns the PREVIOUS value, so
                                        // seq_num=0,1,...  The snapshot fires when
                                        // (seq_num + 1) is a multiple of the interval.
                                        if snap_interval_w > 0
                                            && (seq_num + 1) % snap_interval_w == 0
                                        {
                                            let tables_snap = tables_w.clone();
                                            let dir_snap = snap_dir_ww.clone();
                                            let ts_snap = current_timestamp_nanos();
                                            tokio::spawn(async move {
                                                let result =
                                                    tokio::task::spawn_blocking(move || {
                                                        save_snapshot(
                                                            &tables_snap,
                                                            &dir_snap,
                                                            seq_num,
                                                            ts_snap,
                                                        )
                                                    })
                                                    .await;
                                                match result {
                                                    Ok(Ok(())) => log::info!(
                                                        "Background snapshot written at seq {}",
                                                        seq_num
                                                    ),
                                                    Ok(Err(e)) => log::error!(
                                                        "Snapshot failed at seq {}: {}",
                                                        seq_num,
                                                        e
                                                    ),
                                                    Err(e) => log::error!(
                                                        "Snapshot task panicked at seq {}: {}",
                                                        seq_num,
                                                        e
                                                    ),
                                                }
                                            });
                                        }

                                        ReducerResponse::success(call_id, result_bytes)
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Commit failed call_id={}: {}", call_id, e);
                                ReducerResponse::error(call_id, e.to_string())
                            }
                        },
                        Ok(Err(e)) => {
                            log::warn!("Reducer exec failed call_id={}: {}", call_id, e);
                            ReducerResponse::error(call_id, e.to_string())
                        }
                        Err(_panic) => {
                            log::warn!("Reducer panicked call_id={}", call_id);
                            ReducerResponse::error(call_id, "Reducer panicked".to_string())
                        }
                    },
                };

                if let Err(e) = call.response_tx.send(response) {
                    log::warn!("Failed to send response to client: {}", e);
                }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        });
        worker_handles.push(handle);
    }

    // ── Scheduled reducer tasks ─────────────────────────────────────────
    // One lightweight async task per [[scheduler]] entry.  Each task ticks at
    // `interval_ms`, enqueues a PendingCall into the worker pool, and waits for
    // the result in a fire-and-forget inner task.  Shuts down when the watcher
    // fires so all scheduled reducers drain gracefully.
    let mut scheduler_handles = Vec::new();
    let sched_seq = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX / 2));

    for sched in &config.scheduled_reducers {
        let sched: ScheduledReducerConfig = sched.clone();
        let tx_sched = reducer_tx.clone();
        let seq_sched = sched_seq.clone();
        let mut rx_shutdown_sched = shutdown_rx.clone();

        // Pre-encode args: JSON string → MessagePack bytes.
        // Falls back to empty bytes if args_json is absent or unparseable.
        let args_bytes: Vec<u8> = sched
            .args_json
            .as_deref()
            .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
            .and_then(|v| rmp_serde::to_vec(&v).ok())
            .unwrap_or_default();

        log::info!(
            "Scheduler: '{}' every {}ms",
            sched.reducer,
            sched.interval_ms
        );

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(sched.interval_ms.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately — skip it so the first real fire
            // happens one full interval after startup.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let call_id = seq_sched.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<ReducerResponse>();
                        let call = PendingCall {
                            call_id,
                            reducer_name: sched.reducer.clone(),
                            args: args_bytes.clone(),
                            caller_id: "scheduler".to_string(),
                            response_tx: resp_tx,
                        };
                        if tx_sched.send(call).await.is_ok() {
                            // Await the result in a detached task so we don't block the tick.
                            let name_c = sched.reducer.clone();
                            tokio::spawn(async move {
                                if let Some(resp) = resp_rx.recv().await {
                                    if !resp.success {
                                        log::warn!(
                                            "Scheduled reducer '{}' (call_id={}) failed: {:?}",
                                            name_c, call_id, resp.error
                                        );
                                    } else {
                                        log::debug!("Scheduled reducer '{}' (call_id={}) ok", name_c, call_id);
                                    }
                                }
                            });
                        } else {
                            // Channel closed — workers are shutting down.
                            break;
                        }
                    }
                    _ = rx_shutdown_sched.changed() => break,
                }
            }
            log::debug!("Scheduler for '{}' stopped", sched.reducer);
        });
        scheduler_handles.push(handle);
    }

    // ── Wait for Ctrl-C ────────────────────────────────────────────────
    tokio::signal::ctrl_c().await.ok();
    log::info!("Shutdown signal received");

    // Broadcast shutdown so schedulers and the listener stop accepting work.
    let _ = shutdown_tx.send(());

    // Drop the sender so all workers drain remaining calls then exit.
    drop(reducer_tx);
    for h in worker_handles {
        let _ = h.await;
    }
    for h in scheduler_handles {
        let _ = h.await;
    }

    // Flush and close WAL.
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() {
            log::error!("Error shutting down WAL writer: {}", e);
        }
    }

    let _ = listener_handle.await;
    let _ = metrics_handle.await;
    log::info!("Shutdown complete");
    Ok(())
}

// ── Metrics server ────────────────────────────────────────────────────────────

async fn start_metrics_server(
    host: String,
    port: u16,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse().map_err(|e| {
        neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e))
    })?;

    let make_service = make_service_fn(move |_| {
        let subs = subscription_manager.clone();
        let tbl = tables.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone();
                let tbl = tbl.clone();
                async move { handle_metrics_request(req, subs, tbl).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);
    log::info!("Admin/metrics endpoint available on http://{}", addr);
    log::info!("  GET /metrics          Prometheus-style metrics");
    log::info!("  GET /healthz          Health check");
    log::info!("  GET /tables           List tables + row counts (JSON)");
    log::info!("  GET /tables/<name>    Dump all rows in a table (JSON)");

    server
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .map_err(|e| {
            neondb::error::NeonDBError::network_error(format!("Metrics server error: {}", e))
        })
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    let mut r = Response::new(Body::from(value.to_string()));
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}

async fn handle_metrics_request(
    req: Request<Body>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        (&Method::GET, "/metrics") => {
            let active_subscriptions = subscription_manager.active_subscriptions();
            let active_connections = subscription_manager.active_connections();
            let total_rows = tables.total_row_count();
            let uptime = current_timestamp_nanos();
            let body = format!(
                "# NeonDB metrics\n\
                 active_subscriptions {}\n\
                 active_connections {}\n\
                 total_rows {}\n\
                 uptime_nanos {}\n",
                active_subscriptions, active_connections, total_rows, uptime
            );
            Ok(Response::new(Body::from(body)))
        }

        (&Method::GET, "/healthz") => Ok(json_response(serde_json::json!({
            "status": "ok",
            "total_rows": tables.total_row_count(),
            "active_connections": subscription_manager.active_connections(),
        }))),

        // GET /tables — list all tables with their row counts.
        (&Method::GET, "/tables") => {
            let mut table_list = Vec::new();
            for name in tables.list_tables() {
                let count = tables
                    .list_rows_with_keys(&name)
                    .map(|r| r.len())
                    .unwrap_or(0);
                table_list.push(serde_json::json!({ "name": name, "rows": count }));
            }
            Ok(json_response(serde_json::json!({
                "tables": table_list,
                "total_rows": tables.total_row_count(),
            })))
        }

        // GET /tables/<name> — dump all rows of a single table.
        (&Method::GET, p) if p.starts_with("/tables/") => {
            let table_name = p.trim_start_matches("/tables/");
            match tables.list_rows_with_keys(table_name) {
                Ok(rows) => {
                    let row_objs: Vec<serde_json::Value> = rows
                        .into_iter()
                        .map(|(key, data)| serde_json::json!({ "row_key": key, "data": data }))
                        .collect();
                    Ok(json_response(serde_json::json!({
                        "table": table_name,
                        "count": row_objs.len(),
                        "rows": row_objs,
                    })))
                }
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        _ => {
            let mut r = Response::new(Body::from("Not Found"));
            *r.status_mut() = StatusCode::NOT_FOUND;
            Ok(r)
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn current_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Replay WAL entries from `wal_path` into `tables`.
///
/// Entries whose `sequence_number <= min_seq` are skipped — they are already
/// captured in the snapshot that was loaded before this call.
///
/// Returns `(replayed_count, max_sequence_number_seen)`.  The caller should
/// initialise `global_seq` to `max_seq + 1` to prevent duplicate sequence
/// numbers across restarts.
fn recover_from_wal(
    wal_path: &Path,
    tables: &Arc<TableStore>,
    min_seq: u64,
) -> Result<(usize, u64)> {
    let mut reader = WalReader::open(wal_path)?;
    let entries = reader.read_all_entries()?;
    let mut replayed = 0usize;
    let mut max_seq = min_seq;
    for entry in &entries {
        // Track the highest sequence number regardless of whether we replay.
        max_seq = max_seq.max(entry.header.sequence_number);

        // Skip entries already covered by the loaded snapshot.
        if entry.header.sequence_number <= min_seq {
            continue;
        }

        if !entry.verify_checksum() {
            log::warn!(
                "WAL entry {} has invalid checksum, skipping",
                entry.header.sequence_number
            );
            continue;
        }
        for delta in &entry.payload.deltas {
            tables.apply_delta(delta)?;
        }
        replayed += 1;
    }
    Ok((replayed, max_seq))
}
