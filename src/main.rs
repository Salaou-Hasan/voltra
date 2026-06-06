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
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use tokio::sync::watch;
use neondb::{
    config::Config,
    error::Result,
    network::{start_listener, PendingCall, ReducerResponse},
    reducer::{ReducerContext, ReducerRegistry},
    subscriptions::SubscriptionManager,
    table::TableStore,
    wal::{WalEntry, WalReader, BatchedWalWriter},
};

#[derive(Parser, Debug)]
#[command(name = "neondb")]
#[command(author, version, about = "NeonDB Phase 1 CLI and server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(value_name = "PATH", default_value = ".")]
        path: PathBuf,
    },
    Build {},
    Start {
        #[arg(short = 'a', long)]
        host: Option<String>,
        #[arg(short = 'p', long)]
        port: Option<u16>,
        #[arg(short = 'd', long)]
        data_dir: Option<PathBuf>,
        #[arg(long = "wal-path")]
        wal_path: Option<PathBuf>,
        #[arg(short = 'f', long)]
        fsync_interval_ms: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            init_project(path)?;
            Ok(())
        }
        Commands::Build {} => {
            println!("Native-only MVP: build is not supported for WASM/TypeScript modules.");
            Ok(())
        }
        Commands::Start {
            host,
            port,
            data_dir,
            wal_path,
            fsync_interval_ms,
        } => {
            let mut config = Config::from_env();
            if let Some(h) = host        { config.host = h; }
            if let Some(p) = port        { config.port = p; }
            if let Some(d) = data_dir    { config.wal_path = d.join("neondb.wal"); }
            if let Some(w) = wal_path    { config.wal_path = w; }
            if let Some(f) = fsync_interval_ms { config.fsync_interval_ms = f; }
            run_server(config).await
        }
    }
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

    // ── WAL recovery ──────────────────────────────────────────────────────────
    log::info!("Recovering from WAL: {:?}", config.wal_path);
    if config.wal_path.exists() {
        match recover_from_wal(&config.wal_path, &tables) {
            Ok(n)  => log::info!("Recovered {} entries from WAL", n),
            Err(e) => log::warn!("Failed to recover from WAL: {}", e),
        }
    } else {
        log::info!("WAL file does not exist, starting fresh");
    }

    // ── kanal async channel — replaces SegQueue + sleep(50ms) ────────────────
    let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();

    // ── Subscription manager (Arc, no Mutex — uses DashMap internally) ────────
    let subscription_manager = Arc::new(SubscriptionManager::new());

    let active_connections = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // ── WebSocket listener ────────────────────────────────────────────────────
    // NOTE: tables is passed so the subscribe handler can deliver
    // initial_snapshot frames for all existing matching rows (TODO-003).
    let listener_handle = {
        let config_c    = config.clone();
        let tx_c        = reducer_tx.clone();
        let subs_c      = subscription_manager.clone();
        let tables_c    = tables.clone();          // <── TODO-003 fix
        let conns_c     = active_connections.clone();
        let rx_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host,
                config_c.port,
                tx_c,
                subs_c,
                tables_c,                          // <── TODO-003 fix
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
        let subs_c      = subscription_manager.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c      = config.host.clone();
        let mport       = config.metrics_port;
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, rx_shutdown).await {
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

    let timeout_ms  = config.reducer_timeout_ms;
    // Shared monotonic sequence number across workers for WAL ordering.
    let global_seq  = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx         = reducer_rx.clone();
        let tables_w   = tables.clone();
        let registry_w = registry.clone();
        let subs_w     = subscription_manager.clone();
        let wal_w      = wal_writer.clone();
        let seq_w      = global_seq.clone();

        let handle = tokio::spawn(async move {
            log::debug!("Reducer worker {} started", worker_id);
            loop {
                let call = match rx.recv().await {
                    Ok(c)  => c,
                    Err(_) => break, // channel closed — graceful shutdown
                };

                let call_id    = call.call_id;
                let tables_blk = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args         = call.args.clone();
                let timestamp    = current_timestamp_nanos();

                // Run the (potentially CPU-heavy) reducer on the blocking thread
                // pool so it doesn't starve the Tokio async runtime.
                let blk_result = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, timestamp);
                        let exec = std::panic::catch_unwind(
                            std::panic::AssertUnwindSafe(|| {
                                registry_blk.execute(&reducer_name, &mut ctx, &args)
                            }),
                        );
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
                                let seq_num = seq_w.fetch_add(
                                    1,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
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

    // ── Wait for Ctrl-C ───────────────────────────────────────────────────────
    tokio::signal::ctrl_c().await.ok();
    log::info!("Shutdown signal received");

    // Drop the sender so all workers drain remaining calls then exit.
    drop(reducer_tx);
    for h in worker_handles {
        let _ = h.await;
    }

    // Flush and close WAL.
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() {
            log::error!("Error shutting down WAL writer: {}", e);
        }
    }

    let _ = shutdown_tx.send(());
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
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse().map_err(|e| {
        neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e))
    })?;

    let make_service = make_service_fn(move |_| {
        let subs = subscription_manager.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone();
                async move { handle_metrics_request(req, subs).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);
    log::info!("Metrics endpoint available on http://{}", addr);

    server
        .with_graceful_shutdown(async move { let _ = shutdown.changed().await; })
        .await
        .map_err(|e| {
            neondb::error::NeonDBError::network_error(format!("Metrics server error: {}", e))
        })
}

async fn handle_metrics_request(
    req: Request<Body>,
    subscription_manager: Arc<SubscriptionManager>,
) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let active_subscriptions = subscription_manager.active_subscriptions();
            let uptime = current_timestamp_nanos();
            let body = format!(
                "# NeonDB metrics\nactive_subscriptions {}\nuptime_nanos {}\n",
                active_subscriptions, uptime
            );
            Ok(Response::new(Body::from(body)))
        }
        (&Method::GET, "/healthz") => Ok(Response::new(Body::from("ok"))),
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

fn recover_from_wal(wal_path: &Path, tables: &Arc<TableStore>) -> Result<usize> {
    let mut reader = WalReader::open(wal_path)?;
    let entries    = reader.read_all_entries()?;
    for entry in &entries {
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
    }
    Ok(entries.len())
}
