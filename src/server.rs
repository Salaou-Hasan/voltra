// ============================================================================
// server.rs — Public library entry point for embedded NeonDB projects
//
// Enables users to write a custom binary that embeds NeonDB as a library:
//
//   ```rust
//   // src/main.rs
//   mod reducers;  // loads #[reducer] fns into inventory
//
//   #[tokio::main]
//   async fn main() {
//       let config = neondb::config::Config::from_env();
//       neondb::run_server(config).await.expect("NeonDB server failed");
//   }
//   ```
//
// All #[reducer]-annotated functions in the calling crate are discovered
// automatically at link time via the `inventory` crate — no registration
// boilerplate needed.
// ============================================================================

use crate::auth::{AuthValidator, IdentityIssuer};
use crate::config::Config;
use crate::error::Result;
use crate::metrics::Metrics;
use crate::network::{PendingCall, RateLimiterConfig, RateLimiterRegistry, ReducerResponse};
use crate::persistence::PersistenceEngine;
use crate::presence::PresenceManager;
use crate::reducer::{ReducerContext, ReducerRegistry};
use crate::subscriptions::SubscriptionManager;
use crate::table::TableStore;
use crate::ttl::TtlManager;
use crate::wal::{
    snapshot::{find_latest_snapshot, load_snapshot},
    BatchedWalWriter, WalEntry, WalReader,
};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

const ADMIN_DASHBOARD_HTML: &str = include_str!("admin_dashboard.html");

/// Live handles to a running embedded NeonDB server.
///
/// Returned by [`run_server_with_handle`] after the server finishes bootstrapping.
/// All fields are cheaply cloneable `Arc`s — safe to share across threads/tasks.
pub struct ServerHandle {
    /// Read-only access to all in-memory tables (row counts, row data).
    pub tables:        Arc<TableStore>,
    /// Subscription manager — exposes `active_connections()`.
    pub subs:          Arc<SubscriptionManager>,
    /// Shared WAL byte counter — updated after every flush.
    pub wal_file_size: Arc<AtomicU64>,
}

#[inline]
fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Start an embedded NeonDB server and return a [`ServerHandle`] once the
/// server has finished bootstrapping (snapshot + WAL replay + listener bound).
///
/// The server runs as a background Tokio task; the returned future resolves
/// immediately once the listener is ready.  Use the handle to sample live stats
/// (rows, WAL size, connections) without an HTTP round-trip.
///
/// # Example
/// ```rust,ignore
/// let (handle, server_fut) = neondb::run_server_with_handle(config).await?;
/// tokio::spawn(server_fut);
/// // handle.tables.total_row_count(), etc.
/// ```
pub async fn run_server_with_handle(config: Config)
    -> Result<(ServerHandle, impl std::future::Future<Output = Result<()>>)>
{
    use tokio::sync::oneshot;
    let (tx, rx) = oneshot::channel::<ServerHandle>();
    let fut = run_server_inner(config, Some(tx));
    // Drive the future just far enough to reach the "listener ready" point,
    // then hand the remaining future back to the caller to spawn.
    // We do this by spawning run_server_inner and waiting for the oneshot.
    let handle_task = tokio::spawn(fut);
    let handle = rx.await.map_err(|_| {
        crate::error::NeonDBError::Internal("Server startup failed before sending handle".into())
    })?;
    // Wrap the task back into a plain future the caller can await/drop.
    let server_fut = async move {
        handle_task.await
            .map_err(|e| crate::error::NeonDBError::Internal(format!("Server task panicked: {e}")))?
    };
    Ok((handle, server_fut))
}

/// Start a NeonDB server with the given configuration.
///
/// All `#[reducer]`-annotated functions in the calling binary are discovered
/// automatically via the `inventory` crate.  Call this from `main()` in your
/// embedded NeonDB project.
///
/// # Example
/// ```rust,ignore
/// mod reducers; // loads your #[reducer] functions
///
/// #[tokio::main]
/// async fn main() {
///     neondb::run_server(neondb::config::Config::from_env())
///         .await
///         .expect("server failed");
/// }
/// ```
pub async fn run_server(config: Config) -> Result<()> {
    run_server_inner(config, None).await
}

/// Blocking entry point for embedded game servers.
/// Creates a Tokio runtime internally — callers do not need to add tokio as a dep.
pub fn run_server_blocking(config: Config) -> Result<()> {
    tokio::runtime::Runtime::new()
        .map_err(|e| crate::error::NeonDBError::internal(format!("Tokio runtime: {}", e)))?
        .block_on(run_server(config))
}

async fn run_server_inner(
    config: Config,
    handle_tx: Option<tokio::sync::oneshot::Sender<ServerHandle>>,
) -> Result<()> {
    // ── Graceful shutdown signal ──────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Ctrl-C / SIGTERM → broadcast shutdown
    {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            log::info!("[neondb] Shutdown signal received.");
            let _ = tx.send(());
        });
    }

    // ── Data structures ───────────────────────────────────────────────────────
    let wal_path   = config.wal_path.clone();

    // Apply eviction policy from config so embedded callers (sim, tests) can
    // configure LRU bounds without going through the main CLI binary.
    let eviction_policy = match config.eviction.policy.trim().to_ascii_lowercase().as_str() {
        "lru_row_cap" => crate::table::EvictionPolicy::LruRowCap {
            max_rows_per_table: config.eviction.max_rows_per_table.max(1),
        },
        "lru_byte_cap" => crate::table::EvictionPolicy::LruByteCap {
            max_bytes_total: config.eviction.max_bytes_total.max(1),
        },
        _ => crate::table::EvictionPolicy::None,
    };
    let tables     = Arc::new(crate::table::TableStore::with_eviction(eviction_policy));
    let schema_reg = Arc::new(crate::schema::SchemaRegistry::new());

    let mut initial_seq = 0u64;

    // ── Disk persistence (redb) ───────────────────────────────────────────────
    //
    // If NEONDB_PERSISTENCE_PATH is set we open the redb store and restore all
    // rows BEFORE doing snapshot / WAL replay.  When redb has data we advance
    // initial_seq so WAL replay only applies entries that arrived after the
    // last redb commit, avoiding redundant replays.
    let persistence: Option<Arc<PersistenceEngine>> = match &config.persistence_path {
        Some(path) => {
            match PersistenceEngine::open(path) {
                Ok(pe) => {
                    match pe.load_all(&*tables) {
                        Ok((rows, last_seq)) => {
                            if rows > 0 {
                                initial_seq = last_seq;
                                log::info!(
                                    "[neondb] Loaded {} rows from disk store (last_seq={})",
                                    rows,
                                    last_seq
                                );
                            } else {
                                log::info!(
                                    "[neondb] Disk store is empty; will bootstrap from snapshot+WAL"
                                );
                            }
                            Some(Arc::new(pe))
                        }
                        Err(e) => {
                            log::warn!(
                                "[neondb] Disk store load failed ({}); falling back to snapshot+WAL",
                                e
                            );
                            None
                        }
                    }
                }
                Err(e) => {
                    log::warn!("[neondb] Could not open disk store at {:?}: {}", path, e);
                    None
                }
            }
        }
        None => None,
    };

    // ── WAL + snapshot bootstrap ──────────────────────────────────────────────
    //
    // Only run snapshot+WAL loading when we don't have authoritative redb data.
    let loaded_from_redb = initial_seq > 0;
    if !loaded_from_redb {
        if let Some((snap_path, snap_seq)) = find_latest_snapshot(&wal_path) {
            load_snapshot(&snap_path, &*tables)?;
            initial_seq = snap_seq;
            log::info!("[neondb] Loaded snapshot seq={}", snap_seq);
        }
    }

    let wal_file = wal_path.with_extension("wal");
    if wal_file.exists() {
        let mut reader = WalReader::open(&wal_file)?;
        let entries = reader.read_all_entries()?;
        let mut replayed = 0usize;
        for entry in &entries {
            if entry.header.sequence_number <= initial_seq {
                continue; // already captured by the snapshot or redb
            }
            if !entry.verify_checksum() {
                log::warn!(
                    "[neondb] WAL entry {} bad checksum, skipping",
                    entry.header.sequence_number
                );
                continue;
            }
            for delta in &entry.payload.deltas {
                tables.apply_delta(delta)?;
            }
            initial_seq = initial_seq.max(entry.header.sequence_number);
            replayed += 1;
        }
        log::info!("[neondb] WAL replay complete ({} entries applied).", replayed);
    }

    let wal_writer = Arc::new(
        BatchedWalWriter::open(
            &wal_file,
            config.fsync_interval_ms,
            config.wal_batch_size,
            false, // safe fsync
        )?
    );

    // Global WAL sequence counter — shared across all worker threads.
    let global_seq = Arc::new(AtomicU64::new(initial_seq));

    // ── Reducer registry (discovers #[reducer] functions automatically) ───────
    let registry = Arc::new(ReducerRegistry::new()?);

    // ── Support services ──────────────────────────────────────────────────────
    let subs               = Arc::new(SubscriptionManager::new());
    subs.start_tick_flusher(config.sub_tick_ms);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let metrics            = Arc::new(Metrics::new());

    // Ed25519 identity issuer — load persisted key or generate a fresh one.
    let identity_key_path  = wal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("identity_key.pem");
    let identity_issuer: Arc<IdentityIssuer> = if identity_key_path.exists() {
        match IdentityIssuer::load_from_file(&identity_key_path) {
            Ok(iss) => { log::info!("[identity] Loaded key (kid={})", iss.kid); Arc::new(iss) }
            Err(e)  => {
                log::warn!("[identity] Load failed ({}), generating new key", e);
                let iss = IdentityIssuer::generate();
                let _ = iss.save_to_file(&identity_key_path);
                Arc::new(iss)
            }
        }
    } else {
        let iss = IdentityIssuer::generate();
        if let Err(e) = iss.save_to_file(&identity_key_path) {
            log::warn!("[identity] Could not persist key: {}", e);
        }
        log::info!("[identity] Generated new Ed25519 key (kid={})", iss.kid);
        Arc::new(iss)
    };

    let api_key            = config.api_key.clone();
    let auth_validator     = Arc::new(AuthValidator::from_env());
    let permissions        = Arc::new(config.permissions.clone());
    let rate_limiter       = Arc::new(RateLimiterRegistry::new(RateLimiterConfig {
        capacity:    config.rate_limit_capacity,
        refill_rate: config.rate_limit_refill_rate,
        enabled:     config.rate_limit_capacity > 0,
    }));
    let presence           = Arc::new(PresenceManager::new(
        config.presence_heartbeat_timeout_ms,
        config.presence_offline_timeout_ms,
    ));
    let ttl_manager        = Arc::new(TtlManager::new());
    let tls_config         = None; // set via [tls] TOML section if needed

    // ── Bounded reducer queue ─────────────────────────────────────────────────
    let queue_cap = config.reducer_queue_cap;
    let (tx, rx)  = kanal::bounded_async::<PendingCall>(queue_cap);

    // ── Worker pool ───────────────────────────────────────────────────────────
    let worker_count = if config.workers > 0 { config.workers } else { num_cpus::get().max(2) };

    for _worker_id in 0..worker_count {
        let rx_w        = rx.clone();
        let tables_w    = tables.clone();
        let subs_w      = subs.clone();
        let registry_w  = registry.clone();
        let wal_w       = wal_writer.clone();
        let seq_w       = global_seq.clone();
        let metrics_w   = metrics.clone();
        let schema_w    = schema_reg.clone();
        let ttl_w       = ttl_manager.clone();
        let persist_w   = persistence.clone();
        let mut shut_w  = shutdown_rx.clone();

        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            // Maximum extra calls to drain per wakeup before going back to sleep.
            // Amortises OS thread wakeup cost at high CCU: one wakeup processes up
            // to BATCH+1 calls instead of 1.  Pure-computation reducers (no DB
            // writes) are safe to batch; JS/WASM reducers benefit too because each
            // one is still executed serially on this thread.
            const DRAIN_LIMIT: usize = 15;

            loop {
                // Block until a call arrives or shutdown fires.
                let call: PendingCall = match rt.block_on(async {
                    tokio::select! {
                        c = rx_w.recv() => c.ok(),
                        _ = shut_w.changed() => None,
                    }
                }) {
                    Some(c) => c,
                    None    => break,
                };

                // Build a micro-batch: drain up to DRAIN_LIMIT more calls that are
                // already queued (non-blocking).  If the channel is empty we fall
                // through immediately with a batch of 1.
                let mut batch: smallvec::SmallVec<[PendingCall; 16]> =
                    smallvec::smallvec![call];
                for _ in 0..DRAIN_LIMIT {
                    // Zero-duration timeout = "give me one if available, otherwise skip"
                    match rt.block_on(tokio::time::timeout(
                        std::time::Duration::ZERO,
                        rx_w.recv(),
                    )) {
                        Ok(Ok(extra)) => batch.push(extra),
                        _ => break,
                    }
                }

                // ── Execute every call in the batch serially ──────────────────
                for call in batch {

                let call_id = call.call_id;

                // Replicas are read-only: reject reducer calls until promoted.
                if crate::replication::is_replica() {
                    let resp = ReducerResponse::error(
                        call_id,
                        "This node is a read-only replica.".to_string(),
                    );
                    if let Err(e) = call.response_tx.send(resp) {
                        log::warn!("[neondb] Response delivery failed: {}", e);
                    }
                    continue;
                }

                let ts      = now_nanos();

                // Build execution context with schema validation + TTL support.
                let mut ctx = ReducerContext::new(tables_w.clone(), ts)
                    .with_schema(schema_w.clone())
                    .with_ttl(ttl_w.clone());
                ctx.caller_id   = call.caller_id.clone();
                ctx.caller_role = call.caller_role.clone();

                // Execute with OCC conflict retry: if a concurrent worker
                // committed a row this reducer read AND writes, the commit
                // aborts and we re-execute against fresh state. This is what
                // makes read-modify-write reducers lose zero updates. Heavy
                // game simulations can legitimately race the same hot row many
                // times, so retry generously before surfacing a conflict.
                const MAX_CONFLICT_RETRIES: usize = 64;
                let mut attempt = 0;
                let response = loop {
                    attempt += 1;
                    let exec = registry_w.execute(&call.reducer_name, &mut ctx, &call.args);

                    break match exec {
                    Err(e) => {
                        ctx.rollback();
                        metrics_w.reducer_errors_total.inc();
                        ReducerResponse::error(call_id, e.to_string())
                    }
                    Ok(result_bytes) => {
                        // Commit staged writes atomically.
                        match ctx.commit() {
                            Err(crate::error::NeonDBError::TxnConflict(_))
                                if attempt < MAX_CONFLICT_RETRIES =>
                            {
                                ctx.reset_for_retry();
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => {
                                log::error!(
                                    "[neondb] Commit failed for '{}': {}",
                                    call.reducer_name, e
                                );
                                metrics_w.reducer_errors_total.inc();
                                ReducerResponse::error(call_id, format!("Commit error: {}", e))
                            }
                            Ok(deltas) => {
                                // Fan out live subscription updates — O(1) per subscriber.
                                if !deltas.is_empty() {
                                    subs_w.publish_deltas(&deltas);
                                }

                                // Append to WAL for crash recovery.
                                let seq_num = seq_w.fetch_add(1, Ordering::Relaxed);
                                let entry = WalEntry::new(
                                    ts,
                                    seq_num,
                                    call.reducer_name.clone(),
                                    call.args.clone(),
                                    deltas.clone(),
                                );
                                if let Err(e) = wal_w.append(&entry, seq_num) {
                                    log::warn!("[neondb] WAL append failed: {}", e);
                                } else {
                                    metrics_w.wal_entries_written_total.inc();
                                }

                                // Write-through to disk store (non-fatal on failure).
                                if let Some(ref pe) = persist_w {
                                    if !deltas.is_empty() {
                                        if let Err(e) = pe.persist_deltas(&deltas, seq_num) {
                                            log::warn!("[neondb] Disk persist failed: {}", e);
                                        }
                                    }
                                }

                                metrics_w.reducer_calls_total.inc();
                                ReducerResponse::success(call_id, result_bytes)
                            }
                        }
                    }
                    };
                };

                // Deliver response back to the waiting WebSocket handler.
                if let Err(e) = call.response_tx.send(response) {
                    log::warn!("[neondb] Response delivery failed: {}", e);
                }
                } // end for call in batch
            }
        });
    }

    // ── WebSocket listener ────────────────────────────────────────────────────
    let host      = config.host.clone();
    let port      = config.port;
    let max_conns = config.max_connections;

    log::info!("[neondb] Listening on {}:{}", host, port);

    // ── Replication: replica mode + optional auto-failover ────────────────────
    if config.role.eq_ignore_ascii_case("replica") {
        match config.primary_url.clone() {
            Some(primary) => {
                crate::replication::set_replica(true);
                crate::replication::init_replica_from_local_wal(initial_seq.saturating_sub(1));
                let tables_r = tables.clone();
                let subs_r   = subs.clone();
                let wal_r    = wal_writer.clone();
                let seq_r    = global_seq.clone();
                let poll     = config.replica_poll_ms;
                let af       = config.auto_failover;
                let mc       = config.failover_miss_count;
                let shut_r   = shutdown_rx.clone();
                tokio::spawn(async move {
                    crate::replication::run_replica_loop(
                        primary, tables_r, subs_r, wal_r, seq_r, poll, af, mc, shut_r,
                    ).await;
                });
                log::info!("[replication] Started in REPLICA mode (read-only)");
                println!("[neondb] Replica mode — pulling from {}", config.primary_url.as_deref().unwrap_or(""));
            }
            None => log::error!(
                "[replication] NEONDB_ROLE=replica but NEONDB_PRIMARY_URL is not set — staying primary"
            ),
        }
    }

    // ── Embedded admin/metrics HTTP server ────────────────────────────────────
    {
        let metrics_port = config.metrics_port;
        if metrics_port > 0 {
            let startup = std::time::Instant::now();
            start_embedded_admin_server(
                host.clone(),
                metrics_port,
                tables.clone(),
                subs.clone(),
                registry.clone(),
                schema_reg.clone(),
                wal_writer.clone(),
                global_seq.clone(),
                metrics.clone(),
                startup,
                tx.clone(),
                api_key.clone(),
                config.backup_dir.clone(),
                config.backup_keep,
                wal_file.clone(),
                shutdown_rx.clone(),
            );
        }
    }

    // Send stats handle back to the caller (e.g. neondb-sim) before blocking.
    if let Some(tx) = handle_tx {
        let _ = tx.send(ServerHandle {
            tables:        tables.clone(),
            subs:          subs.clone(),
            wal_file_size: wal_writer.file_size_arc(),
        });
    }

    // ── Redis + PostgreSQL protocol listeners (MVCC engine) ──────────────────
    spawn_protocol_listeners(&config);

    let tenant_registry = crate::tenant::TenantRegistry::load(tables.clone());
    let inline_registry = crate::network::build_inline_registry();
    crate::network::start_listener(
        host,
        port,
        tx,
        subs,
        tables,
        max_conns,
        api_key,
        active_connections,
        permissions,
        config.sql_timeout_ms,
        auth_validator,
        rate_limiter,
        presence,
        ttl_manager,
        identity_issuer,
        shutdown_rx,
        metrics,
        tls_config,
        tenant_registry,
        inline_registry,
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .await
}

// ── Embedded admin HTTP server ────────────────────────────────────────────────
//
// Provides the same /admin dashboard, /healthz, /metrics, and /admin/api/*
// endpoints that the NeonDB CLI binary exposes, so scaffold projects using
// run_server_blocking() get a fully-featured admin console at
// http://127.0.0.1:<metrics_port>/admin — not just a blank connection refusal.

fn admin_json(v: serde_json::Value) -> Response<Body> {
    let mut r = Response::new(Body::from(v.to_string()));
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}
fn admin_bad_request(msg: String) -> Response<Body> {
    let mut r = admin_json(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::BAD_REQUEST;
    r
}
fn admin_server_error(msg: String) -> Response<Body> {
    let mut r = admin_json(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    r
}
fn admin_url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i+1..i+3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Best-effort resident memory of the current process, in bytes.
/// Windows uses GetProcessMemoryInfo; Linux reads /proc/self/statm; other
/// platforms return 0 (the dashboard simply shows 0 MB).
fn embedded_memory_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        #[allow(non_camel_case_types)] type HANDLE = *mut std::ffi::c_void;
        #[allow(non_camel_case_types)] type DWORD = u32;
        #[allow(non_camel_case_types)] type SIZE_T = usize;
        #[repr(C)]
        struct PROCESS_MEMORY_COUNTERS {
            cb: DWORD, page_fault_count: DWORD,
            peak_working_set_size: SIZE_T, working_set_size: SIZE_T,
            quota_peak_paged_pool_usage: SIZE_T, quota_paged_pool_usage: SIZE_T,
            quota_peak_non_paged_pool_usage: SIZE_T, quota_non_paged_pool_usage: SIZE_T,
            pagefile_usage: SIZE_T, peak_pagefile_usage: SIZE_T,
        }
        #[link(name = "kernel32")]
        extern "system" { fn GetCurrentProcess() -> HANDLE; }
        #[link(name = "psapi")]
        extern "system" {
            fn GetProcessMemoryInfo(p: HANDLE, c: *mut PROCESS_MEMORY_COUNTERS, cb: DWORD) -> i32;
        }
        unsafe {
            let mut c: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
            c.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as DWORD;
            if GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) != 0 {
                return c.working_set_size as u64;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()) {
                return pages * 4096;
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    { 0 }
}

#[allow(clippy::too_many_arguments)]
async fn handle_embedded_admin(
    req: Request<Body>,
    tables: Arc<TableStore>,
    subs: Arc<SubscriptionManager>,
    registry: Arc<ReducerRegistry>,
    schema_reg: Arc<crate::schema::SchemaRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    startup: std::time::Instant,
    queue_tx: kanal::AsyncSender<PendingCall>,
    api_key: Option<String>,
    backup_dir: Option<std::path::PathBuf>,
    backup_keep: usize,
    wal_path: std::path::PathBuf,
) -> std::result::Result<Response<Body>, hyper::Error> {
    // Optional auth check — only if NEONDB_API_KEY is set.
    let check_auth = |req: &Request<Body>| -> Option<Response<Body>> {
        let Some(ref key) = api_key else { return None };
        let ok = req.headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_start_matches("Bearer ").trim() == key.as_str())
            .unwrap_or(false);
        if !ok {
            let mut r = admin_json(serde_json::json!({ "error": "Unauthorized" }));
            *r.status_mut() = StatusCode::UNAUTHORIZED;
            Some(r)
        } else {
            None
        }
    };

    let path = req.uri().path().to_string();
    let resp = match (req.method(), path.as_str()) {

        // ── Admin dashboard ───────────────────────────────────────────────────
        (&Method::GET, "/admin") | (&Method::GET, "/admin/") => {
            let mut r = Response::new(Body::from(ADMIN_DASHBOARD_HTML));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/html; charset=utf-8"),
            );
            r
        }

        // ── Health check ──────────────────────────────────────────────────────
        (&Method::GET, "/healthz") => {
            admin_json(serde_json::json!({
                "status": "ok",
                "role": if crate::replication::is_replica() { "replica" } else { "primary" },
                "replication_lag_entries": crate::replication::replication_lag(),
                "total_rows": tables.total_row_count(),
                "active_connections": subs.active_connections(),
                "active_subscriptions": subs.active_subscriptions(),
                "wal_sequence": global_seq.load(Ordering::Relaxed),
                "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
                "uptime_seconds": startup.elapsed().as_secs(),
                "reducer_queue_depth": queue_tx.len(),
                "memory_usage_bytes": embedded_memory_bytes(),
                "presence_tracked": 0u64,
                "ttl_active": 0u64,
            }))
        }

        // ── Prometheus metrics ────────────────────────────────────────────────
        (&Method::GET, "/metrics") => {
            let text = metrics.render();
            let mut r = Response::new(Body::from(text));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(
                    "text/plain; version=0.0.4; charset=utf-8"
                ),
            );
            r
        }

        // ── Table stats ───────────────────────────────────────────────────────
        (&Method::GET, "/stats") => {
            let table_list: Vec<_> = tables.list_tables().into_iter().map(|name| {
                let count = tables.list_rows_with_keys(&name).map(|r| r.len()).unwrap_or(0);
                serde_json::json!({ "name": name, "rows": count })
            }).collect();
            admin_json(serde_json::json!({
                "tables": table_list,
                "total_rows": tables.total_row_count(),
                "wal_sequence": global_seq.load(Ordering::Relaxed),
                "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
            }))
        }

        // ── Schema (table map: { name: { columns, rows, rls } }) ─────────────
        (&Method::GET, "/schema") | (&Method::GET, "/admin/api/schema") => {
            let mut table_map = serde_json::Map::new();
            for table_name in schema_reg.list_tables() {
                if let Some(schema) = schema_reg.get(table_name) {
                    let cols: Vec<_> = schema.columns.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "type": c.type_str,
                        "required": c.required,
                        "default": c.default,
                        "key": schema.primary_key.as_deref() == Some(&c.name),
                    })).collect();
                    let rows = tables.list_rows_with_keys(table_name).map(|r| r.len()).unwrap_or(0);
                    table_map.insert(table_name.to_string(), serde_json::json!({
                        "columns": cols,
                        "primary_key": schema.primary_key,
                        "rls": format!("{:?}", schema.rls),
                        "rows": rows,
                    }));
                }
            }
            for table_name in tables.list_tables() {
                if !table_map.contains_key(&table_name) {
                    let rows = tables.list_rows_with_keys(&table_name).map(|r| r.len()).unwrap_or(0);
                    table_map.insert(table_name, serde_json::json!({ "columns": [], "rows": rows }));
                }
            }
            admin_json(serde_json::json!({
                "tables": serde_json::Value::Object(table_map),
                "reducers": registry.list_reducers(),
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }

        // ── Table list ────────────────────────────────────────────────────────
        (&Method::GET, "/tables") => {
            let list: Vec<_> = tables.list_tables().into_iter().map(|name| {
                let count = tables.list_rows_with_keys(&name).map(|r| r.len()).unwrap_or(0);
                serde_json::json!({ "name": name, "rows": count })
            }).collect();
            admin_json(serde_json::json!({ "tables": list, "total_rows": tables.total_row_count() }))
        }

        // ── Rows of one table ({ row_key, data }) ─────────────────────────────
        (&Method::GET, p) if p.starts_with("/tables/") => {
            let table_name = admin_url_decode(p.trim_start_matches("/tables/"));
            match tables.list_rows_with_keys(&table_name) {
                Ok(rows) => {
                    let row_objs: Vec<_> = rows.into_iter()
                        .map(|(key, data)| serde_json::json!({ "row_key": key, "data": data }))
                        .collect();
                    admin_json(serde_json::json!({
                        "table": table_name, "count": row_objs.len(), "rows": row_objs,
                    }))
                }
                Err(e) => admin_server_error(e.to_string()),
            }
        }

        // ── Replication: primary serves WAL entries to replicas ──────────────
        (&Method::GET, "/replication/wal") => {
            let query = req.uri().query().unwrap_or("");
            let mut from_seq = 0u64;
            let mut max = 2048usize;
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("from_seq"), Some(v)) => from_seq = v.parse().unwrap_or(0),
                    (Some("max"), Some(v))      => max = v.parse::<usize>().unwrap_or(2048).clamp(1, 8192),
                    _ => {}
                }
            }
            let wal = wal_path.clone();
            let result = tokio::task::spawn_blocking(move || {
                crate::replication::serve_wal_entries(&wal, from_seq, max)
            }).await;
            match result {
                Ok(Ok((entries, last_seq))) => admin_json(serde_json::json!({
                    "entries": crate::replication::encode_entries(&entries),
                    "last_seq": last_seq,
                })),
                Ok(Err(e)) => admin_server_error(e.to_string()),
                Err(e)     => admin_server_error(format!("task: {}", e)),
            }
        }

        // ── Replication status / promote ──────────────────────────────────────
        (&Method::GET, "/replication/status") => {
            admin_json(crate::replication::status_json())
        }
        (&Method::POST, "/replication/promote") => {
            let was_replica = crate::replication::is_replica();
            crate::replication::set_replica(false);
            if was_replica {
                log::warn!("[replication] PROMOTED to primary via /replication/promote");
            }
            admin_json(serde_json::json!({
                "promoted": was_replica,
                "role": "primary",
                "last_applied_seq": crate::replication::last_applied_seq(),
            }))
        }

        // ── Backup now ────────────────────────────────────────────────────────
        (&Method::POST, "/backup") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let Some(dir) = backup_dir.clone() else {
                return Ok(admin_bad_request(
                    "No backup directory configured. Set NEONDB_BACKUP_DIR.".into()
                ));
            };
            let tbl = tables.clone();
            let wal = wal_path.clone();
            let keep = backup_keep;
            let last_seq = global_seq.load(Ordering::Relaxed);
            let result = tokio::task::spawn_blocking(move || {
                let path = crate::backup::backup_now(&tbl, &wal, &dir, last_seq)?;
                let _ = crate::backup::rotate_backups(&dir, keep);
                Ok::<_, crate::error::NeonDBError>(path)
            }).await;
            match result {
                Ok(Ok(path)) => {
                    let meta = crate::backup::read_meta(&path);
                    admin_json(serde_json::json!({
                        "path": path.to_string_lossy(),
                        "last_seq": last_seq,
                        "row_count": meta.map(|m| m.row_count).unwrap_or(0),
                    }))
                }
                Ok(Err(e)) => admin_server_error(e.to_string()),
                Err(e)     => admin_server_error(format!("task: {}", e)),
            }
        }

        // ── Apply migration(s) ────────────────────────────────────────────────
        (&Method::POST, "/migrate") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b, Err(e) => return Ok(admin_server_error(e.to_string())),
            };
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v, Err(e) => return Ok(admin_bad_request(format!("Invalid JSON: {}", e))),
            };
            let mig_arr = match payload.get("migrations").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => return Ok(admin_bad_request("Expected {\"migrations\": [...]}".into())),
            };
            let mut applied = 0usize; let mut skipped = 0usize; let mut errors: Vec<String> = Vec::new();
            for entry in &mig_arr {
                let filename = match entry.get("filename").and_then(|v| v.as_str()) {
                    Some(f) => f.to_string(),
                    None => { errors.push("missing filename field".into()); skipped += 1; continue; }
                };
                let content = match entry.get("content").and_then(|v| v.as_str()) {
                    Some(c) => c.to_string(),
                    None => { errors.push(format!("{}: missing content field", filename)); skipped += 1; continue; }
                };
                match crate::migrations::apply_migration_str(&filename, &content, &tables) {
                    Ok(true)  => applied += 1,
                    Ok(false) => skipped += 1,
                    Err(e)    => { errors.push(format!("{}: {}", filename, e)); skipped += 1; }
                }
            }
            let mut body = serde_json::json!({ "applied": applied, "skipped": skipped });
            if !errors.is_empty() {
                body["errors"] = serde_json::Value::Array(
                    errors.into_iter().map(serde_json::Value::String).collect());
            }
            admin_json(body)
        }

        // ── Invoke reducer ────────────────────────────────────────────────────
        (&Method::POST, "/admin/api/call") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b, Err(e) => return Ok(admin_server_error(e.to_string())),
            };
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v, Err(e) => return Ok(admin_bad_request(format!("Invalid JSON: {}", e))),
            };
            let name = match payload.get("name").and_then(|v| v.as_str()) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => return Ok(admin_bad_request("Missing 'name' field".into())),
            };
            let args_val = payload.get("args").cloned().unwrap_or(serde_json::json!([]));
            let args_bytes = match rmp_serde::to_vec(&args_val) {
                Ok(b) => b, Err(e) => return Ok(admin_bad_request(format!("Args encode: {}", e))),
            };
            let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let call = PendingCall {
                call_id: 0,
                reducer_name: name,
                args: args_bytes,
                caller_id: "admin-console".to_string(),
                caller_role: "admin".to_string(),
                tenant_id: None,
                lobby_hint: None,
                response_tx: resp_tx,
            };
            if queue_tx.send(call).await.is_err() {
                return Ok(admin_server_error("Reducer queue closed".into()));
            }
            match tokio::time::timeout(std::time::Duration::from_secs(30), resp_rx.recv()).await {
                Ok(Some(resp)) => {
                    let result_json: serde_json::Value = resp.result.as_deref()
                        .and_then(|b| rmp_serde::from_slice(b).ok())
                        .unwrap_or(serde_json::Value::Null);
                    admin_json(serde_json::json!({
                        "success": resp.success,
                        "result": result_json,
                        "error": resp.error,
                    }))
                }
                Ok(None) => admin_server_error("Worker dropped response channel".into()),
                Err(_)   => admin_server_error("Reducer call timed out after 30s".into()),
            }
        }

        // ── SQL query ─────────────────────────────────────────────────────────
        (&Method::POST, "/admin/api/sql") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b, Err(e) => return Ok(admin_server_error(e.to_string())),
            };
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v, Err(e) => return Ok(admin_bad_request(format!("Invalid JSON: {}", e))),
            };
            let query = match payload.get("query").and_then(|v| v.as_str()) {
                Some(q) if !q.trim().is_empty() => q.to_string(),
                _ => return Ok(admin_bad_request("Missing 'query' field".into())),
            };
            let tbl = tables.clone();
            let result = tokio::task::spawn_blocking(move || -> std::result::Result<_, String> {
                let stmt = crate::sql::parser::parse(&query)
                    .map_err(|e| format!("Parse error: {}", e))?;
                let exec = crate::SqlExecutor::new(tbl);
                exec.execute_statement(&stmt).map_err(|e| format!("Execution error: {}", e))
            }).await;
            match result {
                Ok(Ok(res)) => {
                    let rows: Vec<serde_json::Value> =
                        res.rows.into_iter().map(serde_json::Value::Object).collect();
                    admin_json(serde_json::json!({
                        "columns": res.columns,
                        "rows": rows,
                        "rows_affected": res.rows_affected,
                    }))
                }
                Ok(Err(e)) => admin_bad_request(e),
                Err(e) => admin_server_error(format!("task: {}", e)),
            }
        }

        // ── Upsert row (WAL + live fan-out) ──────────────────────────────────
        (&Method::POST, "/admin/api/row") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b, Err(e) => return Ok(admin_server_error(e.to_string())),
            };
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v, Err(e) => return Ok(admin_bad_request(format!("Invalid JSON: {}", e))),
            };
            let (table, rkey, data) = match (
                payload.get("table").and_then(|v| v.as_str()),
                payload.get("key").and_then(|v| v.as_str()),
                payload.get("data"),
            ) {
                (Some(t), Some(k), Some(d)) if !t.is_empty() && !k.is_empty() =>
                    (t.to_string(), k.to_string(), d.clone()),
                _ => return Ok(admin_bad_request("Expected {table, key, data}".into())),
            };
            match tables.set_row(table.clone(), rkey.clone(), data) {
                Ok(delta) => {
                    let deltas = vec![delta];
                    subs.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, Ordering::Relaxed);
                    let entry = WalEntry::new(now_nanos(), seq,
                        "__admin_set_row".to_string(), vec![], deltas);
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    admin_json(serde_json::json!({ "ok": true, "table": table, "key": rkey }))
                }
                Err(e) => admin_bad_request(e.to_string()),
            }
        }

        // ── Delete row (WAL + live fan-out) ──────────────────────────────────
        (&Method::DELETE, "/admin/api/row") => {
            if let Some(resp) = check_auth(&req) { return Ok(resp); }
            let query = req.uri().query().unwrap_or("");
            let mut table = String::new(); let mut rkey = String::new();
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("table"), Some(v)) => table = admin_url_decode(v),
                    (Some("key"),   Some(v)) => rkey  = admin_url_decode(v),
                    _ => {}
                }
            }
            if table.is_empty() || rkey.is_empty() {
                return Ok(admin_bad_request("Expected ?table=X&key=Y".into()));
            }
            match tables.delete_row(&table, &rkey) {
                Ok(delta) => {
                    let deltas = vec![delta];
                    subs.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, Ordering::Relaxed);
                    let entry = WalEntry::new(now_nanos(), seq,
                        "__admin_delete_row".to_string(), vec![], deltas);
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    admin_json(serde_json::json!({ "ok": true }))
                }
                Err(e) => admin_bad_request(e.to_string()),
            }
        }

        // ── Seed rows (no WAL, no fan-out — dev/test only) ───────────────────
        (&Method::POST, "/seed") => {
            let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b, Err(e) => return Ok(admin_server_error(e.to_string())),
            };
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v, Err(e) => return Ok(admin_bad_request(format!("Invalid JSON: {}", e))),
            };
            let row_arr = match payload.get("rows").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => return Ok(admin_bad_request("Expected {\"rows\": [...]}".into())),
            };
            let mut written = 0usize; let mut skipped = 0usize;
            for item in &row_arr {
                if let Some(triple) = item.as_array() {
                    if triple.len() == 3 {
                        if let (Some(t), Some(k), Some(d)) = (
                            triple[0].as_str(), triple[1].as_str(), Some(&triple[2])
                        ) {
                            if tables.set_row(t.to_string(), k.to_string(), d.clone()).is_ok() {
                                written += 1; continue;
                            }
                        }
                    }
                }
                skipped += 1;
            }
            admin_json(serde_json::json!({ "rows_written": written, "rows_skipped": skipped }))
        }

        // ── Redirect / → /admin ───────────────────────────────────────────────
        (&Method::GET, "/") => {
            let mut r = Response::new(Body::empty());
            r.headers_mut().insert(
                hyper::header::LOCATION,
                hyper::header::HeaderValue::from_static("/admin"),
            );
            *r.status_mut() = StatusCode::FOUND;
            r
        }

        _ => {
            let mut r = admin_json(serde_json::json!({ "error": "not found" }));
            *r.status_mut() = StatusCode::NOT_FOUND;
            r
        }
    };
    Ok(resp)
}

#[allow(clippy::too_many_arguments)]
fn start_embedded_admin_server(
    host: String,
    port: u16,
    tables: Arc<TableStore>,
    subs: Arc<SubscriptionManager>,
    registry: Arc<ReducerRegistry>,
    schema_reg: Arc<crate::schema::SchemaRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    startup: std::time::Instant,
    queue_tx: kanal::AsyncSender<PendingCall>,
    api_key: Option<String>,
    backup_dir: Option<std::path::PathBuf>,
    backup_keep: usize,
    wal_path: std::path::PathBuf,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let addr: SocketAddr = match format!("{}:{}", host, port).parse() {
        Ok(a) => a,
        Err(e) => { log::warn!("[admin] invalid metrics address: {}", e); return; }
    };

    tokio::spawn(async move {
        let make_svc = make_service_fn(move |_| {
            let tbl   = tables.clone();
            let sb    = subs.clone();
            let reg   = registry.clone();
            let sch   = schema_reg.clone();
            let wal   = wal_writer.clone();
            let seq   = global_seq.clone();
            let met   = metrics.clone();
            let qtx   = queue_tx.clone();
            let akey  = api_key.clone();
            let bdir  = backup_dir.clone();
            let bkeep = backup_keep;
            let wpath = wal_path.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| {
                    handle_embedded_admin(
                        req,
                        tbl.clone(), sb.clone(), reg.clone(), sch.clone(),
                        wal.clone(), seq.clone(), met.clone(),
                        startup, qtx.clone(), akey.clone(),
                        bdir.clone(), bkeep, wpath.clone(),
                    )
                }))
            }
        });

        let server = match Server::try_bind(&addr) {
            Ok(s) => s.serve(make_svc),
            Err(e) => {
                log::warn!("[admin] could not bind metrics port {}: {} — admin console disabled", addr, e);
                return;
            }
        };
        log::info!("[admin] Admin console: http://{}/admin", addr);
        println!("[neondb] Admin console: http://{}/admin", addr);
        let graceful = server
            .with_graceful_shutdown(async move { let _ = shutdown_rx.changed().await; });
        if let Err(e) = graceful.await {
            log::warn!("[admin] Metrics server error: {}", e);
        }
    });
}

/// Start the Redis (RESP) and PostgreSQL (pgwire) listeners over a shared
/// MVCC store. Bind failures are non-fatal: the core WebSocket server keeps
/// running (important when parallel test servers race for the same ports).
pub fn spawn_protocol_listeners(config: &Config) {
    if config.redis_port == 0 && config.pg_port == 0 {
        return;
    }
    let mvcc_dir = config.wal_path.parent().map(|p| p.join("mvcc_data"));
    let store = match crate::mvcc::MvccStore::open(crate::mvcc::MvccConfig {
        data_dir: mvcc_dir,
        fsync: crate::mvcc::FsyncPolicy::EverySec,
    }) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("[mvcc] store failed to open ({e}); Redis/PG protocols disabled");
            return;
        }
    };
    if config.redis_port > 0 {
        let ctx = crate::redis::RedisCtx::new(store.clone(), config.redis_password.clone());
        let (host, port) = (config.host.clone(), config.redis_port);
        tokio::spawn(async move {
            if let Err(e) = crate::redis::start_redis_listener(host, port, ctx).await {
                log::warn!("[redis] listener on port {port} unavailable: {e}");
            }
        });
    }
    if config.pg_port > 0 {
        let engine = crate::pg::executor::PgEngine::new(store);
        let ctx = crate::pg::PgCtx::new(engine, config.pg_password.clone());
        let (host, port) = (config.host.clone(), config.pg_port);
        tokio::spawn(async move {
            if let Err(e) = crate::pg::start_pg_listener(host, port, ctx).await {
                log::warn!("[pg] listener on port {port} unavailable: {e}");
            }
        });
    }
}
