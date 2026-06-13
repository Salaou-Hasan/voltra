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
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

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
