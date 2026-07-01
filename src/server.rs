// ============================================================================
// server.rs — Public library entry point for embedded Voltra projects
//
// Enables users to write a custom binary that embeds Voltra as a library:
//
//   ```rust
//   // src/main.rs
//   mod reducers;  // loads #[reducer] fns into inventory
//
//   #[tokio::main]
//   async fn main() {
//       let config = voltra::config::Config::from_env();
//       voltra::run_server(config).await.expect("Voltra server failed");
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
    snapshot::{find_latest_snapshot, load_snapshot, save_snapshot},
    BatchedWalWriter, WalEntry, WalReader,
};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::watch;

/// Live handles to a running embedded Voltra server.
///
/// Returned by [`run_server_with_handle`] after the server finishes bootstrapping.
/// All fields are cheaply cloneable `Arc`s — safe to share across threads/tasks.
pub struct ServerHandle {
    /// Read-only access to all in-memory tables (row counts, row data).
    pub tables: Arc<TableStore>,
    /// Subscription manager — exposes `active_connections()`.
    pub subs: Arc<SubscriptionManager>,
    /// Shared WAL byte counter — updated after every flush.
    pub wal_file_size: Arc<AtomicU64>,
}

use crate::now_nanos;

/// Start an embedded Voltra server and return a [`ServerHandle`] once the
/// server has finished bootstrapping (snapshot + WAL replay + listener bound).
///
/// The server runs as a background Tokio task; the returned future resolves
/// immediately once the listener is ready.  Use the handle to sample live stats
/// (rows, WAL size, connections) without an HTTP round-trip.
///
/// # Example
/// ```rust,ignore
/// let (handle, server_fut) = voltra::run_server_with_handle(config).await?;
/// tokio::spawn(server_fut);
/// // handle.tables.total_row_count(), etc.
/// ```
pub async fn run_server_with_handle(
    config: Config,
) -> Result<(ServerHandle, impl std::future::Future<Output = Result<()>>)> {
    use tokio::sync::oneshot;
    let (tx, rx) = oneshot::channel::<ServerHandle>();
    let fut = run_server_inner(config, Some(tx));
    // Drive the future just far enough to reach the "listener ready" point,
    // then hand the remaining future back to the caller to spawn.
    // We do this by spawning run_server_inner and waiting for the oneshot.
    let handle_task = tokio::spawn(fut);
    let handle = rx.await.map_err(|_| {
        crate::error::VoltraError::Internal("Server startup failed before sending handle".into())
    })?;
    // Wrap the task back into a plain future the caller can await/drop.
    let server_fut = async move {
        handle_task.await.map_err(|e| {
            crate::error::VoltraError::Internal(format!("Server task panicked: {e}"))
        })?
    };
    Ok((handle, server_fut))
}

/// Start a Voltra server with the given configuration.
///
/// All `#[reducer]`-annotated functions in the calling binary are discovered
/// automatically via the `inventory` crate.  Call this from `main()` in your
/// embedded Voltra project.
///
/// # Example
/// ```rust,ignore
/// mod reducers; // loads your #[reducer] functions
///
/// #[tokio::main]
/// async fn main() {
///     voltra::run_server(voltra::config::Config::from_env())
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
        .map_err(|e| crate::error::VoltraError::internal(format!("Tokio runtime: {}", e)))?
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
            log::info!("[voltra] Shutdown signal received.");
            let _ = tx.send(());
        });
    }

    // ── Data structures ───────────────────────────────────────────────────────
    let wal_path = config.wal_path.clone();

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
    let tables = Arc::new(crate::table::TableStore::with_eviction(eviction_policy));
    let schema_reg = Arc::new(crate::schema::SchemaRegistry::new());

    let mut initial_seq = 0u64;

    // ── Disk persistence (redb) ───────────────────────────────────────────────
    //
    // If VOLTRA_PERSISTENCE_PATH is set we open the redb store and restore all
    // rows BEFORE doing snapshot / WAL replay.  When redb has data we advance
    // initial_seq so WAL replay only applies entries that arrived after the
    // last redb commit, avoiding redundant replays.
    let persistence: Option<Arc<PersistenceEngine>> = match &config.persistence_path {
        Some(path) => match PersistenceEngine::open(path) {
            Ok(pe) => match pe.load_all(&tables) {
                Ok((rows, last_seq)) => {
                    if rows > 0 {
                        initial_seq = last_seq;
                        log::info!(
                            "[voltra] Loaded {} rows from disk store (last_seq={})",
                            rows,
                            last_seq
                        );
                    } else {
                        log::info!(
                            "[voltra] Disk store is empty; will bootstrap from snapshot+WAL"
                        );
                    }
                    Some(Arc::new(pe))
                }
                Err(e) => {
                    log::warn!(
                        "[voltra] Disk store load failed ({}); falling back to snapshot+WAL",
                        e
                    );
                    None
                }
            },
            Err(e) => {
                log::warn!("[voltra] Could not open disk store at {:?}: {}", path, e);
                None
            }
        },
        None => None,
    };

    // ── WAL + snapshot bootstrap ──────────────────────────────────────────────
    //
    // Only run snapshot+WAL loading when we don't have authoritative redb data.
    let loaded_from_redb = initial_seq > 0;
    if !loaded_from_redb {
        if let Some((snap_path, snap_seq)) = find_latest_snapshot(&wal_path) {
            load_snapshot(&snap_path, &tables)?;
            initial_seq = snap_seq;
            log::info!("[voltra] Loaded snapshot seq={}", snap_seq);
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
                    "[voltra] WAL entry {} bad checksum, skipping",
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
        log::info!(
            "[voltra] WAL replay complete ({} entries applied).",
            replayed
        );
    }

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &wal_file,
        config.fsync_interval_ms,
        config.wal_batch_size,
        false, // safe fsync
    )?);

    // Global WAL sequence counter — shared across all worker threads.
    let global_seq = Arc::new(AtomicU64::new(initial_seq));

    // ── Reducer registry (discovers #[reducer] functions automatically) ───────
    let registry = Arc::new(ReducerRegistry::new()?);

    // ── Support services ──────────────────────────────────────────────────────
    let subs = Arc::new(SubscriptionManager::new());
    subs.start_tick_flusher(config.sub_tick_ms);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let metrics = Arc::new(Metrics::new());

    // ── Cluster bus (horizontal scaling) ──────────────────────────────────────
    // Reads VOLTRA_PEERS / VOLTRA_SHARD_ID / VOLTRA_SHARD_COUNT from env.
    // No-op (single-node) when VOLTRA_PEERS is unset — fanout_deltas() returns
    // immediately, so the embedded game server pays nothing in standalone mode.
    let my_shard_id: u32 = std::env::var("VOLTRA_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let shard_count: u32 = std::env::var("VOLTRA_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let cluster_bus = crate::cluster::ClusterBus::new(crate::cluster::ClusterConfig::from_env(
        my_shard_id,
        shard_count,
    ));
    if cluster_bus.is_active() {
        log::info!(
            "[cluster] Active — shard {}/{}, {} peer(s)",
            my_shard_id,
            shard_count,
            cluster_bus.peers.len()
        );
        println!(
            "[voltra] Cluster mode — shard {}/{}, {} peer(s)",
            my_shard_id,
            shard_count,
            cluster_bus.peers.len()
        );
    }
    crate::cluster::gossip::start_gossip(cluster_bus.clone(), shutdown_rx.clone());
    crate::cluster::fanout::start_fanout_retry(cluster_bus.clone(), shutdown_rx.clone());

    // Ed25519 identity issuer — load persisted key or generate a fresh one.
    let identity_key_path = wal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("identity_key.pem");
    let identity_issuer: Arc<IdentityIssuer> = if identity_key_path.exists() {
        match IdentityIssuer::load_from_file(&identity_key_path) {
            Ok(iss) => {
                log::info!("[identity] Loaded key (kid={})", iss.kid);
                Arc::new(iss)
            }
            Err(e) => {
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

    let api_key = config.api_key.clone();
    let auth_validator = Arc::new(AuthValidator::from_env());
    let permissions = Arc::new(config.permissions.clone());
    let rate_limiter = Arc::new(RateLimiterRegistry::new(RateLimiterConfig {
        capacity: config.rate_limit_capacity,
        refill_rate: config.rate_limit_refill_rate,
        enabled: config.rate_limit_capacity > 0,
    }));
    let presence = Arc::new(PresenceManager::new(
        config.presence_heartbeat_timeout_ms,
        config.presence_offline_timeout_ms,
    ));
    let ttl_manager = Arc::new(TtlManager::new());
    let tls_config = None; // set via [tls] TOML section if needed

    // ── Bounded reducer queue ─────────────────────────────────────────────────
    let queue_cap = config.reducer_queue_cap;
    let (tx, rx) = kanal::bounded_async::<PendingCall>(queue_cap);

    // Guards against overlapping snapshot tasks: save_snapshot() clones every
    // row into memory before serializing. If a snapshot takes longer than the
    // interval between triggers, a second snapshot would start before the
    // first finishes, piling up full-dataset clones and exploding memory.
    let snapshot_in_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Periodically return allocator-retained memory to the OS so RSS tracks the
    // live working set, not the high-throughput churn peak (see lib.rs).
    crate::spawn_memory_reclaimer(15);

    // ── Worker pool ───────────────────────────────────────────────────────────
    let worker_count = if config.workers > 0 {
        config.workers
    } else {
        num_cpus::get().max(2)
    };

    // Collected so graceful shutdown can await every in-flight reducer call
    // actually finishing before the process exits (mirrors
    // app::bootstrap::run_server, the `voltra start` CLI path). Previously
    // `run_server_inner` returned as soon as the WebSocket listener stopped
    // accepting connections, without ever joining these worker threads or
    // flushing the WAL writer — a Ctrl+C during a burst of writes could exit
    // the process before those writes ever reached disk.
    let mut worker_handles = Vec::with_capacity(worker_count);

    for _worker_id in 0..worker_count {
        let rx_w = rx.clone();
        let tables_w = tables.clone();
        let subs_w = subs.clone();
        let registry_w = registry.clone();
        let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone();
        let metrics_w = metrics.clone();
        let schema_w = schema_reg.clone();
        let ttl_w = ttl_manager.clone();
        let persist_w = persistence.clone();
        let cluster_w = cluster_bus.clone();
        let mut shut_w = shutdown_rx.clone();
        let snap_iv = config.snapshot_interval;
        let snap_dir_w = config.snapshot_dir.clone();
        let snap_busy_w = snapshot_in_progress.clone();

        let handle = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            // Maximum extra calls to drain per wakeup before going back to sleep.
            // Amortises OS thread wakeup cost at high CCU: one wakeup processes up
            // to BATCH+1 calls instead of 1.  Pure-computation reducers (no DB
            // writes) are safe to batch; JS/WASM reducers benefit too because each
            // one is still executed serially on this thread.
            const DRAIN_LIMIT: usize = 63;

            loop {
                // Block until a call arrives or shutdown fires.
                let call: PendingCall = match rt.block_on(async {
                    tokio::select! {
                        c = rx_w.recv() => c.ok(),
                        _ = shut_w.changed() => None,
                    }
                }) {
                    Some(c) => c,
                    None => break,
                };

                // Build a micro-batch: drain up to DRAIN_LIMIT more calls that are
                // already queued (non-blocking).  If the channel is empty we fall
                // through immediately with a batch of 1.
                let mut batch: smallvec::SmallVec<[PendingCall; 16]> = smallvec::smallvec![call];
                for _ in 0..DRAIN_LIMIT {
                    // Zero-duration timeout = "give me one if available, otherwise skip"
                    match rt.block_on(tokio::time::timeout(std::time::Duration::ZERO, rx_w.recv()))
                    {
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
                        // Sync context (inside spawn_blocking): try_send, not
                        // send().await. A Full/Closed result just means the
                        // client's response channel is backed up or gone —
                        // never block a shared worker thread on one client.
                        if let Err(e) = call.response_tx.try_send(resp) {
                            log::warn!("[voltra] Response delivery failed: {}", e);
                        }
                        continue;
                    }

                    let ts = now_nanos();

                    // Build execution context with schema validation + TTL support.
                    let mut ctx = ReducerContext::new(tables_w.clone(), ts)
                        .with_schema(schema_w.clone())
                        .with_ttl(ttl_w.clone());
                    ctx.caller_id = call.caller_id.clone();
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
                                    Err(crate::error::VoltraError::TxnConflict(_))
                                        if attempt < MAX_CONFLICT_RETRIES =>
                                    {
                                        ctx.reset_for_retry();
                                        std::thread::yield_now();
                                        continue;
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "[voltra] Commit failed for '{}': {}",
                                            call.reducer_name,
                                            e
                                        );
                                        metrics_w.reducer_errors_total.inc();
                                        ReducerResponse::error(
                                            call_id,
                                            format!("Commit error: {}", e),
                                        )
                                    }
                                    Ok(deltas) => {
                                        // Fan out live subscription updates — O(1) per subscriber.
                                        if !deltas.is_empty() {
                                            subs_w.publish_deltas(&deltas);
                                            // Replicate to cluster peers (no-op single-node).
                                            cluster_w.fanout_deltas(&deltas);
                                        }

                                        let seq_num = seq_w.fetch_add(1, Ordering::Relaxed);

                                        // Write-through to disk store (non-fatal on failure).
                                        // Done BEFORE the WAL append below so `deltas` can be
                                        // handed to the WAL entry by value instead of cloning
                                        // the whole delta vec on every single call. On a crash
                                        // between this and the WAL write the row is still in the
                                        // disk store, which is loaded before WAL replay.
                                        if let Some(ref pe) = persist_w {
                                            if !deltas.is_empty() {
                                                if let Err(e) = pe.persist_deltas(&deltas, seq_num)
                                                {
                                                    log::warn!(
                                                        "[voltra] Disk persist failed: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }

                                        // Append to WAL for crash recovery. `deltas` is moved in
                                        // (its last use) — no per-call clone of the delta vec.
                                        let entry = WalEntry::new(
                                            ts,
                                            seq_num,
                                            call.reducer_name.clone(),
                                            call.args.clone(),
                                            deltas,
                                        );
                                        if let Err(e) = wal_w.append(&entry, seq_num) {
                                            log::warn!("[voltra] WAL append failed: {}", e);
                                        } else {
                                            metrics_w.wal_entries_written_total.inc();
                                        }

                                        // Periodic snapshot + WAL rotation to bound WAL file size.
                                        // Skip if a snapshot is already in flight — overlapping
                                        // snapshots each clone the full dataset into memory and
                                        // would compound rather than bound memory usage.
                                        if snap_iv > 0
                                            && (seq_num + 1).is_multiple_of(snap_iv)
                                            && !snap_busy_w.swap(true, Ordering::AcqRel)
                                        {
                                            let tbl2 = tables_w.clone();
                                            let dir2 = snap_dir_w.clone();
                                            let dir3 = snap_dir_w.clone();
                                            let wal2 = wal_w.clone();
                                            let busy2 = snap_busy_w.clone();
                                            let ts2 = now_nanos();
                                            tokio::spawn(async move {
                                                let result =
                                                    tokio::task::spawn_blocking(move || {
                                                        save_snapshot(&tbl2, &dir2, seq_num, ts2)
                                                    })
                                                    .await;
                                                match result {
                                                    Ok(Ok(())) => {
                                                        log::info!(
                                                            "[voltra] Snapshot at seq {}",
                                                            seq_num
                                                        );
                                                        if let Err(e) =
                                                            wal2.truncate_before(seq_num)
                                                        {
                                                            log::error!(
                                                                "[voltra] WAL rotation failed: {}",
                                                                e
                                                            );
                                                        }
                                                        // Prune older snapshot files — only the latest is
                                                        // needed for recovery; without this, snapshots
                                                        // accumulate on disk indefinitely over long runs.
                                                        if let Ok(entries) =
                                                            std::fs::read_dir(&dir3)
                                                        {
                                                            for entry in entries.flatten() {
                                                                let name = entry.file_name();
                                                                let name = name.to_string_lossy();
                                                                if let Some(seq_str) = name
                                                                    .strip_prefix(
                                                                        "voltra_snapshot_",
                                                                    )
                                                                    .and_then(|s| {
                                                                        s.strip_suffix(".bin")
                                                                    })
                                                                {
                                                                    if seq_str
                                                                        .parse::<u64>()
                                                                        .map(|s| s < seq_num)
                                                                        .unwrap_or(false)
                                                                    {
                                                                        let _ =
                                                                            std::fs::remove_file(
                                                                                entry.path(),
                                                                            );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Ok(Err(e)) => log::error!(
                                                        "[voltra] Snapshot failed: {}",
                                                        e
                                                    ),
                                                    Err(e) => log::error!(
                                                        "[voltra] Snapshot panic: {}",
                                                        e
                                                    ),
                                                }
                                                busy2.store(false, Ordering::Release);
                                            });
                                        }

                                        metrics_w.reducer_calls_total.inc();
                                        ReducerResponse::success(call_id, result_bytes)
                                    }
                                }
                            }
                        };
                    };

                    // Deliver response back to the waiting WebSocket handler.
                    // Sync context (inside spawn_blocking): try_send, not
                    // send().await — see the replica-rejection branch above.
                    if let Err(e) = call.response_tx.try_send(response) {
                        log::warn!("[voltra] Response delivery failed: {}", e);
                    }
                } // end for call in batch
            }
        });
        worker_handles.push(handle);
    }

    // ── Scheduled reducers ─────────────────────────────────────────────────────
    // Fire each configured reducer on its interval by enqueuing a PendingCall
    // through the same reducer queue as client calls. Without this, embedded
    // servers (run_server / .vol projects) silently never run their schedulers.
    let sched_seq = Arc::new(AtomicU64::new(u64::MAX / 2));
    let mut scheduler_handles = Vec::with_capacity(config.scheduled_reducers.len());
    for sched in &config.scheduled_reducers {
        let sched = sched.clone();
        let tx_sched = tx.clone();
        let seq_sched = sched_seq.clone();
        let mut shutdown_sched = shutdown_rx.clone();
        let args_bytes: Vec<u8> = sched
            .args_json
            .as_deref()
            .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
            .and_then(|v| rmp_serde::to_vec(&v).ok())
            .unwrap_or_default();
        log::info!(
            "[voltra] Scheduler: '{}' every {}ms",
            sched.reducer,
            sched.interval_ms
        );
        scheduler_handles.push(tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(sched.interval_ms.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let call_id = seq_sched.fetch_add(1, Ordering::Relaxed);
                        // Fire-and-forget: nobody reads this response, so the
                        // receiver is dropped immediately. Capacity 1 keeps it
                        // consistent with PendingCall::response_tx's bounded
                        // contract; the worker's send simply fails (Closed)
                        // and is already handled as a no-op there.
                        let (resp_tx, _resp_rx) = tokio::sync::mpsc::channel::<ReducerResponse>(1);
                        let call = PendingCall {
                            call_id,
                            reducer_name: sched.reducer.clone(),
                            args: args_bytes.clone(),
                            caller_id: "scheduler".to_string(),
                            caller_role: "scheduler".to_string(),
                            tenant_id: None,
                            lobby_hint: None,
                            response_tx: resp_tx,
                        };
                        if tx_sched.send(call).await.is_err() { break; }
                    }
                    _ = shutdown_sched.changed() => break,
                }
            }
        }));
    }

    // ── WebSocket listener ────────────────────────────────────────────────────
    let host = config.host.clone();
    let port = config.port;
    let max_conns = config.max_connections;

    log::info!("[voltra] Listening on {}:{}", host, port);

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
                println!("[voltra] Replica mode — pulling from {}", config.primary_url.as_deref().unwrap_or(""));
            }
            None => log::error!(
                "[replication] VOLTRA_ROLE=replica but VOLTRA_PRIMARY_URL is not set — staying primary"
            ),
        }
    }

    // The full admin/metrics console (voltra::admin) is started below, after the
    // tenant registry is built — same console as `voltra start`.

    // Send stats handle back to the caller (e.g. voltra-sim) before blocking.
    if let Some(tx) = handle_tx {
        let _ = tx.send(ServerHandle {
            tables: tables.clone(),
            subs: subs.clone(),
            wal_file_size: wal_writer.file_size_arc(),
        });
    }

    // ── Redis + PostgreSQL protocol listeners (MVCC engine) ──────────────────
    spawn_protocol_listeners(&config);

    let tenant_registry = crate::tenant::TenantRegistry::load(tables.clone());
    let inline_registry = crate::network::build_inline_registry();

    // ── Full admin/metrics console (same as `voltra start`) ──────────────────
    // Shared drain flag so the admin console can toggle drain mode on the same
    // flag the listener observes.
    let drain_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let metrics_port = config.metrics_port;
        if metrics_port > 0 {
            let startup = std::time::Instant::now();

            // SQLite accounts/auth tier (handshake/HTTP only — not the hot path).
            let persistent_db_path = config
                .wal_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("voltra_persistent.db");
            let persistent_store: Arc<crate::persistent::PersistentStore> =
                match crate::persistent::PersistentStore::open(&persistent_db_path) {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        log::warn!(
                            "[persistent] Could not open SQLite ({}); using in-memory fallback",
                            e
                        );
                        Arc::new(
                            crate::persistent::PersistentStore::open(std::path::Path::new(
                                ":memory:",
                            ))
                            .unwrap_or_else(|_| panic!("SQLite in-memory fallback failed")),
                        )
                    }
                };
            let auth_service = Arc::new(crate::auth_service::AuthService::new(
                persistent_store.clone(),
                identity_issuer.clone(),
                std::env::var("VOLTRA_TOKEN_TTL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(86_400),
            ));

            // Cross-region / leaderboard / stat-sync subsystems.
            let region_registry = Arc::new(crate::cluster::RegionRegistry::from_env());
            let lobby_routes = crate::cluster::LobbyRouteRegistry::new(tables.clone());
            let leaderboard = Arc::new(crate::leaderboard::LeaderboardEngine::new());
            leaderboard.create_board(crate::leaderboard::LeaderboardConfig {
                name: config.leaderboard_board.clone(),
                sort_order: crate::leaderboard::SortOrder::HighestFirst,
                time_window: crate::leaderboard::TimeWindow::AllTime,
                max_entries: config.leaderboard_top_n,
            });
            crate::leaderboard::LeaderboardAggregator::new(
                leaderboard.clone(),
                region_registry.clone(),
                config.leaderboard_board.clone(),
                config.leaderboard_interval_secs,
                config.leaderboard_top_n,
            )
            .start(shutdown_rx.clone());
            let stat_sync = crate::stat_sync::StatSyncQueue::new(
                tables.clone(),
                region_registry.clone(),
                config.stat_sync_flush_ms,
                shutdown_rx.clone(),
            );

            let admin_state = Arc::new(crate::admin::AdminState {
                wal_path: wal_file.clone(),
                backup_dir: config.backup_dir.clone(),
                backup_keep: config.backup_keep,
                tenant_registry: tenant_registry.clone(),
                cluster_bus: cluster_bus.clone(),
                drain_flag: drain_flag.clone(),
                active_connections: active_connections.clone(),
                region_registry,
                lobby_routes,
                leaderboard,
                stat_sync,
                // run_server uses single-channel dispatch (no lobby routing).
                lobby_router: None,
                persistent: persistent_store,
                auth_service,
            });

            let host_a = host.clone();
            let subs_a = subs.clone();
            let tables_a = tables.clone();
            let registry_a = registry.clone();
            let wal_a = wal_writer.clone();
            let seq_a = global_seq.clone();
            let presence_a = presence.clone();
            let ttl_a = ttl_manager.clone();
            let metrics_a = metrics.clone();
            let issuer_a = identity_issuer.clone();
            let tx_a = tx.clone();
            let schema_a = schema_reg.clone();
            let shutdown_a = shutdown_rx.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::admin::start_metrics_server(
                    host_a,
                    metrics_port,
                    subs_a,
                    tables_a,
                    registry_a,
                    wal_a,
                    seq_a,
                    startup,
                    presence_a,
                    ttl_a,
                    metrics_a,
                    issuer_a,
                    tx_a,
                    admin_state,
                    schema_a,
                    shutdown_a,
                )
                .await
                {
                    log::error!("[admin] metrics server error: {}", e);
                }
            });
        }
    }

    // `start_listener` returns as soon as the shutdown signal fires (it aborts
    // its own accept-loop tasks internally); it does NOT wait for already-open
    // `handle_client` connections to finish sending their WebSocket Close
    // frames, for in-flight reducer calls to drain, or for the WAL writer's
    // background thread to flush. Do all three explicitly below so Ctrl+C on
    // an embedded (`voltra::run_server`) project is as safe as `voltra start`.
    let listener_result = crate::network::start_listener(
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
        drain_flag,
    )
    .await;

    log::info!("[voltra] Draining in-flight reducer calls before exit...");

    // Wait for every reducer worker (and scheduled-reducer task) to observe
    // the shutdown signal and return, with a bounded deadline so a stuck
    // reducer can't hang the process forever.
    let drain_result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for h in worker_handles {
            let _ = h.await;
        }
        for h in scheduler_handles {
            let _ = h.await;
        }
    })
    .await;
    if drain_result.is_err() {
        log::warn!(
            "[voltra] Worker drain timed out after 30s — some in-flight reducers may be incomplete"
        );
    }

    // Flush any buffered WAL entries to disk, then join the flusher thread.
    if let Err(e) = wal_writer.flush().await {
        log::error!("[voltra] WAL flush failed during shutdown: {}", e);
    }
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() {
            log::error!("[voltra] WAL shutdown: {}", e);
        }
    }

    log::info!("[voltra] Shutdown complete");
    listener_result
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

#[cfg(test)]
mod graceful_shutdown_tests {
    // These tests exercise the exact drain-then-flush pattern added to
    // `run_server_inner`'s shutdown sequence (worker/scheduler handles
    // collected -> shutdown signal -> bounded-timeout join -> WAL flush ->
    // WAL writer shutdown) without depending on `tokio::signal::ctrl_c()`,
    // which requires a real OS signal and cannot be reliably delivered
    // cross-platform (esp. on Windows, where this project builds/tests) from
    // within `cargo test`. Regression target: before this fix,
    // `run_server_inner` returned as soon as the WebSocket listener stopped
    // accepting connections, without ever joining worker tasks or flushing
    // the WAL writer.
    use super::*;
    use std::sync::atomic::{AtomicUsize as StdAtomicUsize, Ordering as StdOrdering};

    #[tokio::test]
    async fn shutdown_signal_unblocks_worker_shaped_tasks_before_timeout() {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let completed = Arc::new(StdAtomicUsize::new(0));

        // Spawn a handful of tasks shaped like the reducer worker loops: they
        // block on a channel recv racing the shutdown signal, and return as
        // soon as shutdown fires (mirrors the `tokio::select!` in both the
        // async worker loop in app::bootstrap::run_server and the
        // spawn_blocking loop in run_server_inner).
        let mut handles = Vec::new();
        for _ in 0..4 {
            let mut rx = shutdown_rx.clone();
            let completed = completed.clone();
            handles.push(tokio::spawn(async move {
                let (_tx, mut never_fires): (
                    tokio::sync::mpsc::Sender<()>,
                    tokio::sync::mpsc::Receiver<()>,
                ) = tokio::sync::mpsc::channel(1);
                tokio::select! {
                    _ = never_fires.recv() => {}
                    _ = rx.changed() => {}
                }
                completed.fetch_add(1, StdOrdering::SeqCst);
            }));
        }

        // Nobody has signalled shutdown yet — the drain must NOT complete
        // instantly (this would indicate the tasks aren't actually blocking
        // on the shutdown signal, which would make the rest of the test
        // meaningless).
        assert_eq!(completed.load(StdOrdering::SeqCst), 0);

        let _ = shutdown_tx.send(());

        let drain_result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for h in handles {
                let _ = h.await;
            }
        })
        .await;

        assert!(
            drain_result.is_ok(),
            "worker-shaped tasks did not drain within the timeout after shutdown fired"
        );
        assert_eq!(
            completed.load(StdOrdering::SeqCst),
            4,
            "all 4 worker-shaped tasks must have observed shutdown and returned"
        );
    }

    #[tokio::test]
    async fn wal_flush_and_shutdown_succeed_after_drain() {
        // Real BatchedWalWriter, exercised the same way run_server_inner's
        // shutdown tail does: flush() then Arc::try_unwrap + shutdown().
        let tmp_dir = std::env::temp_dir().join("voltra_test_graceful_shutdown_wal");
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let wal_path = tmp_dir.join("voltra.wal");

        let wal_writer =
            Arc::new(BatchedWalWriter::open(&wal_path, 50, 1000, true).expect("open WAL writer"));

        for i in 0..10u64 {
            let entry = WalEntry::new(
                1000 + i,
                i,
                "test_reducer".to_string(),
                vec![1, 2, 3],
                vec![],
            );
            wal_writer.append(&entry, i).expect("append");
        }

        // This is the exact call sequence added to run_server_inner's tail.
        wal_writer.flush().await.expect("WAL flush must succeed");
        match Arc::try_unwrap(wal_writer) {
            Ok(writer) => writer.shutdown().expect("WAL shutdown must succeed"),
            Err(_) => panic!(
                "expected sole ownership of wal_writer at this point in the test \
                 (no other clones were created)"
            ),
        }

        // Data must actually be on disk after flush(), before shutdown().
        let size = std::fs::metadata(&wal_path).expect("WAL file exists").len();
        assert!(
            size > 0,
            "flushed WAL file should contain data, got 0 bytes"
        );

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
