// Server bootstrap: `run_server` performs WAL/snapshot recovery, wires every
// subsystem (auth, presence, TTL, metrics, cluster, replication, backups), and
// runs the N-worker reducer dispatch loop until shutdown.

use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use voltra::{
    auth::{AuthValidator, IdentityIssuer},
    config::{Config, ScheduledReducerConfig},
    error::Result,
    metrics::Metrics,
    network::{
        start_listener, PendingCall, RateLimiterConfig, RateLimiterRegistry, ReducerResponse,
    },
    presence::PresenceManager,
    reducer::{ReducerContext, ReducerRegistry},
    subscriptions::SubscriptionManager,
    table::TableStore,
    ttl::TtlManager,
    wal::{
        snapshot::{find_latest_snapshot, load_snapshot, save_snapshot},
        BatchedWalWriter, WalEntry,
    },
};

use crate::app::{current_timestamp_nanos, recover_from_wal};
use voltra::admin::{start_metrics_server, AdminState};

// ═══════════════════════════════════════════════════════════════════════════════
// Server bootstrap
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) async fn run_server(config: Config) -> Result<()> {
    // Installs the `tracing` subscriber (always-on local `fmt` layer, plus an
    // opt-in OTLP exporter when built with `--features otel` and
    // VOLTRA_OTEL_ENDPOINT is set). `tracing_log::LogTracer` bridges the
    // existing `log::*` call sites into the same pipeline, so this replaces
    // the old bare `env_logger` init rather than running alongside it —
    // both would otherwise race to install the global `log` logger.
    let _tracing_guard = voltra::tracing_setup::init(&config);

    log::info!("Starting Voltra Server");

    // Apply global runtime limits (e.g. max blob size) before any data is written.
    config.apply_global_limits();

    let eviction_policy = match config.eviction.policy.trim().to_ascii_lowercase().as_str() {
        "lru_row_cap" => voltra::table::EvictionPolicy::LruRowCap {
            max_rows_per_table: config.eviction.max_rows_per_table.max(1),
        },
        "lru_byte_cap" => voltra::table::EvictionPolicy::LruByteCap {
            max_bytes_total: config.eviction.max_bytes_total.max(1),
        },
        _ => voltra::table::EvictionPolicy::None,
    };
    let mut ts = TableStore::with_eviction(eviction_policy);
    ts.set_shard(config.shard_id, config.shard_count);
    let tables = Arc::new(ts);

    // Build the shared ReducerRegistry ONCE at startup (BUG-2 fix).
    let registry = Arc::new(ReducerRegistry::new()?);
    log::info!("Available reducers: {:?}", registry.list_reducers());

    let mut min_wal_seq: u64 = 0;
    let mut initial_seq: u64 = 0;

    // ── Optional disk persistence (opt-in via persistence_path) ───────────────
    // Parity with run_server: when a disk store has rows it is authoritative —
    // load from it, set the WAL replay floor to its last seq, and SKIP the
    // snapshot. Off by default (persistence_path = None), in which case the
    // WAL + snapshot remain the sole durability/recovery path and the block
    // below behaves exactly as before. Paired with persist_deltas in the worker.
    let persistence: Option<Arc<voltra::persistence::PersistenceEngine>> =
        match &config.persistence_path {
            Some(path) => match voltra::persistence::PersistenceEngine::open(path) {
                Ok(pe) => {
                    match pe.load_all(&tables) {
                        Ok((rows, last_seq)) if rows > 0 => {
                            min_wal_seq = last_seq;
                            initial_seq = last_seq.saturating_add(1);
                            log::info!(
                                "[voltra] Loaded {} rows from disk store (last_seq={})",
                                rows,
                                last_seq
                            );
                        }
                        Ok(_) => {
                            log::info!("[voltra] Disk store empty; bootstrapping from snapshot+WAL")
                        }
                        Err(e) => log::warn!("[voltra] Disk store load failed: {}", e),
                    }
                    Some(Arc::new(pe))
                }
                Err(e) => {
                    log::warn!(
                        "[voltra] Could not open disk store at {:?}: {}; continuing without it",
                        path,
                        e
                    );
                    None
                }
            },
            None => None,
        };

    let snap_dir = config.snapshot_dir.clone();
    // Skip the snapshot when the disk store already provided authoritative rows.
    if initial_seq == 0 {
        if let Some((snap_path, _)) = find_latest_snapshot(&snap_dir) {
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
    }

    if config.wal_path.exists() {
        match recover_from_wal(&config.wal_path, &tables, min_wal_seq) {
            Ok((n, max_seq)) => {
                initial_seq = initial_seq.max(max_seq.saturating_add(1));
                log::info!("Recovered {} WAL entries (last seq={})", n, max_seq);
            }
            Err(e) => log::warn!("WAL recovery failed: {}", e),
        }
    } else {
        log::info!("WAL does not exist, starting fresh");
    }

    let migrations_dir = PathBuf::from("migrations");
    match voltra::migrations::apply_migrations(&migrations_dir, &tables) {
        Ok(0) => {}
        Ok(n) => log::info!("Applied {} migration file(s)", n),
        Err(e) => log::warn!("Migration error: {}", e),
    }

    let schema_registry = Arc::new(
        voltra::schema::SchemaRegistry::load_from_file(Path::new("schema.toml"))
            .unwrap_or_else(|_| voltra::schema::SchemaRegistry::new()),
    );

    // Tenant registry — hydrated from __tenants table (populated by WAL/snapshot replay above).
    let tenant_registry = voltra::tenant::TenantRegistry::load(tables.clone());
    log::info!("[tenant] {} tenant(s) loaded", tenant_registry.count());

    // Redis (RESP) + PostgreSQL (pgwire) protocol listeners over the MVCC engine.
    voltra::server::spawn_protocol_listeners(&config);

    let permissions = Arc::new(config.permissions.clone());

    // ── Cluster bus (horizontal scaling) ────────────────────────────────────
    // Reads VOLTRA_PEERS, VOLTRA_SHARD_ID, VOLTRA_SHARD_COUNT from env.
    // No-op when VOLTRA_PEERS is unset (single-node mode).
    let my_shard_id: u32 = std::env::var("VOLTRA_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let shard_count: u32 = std::env::var("VOLTRA_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let cluster_cfg = voltra::cluster::ClusterConfig::from_env(my_shard_id, shard_count);
    let cluster_bus = voltra::cluster::ClusterBus::new(cluster_cfg);
    if cluster_bus.is_active() {
        log::info!(
            "[cluster] Active — shard {}/{}, {} peer(s): {}",
            my_shard_id,
            shard_count,
            cluster_bus.peers.len(),
            cluster_bus
                .healthy_peers()
                .iter()
                .map(|p| format!("shard{}@{}", p.shard_id, p.metrics_url))
                .collect::<Vec<_>>()
                .join(", ")
        );
    } else {
        log::info!("[voltra] single-node mode (set VOLTRA_PEERS to enable clustering)");
    }

    let (reducer_tx, reducer_rx) = kanal::bounded_async::<PendingCall>(config.reducer_queue_cap);
    let queue_probe = reducer_tx.clone(); // for healthz queue-depth reporting
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(
        config.two_frame_protocol,
    ));
    subscription_manager.start_tick_flusher(config.sub_tick_ms);

    let active_connections = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // ── Auth validator (JWT / API key / open) ────────────────────────────────
    let auth_validator = Arc::new(AuthValidator::from_env());

    // ── Ed25519 identity issuer ───────────────────────────────────────────────
    // Persist key in <wal_dir>/identity_key.pem.  Generated on first start,
    // reloaded on subsequent starts so tokens stay valid across restarts.
    let identity_key_path = config
        .wal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("identity_key.pem");
    let identity_issuer: Arc<IdentityIssuer> = if identity_key_path.exists() {
        match IdentityIssuer::load_from_file(&identity_key_path) {
            Ok(iss) => {
                log::info!("[identity] Loaded Ed25519 key (kid={})", iss.kid);
                Arc::new(iss)
            }
            Err(e) => {
                log::warn!("[identity] Failed to load key ({}), generating new key", e);
                let iss = IdentityIssuer::generate();
                if let Err(e2) = iss.save_to_file(&identity_key_path) {
                    log::warn!("[identity] Could not persist new key: {}", e2);
                }
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
    println!(
        "[voltra] Identity public key:\n{}",
        identity_issuer.public_key_pem()
    );

    // ── Persistent relational store (SQLite) ─────────────────────────────────
    // Stored alongside the WAL directory.  Only accessed at handshake / HTTP
    // endpoints — never from the game reducer hot path.
    let persistent_db_path = config
        .wal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("voltra_persistent.db");
    let persistent_store: Arc<voltra::persistent::PersistentStore> =
        match voltra::persistent::PersistentStore::open(&persistent_db_path) {
            Ok(s) => {
                log::info!(
                    "[persistent] SQLite store opened at {:?}",
                    persistent_db_path
                );
                Arc::new(s)
            }
            Err(e) => {
                log::warn!(
                    "[persistent] Could not open SQLite ({}), auth endpoints will be unavailable",
                    e
                );
                // Create an in-memory fallback so the server still boots.
                Arc::new(
                    voltra::persistent::PersistentStore::open(std::path::Path::new(":memory:"))
                        .unwrap_or_else(|_| panic!("SQLite in-memory fallback failed")),
                )
            }
        };
    let auth_service: Arc<voltra::auth_service::AuthService> =
        Arc::new(voltra::auth_service::AuthService::new(
            persistent_store.clone(),
            identity_issuer.clone(),
            std::env::var("VOLTRA_TOKEN_TTL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(86_400),
        ));

    // ── Rate limiter ─────────────────────────────────────────────────────────
    let rate_limiter = Arc::new(RateLimiterRegistry::new(RateLimiterConfig {
        capacity: config.rate_limit_capacity,
        refill_rate: config.rate_limit_refill_rate,
        enabled: config.rate_limit_capacity > 0,
    }));

    // ── Presence manager ─────────────────────────────────────────────────────
    let presence = Arc::new(PresenceManager::new(
        config.presence_heartbeat_timeout_ms,
        config.presence_offline_timeout_ms,
    ));

    // ── TTL manager ──────────────────────────────────────────────────────────
    let ttl_manager = Arc::new(TtlManager::new());

    // ── Prometheus metrics ────────────────────────────────────────────────────
    let metrics = Arc::new(Metrics::new());

    // ── TLS configuration ────────────────────────────────────────────────────
    let tls_server_config: Option<std::sync::Arc<rustls::ServerConfig>> = if config.tls.enabled {
        match (
            config.tls.cert_path.as_deref(),
            config.tls.key_path.as_deref(),
        ) {
            (Some(cert), Some(key)) => match voltra::network::tls::load_tls_config(cert, key) {
                Ok(cfg) => {
                    log::info!(
                        "TLS enabled: cert={}, key={}",
                        cert.display(),
                        key.display()
                    );
                    Some(cfg)
                }
                Err(e) => {
                    log::error!(
                        "Failed to load TLS config, falling back to plaintext: {}",
                        e
                    );
                    None
                }
            },
            _ => {
                log::warn!(
                    "TLS enabled but cert_path/key_path not set. Falling back to plaintext."
                );
                None
            }
        }
    } else {
        None
    };

    let inline_registry = voltra::network::build_inline_registry();
    let drain_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path,
        config.wal_batch_interval_ms,
        config.wal_batch_size,
        config.unsafe_no_fsync,
    )?);
    let worker_count = if config.workers > 0 {
        config.workers
    } else {
        num_cpus::get().max(1)
    };
    log::info!("Starting {} reducer workers", worker_count);
    let global_seq = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    // Periodically return allocator-retained memory to the OS so RSS tracks the
    // live working set, not the high-throughput churn peak (see lib.rs).
    voltra::spawn_memory_reclaimer(15);

    let lobby_router = {
        let worker_deps = std::sync::Arc::new(voltra::worker_pool::WorkerDeps {
            tables: tables.clone(),
            registry: registry.clone(),
            subscription_manager: subscription_manager.clone(),
            wal_writer: wal_writer.clone(),
            global_seq: global_seq.clone(),
            schema_registry: schema_registry.clone(),
            ttl_manager: ttl_manager.clone(),
            tenant_registry: tenant_registry.clone(),
            cluster_bus: cluster_bus.clone(),
            metrics: metrics.clone(),
            timeout_ms: config.reducer_timeout_ms,
            snapshot_interval: config.snapshot_interval,
            snapshot_dir: config.snapshot_dir.clone(),
        });
        let max_lobbies = config.max_connections / 2;
        Arc::new(voltra::worker_pool::LobbyRouter::new(
            reducer_tx.clone(),
            config.reducer_queue_cap.max(256),
            max_lobbies.max(64),
            worker_deps,
            shutdown_rx.clone(),
        ))
    };

    let listener_handle = {
        let config_c = config.clone();
        let tx_c = reducer_tx.clone();
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone();
        let conns_c = active_connections.clone();
        let rx_shutdown = shutdown_rx.clone();
        let perms_c = permissions.clone();
        let auth_c = auth_validator.clone();
        let rl_c = rate_limiter.clone();
        let pres_c = presence.clone();
        let ttl_c = ttl_manager.clone();
        let metrics_c = metrics.clone();
        let tls_cfg = tls_server_config.clone();
        let iss_c = identity_issuer.clone();
        let tenant_registry_ws = tenant_registry.clone();
        let inl_c = inline_registry.clone();
        let lr_c = lobby_router.clone();
        let df_c = drain_flag.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host,
                config_c.port,
                tx_c,
                subs_c,
                tables_c,
                config_c.max_connections,
                config_c.api_key.clone(),
                conns_c,
                perms_c,
                config_c.sql_timeout_ms,
                auth_c,
                rl_c,
                pres_c,
                ttl_c,
                iss_c,
                rx_shutdown,
                metrics_c,
                tls_cfg,
                tenant_registry_ws,
                inl_c,
                Some(lr_c),
                df_c,
            )
            .await
            {
                log::error!("Listener error: {}", e);
            }
        })
    };

    let timeout_ms = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w = config.snapshot_dir.clone();

    let startup_instant = std::time::Instant::now();

    let metrics_handle = {
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone();
        let mport = config.metrics_port;
        let registry_c = registry.clone();
        let wal_c = wal_writer.clone();
        let seq_c = global_seq.clone();
        let pres_m = presence.clone();
        let ttl_m = ttl_manager.clone();
        let prom_c = metrics.clone();
        let issuer_c = identity_issuer.clone();
        let qprobe_c = queue_probe.clone();

        // ── Multi-region infrastructure ──────────────────────────────────────
        // Override VOLTRA_REGION / VOLTRA_REGIONS via config fields so the
        // same env-var-based construction works whether started from binary
        // or from run_server().
        if !config.region.is_empty() && config.region != "default" {
            std::env::set_var("VOLTRA_REGION", &config.region);
        }
        if !config.regions.is_empty() {
            std::env::set_var("VOLTRA_REGIONS", &config.regions);
        }
        let region_registry = Arc::new(voltra::cluster::RegionRegistry::from_env());
        if region_registry.is_multi_region() {
            log::info!(
                "[regions] Multi-region mode: region='{}', peers={}",
                region_registry.my_region,
                region_registry.peer_regions().len()
            );
        }

        let lobby_routes = voltra::cluster::LobbyRouteRegistry::new(tables.clone());

        let leaderboard = Arc::new(voltra::leaderboard::LeaderboardEngine::new());
        // Register the default leaderboard board.
        leaderboard.create_board(voltra::leaderboard::LeaderboardConfig {
            name: config.leaderboard_board.clone(),
            sort_order: voltra::leaderboard::SortOrder::HighestFirst,
            time_window: voltra::leaderboard::TimeWindow::AllTime,
            max_entries: config.leaderboard_top_n,
        });
        // Start cross-region aggregation if multi-region.
        voltra::leaderboard::LeaderboardAggregator::new(
            leaderboard.clone(),
            region_registry.clone(),
            config.leaderboard_board.clone(),
            config.leaderboard_interval_secs,
            config.leaderboard_top_n,
        )
        .start(shutdown_rx.clone());

        let stat_sync = voltra::stat_sync::StatSyncQueue::new(
            tables.clone(),
            region_registry.clone(),
            config.stat_sync_flush_ms,
            shutdown_rx.clone(),
        );

        let admin_c = Arc::new(AdminState {
            wal_path: config.wal_path.clone(),
            backup_dir: config.backup_dir.clone(),
            backup_keep: config.backup_keep,
            tenant_registry: tenant_registry.clone(),
            cluster_bus: cluster_bus.clone(),
            drain_flag: drain_flag.clone(),
            active_connections: active_connections.clone(),
            region_registry: region_registry.clone(),
            lobby_routes: lobby_routes.clone(),
            leaderboard: leaderboard.clone(),
            stat_sync: stat_sync.clone(),
            lobby_router: Some(lobby_router.clone()),
            persistent: persistent_store.clone(),
            auth_service: auth_service.clone(),
        });
        let schema_c = schema_registry.clone();
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(
                host_c,
                mport,
                subs_c,
                tables_c,
                registry_c,
                wal_c,
                seq_c,
                startup_instant,
                pres_m,
                ttl_m,
                prom_c,
                issuer_c,
                qprobe_c,
                admin_c,
                schema_c,
                rx_shutdown,
            )
            .await
            {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    // ── Replication: replica mode ────────────────────────────────────────────
    // A replica pulls committed WAL entries from the primary, applies them
    // locally, and rejects reducer calls until promoted (POST /replication/promote).
    if config.role.eq_ignore_ascii_case("replica") {
        match config.primary_url.clone() {
            Some(primary) => {
                voltra::replication::set_replica(true);
                // Resume from the highest locally recovered sequence.
                voltra::replication::init_replica_from_local_wal(initial_seq.saturating_sub(1));
                let tables_r = tables.clone();
                let subs_r = subscription_manager.clone();
                let wal_r = wal_writer.clone();
                let seq_r = global_seq.clone();
                let poll = config.replica_poll_ms;
                let auto_failover = config.auto_failover;
                let miss_count = config.failover_miss_count;
                let shut_r = shutdown_rx.clone();
                tokio::spawn(async move {
                    voltra::replication::run_replica_loop(
                        primary,
                        tables_r,
                        subs_r,
                        wal_r,
                        seq_r,
                        poll,
                        auto_failover,
                        miss_count,
                        shut_r,
                    )
                    .await;
                });
                log::info!("[replication] Started in REPLICA mode (read-only)");
            }
            None => {
                log::error!(
                    "[replication] VOLTRA_ROLE=replica but VOLTRA_PRIMARY_URL is not set — \
                     starting as primary instead"
                );
            }
        }
    }

    // ── Automated backups ────────────────────────────────────────────────────
    if let (Some(backup_dir), true) = (config.backup_dir.clone(), config.backup_interval_secs > 0) {
        let tables_b = tables.clone();
        let wal_path_b = config.wal_path.clone();
        let seq_b = global_seq.clone();
        let keep = config.backup_keep;
        let interval_secs = config.backup_interval_secs;
        let mut shut_b = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(10)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip the immediate first tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let tbl = tables_b.clone();
                        let wal = wal_path_b.clone();
                        let dir = backup_dir.clone();
                        let seq = seq_b.load(std::sync::atomic::Ordering::Relaxed);
                        let res = tokio::task::spawn_blocking(move || {
                            let p = voltra::backup::backup_now(&tbl, &wal, &dir, seq)?;
                            let removed = voltra::backup::rotate_backups(&dir, keep)?;
                            Ok::<_, voltra::error::VoltraError>((p, removed))
                        }).await;
                        match res {
                            Ok(Ok((path, removed))) => log::info!(
                                "[backup] Automated backup at {:?} ({} old rotated out)", path, removed
                            ),
                            Ok(Err(e)) => log::error!("[backup] Automated backup failed: {}", e),
                            Err(e)     => log::error!("[backup] Backup task panicked: {}", e),
                        }
                    }
                    _ = shut_b.changed() => break,
                }
            }
        });
        log::info!(
            "[backup] Automated backups every {}s (keep {})",
            interval_secs,
            keep
        );
    }

    // ── Cluster gossip + fan-out retry tasks ─────────────────────────────────
    voltra::cluster::gossip::start_gossip(cluster_bus.clone(), shutdown_rx.clone());
    voltra::cluster::fanout::start_fanout_retry(cluster_bus.clone(), shutdown_rx.clone());

    // Guards against overlapping snapshot tasks: save_snapshot() clones every
    // row into memory before serializing. If a snapshot takes longer than the
    // interval between triggers, a second snapshot would start before the first
    // finishes, piling up full-dataset clones and exploding memory. Shared
    // across all workers so only one snapshot is ever in flight process-wide.
    let snapshot_in_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone();
        let tables_w = tables.clone();
        let registry_w = registry.clone();
        let subs_w = subscription_manager.clone();
        let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone();
        let snap_iv = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone();
        let schema_w = schema_registry.clone();
        let ttl_w = ttl_manager.clone();
        let tenant_w = tenant_registry.clone();
        let cluster_w = cluster_bus.clone();
        let persist_w = persistence.clone();
        let snap_busy_w = snapshot_in_progress.clone();
        let mut rx_shutdown_w = shutdown_rx.clone();
        let metrics_w = metrics.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let call = tokio::select! {
                    result = rx.recv() => match result { Ok(c) => c, Err(_) => break },
                    _ = rx_shutdown_w.changed() => break,
                };
                let call_id     = call.call_id;
                let queue_wait_ms = call.enqueued_at.elapsed().as_secs_f64() * 1000.0;

                // Root span for this reducer call's full lifecycle: queue wait
                // (already elapsed by the time we get here, recorded as a
                // field since the wait itself can't be "entered" retroactively)
                // → dispatch/execute → commit → WAL append → subscription
                // fan-out. `call_id` is the correlation ID a client can match
                // against its own `ReducerCall.call_id` (already present on
                // the wire — no protocol change needed to correlate).
                //
                // NB: this span is entered with `.enter()` (sync guard) only
                // across code that never awaits before the guard drops. The
                // `spawn_blocking` + `.await` further down is wrapped with
                // `.instrument()` instead (see below) — an `EnteredSpan` guard
                // is `!Send` and cannot be held across an `.await` inside a
                // future that gets `tokio::spawn`-ed onto a multi-threaded
                // runtime.
                let call_span = tracing::info_span!(
                    "reducer_call",
                    call_id = call_id,
                    reducer = %call.reducer_name,
                    caller_id = %call.caller_id,
                    queue_wait_ms = queue_wait_ms,
                );

                // ── Client transaction batch (BEGIN/COMMIT) ──────────────────
                // Bundles every reducer call the client staged between
                // BeginTransaction/CommitTransaction into one atomic unit:
                // each executes in order against a single ReducerContext,
                // then the whole batch commits in one
                // `apply_delta_batch_versioned()` call. See
                // `ServerMessage::TransactionResult` for the isolation level
                // this actually provides (read-committed + whole-transaction
                // OCC, not serializable).
                if let Some(batch_calls) = call.tx_batch {
                    let _tx_enter = call_span.enter();
                    let tenant_blk = tenant_w.clone();
                    let outcome = if voltra::replication::is_replica() {
                        voltra::network::TransactionOutcome {
                            tx_id: call_id,
                            success: false,
                            responses: vec![],
                            error: Some("This node is a read-only replica.".to_string()),
                        }
                    } else {
                        let ts = current_timestamp_nanos();
                        let mut ctx = ReducerContext::new(tables_w.clone(), ts)
                            .with_schema(schema_w.clone())
                            .with_ttl(ttl_w.clone());
                        ctx.caller_id = call.caller_id.clone();
                        ctx.caller_role = call.caller_role.clone();
                        if let Some(tid) = call.tenant_id.clone() {
                            ctx = ctx.with_tenant(tid, tenant_blk);
                        }

                        let mut responses = Vec::with_capacity(batch_calls.len());
                        let mut aborted: Option<String> = None;
                        for (reducer_name, args) in &batch_calls {
                            let exec = tracing::info_span!("dispatch", reducer = %reducer_name)
                                .in_scope(|| registry_w.execute(reducer_name, &mut ctx, args));
                            match exec {
                                Ok(bytes) => responses.push(ReducerResponse::success(0, bytes)),
                                Err(e) => {
                                    aborted =
                                        Some(format!("reducer '{}' failed: {}", reducer_name, e));
                                    break;
                                }
                            }
                        }

                        if let Some(err) = aborted {
                            ctx.rollback();
                            metrics_w.reducer_errors_total.inc();
                            voltra::network::TransactionOutcome {
                                tx_id: call_id,
                                success: false,
                                responses: vec![],
                                error: Some(err),
                            }
                        } else {
                            let commit_result =
                                tracing::info_span!("commit").in_scope(|| ctx.commit());
                            match commit_result {
                                Ok(deltas) => {
                                    let _fanout_enter = tracing::info_span!(
                                        "subscription_fanout",
                                        rows = deltas.len()
                                    )
                                    .entered();
                                    if !deltas.is_empty() {
                                        subs_w.publish_deltas(&deltas);
                                        cluster_w.fanout_deltas(&deltas);
                                    }
                                    drop(_fanout_enter);

                                    let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let _wal_enter =
                                        tracing::info_span!("wal_append", seq_num).entered();
                                    let entry = WalEntry::new(
                                        ts,
                                        seq_num,
                                        "__transaction".to_string(),
                                        Vec::new(),
                                        deltas,
                                    );
                                    if let Err(e) = wal_w.append(&entry, seq_num) {
                                        log::warn!("[voltra] WAL append failed: {}", e);
                                    } else {
                                        metrics_w.wal_entries_written_total.inc();
                                    }
                                    drop(_wal_enter);

                                    metrics_w.reducer_calls_total.inc_by(batch_calls.len() as u64);
                                    voltra::network::TransactionOutcome {
                                        tx_id: call_id,
                                        success: true,
                                        responses,
                                        error: None,
                                    }
                                }
                                Err(e) => {
                                    metrics_w.reducer_errors_total.inc();
                                    voltra::network::TransactionOutcome {
                                        tx_id: call_id,
                                        success: false,
                                        responses: vec![],
                                        error: Some(format!("Commit error: {}", e)),
                                    }
                                }
                            }
                        }
                    };

                    if let Some(tx_tx) = &call.tx_response_tx {
                        if let Err(e) = tx_tx.send(outcome) {
                            log::warn!("[voltra] Transaction result delivery failed: {}", e);
                        }
                    }
                    continue;
                }

                // Replicas are read-only: reject reducer calls until promoted.
                if voltra::replication::is_replica() {
                    let _enter = call_span.enter();
                    let resp = ReducerResponse::error(
                        call_id,
                        "This node is a read-only replica. Write to the primary, or promote this node via POST /replication/promote.".to_string(),
                    );
                    // Async context: bounded send() backpressures on this one
                    // connection without blocking the worker loop indefinitely
                    // (the loop only awaits this connection's own channel).
                    if let Err(e) = call.response_tx.send(resp).await { log::warn!("send response: {}", e); }
                    continue;
                }

                let caller_id    = call.caller_id.clone();
                let caller_role  = call.caller_role.clone();
                let call_tenant  = call.tenant_id.clone();
                let tables_blk   = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args         = call.args.clone();
                let ts           = current_timestamp_nanos();
                let schema_blk   = schema_w.clone();
                let ttl_blk      = ttl_w.clone();
                let tenant_blk   = tenant_w.clone();
                let call_start   = std::time::Instant::now();

                // Execute + commit with OCC conflict retry: when a concurrent
                // worker committed a row this reducer read AND writes, the
                // commit aborts and we re-execute against fresh state (max 5).
                // Zero silent lost updates in read-modify-write reducers.
                enum Outcome {
                    Done(Vec<u8>, Vec<voltra::table::RowDelta>),
                    ReducerErr(String),
                    Panicked,
                    CommitErr(String),
                }

                // `spawn_blocking` runs on a dedicated OS thread — a `tracing`
                // span isn't implicitly carried across that boundary the way
                // it is across a plain `.await`. Clone the span handle in and
                // `.in_scope()` it inside the closure so "dispatch/execute"
                // and "commit" (child spans) nest under the same call_id.
                let exec_span = call_span.clone();
                let blk = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || exec_span.in_scope(|| {
                        let mut ctx = ReducerContext::new(tables_blk, ts)
                            .with_schema(schema_blk)
                            .with_ttl(ttl_blk);
                        ctx.caller_id   = caller_id;
                        ctx.caller_role = caller_role;
                        if let Some(tid) = call_tenant {
                            ctx = ctx.with_tenant(tid, tenant_blk);
                        }
                        const MAX_CONFLICT_RETRIES: usize = 64;
                        let mut attempt = 0;
                        loop {
                            attempt += 1;
                            let exec = tracing::info_span!("dispatch", attempt).in_scope(|| {
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                    || registry_blk.execute(&reducer_name, &mut ctx, &args)
                                ))
                            });
                            break match exec {
                                Ok(Ok(result_bytes)) => {
                                    let commit_result = tracing::info_span!("commit", attempt)
                                        .in_scope(|| ctx.commit());
                                    match commit_result {
                                        Ok(deltas) => Outcome::Done(result_bytes, deltas),
                                        Err(voltra::error::VoltraError::TxnConflict(_))
                                            if attempt < MAX_CONFLICT_RETRIES =>
                                        {
                                            ctx.reset_for_retry();
                                            std::thread::yield_now();
                                            continue;
                                        }
                                        Err(e) => Outcome::CommitErr(e.to_string()),
                                    }
                                }
                                Ok(Err(e)) => Outcome::ReducerErr(e.to_string()),
                                Err(_) => Outcome::Panicked,
                            };
                        }
                    })),
                ).await;

                let response = match blk {
                    Err(_) => {
                        log::warn!("call_id={} timed out", call_id);
                        metrics_w.reducer_errors_total.inc();
                        ReducerResponse::error(call_id, "Reducer timed out".to_string())
                    }
                    Ok(Err(e)) => {
                        log::error!("Join error: {}", e);
                        metrics_w.reducer_errors_total.inc();
                        ReducerResponse::error(call_id, "Internal task error".to_string())
                    }
                    Ok(Ok(outcome)) => match outcome {
                        Outcome::Done(result_bytes, deltas) => {
                            // ── Single-node write path (commit already applied) ──────────────
                            // Fan out to live subscribers, then append to the WAL for crash
                            // recovery. Distribution/consensus was removed in Session 44.
                            let fanout_span = tracing::info_span!("subscription_fanout", rows = deltas.len());
                            let _fanout_enter = fanout_span.enter();
                            if !deltas.is_empty() {
                                subs_w.publish_deltas(&deltas);
                                // Fan out to cluster peers (fire-and-forget, no-op if single-node).
                                cluster_w.fanout_deltas(&deltas);
                            }
                            drop(_fanout_enter);
                            let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // Opt-in disk store write-through (before `deltas` is moved into
                            // the WAL entry). On a crash between here and the WAL write the row
                            // is still in the disk store, which is loaded before WAL replay.
                            if let Some(ref pe) = persist_w {
                                if let Err(e) = pe.persist_deltas(&deltas, seq_num) {
                                    log::warn!("[voltra] Disk persist failed: {}", e);
                                }
                            }
                            let wal_span = tracing::info_span!("wal_append", seq_num).entered();
                            // `deltas` is moved into the WAL entry (its last use) — no
                            // per-call clone of the delta vec.
                            let entry = WalEntry::new(ts, seq_num, call.reducer_name.clone(), call.args.clone(), deltas);
                            if let Err(e) = wal_w.append(&entry, seq_num) {
                                log::warn!("WAL append failed: {}", e);
                            } else {
                                metrics_w.wal_entries_written_total.inc();
                            }
                            drop(wal_span);
                            // Periodic snapshot + WAL rotation. Skip if a snapshot is
                            // already in flight — overlapping snapshots each clone the
                            // full dataset into memory and would compound, not bound it.
                            if snap_iv > 0 && (seq_num + 1).is_multiple_of(snap_iv)
                                && !snap_busy_w.swap(true, std::sync::atomic::Ordering::AcqRel)
                            {
                                let tbl = tables_w.clone(); let dir = snap_dir_ww.clone(); let ts2 = current_timestamp_nanos();
                                let dir_prune = snap_dir_ww.clone();
                                let wal_rotate = wal_w.clone();
                                let busy = snap_busy_w.clone();
                                tokio::spawn(async move {
                                    match tokio::task::spawn_blocking(move || save_snapshot(&tbl, &dir, seq_num, ts2)).await {
                                        Ok(Ok(())) => {
                                            log::info!("Snapshot written at seq {}", seq_num);
                                            if let Err(e) = wal_rotate.truncate_before(seq_num) {
                                                log::error!("WAL rotation after snapshot failed: {}", e);
                                            }
                                            // Prune older snapshot files — only the latest
                                            // is needed for recovery; without this they
                                            // accumulate on disk indefinitely over long runs.
                                            if let Ok(entries) = std::fs::read_dir(&dir_prune) {
                                                for entry in entries.flatten() {
                                                    let name = entry.file_name();
                                                    let name = name.to_string_lossy();
                                                    if let Some(seq_str) = name.strip_prefix("voltra_snapshot_").and_then(|s| s.strip_suffix(".bin")) {
                                                        if seq_str.parse::<u64>().map(|s| s < seq_num).unwrap_or(false) {
                                                            let _ = std::fs::remove_file(entry.path());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Ok(Err(e)) => log::error!("Snapshot failed: {}", e),
                                        Err(e)     => log::error!("Snapshot panicked: {}", e),
                                    }
                                    busy.store(false, std::sync::atomic::Ordering::Release);
                                });
                            }
                            // Record successful reducer call + duration.
                            metrics_w.reducer_calls_total.inc();
                            metrics_w.reducer_duration_seconds.observe(call_start.elapsed().as_secs_f64());
                            ReducerResponse::success(call_id, result_bytes)
                        }
                        Outcome::CommitErr(e) => {
                            log::error!("Commit failed call_id={}: {}", call_id, e);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, format!("Commit error: {}", e))
                        }
                        Outcome::ReducerErr(e) => {
                            log::warn!("Reducer error: {}", e);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, e)
                        }
                        Outcome::Panicked => {
                            log::warn!("Reducer panicked call_id={}", call_id);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, "Reducer panicked".to_string())
                        }
                    },
                };
                // Async context: bounded send() backpressures on this one
                // connection without stalling other workers or connections.
                if let Err(e) = call.response_tx.send(response).await { log::warn!("send response: {}", e); }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        }));
    }

    // ── Presence sweep background task ─────────────────────────────────────────
    let presence_handle = {
        let pres = presence.clone();
        let mut rx_pres = shutdown_rx.clone();
        let sweep_interval = if config.presence_heartbeat_timeout_ms > 0 {
            config.presence_heartbeat_timeout_ms / 2
        } else {
            30_000 // default to 30s if disabled (task will be a no-op)
        };
        tokio::spawn(async move {
            if sweep_interval == 0 {
                return;
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(sweep_interval.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let (newly_idle, removed) = pres.sweep(now_ms);
                        for uid in &newly_idle {
                            log::debug!("Presence: user '{}' is now idle", uid);
                        }
                        for uid in &removed {
                            log::debug!("Presence: user '{}' removed (offline timeout)", uid);
                        }
                    }
                    _ = rx_pres.changed() => break,
                }
            }
        })
    };

    // ── TTL sweep background task ────────────────────────────────────────────
    let ttl_handle = {
        let ttl_mgr = ttl_manager.clone();
        let tables_ttl = tables.clone();
        let subs_ttl = subscription_manager.clone();
        let mut rx_ttl = shutdown_rx.clone();
        let sweep_ms = config.ttl_sweep_interval_ms;
        tokio::spawn(async move {
            if sweep_ms == 0 {
                return;
            }
            let mut ticker = tokio::time::interval(Duration::from_millis(sweep_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let expired = ttl_mgr.collect_expired(now_ms);
                        if expired.is_empty() { continue; }
                        let mut deltas = Vec::new();
                        for entry in &expired {
                            match tables_ttl.delete_row(&entry.table_name, &entry.row_key) {
                                Ok(delta) => deltas.push(delta),
                                Err(e) => {
                                    log::warn!("TTL delete {}.{} failed: {}", entry.table_name, entry.row_key, e);
                                }
                            }
                        }
                        if !deltas.is_empty() {
                            log::debug!("TTL sweep: deleted {} expired rows", deltas.len());
                            subs_ttl.publish_deltas(&deltas);
                        }
                    }
                    _ = rx_ttl.changed() => break,
                }
            }
        })
    };

    let mut scheduler_handles = Vec::new();
    let sched_seq = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX / 2));
    for sched in &config.scheduled_reducers {
        let sched: ScheduledReducerConfig = sched.clone();
        let tx_sched = reducer_tx.clone();
        let seq_sched = sched_seq.clone();
        let mut rx_shutdown_sched = shutdown_rx.clone();
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
        scheduler_handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(sched.interval_ms.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let call_id = seq_sched.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // Single response expected — capacity 1 (see
                        // PendingCall::response_tx's bounded contract).
                        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<ReducerResponse>(1);
                        let call = PendingCall {
                            call_id,
                            reducer_name: sched.reducer.clone(),
                            args: args_bytes.clone(),
                            caller_id: "scheduler".to_string(),
                            caller_role: "scheduler".to_string(),
                            tenant_id: None,
                            lobby_hint: None,
                            enqueued_at: std::time::Instant::now(),
                            tx_batch: None,
                            tx_response_tx: None,
                            response_tx: resp_tx,
                        };
                        if tx_sched.send(call).await.is_ok() {
                            let name_c = sched.reducer.clone();
                            tokio::spawn(async move {
                                if let Some(resp) = resp_rx.recv().await {
                                    if !resp.success { log::warn!("Scheduler '{}' failed: {:?}", name_c, resp.error); }
                                }
                            });
                        } else { break; }
                    }
                    _ = rx_shutdown_sched.changed() => break,
                }
            }
        }));
    }

    // ── Periodic gauge-refresh task (every 5 s) ──────────────────────────────
    // Reads snapshot of current row count / subscription count / Raft state
    // and pushes them into the Prometheus gauges.  This is intentionally
    // separate from the hot path — no lock contention on the hot path.
    let gauge_handle = {
        let tables_g = tables.clone();
        let subs_g = subscription_manager.clone();
        let prom_g = metrics.clone();
        let mut rx_g = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Row count
                        prom_g.rows_total.set(tables_g.total_row_count() as i64);
                        // Subscription count
                        prom_g.subscriptions_active.set(
                            subs_g.active_subscriptions() as i64
                        );
                    }
                    _ = rx_g.changed() => break,
                }
            }
        })
    };

    tokio::signal::ctrl_c().await.ok();
    eprintln!("\n[voltra] Shutdown signal — draining...");
    log::info!("Shutdown signal received");

    // 1. Stop accepting new connections and signal all background tasks.
    let _ = shutdown_tx.send(());

    // 2. Drop the sender side of the reducer channel so workers drain and exit.
    drop(reducer_tx);

    // 3. Wait for all in-flight reducer workers to finish, with a 30-second deadline.
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

    // 4. Flush any buffered WAL entries to disk before shutting down the writer.
    if let Err(e) = wal_writer.flush().await {
        log::error!("WAL flush failed during shutdown: {}", e);
    }
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() {
            log::error!("WAL shutdown: {}", e);
        }
    }

    // 5. Await all remaining task handles (listener sends WebSocket Close frames).
    let _ = listener_handle.await;
    let _ = metrics_handle.await;
    let _ = presence_handle.await;
    let _ = ttl_handle.await;
    let _ = gauge_handle.await;

    eprintln!("[voltra] Shutdown complete.");
    log::info!("Shutdown complete");
    Ok(())
}
