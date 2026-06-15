use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AOrdering};
use std::collections::HashMap;

use dashmap::DashMap;
use serde::Serialize;

use crate::network::PendingCall;

/// Lightweight per-lobby runtime statistics collected by the dedicated worker.
///
/// All fields are atomically updated; no locking needed. Min/max/avg are
/// approximate (race windows are harmless — we accept occasional stale reads).
pub struct LobbyStats {
    pub calls: AtomicU64,
    pub errors: AtomicU64,
    /// Sum of all call latencies in nanoseconds (for mean calculation).
    pub total_latency_ns: AtomicU64,
    /// Minimum observed call latency in nanoseconds (u64::MAX if no calls yet).
    pub min_latency_ns: AtomicU64,
    /// Maximum observed call latency in nanoseconds.
    pub max_latency_ns: AtomicU64,
}

impl LobbyStats {
    fn new() -> Arc<Self> {
        Arc::new(LobbyStats {
            calls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
        })
    }

    fn record_call(&self, latency_ns: u64) {
        self.calls.fetch_add(1, AOrdering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, AOrdering::Relaxed);
        // Optimistic update for min — occasional stale races are acceptable.
        let old = self.min_latency_ns.load(AOrdering::Relaxed);
        if latency_ns < old {
            let _ = self.min_latency_ns.compare_exchange_weak(
                old, latency_ns, AOrdering::Relaxed, AOrdering::Relaxed,
            );
        }
        let old = self.max_latency_ns.load(AOrdering::Relaxed);
        if latency_ns > old {
            let _ = self.max_latency_ns.compare_exchange_weak(
                old, latency_ns, AOrdering::Relaxed, AOrdering::Relaxed,
            );
        }
    }
}

/// A point-in-time snapshot of one lobby's stats — serialisable to JSON.
#[derive(Serialize)]
pub struct LobbySnapshot {
    pub lobby_id: String,
    pub queue_depth: usize,
    pub calls_total: u64,
    pub errors_total: u64,
    pub avg_latency_ms: f64,
    pub min_latency_ms: f64,
    pub max_latency_ms: f64,
}

/// Per-lobby worker routing.
///
/// Calls with a `lobby_hint` are dispatched to a dedicated channel (and thus a
/// dedicated OS thread) for that lobby.  Calls without a hint go to the global
/// channel and are handled by the shared worker pool.
///
/// This eliminates cross-lobby contention: lobby 0's heavy combat reducer
/// cannot block lobby 1's position updates.
pub struct LobbyRouter {
    /// lobby_id → dedicated sender.  Created lazily on first call for a lobby.
    lobby_channels: DashMap<String, kanal::AsyncSender<PendingCall>>,
    /// Global pool for non-lobby calls.
    global_tx: kanal::AsyncSender<PendingCall>,
    /// Per-lobby channel capacity.
    channel_cap: usize,
    /// Maximum number of lobby workers (soft cap — prevents unbounded thread creation).
    max_lobbies: usize,
    /// Handles to lobby worker threads (for stats / shutdown).
    worker_handles: DashMap<String, std::thread::JoinHandle<()>>,
    /// Shared state needed by each lobby worker.
    worker_deps: Arc<WorkerDeps>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Receiver<()>,
    /// Available CPU core IDs for pinning lobby threads.
    /// Empty on platforms where core_affinity isn't supported.
    core_ids: Vec<core_affinity::CoreId>,
    /// Round-robin counter for core assignment.
    next_core: Arc<AtomicUsize>,
    /// Per-lobby runtime statistics (created when the lobby worker spawns).
    lobby_stats: DashMap<String, Arc<LobbyStats>>,
}

/// All the shared dependencies a lobby worker thread needs.
pub struct WorkerDeps {
    pub tables: Arc<crate::table::TableStore>,
    pub registry: Arc<crate::reducer::ReducerRegistry>,
    pub subscription_manager: Arc<crate::subscriptions::SubscriptionManager>,
    pub wal_writer: Arc<crate::wal::BatchedWalWriter>,
    pub global_seq: Arc<std::sync::atomic::AtomicU64>,
    pub schema_registry: Arc<crate::schema::SchemaRegistry>,
    pub ttl_manager: Arc<crate::ttl::TtlManager>,
    pub tenant_registry: Arc<crate::tenant::TenantRegistry>,
    pub cluster_bus: Arc<crate::cluster::ClusterBus>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub timeout_ms: u64,
    pub snapshot_interval: u64,
    pub snapshot_dir: std::path::PathBuf,
}

impl LobbyRouter {
    pub fn new(
        global_tx: kanal::AsyncSender<PendingCall>,
        channel_cap: usize,
        max_lobbies: usize,
        worker_deps: Arc<WorkerDeps>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if !core_ids.is_empty() {
            log::info!("[worker_pool] Core pinning enabled — {} logical cores available", core_ids.len());
        }
        LobbyRouter {
            lobby_channels: DashMap::new(),
            global_tx,
            channel_cap,
            max_lobbies,
            worker_handles: DashMap::new(),
            worker_deps,
            shutdown,
            core_ids,
            next_core: Arc::new(AtomicUsize::new(0)),
            lobby_stats: DashMap::new(),
        }
    }

    /// Pick the next core ID for a new lobby worker (round-robin).
    fn next_core_id(&self) -> Option<core_affinity::CoreId> {
        if self.core_ids.is_empty() { return None; }
        let idx = self.next_core.fetch_add(1, AOrdering::Relaxed) % self.core_ids.len();
        Some(self.core_ids[idx])
    }

    /// Route a `PendingCall` to the appropriate channel.
    ///
    /// If `call.lobby_hint` is set and we haven't exceeded `max_lobbies`,
    /// the call goes to a dedicated per-lobby worker.  Otherwise it goes
    /// to the global pool.
    pub async fn dispatch(&self, call: PendingCall) -> std::result::Result<(), String> {
        let lobby_id = match &call.lobby_hint {
            Some(lid) if !lid.is_empty() => lid.clone(),
            _ => {
                self.global_tx.send(call).await.map_err(|_| "global channel closed".to_string())?;
                return Ok(());
            }
        };

        // Fast path: lobby channel already exists.
        if let Some(tx) = self.lobby_channels.get(&lobby_id) {
            if tx.try_send(call).is_ok() {
                return Ok(());
            }
            // Channel full — the call was consumed by try_send's error.
            // This shouldn't happen in practice because the lobby worker drains
            // fast. Log and drop the call (it's already gone).
            log::warn!("Lobby {} channel full, call dropped", lobby_id);
            return Err("lobby channel full".to_string());
        }

        // Slow path: create a new lobby worker if under the cap.
        if self.lobby_channels.len() >= self.max_lobbies {
            // Over the soft cap — route to global pool.
            self.global_tx.send(call).await.map_err(|_| "global channel closed".to_string())?;
            return Ok(());
        }

        let (tx, rx) = kanal::bounded_async::<PendingCall>(self.channel_cap);
        self.lobby_channels.insert(lobby_id.clone(), tx.clone());

        let stats = LobbyStats::new();
        self.lobby_stats.insert(lobby_id.clone(), stats.clone());

        // Spawn a dedicated OS thread for this lobby, pinned to a core.
        let deps = self.worker_deps.clone();
        let lid = lobby_id.clone();
        let mut shut = self.shutdown.clone();
        let core_id = self.next_core_id();
        let handle = std::thread::Builder::new()
            .name(format!("lobby-{}", lid))
            .spawn(move || {
                lobby_worker_loop(&lid, rx, deps, &mut shut, core_id, stats);
            })
            .expect("Failed to spawn lobby worker thread");

        self.worker_handles.insert(lobby_id.clone(), handle);

        // Now send the call through the freshly created channel.
        tx.send(call).await.map_err(|_| "lobby channel closed".to_string())?;
        Ok(())
    }

    /// Number of active lobby worker threads.
    pub fn active_lobby_count(&self) -> usize {
        self.lobby_channels.len()
    }

    /// Synchronous try-send variant for use in the WebSocket handler.
    ///
    /// Returns `true` if the call was routed successfully, `false` if
    /// the global channel and lobby channel are both full/closed.
    pub fn try_dispatch(&self, call: PendingCall) -> bool {
        let lobby_id = match &call.lobby_hint {
            Some(lid) if !lid.is_empty() => lid.clone(),
            _ => return self.global_tx.try_send(call).is_ok(),
        };

        // Fast path: lobby channel exists.
        if let Some(tx) = self.lobby_channels.get(&lobby_id) {
            if tx.try_send(call).is_ok() {
                return true;
            }
            return false;
        }

        // Over the cap — fall back to global.
        if self.lobby_channels.len() >= self.max_lobbies {
            return self.global_tx.try_send(call).is_ok();
        }

        // Create a new lobby worker, pinned to a core.
        let (tx, rx) = kanal::bounded_async::<PendingCall>(self.channel_cap);
        self.lobby_channels.insert(lobby_id.clone(), tx.clone());

        let stats = LobbyStats::new();
        self.lobby_stats.insert(lobby_id.clone(), stats.clone());

        let deps = self.worker_deps.clone();
        let lid = lobby_id.clone();
        let mut shut = self.shutdown.clone();
        let core_id = self.next_core_id();
        let handle = std::thread::Builder::new()
            .name(format!("lobby-{}", lid))
            .spawn(move || {
                lobby_worker_loop(&lid, rx, deps, &mut shut, core_id, stats);
            })
            .expect("Failed to spawn lobby worker thread");

        self.worker_handles.insert(lobby_id, handle);
        tx.try_send(call).is_ok()
    }

    /// Stats: lobby IDs with their queue depths.
    pub fn lobby_queue_depths(&self) -> HashMap<String, usize> {
        self.lobby_channels
            .iter()
            .map(|e| (e.key().clone(), e.value().len()))
            .collect()
    }

    /// Snapshot of all active lobbies for the admin dashboard.
    pub fn lobbies_snapshot(&self) -> Vec<LobbySnapshot> {
        let mut out: Vec<LobbySnapshot> = self.lobby_stats
            .iter()
            .map(|e| {
                let id = e.key().clone();
                let st = e.value().clone();
                let calls = st.calls.load(AOrdering::Relaxed);
                let errors = st.errors.load(AOrdering::Relaxed);
                let total_ns = st.total_latency_ns.load(AOrdering::Relaxed);
                let min_ns = st.min_latency_ns.load(AOrdering::Relaxed);
                let max_ns = st.max_latency_ns.load(AOrdering::Relaxed);
                let queue = self.lobby_channels.get(&id).map(|c| c.len()).unwrap_or(0);

                let avg_ms = if calls > 0 { (total_ns / calls) as f64 / 1_000_000.0 } else { 0.0 };
                let min_ms = if min_ns == u64::MAX { 0.0 } else { min_ns as f64 / 1_000_000.0 };
                let max_ms = max_ns as f64 / 1_000_000.0;

                LobbySnapshot {
                    lobby_id: id,
                    queue_depth: queue,
                    calls_total: calls,
                    errors_total: errors,
                    avg_latency_ms: avg_ms,
                    min_latency_ms: min_ms,
                    max_latency_ms: max_ms,
                }
            })
            .collect();
        out.sort_by(|a, b| a.lobby_id.cmp(&b.lobby_id));
        out
    }
}

/// The worker loop for a single lobby's dedicated thread.
///
/// Identical logic to the global worker in server.rs, but runs on a dedicated
/// OS thread. Uses a blocking runtime handle to bridge async channel receives.
fn lobby_worker_loop(
    lobby_id: &str,
    rx: kanal::AsyncReceiver<PendingCall>,
    deps: Arc<WorkerDeps>,
    shutdown: &mut tokio::sync::watch::Receiver<()>,
    core_id: Option<core_affinity::CoreId>,
    stats: Arc<LobbyStats>,
) {
    // Pin this thread to a dedicated CPU core — prevents cache invalidation
    // from OS migration and eliminates cross-core latency variance per lobby.
    if let Some(id) = core_id {
        if core_affinity::set_for_current(id) {
            log::debug!("[lobby-{}] Pinned to core {:?}", lobby_id, id);
        }
    }
    use crate::network::ReducerResponse;
    use crate::reducer::ReducerContext;
    use crate::wal::WalEntry;
    use std::sync::atomic::Ordering;

    let rt = tokio::runtime::Handle::current();
    const DRAIN_LIMIT: usize = 15;

    loop {
        // Block until a call arrives or shutdown fires.
        let call: PendingCall = match rt.block_on(async {
            tokio::select! {
                c = rx.recv() => c.ok(),
                _ = shutdown.changed() => None,
            }
        }) {
            Some(c) => c,
            None => break,
        };

        // Drain up to DRAIN_LIMIT more queued calls.
        let mut batch: smallvec::SmallVec<[PendingCall; 16]> = smallvec::smallvec![call];
        for _ in 0..DRAIN_LIMIT {
            match rt.block_on(tokio::time::timeout(
                std::time::Duration::ZERO,
                rx.recv(),
            )) {
                Ok(Ok(extra)) => batch.push(extra),
                _ => break,
            }
        }

        for call in batch {
            let call_id = call.call_id;

            if crate::replication::is_replica() {
                let resp = ReducerResponse::error(
                    call_id,
                    "This node is a read-only replica.".to_string(),
                );
                let _ = call.response_tx.send(resp);
                continue;
            }

            let ts = now_nanos();
            let mut ctx = ReducerContext::new(deps.tables.clone(), ts)
                .with_schema(deps.schema_registry.clone())
                .with_ttl(deps.ttl_manager.clone());
            ctx.caller_id = call.caller_id.clone();
            ctx.caller_role = call.caller_role.clone();

            const MAX_CONFLICT_RETRIES: usize = 64;
            let mut attempt = 0;
            let call_start = std::time::Instant::now();
            let response = loop {
                attempt += 1;
                let exec = deps.registry.execute(&call.reducer_name, &mut ctx, &call.args);

                break match exec {
                    Err(e) => {
                        ctx.rollback();
                        deps.metrics.reducer_errors_total.inc();
                        stats.errors.fetch_add(1, AOrdering::Relaxed);
                        ReducerResponse::error(call_id, e.to_string())
                    }
                    Ok(result_bytes) => match ctx.commit() {
                        Err(crate::error::NeonDBError::TxnConflict(_))
                            if attempt < MAX_CONFLICT_RETRIES =>
                        {
                            ctx.reset_for_retry();
                            std::thread::yield_now();
                            continue;
                        }
                        Err(e) => {
                            log::error!(
                                "[lobby-{}] Commit failed for '{}': {}",
                                lobby_id, call.reducer_name, e
                            );
                            deps.metrics.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, format!("Commit error: {}", e))
                        }
                        Ok(deltas) => {
                            if !deltas.is_empty() {
                                deps.subscription_manager.publish_deltas(&deltas);
                                deps.cluster_bus.fanout_deltas(&deltas);
                            }

                            let seq_num =
                                deps.global_seq.fetch_add(1, Ordering::Relaxed);
                            let entry = WalEntry::new(
                                ts,
                                seq_num,
                                call.reducer_name.clone(),
                                call.args.clone(),
                                deltas.clone(),
                            );
                            if let Err(e) = deps.wal_writer.append(&entry, seq_num) {
                                log::warn!("[lobby-{}] WAL append failed: {}", lobby_id, e);
                            } else {
                                deps.metrics.wal_entries_written_total.inc();
                            }

                            deps.metrics.reducer_calls_total.inc();
                            deps.metrics
                                .reducer_duration_seconds
                                .observe(call_start.elapsed().as_secs_f64());
                            stats.record_call(call_start.elapsed().as_nanos() as u64);
                            ReducerResponse::success(call_id, result_bytes)
                        }
                    },
                };
            };

            if let Err(e) = call.response_tx.send(response) {
                log::warn!("[lobby-{}] Response delivery failed: {}", lobby_id, e);
            }
        }
    }

    log::debug!("Lobby worker {} stopped", lobby_id);
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use crate::table::parse_lobby_key;

    #[test]
    fn lobby_hint_extraction() {
        assert_eq!(parse_lobby_key("l0_p123"), Some(("0".into(), "p123".into())));
        assert_eq!(parse_lobby_key("l42_sim_players"), Some(("42".into(), "sim_players".into())));
        assert_eq!(parse_lobby_key("global_table"), None);
        assert_eq!(parse_lobby_key("__tenants"), None);
        assert_eq!(parse_lobby_key("l_missing_digits"), None);
    }
}
