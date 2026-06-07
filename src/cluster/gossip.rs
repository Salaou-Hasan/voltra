// ============================================================================
// src/cluster/gossip.rs
//
// Background gossip/heartbeat task.
//
// Pings each peer's GET /cluster/health endpoint every
// NEONDB_GOSSIP_INTERVAL_MS (default 5 000 ms).
//
// On success:  calls ClusterBus::mark_healthy(shard_id).
// On failure:  calls ClusterBus::mark_unhealthy(shard_id);
//              after 3 consecutive failures the peer is marked unhealthy
//              and skipped in fan-out until it recovers.
// ============================================================================

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;

use super::ClusterBus;

/// Spawn the gossip heartbeat task.
/// Returns a JoinHandle that resolves when `shutdown` fires.
pub fn start_gossip(bus: Arc<ClusterBus>, mut shutdown: watch::Receiver<()>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !bus.is_active() {
            log::debug!("[cluster/gossip] Single-node mode — gossip disabled");
            return;
        }

        let interval_ms = bus.config.gossip_interval_ms;
        let mut ticker = time::interval(Duration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        log::info!(
            "[cluster/gossip] Started — pinging {} peer(s) every {}ms",
            bus.config.peers.len(),
            interval_ms
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    ping_all_peers(&bus).await;
                }
                _ = shutdown.changed() => {
                    log::info!("[cluster/gossip] Shutdown received — stopping");
                    break;
                }
            }
        }
    })
}

async fn ping_all_peers(bus: &Arc<ClusterBus>) {
    let peers: Vec<_> = bus.peers.iter().map(|e| (e.key().clone(), e.value().node.clone())).collect();

    for (shard_id, node) in peers {
        let bus_c = bus.clone();

        tokio::task::spawn_blocking(move || {
            let url = format!("{}/cluster/health", node.metrics_url);
            let mut req = bus_c.http_client().get(&url);
            if let Some((name, value)) = bus_c.secret_header() {
                req = req.header(name, value);
            }

            match req.send() {
                Ok(resp) if resp.status().is_success() => {
                    bus_c.mark_healthy(shard_id);
                    log::debug!(
                        "[cluster/gossip] shard{} OK ({})",
                        shard_id,
                        node.metrics_url
                    );
                }
                Ok(resp) => {
                    let status = resp.status();
                    bus_c.mark_unhealthy(shard_id);
                    log::warn!(
                        "[cluster/gossip] shard{} unhealthy — HTTP {}",
                        shard_id,
                        status
                    );
                }
                Err(e) => {
                    bus_c.mark_unhealthy(shard_id);
                    log::warn!(
                        "[cluster/gossip] shard{} unreachable — {}",
                        shard_id,
                        e
                    );
                }
            }
        });
    }
}
