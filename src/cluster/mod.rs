// ============================================================================
// src/cluster/mod.rs
//
// Horizontal-scaling cluster bus for NeonDB.
//
// Architecture
// ------------
//   Each NeonDB node owns one or more logical shards.  Nodes discover each
//   other via a static peer list supplied through environment variables:
//
//     NEONDB_PEERS=shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001
//
//   After a reducer commit on this node:
//     1. Committed RowDeltas are fanned out to ALL peer nodes via
//        POST /cluster/deltas  (fanout.rs).
//     2. Peer nodes apply the deltas to their own TableStore and push them
//        to their local subscribers.
//
//   If a reducer call arrives for a row that belongs to a different shard:
//     1. ClusterBus::proxy_call() forwards the call to the owning node via
//        POST /cluster/call  (proxy.rs).
//     2. The owning node executes, commits, and fans out deltas.
//
//   Gossip / heartbeat  (gossip.rs):
//     A background task pings each peer's GET /cluster/health endpoint every
//     NEONDB_GOSSIP_INTERVAL_MS (default 5 000 ms).  Dead peers are marked
//     unhealthy and skipped in fan-out until they recover.
//
// Security
// --------
//   All cluster-to-cluster HTTP requests carry the header
//     x-neondb-cluster-secret: <NEONDB_CLUSTER_SECRET>
//   The receiving node validates it before processing the request.
//   If NEONDB_CLUSTER_SECRET is not set, no secret is checked (dev mode).
//
// ============================================================================

pub mod fanout;
pub mod gossip;
pub mod proxy;

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use reqwest::blocking::Client as BlockingClient;
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Lazy global HTTP client.
//
// Why a static, not a field?
//   reqwest::blocking::Client owns a Tokio runtime. Constructing it inside an
//   async context (e.g. `async fn run_server`) is fine, but DROPPING it inside
//   an async context panics with "Cannot drop a runtime in a context where
//   blocking is not allowed".  Holding the client in a never-dropped static
//   sidesteps this entirely.  The client is built lazily on first use, which
//   happens inside `spawn_blocking` tasks in fanout/gossip/proxy — well clear
//   of any async stack.
// ─────────────────────────────────────────────────────────────────────────────

static GLOBAL_HTTP_CLIENT: OnceLock<BlockingClient> = OnceLock::new();

/// Returns the process-wide blocking HTTP client.  Constructed on first call
/// with `timeout_ms` (later callers' timeouts are ignored; the first wins).
pub(crate) fn global_http_client(timeout_ms: u64) -> &'static BlockingClient {
    GLOBAL_HTTP_CLIENT.get_or_init(|| {
        BlockingClient::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .unwrap_or_default()
    })
}

use crate::subscriptions::SubscriptionManager;
use crate::table::{RowDelta, TableStore};
use crate::error::{NeonDBError, Result};

// ─────────────────────────────────────────────────────────────────────────────
// NodeInfo — a single peer in the cluster
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Logical shard ID owned by this peer.
    pub shard_id: u32,
    /// Base URL of the peer's admin/metrics HTTP server, e.g. "http://10.0.0.2:3001".
    pub metrics_url: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// PeerHealth — mutable health state tracked by the gossip task
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PeerHealth {
    pub healthy: bool,
    pub last_seen: Option<Instant>,
    pub consecutive_failures: u32,
}

impl Default for PeerHealth {
    fn default() -> Self {
        Self { healthy: true, last_seen: None, consecutive_failures: 0 }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PeerEntry — combined node info + live health
// Exposed publicly so the GET /cluster/peers endpoint can iterate it.
// ─────────────────────────────────────────────────────────────────────────────

pub struct PeerEntry {
    pub node: NodeInfo,
    health: std::sync::Mutex<PeerHealth>,
}

impl PeerEntry {
    /// Public constructor — initialises health to default (healthy, no last_seen).
    /// Use this instead of constructing the struct literal, which would fail
    /// because `health` is private.
    pub fn new(node: NodeInfo) -> Self {
        Self {
            node,
            health: std::sync::Mutex::new(PeerHealth::default()),
        }
    }

    /// Returns true when the peer is currently considered healthy.
    pub fn is_healthy(&self) -> bool {
        self.health.lock().map(|h| h.healthy).unwrap_or(true)
    }

    fn mark_healthy(&self) {
        if let Ok(mut h) = self.health.lock() {
            h.healthy = true;
            h.last_seen = Some(Instant::now());
            h.consecutive_failures = 0;
        }
    }

    fn mark_failure(&self) -> bool {
        if let Ok(mut h) = self.health.lock() {
            h.consecutive_failures += 1;
            if h.consecutive_failures >= 3 {
                h.healthy = false;
            }
            return !h.healthy;
        }
        false
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ClusterConfig — loaded from environment at startup
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Whether clustering is enabled (true when NEONDB_PEERS is non-empty).
    pub enabled: bool,
    /// This node's shard ID.
    pub my_shard_id: u32,
    /// Total shards in the cluster.
    pub shard_count: u32,
    /// Known peers (not including self).
    pub peers: Vec<NodeInfo>,
    /// Shared secret for cluster authentication (from NEONDB_CLUSTER_SECRET).
    pub cluster_secret: Option<String>,
    /// Gossip interval in milliseconds (default 5 000).
    pub gossip_interval_ms: u64,
    /// HTTP timeout for cluster calls in milliseconds (default 2 000).
    pub http_timeout_ms: u64,
}

impl ClusterConfig {
    /// Build ClusterConfig from environment variables.
    ///
    /// `NEONDB_PEERS` format (comma-separated):
    ///   `shard<ID>=<metrics_url>`
    ///   e.g.  `NEONDB_PEERS=shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001`
    ///
    /// Simple URL list (shard IDs assigned by position, 0 = self):
    ///   `http://10.0.0.2:3001,http://10.0.0.3:3001`
    pub fn from_env(my_shard_id: u32, shard_count: u32) -> Self {
        let cluster_secret = env::var("NEONDB_CLUSTER_SECRET").ok();
        let gossip_interval_ms = env::var("NEONDB_GOSSIP_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        let http_timeout_ms = env::var("NEONDB_CLUSTER_HTTP_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000);

        let peers_raw = env::var("NEONDB_PEERS").unwrap_or_default();
        let peers_raw = peers_raw.trim().to_string();

        if peers_raw.is_empty() {
            return ClusterConfig {
                enabled: false,
                my_shard_id,
                shard_count,
                peers: vec![],
                cluster_secret,
                gossip_interval_ms,
                http_timeout_ms,
            };
        }

        let peers = Self::parse_peers(&peers_raw, my_shard_id);
        let enabled = !peers.is_empty();

        // Security warning: clustering enabled without a shared secret leaves
        // every /cluster/* endpoint unauthenticated.  Any host that can reach
        // the metrics port can write arbitrary rows via /cluster/deltas.
        if enabled && cluster_secret.is_none() {
            log::warn!(
                "SECURITY WARNING: clustering is enabled (NEONDB_PEERS is set) but \
                 NEONDB_CLUSTER_SECRET is not set — peer endpoints (/cluster/deltas, \
                 /cluster/call, /cluster/health) are unauthenticated and will accept \
                 requests from any host that can reach this node's metrics port. \
                 Set NEONDB_CLUSTER_SECRET=<long-random-secret> on every node before \
                 deploying to production."
            );
        }

        ClusterConfig {
            enabled,
            my_shard_id,
            shard_count,
            peers,
            cluster_secret,
            gossip_interval_ms,
            http_timeout_ms,
        }
    }

    pub(crate) fn parse_peers(raw: &str, my_shard_id: u32) -> Vec<NodeInfo> {
        let mut peers = Vec::new();
        for (idx, part) in raw.split(',').enumerate() {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            // Named format: shard1=http://...
            if let Some(eq) = part.find('=') {
                let key = &part[..eq];
                let url = part[eq + 1..].to_string();
                if let Some(id_str) = key.strip_prefix("shard") {
                    if let Ok(shard_id) = id_str.parse::<u32>() {
                        if shard_id != my_shard_id {
                            peers.push(NodeInfo { shard_id, metrics_url: url });
                        }
                        continue;
                    }
                }
                let shard_id = idx as u32;
                if shard_id != my_shard_id {
                    peers.push(NodeInfo { shard_id, metrics_url: url });
                }
            } else {
                // Plain URL: assign IDs by position
                let shard_id = idx as u32;
                if shard_id != my_shard_id {
                    peers.push(NodeInfo { shard_id, metrics_url: part.to_string() });
                }
            }
        }
        peers
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ClusterBus — the main handle used everywhere in the server
// ─────────────────────────────────────────────────────────────────────────────

pub struct ClusterBus {
    pub config: ClusterConfig,
    /// Per-peer entry (node info + health), keyed by shard_id.
    /// Public so the GET /cluster/peers HTTP endpoint can iterate it.
    pub peers: Arc<DashMap<u32, PeerEntry>>,
}

impl ClusterBus {
    pub fn new(config: ClusterConfig) -> Arc<Self> {
        let peers: Arc<DashMap<u32, PeerEntry>> = Arc::new(DashMap::new());
        for peer in &config.peers {
            peers.insert(peer.shard_id, PeerEntry::new(peer.clone()));
        }

        Arc::new(ClusterBus { config, peers })
    }

    // ── Active check ─────────────────────────────────────────────────────────

    pub fn is_active(&self) -> bool {
        self.config.enabled && !self.peers.is_empty()
    }

    // ── Secret validation ────────────────────────────────────────────────────

    pub fn validate_secret(&self, provided: Option<&str>) -> bool {
        match &self.config.cluster_secret {
            None => true,
            Some(expected) => provided.map(|v| v == expected).unwrap_or(false),
        }
    }

    // ── Secret header helper ─────────────────────────────────────────────────

    pub fn secret_header(&self) -> Option<(&'static str, String)> {
        self.config.cluster_secret.as_ref().map(|s| ("x-neondb-cluster-secret", s.clone()))
    }

    // ── Health state (called by gossip task) ─────────────────────────────────

    pub fn mark_healthy(&self, shard_id: u32) {
        if let Some(entry) = self.peers.get(&shard_id) {
            entry.mark_healthy();
        }
    }

    pub fn mark_unhealthy(&self, shard_id: u32) {
        if let Some(entry) = self.peers.get(&shard_id) {
            let became_unhealthy = entry.mark_failure();
            if became_unhealthy {
                log::warn!("[cluster] shard{} marked unhealthy after 3 consecutive failures", shard_id);
            }
        }
    }

    pub fn http_client(&self) -> &'static BlockingClient {
        global_http_client(self.config.http_timeout_ms)
    }

    pub fn healthy_peers(&self) -> Vec<NodeInfo> {
        self.peers
            .iter()
            .filter(|e| e.value().is_healthy())
            .map(|e| e.value().node.clone())
            .collect()
    }

    // ── Delta fan-out ─────────────────────────────────────────────────────────

    pub fn fanout_deltas(self: &Arc<Self>, deltas: &[RowDelta]) {
        if !self.is_active() || deltas.is_empty() {
            return;
        }
        fanout::fanout_to_peers(self, deltas);
    }

    // ── Apply incoming peer deltas ────────────────────────────────────────────

    pub fn apply_peer_deltas(
        deltas: &[RowDelta],
        tables: &Arc<TableStore>,
        subs: &Arc<SubscriptionManager>,
    ) -> Result<()> {
        for delta in deltas {
            tables.apply_delta(delta)?;
        }
        subs.publish_deltas(deltas);
        Ok(())
    }

    // ── Proxy a reducer call to the owning shard ──────────────────────────────

    pub fn proxy_call(
        self: &Arc<Self>,
        target_shard_id: u32,
        reducer_name: &str,
        args: &[u8],
        caller_id: &str,
        caller_role: &str,
    ) -> Result<Vec<u8>> {
        let entry = self.peers.get(&target_shard_id);
        let node = match entry {
            Some(ref e) => e.value().node.clone(),
            None => {
                return Err(NeonDBError::internal(format!(
                    "[cluster] No peer found for shard {}",
                    target_shard_id
                )))
            }
        };
        proxy::proxy_call(self, &node, reducer_name, args, caller_id, caller_role)
    }
}

// ClusterBus is safe to share across threads — DashMap + Arc internals.
unsafe impl Send for ClusterBus {}
unsafe impl Sync for ClusterBus {}

// ─────────────────────────────────────────────────────────────────────────────
// Shard routing helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Determine which shard owns a given row key using a stable FNV-1a hash.
///
/// This function must produce identical results on every node in the cluster.
/// All nodes use the same `shard_count`, so they always agree on ownership.
///
/// # Example
/// ```
/// use neondb::cluster::shard_for_key;
/// assert_eq!(shard_for_key("alice", 3), shard_for_key("alice", 3));
/// ```
pub fn shard_for_key(key: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    // FNV-1a 64-bit
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in key.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    (hash % u64::from(shard_count)) as u32
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── shard_for_key ─────────────────────────────────────────────────────────

    #[test]
    fn shard_for_key_single_node_always_zero() {
        assert_eq!(shard_for_key("alice", 1), 0);
        assert_eq!(shard_for_key("bob", 1), 0);
        assert_eq!(shard_for_key("", 1), 0);
    }

    #[test]
    fn shard_for_key_zero_count_treated_as_single() {
        // shard_count=0 is nonsensical; function should not panic.
        assert_eq!(shard_for_key("alice", 0), 0);
    }

    #[test]
    fn shard_for_key_deterministic() {
        // Same key + count must always produce the same shard.
        for key in ["alice", "bob", "player_001", "", "zone_0_0"] {
            let a = shard_for_key(key, 4);
            let b = shard_for_key(key, 4);
            assert_eq!(a, b, "shard_for_key must be deterministic for key={:?}", key);
        }
    }

    #[test]
    fn shard_for_key_output_in_range() {
        // Every result must be < shard_count.
        let shard_count = 5u32;
        for key in ["a", "b", "abc", "hello_world", "123456789"] {
            let s = shard_for_key(key, shard_count);
            assert!(
                s < shard_count,
                "shard {} out of range for shard_count={}",
                s, shard_count
            );
        }
    }

    #[test]
    fn shard_for_key_distributes_across_shards() {
        // With enough distinct keys, we should see more than one shard used.
        let shard_count = 4u32;
        let shards_seen: std::collections::HashSet<u32> = (0..200)
            .map(|i| shard_for_key(&format!("key_{}", i), shard_count))
            .collect();
        assert!(
            shards_seen.len() > 1,
            "FNV hash should distribute keys across multiple shards"
        );
    }

    // ── ClusterConfig::from_env / parse_peers ────────────────────────────────

    #[test]
    fn cluster_config_no_peers_is_disabled() {
        // Parsing an empty peer list must produce an inactive config.
        // We can't guarantee NEONDB_PEERS is unset in all CI envs,
        // so just check that parse_peers works correctly for the empty string.
        let _cfg = ClusterConfig::from_env(0, 1);
        let peers = ClusterConfig::parse_peers("", 0);
        assert!(peers.is_empty(), "empty peer string should produce no peers");
    }

    #[test]
    fn cluster_config_named_format_parses_correctly() {
        let raw = "shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001";
        let peers = ClusterConfig::parse_peers(raw, 0); // my_shard_id=0, so both are peers
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].shard_id, 1);
        assert_eq!(peers[0].metrics_url, "http://10.0.0.2:3001");
        assert_eq!(peers[1].shard_id, 2);
        assert_eq!(peers[1].metrics_url, "http://10.0.0.3:3001");
    }

    #[test]
    fn cluster_config_skips_self_in_named_format() {
        // my_shard_id=1 — shard1 entry should be filtered out.
        let raw = "shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001";
        let peers = ClusterConfig::parse_peers(raw, 1);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].shard_id, 2);
    }

    #[test]
    fn cluster_config_plain_url_format_parses_correctly() {
        // Plain URL list — shard IDs assigned by position.
        let raw = "http://node-a:3001,http://node-b:3001,http://node-c:3001";
        let peers = ClusterConfig::parse_peers(raw, 0); // skip position 0 (self)
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].shard_id, 1);
        assert_eq!(peers[0].metrics_url, "http://node-b:3001");
    }

    #[test]
    fn cluster_config_ignores_trailing_commas() {
        let raw = "shard1=http://10.0.0.2:3001,";
        let peers = ClusterConfig::parse_peers(raw, 0);
        assert_eq!(peers.len(), 1);
    }

    // ── ClusterBus::validate_secret ──────────────────────────────────────────

    #[test]
    fn validate_secret_no_secret_configured_always_passes() {
        let cfg = ClusterConfig {
            enabled: true,
            my_shard_id: 0,
            shard_count: 1,
            peers: vec![],
            cluster_secret: None,
            gossip_interval_ms: 5000,
            http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(bus.validate_secret(None));
        assert!(bus.validate_secret(Some("anything")));
    }

    #[test]
    fn validate_secret_correct_secret_passes() {
        let cfg = ClusterConfig {
            enabled: true,
            my_shard_id: 0,
            shard_count: 1,
            peers: vec![],
            cluster_secret: Some("s3cr3t".to_string()),
            gossip_interval_ms: 5000,
            http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(bus.validate_secret(Some("s3cr3t")));
    }

    #[test]
    fn validate_secret_wrong_secret_rejected() {
        let cfg = ClusterConfig {
            enabled: true,
            my_shard_id: 0,
            shard_count: 1,
            peers: vec![],
            cluster_secret: Some("s3cr3t".to_string()),
            gossip_interval_ms: 5000,
            http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(!bus.validate_secret(None));
        assert!(!bus.validate_secret(Some("wrong")));
    }

    // ── healthy_peers ────────────────────────────────────────────────────────

    #[test]
    fn healthy_peers_excludes_unhealthy_nodes() {
        let cfg = ClusterConfig {
            enabled: true,
            my_shard_id: 0,
            shard_count: 3,
            peers: vec![
                NodeInfo { shard_id: 1, metrics_url: "http://node1:3001".to_string() },
                NodeInfo { shard_id: 2, metrics_url: "http://node2:3001".to_string() },
            ],
            cluster_secret: None,
            gossip_interval_ms: 5000,
            http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);

        // Both start healthy — should see 2.
        assert_eq!(bus.healthy_peers().len(), 2);

        // Mark shard1 unhealthy 3 times.
        bus.mark_unhealthy(1);
        bus.mark_unhealthy(1);
        bus.mark_unhealthy(1);

        let healthy = bus.healthy_peers();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].shard_id, 2);
    }

    #[test]
    fn mark_healthy_recovers_unhealthy_peer() {
        let cfg = ClusterConfig {
            enabled: true,
            my_shard_id: 0,
            shard_count: 2,
            peers: vec![
                NodeInfo { shard_id: 1, metrics_url: "http://node1:3001".to_string() },
            ],
            cluster_secret: None,
            gossip_interval_ms: 5000,
            http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);

        bus.mark_unhealthy(1);
        bus.mark_unhealthy(1);
        bus.mark_unhealthy(1);
        assert_eq!(bus.healthy_peers().len(), 0, "should be unhealthy after 3 failures");

        bus.mark_healthy(1);
        assert_eq!(bus.healthy_peers().len(), 1, "should recover after mark_healthy");
    }
}
