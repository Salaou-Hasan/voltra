// ============================================================================
// src/cluster/mod.rs — Horizontal-scaling cluster bus for NeonDB
//
// Each NeonDB node owns one or more logical shards. Nodes discover each
// other via a static peer list supplied through environment variables:
//
//   NEONDB_PEERS=shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001
//
// After a reducer commit on this node:
//   1. Committed RowDeltas are fanned out to ALL peer nodes via
//      POST /cluster/deltas  (fanout.rs).
//   2. Peer nodes apply the deltas to their own TableStore and push them
//      to their local subscribers.
//
// If a reducer call arrives for a row that belongs to a different shard:
//   1. ClusterBus::proxy_call() forwards the call to the owning node via
//      POST /cluster/call  (proxy.rs).
//   2. The owning node executes, commits, and fans out deltas.
//
// Gossip / heartbeat (gossip.rs):
//   A background task pings each peer's GET /cluster/health endpoint every
//   NEONDB_GOSSIP_INTERVAL_MS (default 5 000 ms). Dead peers are marked
//   unhealthy and skipped in fan-out until they recover.
//
// Security:
//   All cluster-to-cluster HTTP requests carry the header
//     x-neondb-cluster-secret: <NEONDB_CLUSTER_SECRET>
//   The receiving node validates it before processing the request.
//   If NEONDB_CLUSTER_SECRET is not set, no secret is checked (dev mode).
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

use crate::subscriptions::SubscriptionManager;
use crate::table::{RowDelta, TableStore};
use crate::error::{NeonDBError, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Lazy global HTTP client
// ─────────────────────────────────────────────────────────────────────────────

static GLOBAL_HTTP_CLIENT: OnceLock<BlockingClient> = OnceLock::new();

pub(crate) fn global_http_client(timeout_ms: u64) -> &'static BlockingClient {
    GLOBAL_HTTP_CLIENT.get_or_init(|| {
        BlockingClient::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .unwrap_or_default()
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeInfo
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub shard_id: u32,
    pub metrics_url: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// PeerHealth
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
// PeerEntry
// ─────────────────────────────────────────────────────────────────────────────

pub struct PeerEntry {
    pub node: NodeInfo,
    health: std::sync::Mutex<PeerHealth>,
}

impl PeerEntry {
    pub fn new(node: NodeInfo) -> Self {
        Self { node, health: std::sync::Mutex::new(PeerHealth::default()) }
    }

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
// ClusterConfig
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub my_shard_id: u32,
    pub shard_count: u32,
    pub peers: Vec<NodeInfo>,
    pub cluster_secret: Option<String>,
    pub gossip_interval_ms: u64,
    pub http_timeout_ms: u64,
}

impl ClusterConfig {
    pub fn from_env(my_shard_id: u32, shard_count: u32) -> Self {
        let cluster_secret = env::var("NEONDB_CLUSTER_SECRET").ok();
        let gossip_interval_ms = env::var("NEONDB_GOSSIP_INTERVAL_MS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(5_000);
        let http_timeout_ms = env::var("NEONDB_CLUSTER_HTTP_TIMEOUT_MS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(2_000);

        let peers_raw = env::var("NEONDB_PEERS").unwrap_or_default();
        let peers_raw = peers_raw.trim().to_string();

        if peers_raw.is_empty() {
            return ClusterConfig {
                enabled: false, my_shard_id, shard_count, peers: vec![],
                cluster_secret, gossip_interval_ms, http_timeout_ms,
            };
        }

        let peers = Self::parse_peers(&peers_raw, my_shard_id);
        let enabled = !peers.is_empty();

        if enabled && cluster_secret.is_none() {
            log::warn!(
                "SECURITY WARNING: clustering is enabled but NEONDB_CLUSTER_SECRET is not set — \
                 peer endpoints are unauthenticated. Set NEONDB_CLUSTER_SECRET before deploying."
            );
        }

        ClusterConfig { enabled, my_shard_id, shard_count, peers, cluster_secret, gossip_interval_ms, http_timeout_ms }
    }

    pub(crate) fn parse_peers(raw: &str, my_shard_id: u32) -> Vec<NodeInfo> {
        let mut peers = Vec::new();
        for (idx, part) in raw.split(',').enumerate() {
            let part = part.trim();
            if part.is_empty() { continue; }
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
// ClusterBus
// ─────────────────────────────────────────────────────────────────────────────

pub struct ClusterBus {
    pub config: ClusterConfig,
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

    pub fn is_active(&self) -> bool {
        self.config.enabled && !self.peers.is_empty()
    }

    pub fn validate_secret(&self, provided: Option<&str>) -> bool {
        match &self.config.cluster_secret {
            None => true,
            Some(expected) => provided.map(|v| v == expected).unwrap_or(false),
        }
    }

    pub fn secret_header(&self) -> Option<(&'static str, String)> {
        self.config.cluster_secret.as_ref().map(|s| ("x-neondb-cluster-secret", s.clone()))
    }

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
        self.peers.iter()
            .filter(|e| e.value().is_healthy())
            .map(|e| e.value().node.clone())
            .collect()
    }

    pub fn fanout_deltas(self: &Arc<Self>, deltas: &[RowDelta]) {
        if !self.is_active() || deltas.is_empty() { return; }
        fanout::fanout_to_peers(self, deltas);
    }

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
            None => return Err(NeonDBError::internal(format!(
                "[cluster] No peer found for shard {}", target_shard_id
            ))),
        };
        proxy::proxy_call(self, &node, reducer_name, args, caller_id, caller_role)
    }

    /// Register a new peer dynamically (for /cluster/join).
    pub fn add_peer(&self, node: NodeInfo) {
        self.peers.entry(node.shard_id).or_insert_with(|| PeerEntry::new(node));
    }

    /// All peers as a JSON-serializable snapshot.
    pub fn peers_snapshot(&self) -> Vec<serde_json::Value> {
        self.peers.iter().map(|e| {
            serde_json::json!({
                "shard_id":    e.value().node.shard_id,
                "metrics_url": e.value().node.metrics_url,
                "healthy":     e.value().is_healthy(),
            })
        }).collect()
    }
}

unsafe impl Send for ClusterBus {}
unsafe impl Sync for ClusterBus {}

// ─────────────────────────────────────────────────────────────────────────────
// Shard routing
// ─────────────────────────────────────────────────────────────────────────────

/// Determine which shard owns a given row key using FNV-1a hash.
/// Identical result on every node — deterministic shard assignment.
pub fn shard_for_key(key: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 { return 0; }
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

    #[test]
    fn shard_for_key_single_node_always_zero() {
        assert_eq!(shard_for_key("alice", 1), 0);
        assert_eq!(shard_for_key("bob", 1), 0);
        assert_eq!(shard_for_key("", 1), 0);
    }

    #[test]
    fn shard_for_key_zero_count_treated_as_single() {
        assert_eq!(shard_for_key("alice", 0), 0);
    }

    #[test]
    fn shard_for_key_deterministic() {
        for key in ["alice", "bob", "player_001", "", "zone_0_0"] {
            assert_eq!(shard_for_key(key, 4), shard_for_key(key, 4));
        }
    }

    #[test]
    fn shard_for_key_output_in_range() {
        let shard_count = 5u32;
        for key in ["a", "b", "abc", "hello_world", "123456789"] {
            let s = shard_for_key(key, shard_count);
            assert!(s < shard_count);
        }
    }

    #[test]
    fn shard_for_key_distributes_across_shards() {
        let shard_count = 4u32;
        let shards_seen: std::collections::HashSet<u32> = (0..200)
            .map(|i| shard_for_key(&format!("key_{}", i), shard_count))
            .collect();
        assert!(shards_seen.len() > 1);
    }

    #[test]
    fn cluster_config_no_peers_is_disabled() {
        let peers = ClusterConfig::parse_peers("", 0);
        assert!(peers.is_empty());
    }

    #[test]
    fn cluster_config_named_format_parses_correctly() {
        let raw = "shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001";
        let peers = ClusterConfig::parse_peers(raw, 0);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].shard_id, 1);
        assert_eq!(peers[0].metrics_url, "http://10.0.0.2:3001");
    }

    #[test]
    fn cluster_config_skips_self_in_named_format() {
        let raw = "shard1=http://10.0.0.2:3001,shard2=http://10.0.0.3:3001";
        let peers = ClusterConfig::parse_peers(raw, 1);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].shard_id, 2);
    }

    #[test]
    fn cluster_config_plain_url_format_parses_correctly() {
        let raw = "http://node-a:3001,http://node-b:3001,http://node-c:3001";
        let peers = ClusterConfig::parse_peers(raw, 0);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].shard_id, 1);
    }

    #[test]
    fn cluster_config_ignores_trailing_commas() {
        let raw = "shard1=http://10.0.0.2:3001,";
        let peers = ClusterConfig::parse_peers(raw, 0);
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn validate_secret_no_secret_configured_always_passes() {
        let cfg = ClusterConfig {
            enabled: true, my_shard_id: 0, shard_count: 1, peers: vec![],
            cluster_secret: None, gossip_interval_ms: 5000, http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(bus.validate_secret(None));
        assert!(bus.validate_secret(Some("anything")));
    }

    #[test]
    fn validate_secret_correct_secret_passes() {
        let cfg = ClusterConfig {
            enabled: true, my_shard_id: 0, shard_count: 1, peers: vec![],
            cluster_secret: Some("s3cr3t".to_string()), gossip_interval_ms: 5000, http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(bus.validate_secret(Some("s3cr3t")));
    }

    #[test]
    fn validate_secret_wrong_secret_rejected() {
        let cfg = ClusterConfig {
            enabled: true, my_shard_id: 0, shard_count: 1, peers: vec![],
            cluster_secret: Some("s3cr3t".to_string()), gossip_interval_ms: 5000, http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert!(!bus.validate_secret(None));
        assert!(!bus.validate_secret(Some("wrong")));
    }

    #[test]
    fn healthy_peers_excludes_unhealthy_nodes() {
        let cfg = ClusterConfig {
            enabled: true, my_shard_id: 0, shard_count: 3,
            peers: vec![
                NodeInfo { shard_id: 1, metrics_url: "http://node1:3001".to_string() },
                NodeInfo { shard_id: 2, metrics_url: "http://node2:3001".to_string() },
            ],
            cluster_secret: None, gossip_interval_ms: 5000, http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        assert_eq!(bus.healthy_peers().len(), 2);
        bus.mark_unhealthy(1); bus.mark_unhealthy(1); bus.mark_unhealthy(1);
        assert_eq!(bus.healthy_peers().len(), 1);
        assert_eq!(bus.healthy_peers()[0].shard_id, 2);
    }

    #[test]
    fn mark_healthy_recovers_unhealthy_peer() {
        let cfg = ClusterConfig {
            enabled: true, my_shard_id: 0, shard_count: 2,
            peers: vec![NodeInfo { shard_id: 1, metrics_url: "http://node1:3001".to_string() }],
            cluster_secret: None, gossip_interval_ms: 5000, http_timeout_ms: 2000,
        };
        let bus = ClusterBus::new(cfg);
        bus.mark_unhealthy(1); bus.mark_unhealthy(1); bus.mark_unhealthy(1);
        assert_eq!(bus.healthy_peers().len(), 0);
        bus.mark_healthy(1);
        assert_eq!(bus.healthy_peers().len(), 1);
    }
}
