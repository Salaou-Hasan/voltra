// src/cluster/regions.rs — Regional cluster registry
//
// Each Voltra deployment can span multiple named regions (e.g. "europe",
// "asia", "africa").  This module tracks:
//   - Which region THIS node belongs to  (VOLTRA_REGION)
//   - The WebSocket + metrics URLs for every peer region
//     (VOLTRA_REGIONS=europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001)
//
// Clients use GET /cluster/lobby-route?lobby_id=X to discover which region
// hosts a given lobby, then reconnect directly to that region's ws_url.

use std::env;
use std::sync::Arc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::ring::SharedRing;

/// A single named region in the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRegion {
    /// Short identifier, e.g. "europe", "asia", "africa", "na".
    pub id: String,
    /// WebSocket URL clients connect to, e.g. "ws://eu.example.com:3000".
    pub ws_url: String,
    /// HTTP metrics URL used for server-to-server calls, e.g. "http://eu.example.com:3001".
    pub metrics_url: String,
}

/// Registry of all known regions, loaded once at startup.
pub struct RegionRegistry {
    /// This node's own region ID.
    pub my_region: String,
    /// All known regions keyed by region ID (includes this node's region).
    regions: DashMap<String, ClusterRegion>,
}

impl RegionRegistry {
    /// Build from environment variables.
    ///
    /// `VOLTRA_REGION`  — this node's region ID (default "default").
    /// `VOLTRA_REGIONS` — comma-separated list of `id=ws_url|metrics_url` pairs.
    ///
    /// Example:
    ///   VOLTRA_REGION=europe
    ///   VOLTRA_REGIONS=europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001
    pub fn from_env() -> Self {
        let my_region = env::var("VOLTRA_REGION").unwrap_or_else(|_| "default".to_string());
        let regions: DashMap<String, ClusterRegion> = DashMap::new();

        if let Ok(raw) = env::var("VOLTRA_REGIONS") {
            for entry in raw.split(',') {
                let entry = entry.trim();
                if entry.is_empty() { continue; }
                // Format: id=ws_url|metrics_url
                if let Some((id, urls)) = entry.split_once('=') {
                    let id = id.trim().to_string();
                    let (ws_url, metrics_url) = if let Some((w, m)) = urls.split_once('|') {
                        (w.trim().to_string(), m.trim().to_string())
                    } else {
                        (urls.trim().to_string(), String::new())
                    };
                    regions.insert(id.clone(), ClusterRegion { id, ws_url, metrics_url });
                }
            }
        }

        RegionRegistry { my_region, regions }
    }

    /// Returns all known regions.
    pub fn all(&self) -> Vec<ClusterRegion> {
        self.regions.iter().map(|e| e.value().clone()).collect()
    }

    /// Returns all regions except this node's own.
    pub fn peer_regions(&self) -> Vec<ClusterRegion> {
        self.regions
            .iter()
            .filter(|e| e.key() != &self.my_region)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Look up a region by ID.
    pub fn get(&self, id: &str) -> Option<ClusterRegion> {
        self.regions.get(id).map(|e| e.clone())
    }

    /// WebSocket URL for a region ID.
    pub fn ws_url_for(&self, id: &str) -> Option<String> {
        self.regions.get(id).map(|r| r.ws_url.clone())
    }

    /// Metrics URL for a region ID.
    pub fn metrics_url_for(&self, id: &str) -> Option<String> {
        self.regions.get(id).map(|r| r.metrics_url.clone())
    }

    /// Returns true if more than one region is configured (multi-region mode).
    pub fn is_multi_region(&self) -> bool {
        self.regions.len() > 1
    }

    /// Returns this node's own ClusterRegion entry, if configured.
    pub fn my_region_info(&self) -> Option<ClusterRegion> {
        self.get(&self.my_region)
    }

    /// Build a consistent-hash ring seeded with all known region IDs.
    /// Use this for deterministic lobby-to-region assignment without manual registration.
    pub fn build_ring(&self) -> Arc<SharedRing> {
        let ring = SharedRing::new();
        for r in self.all() {
            ring.add_cluster(&r.id);
        }
        // Always include this node's own region, even if VOLTRA_REGIONS omits it.
        ring.add_cluster(&self.my_region);
        ring
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry(my: &str, regions_str: &str) -> RegionRegistry {
        // Build manually to avoid env var side effects in parallel tests.
        let regions: DashMap<String, ClusterRegion> = DashMap::new();
        for entry in regions_str.split(',') {
            let entry = entry.trim();
            if entry.is_empty() { continue; }
            if let Some((id, urls)) = entry.split_once('=') {
                let id = id.trim().to_string();
                let (ws_url, metrics_url) = if let Some((w, m)) = urls.split_once('|') {
                    (w.trim().to_string(), m.trim().to_string())
                } else {
                    (urls.trim().to_string(), String::new())
                };
                regions.insert(id.clone(), ClusterRegion { id, ws_url, metrics_url });
            }
        }
        RegionRegistry { my_region: my.to_string(), regions }
    }

    #[test]
    fn single_region_not_multi() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001");
        assert!(!r.is_multi_region());
    }

    #[test]
    fn two_regions_is_multi() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001");
        assert!(r.is_multi_region());
    }

    #[test]
    fn peer_regions_excludes_self() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001");
        let peers = r.peer_regions();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "asia");
    }

    #[test]
    fn ws_url_lookup() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001");
        assert_eq!(r.ws_url_for("asia"), Some("ws://as:3000".to_string()));
        assert_eq!(r.ws_url_for("africa"), None);
    }

    #[test]
    fn metrics_url_lookup() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001");
        assert_eq!(r.metrics_url_for("europe"), Some("http://eu:3001".to_string()));
    }

    #[test]
    fn empty_regions_string_is_valid() {
        let r = make_registry("default", "");
        assert!(r.all().is_empty());
        assert!(!r.is_multi_region());
    }

    #[test]
    fn build_ring_assigns_all_regions() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001");
        let ring = r.build_ring();
        assert_eq!(ring.len(), 2);
        // Every lobby gets assigned somewhere.
        for i in 0..50 {
            let owner = ring.get_cluster(&format!("lobby_{i}"));
            assert!(owner.is_some(), "lobby_{i} has no owner");
        }
    }

    #[test]
    fn build_ring_assignment_is_deterministic() {
        let r = make_registry("europe", "europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001");
        let ring = r.build_ring();
        // Same lobby_id must always map to same region on any node.
        for lobby in &["lobby_1", "lobby_42", "lobby_999"] {
            let a = ring.get_cluster(lobby);
            let b = ring.get_cluster(lobby);
            assert_eq!(a, b, "{lobby} mapping is not deterministic");
        }
    }

    #[test]
    fn build_ring_includes_own_region_when_missing_from_list() {
        // my_region not in VOLTRA_REGIONS — still added by build_ring().
        let r = make_registry("na", "europe=ws://eu:3000|http://eu:3001");
        let ring = r.build_ring();
        assert_eq!(ring.len(), 2); // europe + na
        let ids = ring.cluster_ids();
        assert!(ids.contains(&"na".to_string()));
    }
}
