// src/cluster/regions.rs — Regional cluster registry
//
// Each NeonDB deployment can span multiple named regions (e.g. "europe",
// "asia", "africa").  This module tracks:
//   - Which region THIS node belongs to  (NEONDB_REGION)
//   - The WebSocket + metrics URLs for every peer region
//     (NEONDB_REGIONS=europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001)
//
// Clients use GET /cluster/lobby-route?lobby_id=X to discover which region
// hosts a given lobby, then reconnect directly to that region's ws_url.

use std::env;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

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
    /// `NEONDB_REGION`  — this node's region ID (default "default").
    /// `NEONDB_REGIONS` — comma-separated list of `id=ws_url|metrics_url` pairs.
    ///
    /// Example:
    ///   NEONDB_REGION=europe
    ///   NEONDB_REGIONS=europe=ws://eu:3000|http://eu:3001,asia=ws://as:3000|http://as:3001
    pub fn from_env() -> Self {
        let my_region = env::var("NEONDB_REGION").unwrap_or_else(|_| "default".to_string());
        let regions: DashMap<String, ClusterRegion> = DashMap::new();

        if let Ok(raw) = env::var("NEONDB_REGIONS") {
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
}
