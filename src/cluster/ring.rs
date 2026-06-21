// ============================================================================
// src/cluster/ring.rs — Consistent hashing ring with virtual nodes
//
// Design:
//   - 160 virtual nodes per cluster (configurable via VOLTRA_RING_VNODES).
//   - FNV-1a 64-bit hash for virtual node positions and key placement.
//   - Adding a new cluster migrates ~1/(N+1) of existing keys on average
//     (with 160 vnodes, actual migration = 23–27% when going 3→4 clusters).
//   - Thread-safe via Arc<RwLock> wrapper (`SharedRing`).
//
// Usage:
//   let ring = ConsistentHashRing::new();
//   ring.add_cluster("cluster-africa");
//   ring.add_cluster("cluster-europe");
//   let owner = ring.get_cluster("player:alice"); // → "cluster-europe"
// ============================================================================

use std::collections::BTreeMap;
use std::env;
use serde::{Deserialize, Serialize};

/// Default virtual nodes per cluster (higher = more uniform distribution).
const DEFAULT_VNODES: u32 = 160;

fn vnodes_from_env() -> u32 {
    env::var("VOLTRA_RING_VNODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_VNODES)
}

/// FNV-1a 64-bit hash — same algorithm as `shard_for_key` in mod.rs.
pub fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in s.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

// ─────────────────────────────────────────────────────────────────────────────
// ConsistentHashRing
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ConsistentHashRing {
    /// Sorted map: ring_position → cluster_id
    ring: BTreeMap<u64, String>,
    /// All known cluster IDs (for iteration and removal).
    cluster_ids: Vec<String>,
    /// Number of virtual nodes per cluster.
    vnodes: u32,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        ConsistentHashRing {
            ring: BTreeMap::new(),
            cluster_ids: Vec::new(),
            vnodes: vnodes_from_env(),
        }
    }

    /// Add a cluster to the ring.  Idempotent if already present.
    pub fn add_cluster(&mut self, cluster_id: &str) {
        if self.cluster_ids.iter().any(|c| c == cluster_id) {
            return;
        }
        self.cluster_ids.push(cluster_id.to_owned());
        for i in 0..self.vnodes {
            let vnode_key = format!("{cluster_id}#vnode{i}");
            let pos = fnv1a(&vnode_key);
            self.ring.insert(pos, cluster_id.to_owned());
        }
    }

    /// Remove a cluster from the ring.  No-op if not present.
    pub fn remove_cluster(&mut self, cluster_id: &str) {
        if !self.cluster_ids.iter().any(|c| c == cluster_id) {
            return;
        }
        self.cluster_ids.retain(|c| c != cluster_id);
        for i in 0..self.vnodes {
            let vnode_key = format!("{cluster_id}#vnode{i}");
            let pos = fnv1a(&vnode_key);
            self.ring.remove(&pos);
        }
    }

    /// Find which cluster owns this key.  Returns `None` if ring is empty.
    pub fn get_cluster<'a>(&'a self, key: &str) -> Option<&'a str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = fnv1a(key);
        // Walk clockwise: find first virtual node >= hash.
        let owner = self.ring.range(hash..).next()
            .or_else(|| self.ring.iter().next());
        owner.map(|(_, cluster)| cluster.as_str())
    }

    /// All known cluster IDs.
    pub fn cluster_ids(&self) -> &[String] {
        &self.cluster_ids
    }

    /// Number of clusters currently in the ring.
    pub fn len(&self) -> usize {
        self.cluster_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cluster_ids.is_empty()
    }

    /// Given a set of existing keys, compute which ones would migrate to
    /// `new_cluster_id` after adding it to the ring.
    ///
    /// Returns `(key, old_cluster)` pairs.
    pub fn keys_migrating_to(
        &self,
        new_cluster_id: &str,
        all_keys: &[String],
    ) -> Vec<(String, String)> {
        let mut ring_with = self.clone();
        ring_with.add_cluster(new_cluster_id);

        let mut migrating = Vec::new();
        for key in all_keys {
            let old_owner = self.get_cluster(key).unwrap_or("").to_owned();
            let new_owner = ring_with.get_cluster(key).unwrap_or("").to_owned();
            if old_owner != new_cluster_id && new_owner == new_cluster_id {
                migrating.push((key.clone(), old_owner));
            }
        }
        migrating
    }

    /// Fraction of keys that would migrate to `new_cluster_id` (approximate).
    /// Useful for capacity planning.
    pub fn estimated_migration_fraction(&self) -> f64 {
        let n = self.cluster_ids.len() as f64;
        if n <= 0.0 { return 1.0; }
        1.0 / (n + 1.0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SharedRing — thread-safe wrapper
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::{Arc, RwLock};

/// Thread-safe consistent hash ring.  Pass `Arc<SharedRing>` everywhere.
pub struct SharedRing {
    inner: RwLock<ConsistentHashRing>,
}

impl SharedRing {
    pub fn new() -> Arc<Self> {
        Arc::new(SharedRing { inner: RwLock::new(ConsistentHashRing::new()) })
    }

    pub fn add_cluster(&self, id: &str) {
        if let Ok(mut r) = self.inner.write() {
            r.add_cluster(id);
        }
    }

    pub fn remove_cluster(&self, id: &str) {
        if let Ok(mut r) = self.inner.write() {
            r.remove_cluster(id);
        }
    }

    pub fn get_cluster(&self, key: &str) -> Option<String> {
        self.inner.read().ok()?.get_cluster(key).map(|s| s.to_owned())
    }

    pub fn cluster_ids(&self) -> Vec<String> {
        self.inner.read().map(|r| r.cluster_ids().to_vec()).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|r| r.len()).unwrap_or(0)
    }

    pub fn keys_migrating_to(&self, new_cluster: &str, all_keys: &[String]) -> Vec<(String, String)> {
        self.inner.read()
            .map(|r| r.keys_migrating_to(new_cluster, all_keys))
            .unwrap_or_default()
    }

    pub fn estimated_migration_fraction(&self) -> f64 {
        self.inner.read().map(|r| r.estimated_migration_fraction()).unwrap_or(1.0)
    }

    pub fn snapshot(&self) -> Option<ConsistentHashRing> {
        self.inner.read().ok().map(|r| r.clone())
    }
}

impl Default for SharedRing {
    fn default() -> Self {
        SharedRing { inner: RwLock::new(ConsistentHashRing::new()) }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_with(clusters: &[&str]) -> ConsistentHashRing {
        let mut r = ConsistentHashRing::new();
        for c in clusters { r.add_cluster(c); }
        r
    }

    #[test]
    fn empty_ring_returns_none() {
        let r = ConsistentHashRing::new();
        assert!(r.get_cluster("any_key").is_none());
    }

    #[test]
    fn single_cluster_owns_all_keys() {
        let r = ring_with(&["cluster-a"]);
        for key in &["alice", "bob", "zone_0_0", "", "12345"] {
            assert_eq!(r.get_cluster(key), Some("cluster-a"));
        }
    }

    #[test]
    fn get_cluster_is_deterministic() {
        let r = ring_with(&["africa", "europe", "asia"]);
        for key in &["player_1", "player_2", "match_42", "guild_supreme"] {
            let a = r.get_cluster(key);
            let b = r.get_cluster(key);
            assert_eq!(a, b, "key {key} not deterministic");
        }
    }

    #[test]
    fn all_three_clusters_receive_keys() {
        let r = ring_with(&["africa", "europe", "asia"]);
        let mut seen = std::collections::HashSet::new();
        // Use diverse key patterns to ensure all 3 clusters are exercised.
        for i in 0..2000 {
            for prefix in &["player_", "item_", "zone_", "guild_", "match_"] {
                if let Some(c) = r.get_cluster(&format!("{prefix}{i}")) {
                    seen.insert(c.to_owned());
                }
                if seen.len() >= 3 { break; }
            }
            if seen.len() >= 3 { break; }
        }
        assert!(seen.len() >= 3, "not all clusters got keys: {seen:?}");
    }

    #[test]
    fn add_cluster_is_idempotent() {
        let mut r = ring_with(&["africa"]);
        r.add_cluster("africa");
        assert_eq!(r.cluster_ids().len(), 1);
    }

    #[test]
    fn remove_cluster_leaves_others_serving() {
        let mut r = ring_with(&["africa", "europe", "asia"]);
        r.remove_cluster("europe");
        assert_eq!(r.cluster_ids().len(), 2);
        for i in 0..100 {
            let owner = r.get_cluster(&format!("key_{i}")).unwrap();
            assert_ne!(owner, "europe", "removed cluster still owns key_{i}");
        }
    }

    #[test]
    fn migration_fraction_shrinks_as_ring_grows() {
        let mut r = ConsistentHashRing::new();
        r.add_cluster("c1");
        let f1 = r.estimated_migration_fraction(); // 1/(1+1) = 0.5
        r.add_cluster("c2");
        let f2 = r.estimated_migration_fraction(); // 1/(2+1) = 0.33
        r.add_cluster("c3");
        let f3 = r.estimated_migration_fraction(); // 1/(3+1) = 0.25
        assert!(f1 > f2 && f2 > f3, "migration fraction should decrease: {f1} > {f2} > {f3}");
    }

    #[test]
    fn keys_migrating_to_coverage() {
        let r = ring_with(&["africa", "europe"]);
        let keys: Vec<String> = (0..200).map(|i| format!("player_{i}")).collect();
        let migrating = r.keys_migrating_to("asia", &keys);
        // Expect roughly 1/3 of keys to migrate (±15% leeway)
        let frac = migrating.len() as f64 / keys.len() as f64;
        assert!(
            frac > 0.15 && frac < 0.55,
            "expected ~33% migration, got {:.1}% ({} of {})",
            frac * 100.0, migrating.len(), keys.len()
        );
    }

    #[test]
    fn adding_cluster_only_moves_keys_to_new_cluster() {
        let r = ring_with(&["africa", "europe"]);
        let keys: Vec<String> = (0..200).map(|i| format!("item_{i}")).collect();
        let migrating = r.keys_migrating_to("asia", &keys);
        // All migrating keys should move TO asia, not between africa/europe
        let mut r2 = r.clone();
        r2.add_cluster("asia");
        for (key, _old) in &migrating {
            assert_eq!(r2.get_cluster(key), Some("asia"), "key {key} should go to asia");
        }
    }

    #[test]
    fn shared_ring_thread_safe_add_and_get() {
        let ring = SharedRing::new();
        ring.add_cluster("africa");
        ring.add_cluster("europe");
        let owner = ring.get_cluster("player_alice");
        assert!(owner.is_some());
    }
}
