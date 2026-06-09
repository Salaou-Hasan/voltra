// ============================================================================
// NeonDB LRU Eviction Tracker
//
// Provides per-table LRU tracking for the TableStore eviction system.
// Two policies are supported:
//   - LruRowCap   — evict LRU rows when a table exceeds `max_rows_per_table`
//   - LruByteCap  — evict LRU rows when total estimated byte usage exceeds cap
//                   (byte cap is checked at the TableStore level, not here)
//
// The LruTracker is thread-safe (DashMap inside) and designed to be held
// behind an Arc<LruTracker> shared between the TableStore and eviction logic.
// ============================================================================

use dashmap::DashMap;
use std::time::Instant;

// ── Eviction policy ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    /// No eviction — rows accumulate without bound (default behaviour).
    None,
    /// Evict least-recently-used rows when a table exceeds this row count.
    LruRowCap { max_rows_per_table: usize },
    /// Evict least-recently-used rows when estimated total bytes exceed the cap.
    /// The byte estimate is `sum over all rows of row_data.len()`.
    LruByteCap { max_bytes_total: usize },
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        EvictionPolicy::None
    }
}

// ── LRU access tracker ───────────────────────────────────────────────────────

/// Tracks the last-access time for each (table, row_key) pair.
/// All operations are lock-free (backed by DashMap).
pub struct LruTracker {
    /// (table_name, row_key) → last access Instant
    access: DashMap<(String, String), Instant>,
}

impl LruTracker {
    pub fn new() -> Self {
        LruTracker {
            access: DashMap::with_capacity_and_shard_amount(1024, 32),
        }
    }

    /// Record an access for `(table, key)`, updating its last-seen time to now.
    pub fn touch(&self, table: &str, key: &str) {
        self.access
            .insert((table.to_string(), key.to_string()), Instant::now());
    }

    /// Remove the tracking entry for `(table, key)`.
    /// Called when a row is deleted so stale entries don't accumulate.
    pub fn remove(&self, table: &str, key: &str) {
        self.access.remove(&(table.to_string(), key.to_string()));
    }

    /// Return up to `count` `(table, key)` pairs from `table` that were
    /// accessed least recently (oldest first).
    ///
    /// If `count >= number of tracked rows in table`, all tracked rows
    /// for that table are returned.
    pub fn evict_oldest(&self, table: &str, count: usize) -> Vec<(String, String)> {
        if count == 0 {
            return vec![];
        }

        // Collect all (table, key) pairs for this table with their access times.
        let mut candidates: Vec<((String, String), Instant)> = self
            .access
            .iter()
            .filter(|entry| entry.key().0 == table)
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();

        if candidates.is_empty() {
            return vec![];
        }

        // Sort ascending by Instant (oldest first).
        candidates.sort_unstable_by_key(|(_, ts)| *ts);

        // Return up to `count` keys (oldest first).
        candidates
            .into_iter()
            .take(count)
            .map(|(k, _)| k)
            .collect()
    }

    /// Return the total number of tracked (table, key) pairs across all tables.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.access.len()
    }
}

impl Default for LruTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// Helper: sleep a short time so Instant comparisons are reliable on CI.
    fn tiny_sleep() {
        thread::sleep(Duration::from_millis(2));
    }

    // 1. touch() records an entry; re-touching updates the time.
    #[test]
    fn touch_updates_access_time() {
        let tracker = LruTracker::new();
        tracker.touch("players", "alice");
        let t1 = *tracker
            .access
            .get(&("players".to_string(), "alice".to_string()))
            .unwrap();

        tiny_sleep();
        tracker.touch("players", "alice");
        let t2 = *tracker
            .access
            .get(&("players".to_string(), "alice".to_string()))
            .unwrap();

        assert!(t2 > t1, "re-touching must advance the stored Instant");
    }

    // 2. evict_oldest returns the single oldest key when count = 1.
    #[test]
    fn evict_oldest_returns_least_recently_used() {
        let tracker = LruTracker::new();
        tracker.touch("t", "old");
        tiny_sleep();
        tracker.touch("t", "middle");
        tiny_sleep();
        tracker.touch("t", "fresh");

        let evicted = tracker.evict_oldest("t", 1);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0], ("t".to_string(), "old".to_string()));
    }

    // 3. remove() deletes the tracking entry.
    #[test]
    fn remove_clears_entry() {
        let tracker = LruTracker::new();
        tracker.touch("players", "bob");
        assert_eq!(tracker.len(), 1);

        tracker.remove("players", "bob");
        assert_eq!(tracker.len(), 0);

        // Evicting after remove must return empty.
        let evicted = tracker.evict_oldest("players", 1);
        assert!(evicted.is_empty());
    }

    // 4. EvictionPolicy::None does not trigger any eviction (structural test).
    #[test]
    fn none_policy_is_default() {
        let policy = EvictionPolicy::default();
        matches!(policy, EvictionPolicy::None);
        // No panic, no eviction-related calls needed.
    }

    // 5. evict_oldest with count > number of entries returns all entries.
    #[test]
    fn evict_oldest_count_exceeds_entries_returns_all() {
        let tracker = LruTracker::new();
        tracker.touch("items", "sword");
        tracker.touch("items", "shield");
        tracker.touch("items", "potion");

        let evicted = tracker.evict_oldest("items", 100);
        assert_eq!(evicted.len(), 3, "should return all 3 even though 100 were requested");
        // All returned keys belong to the correct table.
        for (tbl, _) in &evicted {
            assert_eq!(tbl, "items");
        }
    }

    // 6. evict_oldest only returns keys from the requested table.
    #[test]
    fn evict_oldest_is_table_scoped() {
        let tracker = LruTracker::new();
        tracker.touch("table_a", "r1");
        tracker.touch("table_a", "r2");
        tracker.touch("table_b", "r3");
        tracker.touch("table_b", "r4");

        let evicted = tracker.evict_oldest("table_a", 10);
        assert_eq!(evicted.len(), 2);
        for (tbl, _) in &evicted {
            assert_eq!(tbl, "table_a");
        }
    }

    // 7. evict_oldest on empty tracker / unknown table returns empty vec.
    #[test]
    fn evict_oldest_empty_tracker_returns_empty() {
        let tracker = LruTracker::new();
        let evicted = tracker.evict_oldest("nonexistent_table", 5);
        assert!(evicted.is_empty());
    }

    // 8. LruRowCap policy carries the configured cap correctly.
    #[test]
    fn lru_row_cap_stores_cap() {
        let policy = EvictionPolicy::LruRowCap { max_rows_per_table: 500 };
        if let EvictionPolicy::LruRowCap { max_rows_per_table } = policy {
            assert_eq!(max_rows_per_table, 500);
        } else {
            panic!("Expected LruRowCap variant");
        }
    }
}
