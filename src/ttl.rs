use dashmap::DashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Mutex;

/// A scheduled expiration for a single row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtlEntry {
    pub table_name: String,
    pub row_key: String,
    pub expires_at_ms: u64, // Unix timestamp ms when this row should be deleted
}

impl Ord for TtlEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Natural ordering on expires_at_ms. BinaryHeap<Reverse<TtlEntry>> gives min-heap.
        self.expires_at_ms.cmp(&other.expires_at_ms)
    }
}

impl PartialOrd for TtlEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// TTL manager — tracks row expirations and provides expired-row collection.
///
/// Design:
/// - A min-heap (BinaryHeap<Reverse<TtlEntry>>) for efficient "what's expired?" queries.
///   Because `TtlEntry::cmp` reverses the natural ordering on `expires_at_ms`, wrapping
///   in `Reverse` gives us a min-heap where the earliest expiry is at the top.
/// - A lookup DashMap for O(1) "does this row have a TTL?" and "cancel TTL" operations.
/// - The actual deletion is NOT performed here — the caller (a background task in main.rs)
///   calls `collect_expired()` and feeds the results to TableStore.delete_row().
///
/// This separation keeps TTL logic independent of storage — easy to test.
pub struct TtlManager {
    /// Min-heap: earliest expiry at top.
    heap: Mutex<BinaryHeap<Reverse<TtlEntry>>>,
    /// Quick lookup: (table_name, row_key) → expires_at_ms.
    index: DashMap<(String, String), u64>,
}

impl TtlManager {
    /// Create an empty TtlManager.
    pub fn new() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            index: DashMap::new(),
        }
    }

    /// Set a TTL on a row. If one already exists, it's replaced.
    /// `ttl_ms` is relative (e.g., 60000 = expires in 60 seconds from `now_ms`).
    pub fn set_ttl(&self, table_name: &str, row_key: &str, now_ms: u64, ttl_ms: u64) {
        let expires_at_ms = now_ms + ttl_ms;
        self.set_expires_at(table_name, row_key, expires_at_ms);
    }

    /// Set an absolute expiration time.
    /// If the row already has a TTL, the old entry remains in the heap as a ghost
    /// (it will be skipped during `collect_expired` because the index won't match).
    pub fn set_expires_at(&self, table_name: &str, row_key: &str, expires_at_ms: u64) {
        let key = (table_name.to_string(), row_key.to_string());

        // Update the index (overwrite any previous value)
        self.index.insert(key, expires_at_ms);

        // Push new entry into heap
        let entry = TtlEntry {
            table_name: table_name.to_string(),
            row_key: row_key.to_string(),
            expires_at_ms,
        };
        let mut heap = self.heap.lock().unwrap();
        heap.push(Reverse(entry));
    }

    /// Cancel the TTL on a row (make it permanent).
    /// Returns true if a TTL was cancelled, false if none existed.
    /// The ghost entry remains in the heap but will be skipped during collect_expired.
    pub fn cancel_ttl(&self, table_name: &str, row_key: &str) -> bool {
        let key = (table_name.to_string(), row_key.to_string());
        self.index.remove(&key).is_some()
    }

    /// Get the expiration time for a row, if set.
    pub fn get_ttl(&self, table_name: &str, row_key: &str) -> Option<u64> {
        let key = (table_name.to_string(), row_key.to_string());
        self.index.get(&key).map(|v| *v)
    }

    /// Check if a row has an active TTL.
    pub fn has_ttl(&self, table_name: &str, row_key: &str) -> bool {
        let key = (table_name.to_string(), row_key.to_string());
        self.index.contains_key(&key)
    }

    /// Collect all entries that have expired as of `now_ms`.
    /// Returns them and removes from the internal structures.
    /// The caller should delete these rows from the TableStore.
    ///
    /// Algorithm:
    /// 1. Lock the heap.
    /// 2. Peek at the top entry (earliest expiry).
    /// 3. If `expires_at_ms <= now_ms`:
    ///    a. Pop it from the heap.
    ///    b. Check the index for this (table, key) pair:
    ///       - If missing → this is a ghost (TTL was cancelled or overwritten). Skip.
    ///       - If present but `expires_at_ms` differs → ghost from an overwrite. Skip.
    ///       - If present and matches → genuine expiry. Remove from index, add to result.
    ///    c. Repeat from step 2.
    /// 4. If top entry is in the future or heap is empty → done. Return collected entries.
    ///
    /// Ghost entries are lazily cleaned: they are only discarded when they bubble to the
    /// top of the heap and fail the index check. This avoids O(n) scans on cancel/overwrite.
    pub fn collect_expired(&self, now_ms: u64) -> Vec<TtlEntry> {
        let mut expired = Vec::new();
        let mut heap = self.heap.lock().unwrap();

        loop {
            // Peek at top
            let should_pop = match heap.peek() {
                Some(Reverse(entry)) => entry.expires_at_ms <= now_ms,
                None => false,
            };

            if !should_pop {
                break;
            }

            let Reverse(entry) = heap.pop().unwrap();
            let key = (entry.table_name.clone(), entry.row_key.clone());

            // Validate against index
            let is_valid = match self.index.get(&key) {
                Some(ref_val) => *ref_val == entry.expires_at_ms,
                None => false, // cancelled
            };

            if is_valid {
                // Genuine expiry — remove from index and collect
                self.index.remove(&key);
                expired.push(entry);
            }
            // Otherwise it's a ghost — discard silently
        }

        expired
    }

    /// Number of active TTLs (entries in the index, not the heap which may contain ghosts).
    pub fn count(&self) -> usize {
        self.index.len()
    }
}

impl Default for TtlManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_ttl_creates_entry() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "sess_abc", 1000, 60000);

        assert!(mgr.has_ttl("sessions", "sess_abc"));
        assert_eq!(mgr.get_ttl("sessions", "sess_abc"), Some(61000));
        assert_eq!(mgr.count(), 1);
    }

    #[test]
    fn test_collect_expired_returns_due_entries() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 1000, 5000); // expires at 6000
        mgr.set_ttl("sessions", "s2", 1000, 10000); // expires at 11000
        mgr.set_ttl("tokens", "t1", 1000, 3000); // expires at 4000

        let expired = mgr.collect_expired(7000);
        assert_eq!(expired.len(), 2);

        let keys: Vec<(&str, &str)> = expired
            .iter()
            .map(|e| (e.table_name.as_str(), e.row_key.as_str()))
            .collect();
        assert!(keys.contains(&("sessions", "s1")));
        assert!(keys.contains(&("tokens", "t1")));

        // s2 should still be active
        assert!(mgr.has_ttl("sessions", "s2"));
        assert_eq!(mgr.count(), 1);
    }

    #[test]
    fn test_collect_expired_skips_future_entries() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 1000, 60000); // expires at 61000

        let expired = mgr.collect_expired(5000);
        assert!(expired.is_empty());
        assert_eq!(mgr.count(), 1);
    }

    #[test]
    fn test_cancel_ttl_prevents_expiration() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 1000, 5000); // expires at 6000

        let cancelled = mgr.cancel_ttl("sessions", "s1");
        assert!(cancelled);
        assert!(!mgr.has_ttl("sessions", "s1"));
        assert_eq!(mgr.count(), 0);

        // Even after expiry time, collect should return nothing (ghost entry skipped)
        let expired = mgr.collect_expired(100000);
        assert!(expired.is_empty());
    }

    #[test]
    fn test_set_ttl_overwrites_previous() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 1000, 5000); // expires at 6000
        mgr.set_ttl("sessions", "s1", 1000, 20000); // expires at 21000, overwrites

        assert_eq!(mgr.get_ttl("sessions", "s1"), Some(21000));
        assert_eq!(mgr.count(), 1);

        // At t=7000, the old entry would have expired, but it's now a ghost
        let expired = mgr.collect_expired(7000);
        assert!(expired.is_empty());
        assert_eq!(mgr.count(), 1); // still active with new TTL

        // At t=22000, the new entry expires
        let expired = mgr.collect_expired(22000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].row_key, "s1");
        assert_eq!(expired[0].expires_at_ms, 21000);
    }

    #[test]
    fn test_has_ttl_and_get_ttl() {
        let mgr = TtlManager::new();

        assert!(!mgr.has_ttl("sessions", "s1"));
        assert_eq!(mgr.get_ttl("sessions", "s1"), None);

        mgr.set_expires_at("sessions", "s1", 50000);
        assert!(mgr.has_ttl("sessions", "s1"));
        assert_eq!(mgr.get_ttl("sessions", "s1"), Some(50000));
    }

    #[test]
    fn test_count_tracks_active_ttls() {
        let mgr = TtlManager::new();
        assert_eq!(mgr.count(), 0);

        mgr.set_ttl("a", "k1", 0, 1000);
        mgr.set_ttl("b", "k2", 0, 2000);
        mgr.set_ttl("c", "k3", 0, 3000);
        assert_eq!(mgr.count(), 3);

        mgr.cancel_ttl("b", "k2");
        assert_eq!(mgr.count(), 2);

        mgr.collect_expired(1500); // expires a/k1
        assert_eq!(mgr.count(), 1);
    }

    #[test]
    fn test_collect_expired_removes_from_index() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 0, 1000); // expires at 1000

        // Before collection, it's in the index
        assert!(mgr.has_ttl("sessions", "s1"));

        let expired = mgr.collect_expired(2000);
        assert_eq!(expired.len(), 1);

        // After collection, removed from index
        assert!(!mgr.has_ttl("sessions", "s1"));
        assert_eq!(mgr.get_ttl("sessions", "s1"), None);
    }

    #[test]
    fn test_cancel_nonexistent_returns_false() {
        let mgr = TtlManager::new();
        assert!(!mgr.cancel_ttl("no_table", "no_key"));
    }

    #[test]
    fn test_multiple_tables_independent() {
        let mgr = TtlManager::new();
        mgr.set_ttl("sessions", "s1", 0, 5000);
        mgr.set_ttl("tokens", "s1", 0, 10000); // same row_key, different table

        assert_eq!(mgr.count(), 2);
        assert_eq!(mgr.get_ttl("sessions", "s1"), Some(5000));
        assert_eq!(mgr.get_ttl("tokens", "s1"), Some(10000));

        let expired = mgr.collect_expired(6000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].table_name, "sessions");

        // tokens/s1 still active
        assert!(mgr.has_ttl("tokens", "s1"));
    }

    #[test]
    fn test_collect_expired_boundary_exact_time() {
        let mgr = TtlManager::new();
        mgr.set_expires_at("t", "k", 5000);

        // Exactly at expiry time: should be collected (expires_at_ms <= now_ms)
        let expired = mgr.collect_expired(5000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].expires_at_ms, 5000);
    }
}
