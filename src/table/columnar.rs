/// Column-oriented indexes for fast predicate matching.
///
/// This module provides secondary column indexes that enable O(matching_rows) predicate
/// evaluation instead of O(all_rows). For example:
///
/// Query: "WHERE status='alive' AND zone='spawn'"
/// Without indexes: scan all 20K players, check each one
/// With indexes: lookup status column for 'alive' (~5K rows) ∩ zone column for 'spawn' (~500 rows)
///
/// Indexes are automatically maintained alongside row writes via apply_delta_batch.
/// They are optional per-column (controlled by schema.toml `indexed = true`).

use crate::error::Result;
use dashmap::DashMap;
use serde_json::Value;

/// A column index: maps field values to the set of row keys that carry that value.
///
/// Structure: `field_value (as string) → Set<row_key>`
///
/// For example, the "status" column in a players table might have:
///   "alive" → {"p1", "p2", "p5", "p7", ...}
///   "dead" → {"p3", "p4", "p6", ...}
///   "spectating" → {"p8", "p9", ...}
pub struct ColumnIndex {
    /// Map from field value (serialized to string) to set of row keys.
    /// Inner DashMap is used as a set (key = row_key, value = ()).
    buckets: DashMap<String, DashMap<String, ()>>,
    /// Field name for debugging/monitoring.
    pub field_name: String,
}

impl ColumnIndex {
    /// Create a new column index for a given field name.
    pub fn new(field_name: impl Into<String>) -> Self {
        ColumnIndex {
            buckets: DashMap::new(),
            field_name: field_name.into(),
        }
    }

    /// Add a row to the index under the given field value.
    ///
    /// If the row was previously indexed under a different value, that entry
    /// is NOT automatically removed — the caller must call `remove()` first.
    /// This is intentional: during `apply_delta_batch`, old values are removed
    /// before inserting under new values.
    pub fn insert(&self, field_value: &Value, row_key: &str) -> Result<()> {
        let field_str = self.value_to_key(field_value);
        self.buckets
            .entry(field_str)
            .or_insert_with(DashMap::new)
            .insert(row_key.to_string(), ());
        Ok(())
    }

    /// Remove a row from the index under the given field value.
    ///
    /// If the row is not in the bucket, this is a no-op (idempotent).
    /// Empty buckets are dropped automatically.
    pub fn remove(&self, field_value: &Value, row_key: &str) -> Result<()> {
        let field_str = self.value_to_key(field_value);
        if let Some(bucket) = self.buckets.get(&field_str) {
            bucket.remove(row_key);
        }
        // Drop the bucket if it's now empty
        self.buckets.remove_if(&field_str, |_, bucket| bucket.is_empty());
        Ok(())
    }

    /// Look up all row keys that have the given field value.
    ///
    /// Returns an empty Vec if no rows match (bucket doesn't exist or is empty).
    pub fn lookup(&self, field_value: &Value) -> Vec<String> {
        let field_str = self.value_to_key(field_value);
        match self.buckets.get(&field_str) {
            None => Vec::new(),
            Some(bucket) => bucket.iter().map(|e| e.key().clone()).collect(),
        }
    }

    /// Look up all row keys with a field value matching a string predicate (e.g. "starts with").
    ///
    /// Useful for prefix matching on indexed string columns.
    /// Returns all rows whose field value starts with the given prefix.
    pub fn lookup_prefix(&self, prefix: &str) -> Vec<String> {
        let mut results = Vec::new();
        for entry in self.buckets.iter() {
            if entry.key().starts_with(prefix) {
                for row_entry in entry.value().iter() {
                    results.push(row_entry.key().clone());
                }
            }
        }
        results
    }

    /// Get the number of distinct values in this column.
    pub fn distinct_value_count(&self) -> usize {
        self.buckets.len()
    }

    /// Get the total number of indexed rows (sum of all bucket sizes).
    pub fn indexed_row_count(&self) -> usize {
        self.buckets.iter().map(|e| e.value().len()).sum()
    }

    /// Convert a JSON value to a string key for the index.
    ///
    /// Strategy:
    /// - Null → "null" (quoted literal)
    /// - String → as-is
    /// - Number → string repr (e.g. "42" or "3.14")
    /// - Bool → "true" or "false"
    /// - Array/Object → serialize to JSON (expensive but correct for complex types)
    fn value_to_key(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            Value::Array(_) | Value::Object(_) => {
                // For complex types, use JSON serialization.
                // This is slower but correct. Callers should only index scalar types.
                serde_json::to_string(value).unwrap_or_else(|_| "?error".to_string())
            }
        }
    }

    /// Clear all entries from the index (used during table drops/clears).
    pub fn clear(&self) {
        self.buckets.clear();
    }
}

/// Intersection of multiple column index results.
///
/// Used for multi-column predicates (AND). Given results from multiple columns,
/// finds the set of row keys present in all of them.
///
/// Example:
///   col_status.lookup("alive") → ["p1", "p2", "p3", "p5"]
///   col_zone.lookup("spawn") → ["p1", "p3", "p4"]
///   intersect([["p1","p2","p3","p5"], ["p1","p3","p4"]]) → ["p1", "p3"]
pub fn intersect_results(results: &[Vec<String>]) -> Vec<String> {
    if results.is_empty() {
        return Vec::new();
    }
    if results.len() == 1 {
        return results[0].clone();
    }

    // Use the smallest set as the base and filter against others
    let mut smallest_idx = 0;
    for (i, r) in results.iter().enumerate() {
        if r.len() < results[smallest_idx].len() {
            smallest_idx = i;
        }
    }

    let base = &results[smallest_idx];
    let mut intersection = Vec::new();

    for key in base {
        if results.iter().all(|r| r.contains(key)) {
            intersection.push(key.clone());
        }
    }

    intersection
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_index_insert_and_lookup() {
        let idx = ColumnIndex::new("status");

        idx.insert(&Value::String("alive".to_string()), "p1").ok();
        idx.insert(&Value::String("alive".to_string()), "p2").ok();
        idx.insert(&Value::String("dead".to_string()), "p3").ok();

        let alive = idx.lookup(&Value::String("alive".to_string()));
        assert_eq!(alive.len(), 2);
        assert!(alive.contains(&"p1".to_string()));
        assert!(alive.contains(&"p2".to_string()));

        let dead = idx.lookup(&Value::String("dead".to_string()));
        assert_eq!(dead.len(), 1);
        assert!(dead.contains(&"p3".to_string()));
    }

    #[test]
    fn test_column_index_remove() {
        let idx = ColumnIndex::new("status");

        idx.insert(&Value::String("alive".to_string()), "p1").ok();
        idx.insert(&Value::String("alive".to_string()), "p2").ok();

        // Remove one row
        idx.remove(&Value::String("alive".to_string()), "p1").ok();

        let alive = idx.lookup(&Value::String("alive".to_string()));
        assert_eq!(alive.len(), 1);
        assert!(alive.contains(&"p2".to_string()));

        // Remove the other row
        idx.remove(&Value::String("alive".to_string()), "p2").ok();

        let alive = idx.lookup(&Value::String("alive".to_string()));
        assert_eq!(alive.len(), 0);

        // Bucket should be dropped
        assert_eq!(idx.distinct_value_count(), 0);
    }

    #[test]
    fn test_column_index_numeric_values() {
        let idx = ColumnIndex::new("level");

        idx.insert(&Value::Number(10.into()), "p1").ok();
        idx.insert(&Value::Number(20.into()), "p2").ok();
        idx.insert(&Value::Number(10.into()), "p3").ok();

        let level_10 = idx.lookup(&Value::Number(10.into()));
        assert_eq!(level_10.len(), 2);
        assert!(level_10.contains(&"p1".to_string()));
        assert!(level_10.contains(&"p3".to_string()));
    }

    #[test]
    fn test_column_index_boolean_values() {
        let idx = ColumnIndex::new("active");

        idx.insert(&Value::Bool(true), "p1").ok();
        idx.insert(&Value::Bool(true), "p2").ok();
        idx.insert(&Value::Bool(false), "p3").ok();

        let active = idx.lookup(&Value::Bool(true));
        assert_eq!(active.len(), 2);

        let inactive = idx.lookup(&Value::Bool(false));
        assert_eq!(inactive.len(), 1);
    }

    #[test]
    fn test_column_index_null_values() {
        let idx = ColumnIndex::new("opt_field");

        idx.insert(&Value::Null, "p1").ok();
        idx.insert(&Value::String("value".to_string()), "p2").ok();

        let nulls = idx.lookup(&Value::Null);
        assert_eq!(nulls.len(), 1);
        assert!(nulls.contains(&"p1".to_string()));
    }

    #[test]
    fn test_column_index_prefix_lookup() {
        let idx = ColumnIndex::new("name");

        idx.insert(&Value::String("alice".to_string()), "p1").ok();
        idx.insert(&Value::String("albert".to_string()), "p2").ok();
        idx.insert(&Value::String("bob".to_string()), "p3").ok();

        let al_prefix = idx.lookup_prefix("al");
        assert_eq!(al_prefix.len(), 2);
        assert!(al_prefix.contains(&"p1".to_string()));
        assert!(al_prefix.contains(&"p2".to_string()));
    }

    #[test]
    fn test_column_index_distinct_and_count() {
        let idx = ColumnIndex::new("status");

        idx.insert(&Value::String("alive".to_string()), "p1").ok();
        idx.insert(&Value::String("alive".to_string()), "p2").ok();
        idx.insert(&Value::String("dead".to_string()), "p3").ok();

        assert_eq!(idx.distinct_value_count(), 2);  // alive, dead
        assert_eq!(idx.indexed_row_count(), 3);      // 2 alive + 1 dead
    }

    #[test]
    fn test_intersect_results_multiple_columns() {
        let status_results = vec!["p1".to_string(), "p2".to_string(), "p3".to_string(), "p5".to_string()];
        let zone_results = vec!["p1".to_string(), "p3".to_string(), "p4".to_string()];
        let level_results = vec!["p1".to_string(), "p2".to_string(), "p3".to_string(), "p6".to_string()];

        let intersection = intersect_results(&[status_results, zone_results, level_results]);

        // Only p1 and p3 are in all three sets
        assert_eq!(intersection.len(), 2);
        assert!(intersection.contains(&"p1".to_string()));
        assert!(intersection.contains(&"p3".to_string()));
    }

    #[test]
    fn test_intersect_results_empty() {
        let results = vec![];
        let intersection = intersect_results(&results);
        assert_eq!(intersection.len(), 0);
    }

    #[test]
    fn test_intersect_results_single_set() {
        let results = vec![vec!["p1".to_string(), "p2".to_string()]];
        let intersection = intersect_results(&results);
        assert_eq!(intersection.len(), 2);
        assert!(intersection.contains(&"p1".to_string()));
    }

    #[test]
    fn test_intersect_results_no_overlap() {
        let set1 = vec!["p1".to_string(), "p2".to_string()];
        let set2 = vec!["p3".to_string(), "p4".to_string()];
        let intersection = intersect_results(&[set1, set2]);
        assert_eq!(intersection.len(), 0);
    }

    #[test]
    fn test_column_index_clear() {
        let idx = ColumnIndex::new("test");

        idx.insert(&Value::String("a".to_string()), "p1").ok();
        idx.insert(&Value::String("b".to_string()), "p2").ok();

        assert_eq!(idx.distinct_value_count(), 2);
        idx.clear();
        assert_eq!(idx.distinct_value_count(), 0);
    }
}
