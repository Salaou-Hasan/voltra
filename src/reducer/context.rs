// ============================================================================
// ReducerContext — high-throughput rewrite
//
// Session 7 changes:
//  1. commit() calls apply_delta_batch() — atomic entry point for commits.
//
// Session 10 changes:
//  2. set_counter() now stages a "counter_add" delta (the amount to add)
//     instead of an absolute new value.  apply_delta_batch() re-reads the
//     current committed value under the row lock and adds the amount.
//     This makes the full read-modify-write cycle atomic: the read is no
//     longer outside the lock window, which is the root cause of lost updates
//     in the concurrent-increment test.
//  3. commit() returns Vec<RowDelta> from apply_delta_batch() — the committed
//     deltas now carry the actual written values (row_data is filled in by
//     apply_delta_batch for counter_add).
//  4. IncrementResult is constructed from the committed delta's row_data
//     instead of the pre-lock pending value, so callers always see the real
//     committed value.
//
// Session 28 changes (TODO-022):
//  5. Added `caller_role: String` field alongside existing `caller_id`.
//     Reducers can read `ctx.caller_role` to make role-based decisions.
//
// Previous changes:
//  1. Takes Arc<TableStore> directly — no Mutex wrapper.
//  2. Pending deltas pre-allocated Vec — no per-call heap alloc.
//  3. set_row builds Arc<Bytes> payload once and reuses it.
// ============================================================================

use crate::error::{NeonDBError, Result};
use crate::schema::SchemaRegistry;
use crate::table::{Counter, RowDelta, TableStore};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub struct SubscriptionDiff {
    pub table_name: String,
    pub row_key: String,
    pub payload: Arc<Bytes>,
}

pub struct ReducerContext {
    pub tables: Arc<TableStore>,
    pub timestamp: u64,
    /// Identity of the calling client (X-NeonDB-Identity header or TCP peer address).
    pub caller_id: String,
    /// Role of the calling client, parsed from the Bearer token suffix.
    /// Format: `Bearer <api_key>:<role>` → role = the part after the colon.
    /// Empty string when no role was supplied (open / anonymous access).
    pub caller_role: String,
    pub schema: Option<Arc<SchemaRegistry>>,
    /// Optional TTL manager — lets reducers set row expiration times.
    pub ttl: Option<Arc<crate::ttl::TtlManager>>,
    pending_deltas: Vec<RowDelta>,
    pub pending_diffs: Vec<SubscriptionDiff>,
}

impl ReducerContext {
    pub fn new(tables: Arc<TableStore>, timestamp: u64) -> Self {
        ReducerContext {
            tables,
            timestamp,
            caller_id: String::new(),
            caller_role: String::new(),
            schema: None,
            ttl: None,
            pending_deltas: Vec::with_capacity(4),
            pending_diffs: Vec::with_capacity(4),
        }
    }

    pub fn with_schema(mut self, schema: Arc<SchemaRegistry>) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_ttl(mut self, ttl: Arc<crate::ttl::TtlManager>) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set a TTL on a row so it expires after `ttl_ms` milliseconds from now.
    /// No-op if the TTL manager is not wired.
    pub fn set_ttl(&self, table_name: &str, row_key: &str, ttl_ms: u64) {
        if let Some(ttl) = &self.ttl {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            ttl.set_ttl(table_name, row_key, now, ttl_ms);
        }
    }

    /// Cancel the TTL on a row, making it permanent.
    /// No-op if the TTL manager is not wired.
    pub fn cancel_ttl(&self, table_name: &str, row_key: &str) {
        if let Some(ttl) = &self.ttl {
            ttl.cancel_ttl(table_name, row_key);
        }
    }

    // ── Reads (check pending deltas first for read-your-writes) ──────────────

    pub fn get_row(&self, table_name: &str, row_key: &str) -> Result<Option<Value>> {
        // Read-your-writes: check uncommitted deltas first (reverse order).
        for delta in self.pending_deltas.iter().rev() {
            if delta.table_name == table_name && delta.row_key == row_key {
                // Pending-delta reads bypass RLS — the caller already passed the
                // write-time check when staging the delta.
                return match delta.operation.as_str() {
                    "delete" => Ok(None),
                    _ => Ok(delta.row_data_value()),
                };
            }
        }

        // ── RLS check on committed rows ────────────────────────────────────────
        // Schedulers and system callers bypass all policies.
        if let Some(schema) = &self.schema {
            if let Some(table_schema) = schema.get(table_name) {
                if !crate::schema::rls_check(
                    &table_schema.rls,
                    // Read the row to evaluate ownership — only possible if
                    // the policy needs it; for Public this is not called.
                    self.tables.get_row(table_name, row_key)?.as_ref(),
                    &self.caller_id,
                    &self.caller_role,
                ) {
                    // Return None — do not leak row existence to unauthorised callers.
                    return Ok(None);
                }
            }
        }

        self.tables.get_row(table_name, row_key)
    }

    pub fn get_row_json(&self, table_name: &str, row_key: &str) -> Result<Option<Value>> {
        self.get_row(table_name, row_key)
    }

    pub fn list_counters(&self) -> Result<Vec<Counter>> {
        self.tables.list_counters()
    }

    // ── Writes (staged, applied atomically on commit) ─────────────────────────

    pub fn set_row(
        &mut self,
        table_name: String,
        row_key: String,
        row_value: Value,
    ) -> Result<RowDelta> {
        let row_value = if let Some(schema) = &self.schema {
            schema.validate(&table_name, row_value)?
        } else {
            row_value
        };

        let existing = self.get_row(&table_name, &row_key)?;
        let operation = if existing.is_some() {
            "update"
        } else {
            "insert"
        }
        .to_string();

        let encoded = serde_json::to_vec(&row_value)
            .map_err(|e| NeonDBError::SerializationError(format!("Row encode: {}", e)))?;
        let payload_arc = Arc::new(Bytes::from(encoded));

        let delta = RowDelta {
            table_name,
            operation,
            row_key,
            row_id: 0,
            shard_id: self.tables.shard_id(),
            payload_arc: Some(payload_arc),
            row_data: Some(row_value),
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        };
        self.pending_deltas.push(delta.clone());
        Ok(delta)
    }

    pub fn delete_row(&mut self, table_name: String, row_key: String) -> Result<RowDelta> {
        let delta = RowDelta {
            table_name,
            operation: "delete".to_string(),
            row_key,
            row_id: 0,
            shard_id: self.tables.shard_id(),
            payload_arc: None,
            row_data: None,
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        };
        self.pending_deltas.push(delta.clone());
        Ok(delta)
    }

    pub fn get_counter(&self, name: &str) -> Result<Option<Counter>> {
        let mut base = self.tables.get_counter(name)?;
        for delta in &self.pending_deltas {
            if delta.table_name == "counters" && delta.row_key == name {
                match delta.operation.as_str() {
                    "delete" => {
                        base = None;
                    }
                    "counter_add" => {
                        let cur = base.as_ref().map(|c| c.value).unwrap_or(0);
                        let id = base.as_ref().map(|c| c.id).unwrap_or(0);
                        base = Some(Counter {
                            id,
                            name: name.to_string(),
                            value: cur + delta.counter_add_amount,
                            last_modified: delta.counter_add_timestamp,
                        });
                    }
                    _ => {
                        if let Some(v) = delta.row_data_value() {
                            if let Ok(c) = serde_json::from_value::<Counter>(v) {
                                base = Some(c);
                            }
                        }
                    }
                }
            }
        }
        Ok(base)
    }

    pub fn set_counter(&mut self, name: String, amount: i32) -> Result<RowDelta> {
        let delta = RowDelta {
            table_name: "counters".to_string(),
            operation: "counter_add".to_string(),
            row_key: name,
            row_id: 0,
            shard_id: self.tables.shard_id(),
            payload_arc: None,
            row_data: None,
            counter_add_amount: amount,
            counter_add_timestamp: self.timestamp as i64,
        };
        self.pending_deltas.push(delta.clone());
        Ok(delta)
    }

    pub fn delete_counter(&mut self, name: &str) -> Result<RowDelta> {
        self.delete_row("counters".to_string(), name.to_string())
    }

    pub fn emit_diff(
        &mut self,
        table_name: String,
        row_key: String,
        payload: Arc<Bytes>,
    ) -> Result<()> {
        self.pending_diffs.push(SubscriptionDiff {
            table_name,
            row_key,
            payload,
        });
        Ok(())
    }

    pub fn commit(&mut self) -> Result<Vec<RowDelta>> {
        // ── RLS enforcement ────────────────────────────────────────────────────
        // Before handing deltas to apply_delta_batch, verify every staged write
        // passes the table's RLS policy.  This enforces per-row ownership checks
        // so that e.g. `alice` cannot modify `bob`'s rows.
        //
        // Bypass: caller_role == "scheduler" | "system" skips all checks.
        // (rls_check handles this internally.)
        if let Some(schema) = &self.schema {
            let mut denied_keys: Vec<String> = Vec::new();

            for delta in &self.pending_deltas {
                // counter_add deltas target the "counters" system table which is
                // schema-free — skip RLS check for counters.
                if delta.operation == "counter_add" {
                    continue;
                }

                let table_schema = match schema.get(&delta.table_name) {
                    Some(ts) => ts,
                    None => continue, // No schema → Public policy → allow.
                };

                // For write RLS: fetch the CURRENT committed row (before this
                // delta is applied) to check ownership.  This prevents a caller
                // from modifying another user's row even if the operation would
                // overwrite the owner field.
                let current_row = if delta.operation == "insert" {
                    None // New row — ownership will be set by the reducer.
                } else {
                    self.tables.get_row(&delta.table_name, &delta.row_key)?
                };

                if !crate::schema::rls_check(
                    &table_schema.rls,
                    current_row.as_ref(),
                    &self.caller_id,
                    &self.caller_role,
                ) {
                    denied_keys.push(format!("{}/{}", delta.table_name, delta.row_key));
                }
            }

            if !denied_keys.is_empty() {
                self.pending_deltas.clear();
                self.pending_diffs.clear();
                return Err(NeonDBError::PermissionDenied(format!(
                    "Access denied to rows: {:?}",
                    denied_keys
                )));
            }
        }

        let committed = self.tables.apply_delta_batch(&self.pending_deltas)?;
        self.pending_deltas.clear();
        self.pending_diffs.clear();
        Ok(committed)
    }

    /// Extract staged deltas WITHOUT applying them to the TableStore.
    ///
    /// Used by the Raft write path: the caller forwards the returned deltas to
    /// `Raft::client_write(RaftRequest { deltas, … })`. The Raft state machine
    /// then applies the deltas on every node (including the leader) via
    /// `apply_delta_batch` once the entry is committed.
    ///
    /// After calling this, `pending_deltas` is empty and subsequent reads
    /// will see the old committed state (read-your-writes is now the caller's
    /// responsibility, since the deltas haven't been applied yet).
    pub fn drain_pending_deltas(&mut self) -> Vec<RowDelta> {
        self.pending_diffs.clear();
        std::mem::take(&mut self.pending_deltas)
    }

    pub fn rollback(&mut self) {
        self.pending_deltas.clear();
        self.pending_diffs.clear();
    }
}

// ── Increment reducer ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IncrementResult {
    pub new_value: i32,
    pub timestamp: i64,
}

pub fn increment_reducer(
    ctx: &mut ReducerContext,
    name: String,
    delta: i32,
) -> Result<(IncrementResult, Vec<RowDelta>)> {
    let current = ctx.get_counter(&name)?.map(|c| c.value).unwrap_or(0);
    let provisional_new = current + delta;
    let row_delta = ctx.set_counter(name, delta)?;
    Ok((
        IncrementResult {
            new_value: provisional_new,
            timestamp: ctx.timestamp as i64,
        },
        vec![row_delta],
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ReducerContext {
        ReducerContext::new(Arc::new(TableStore::new()), 1000)
    }

    #[test]
    fn test_increment_reducer() {
        let mut c = ctx();
        let (r, deltas) = increment_reducer(&mut c, "foo".to_string(), 5).unwrap();
        assert_eq!(r.new_value, 5);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, "counter_add");

        let (r2, _) = increment_reducer(&mut c, "foo".to_string(), 3).unwrap();
        assert_eq!(r2.new_value, 8);
    }

    #[test]
    fn test_increment_reducer_negative() {
        let mut c = ctx();
        increment_reducer(&mut c, "bar".to_string(), 10).unwrap();
        let (r, _) = increment_reducer(&mut c, "bar".to_string(), -3).unwrap();
        assert_eq!(r.new_value, 7);
    }

    #[test]
    fn test_commit_applies_writes_atomically() {
        let tables = Arc::new(TableStore::new());
        let mut c = ReducerContext::new(tables.clone(), 1000);
        increment_reducer(&mut c, "x".to_string(), 99).unwrap();
        let committed = c.commit().unwrap();
        assert_eq!(tables.get_counter("x").unwrap().unwrap().value, 99);
        assert!(committed[0].row_data.is_some());
    }

    #[test]
    fn test_rollback_discards_pending_deltas() {
        let tables = Arc::new(TableStore::new());
        let mut c = ReducerContext::new(tables.clone(), 1000);
        increment_reducer(&mut c, "y".to_string(), 50).unwrap();
        c.rollback();
        assert!(tables.get_counter("y").unwrap().is_none());
    }

    #[test]
    fn test_payload_arc_is_shared() {
        let mut c = ctx();
        let delta = c
            .set_row(
                "test_table".to_string(),
                "z".to_string(),
                serde_json::json!({"value": 42}),
            )
            .unwrap();
        let arc1 = delta.payload_arc.clone().unwrap();
        let arc2 = arc1.clone();
        assert_eq!(arc1.as_ptr(), arc2.as_ptr());
    }

    #[test]
    fn test_read_your_writes_counter_add() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("pts".to_string(), 10, 0).unwrap();

        let mut c = ReducerContext::new(tables.clone(), 0);
        increment_reducer(&mut c, "pts".to_string(), 5).unwrap();
        let (r, _) = increment_reducer(&mut c, "pts".to_string(), 3).unwrap();
        assert_eq!(r.new_value, 18);

        c.commit().unwrap();
        assert_eq!(tables.get_counter("pts").unwrap().unwrap().value, 18);
    }

    #[test]
    fn test_caller_id_default_is_empty() {
        let c = ctx();
        assert_eq!(c.caller_id, "");
    }

    #[test]
    fn test_caller_id_can_be_set() {
        let mut c = ctx();
        c.caller_id = "player-42".to_string();
        assert_eq!(c.caller_id, "player-42");
    }

    #[test]
    fn test_caller_role_default_is_empty() {
        let c = ctx();
        assert_eq!(c.caller_role, "");
    }

    #[test]
    fn test_caller_role_can_be_set() {
        let mut c = ctx();
        c.caller_role = "admin".to_string();
        assert_eq!(c.caller_role, "admin");
    }
}
