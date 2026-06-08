// ============================================================================
// src/raft/state_machine.rs — Raft state machine for NeonDB
//
// Implements openraft's RaftStateMachine trait.
//
// The state machine is the "what gets applied" half of Raft:
//   • Every committed log entry flows through apply() here.
//   • apply() calls TableStore::apply_delta_batch() then fires subscription
//     fan-out so connected clients see the change in real time.
//   • Snapshots are built by serialising all current TableStore rows to JSON
//     and stored as Cursor<Vec<u8>>.  On install_snapshot the TableStore is
//     cleared and reloaded from the serialised state.
// ============================================================================

use std::io::Cursor;
use std::sync::Arc;

use openraft::{
    BasicNode, EntryPayload, LogId, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use openraft::storage::RaftStateMachine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::raft::{RaftResponse, TypeConfig};
use crate::subscriptions::SubscriptionManager;
use crate::table::TableStore;

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot data format
// ─────────────────────────────────────────────────────────────────────────────

/// Serialised form of the entire TableStore, used for Raft snapshot transfer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SerializedState {
    /// All rows in every table: `{ table_name → { row_key → row_data } }`.
    pub tables: std::collections::HashMap<String, std::collections::HashMap<String, Value>>,
    /// Counter values: `{ counter_name → value }`.
    pub counters: std::collections::HashMap<String, i32>,
    /// The last applied log id at the time this snapshot was taken.
    pub last_applied_log_id: Option<LogId<u64>>,
    /// The last applied membership config.
    pub last_membership: StoredMembership<u64, BasicNode>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot builder
// ─────────────────────────────────────────────────────────────────────────────

pub struct NeonSnapshotBuilder {
    state: Arc<parking_lot::RwLock<StateMachineInner>>,
}

impl RaftSnapshotBuilder<TypeConfig> for NeonSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let inner = self.state.read();

        // Collect all rows from the TableStore.
        let mut tables = std::collections::HashMap::new();
        for table_name in inner.tables.list_tables() {
            let rows = inner.tables.get_all_rows(&table_name);
            tables.insert(table_name, rows);
        }

        // Collect all counters.
        let counters = inner.tables.get_all_counters_map();

        let snapshot_data = SerializedState {
            tables,
            counters,
            last_applied_log_id: inner.last_applied_log_id,
            last_membership: inner.last_membership.clone(),
        };

        let bytes = serde_json::to_vec(&snapshot_data).map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::write_state_machine(
                    anyerror::AnyError::new(&e),
                ),
            }
        })?;

        let snapshot_id = format!(
            "snapshot-{}",
            snapshot_data
                .last_applied_log_id
                .map(|id| id.index)
                .unwrap_or(0)
        );

        let meta = SnapshotMeta {
            last_log_id: inner.last_applied_log_id,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// State machine inner state
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct StateMachineInner {
    pub tables: Arc<TableStore>,
    pub subs: Arc<SubscriptionManager>,
    pub last_applied_log_id: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    /// The serialised snapshot currently held (for get_current_snapshot).
    pub current_snapshot: Option<Snapshot<TypeConfig>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// NeonStateMachine — the main state machine implementation
// ─────────────────────────────────────────────────────────────────────────────

pub struct NeonStateMachine {
    pub(crate) inner: Arc<parking_lot::RwLock<StateMachineInner>>,
}

impl NeonStateMachine {
    pub fn new(tables: Arc<TableStore>, subs: Arc<SubscriptionManager>) -> Self {
        Self {
            inner: Arc::new(parking_lot::RwLock::new(StateMachineInner {
                tables,
                subs,
                last_applied_log_id: None,
                last_membership: StoredMembership::default(),
                current_snapshot: None,
            })),
        }
    }
}

impl RaftStateMachine<TypeConfig> for NeonStateMachine {
    type SnapshotBuilder = NeonSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let inner = self.inner.read();
        Ok((inner.last_applied_log_id, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse { applied: 0 },

                EntryPayload::Normal(req) => {
                    // Apply the committed deltas to the TableStore.
                    let applied = {
                        let inner = self.inner.read();
                        match inner.tables.apply_delta_batch(&req.deltas) {
                            Ok(committed) => {
                                // Fan-out to subscribers (best-effort; errors are non-fatal).
                                let _ = inner.subs.publish_deltas(&committed);
                                committed.len()
                            }
                            Err(e) => {
                                log::warn!("[raft] apply_delta_batch failed: {}", e);
                                0
                            }
                        }
                    };
                    RaftResponse { applied }
                }

                EntryPayload::Membership(membership) => {
                    // Store the new membership — no table changes needed.
                    {
                        let mut inner = self.inner.write();
                        inner.last_membership = StoredMembership::new(Some(log_id), membership);
                    }
                    RaftResponse { applied: 0 }
                }
            };

            // Update last_applied_log_id for every entry type.
            {
                let mut inner = self.inner.write();
                inner.last_applied_log_id = Some(log_id);
            }

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> NeonSnapshotBuilder {
        NeonSnapshotBuilder {
            state: self.inner.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let state: SerializedState = serde_json::from_slice(&bytes).map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::read_state_machine(
                    anyerror::AnyError::new(&e),
                ),
            }
        })?;

        let mut inner = self.inner.write();

        // Clear existing data and reload from snapshot.
        inner.tables.clear_all();
        for (table_name, rows) in &state.tables {
            for (key, value) in rows {
                let _ = inner.tables.set_row(table_name.clone(), key.clone(), value.clone());
            }
        }
        for (name, value) in &state.counters {
            let _ = inner.tables.set_counter(
                name.clone(),
                *value,
                chrono_now_ms(),
            );
        }

        inner.last_applied_log_id = state.last_applied_log_id;
        inner.last_membership     = state.last_membership;

        // Cache the snapshot for get_current_snapshot.
        inner.current_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(bytes)),
        });

        log::info!("[raft] installed snapshot at log_id={:?}", inner.last_applied_log_id);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.read();
        // Re-wrap the cached bytes in a new Cursor (Cursor is not Clone).
        Ok(inner.current_snapshot.as_ref().map(|s| {
            let bytes = s.snapshot.get_ref().clone();
            Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(bytes)),
            }
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────────────────────────────────────

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::SubscriptionManager;
    use crate::table::TableStore;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    fn make_sm() -> NeonStateMachine {
        NeonStateMachine::new(
            Arc::new(TableStore::new()),
            Arc::new(SubscriptionManager::new()),
        )
    }

    fn make_log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    #[tokio::test]
    async fn test_applied_state_initially_none() {
        let mut sm = make_sm();
        let (log_id, _membership) = sm.applied_state().await.unwrap();
        assert!(log_id.is_none());
    }

    #[tokio::test]
    async fn test_apply_blank_entry() {
        let mut sm = make_sm();
        let entry = Entry::<TypeConfig> {
            log_id:  make_log_id(1, 1),
            payload: EntryPayload::Blank,
        };
        let resps = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(resps.len(), 1);
        assert_eq!(resps[0].applied, 0);

        let (log_id, _) = sm.applied_state().await.unwrap();
        assert_eq!(log_id, Some(make_log_id(1, 1)));
    }

    #[tokio::test]
    async fn test_apply_normal_entry_with_deltas() {
        let mut sm = make_sm();
        let req = crate::raft::RaftRequest {
            reducer_name: "test".to_string(),
            args:         vec![],
            deltas:       vec![crate::table::RowDelta {
                table_name:           "players".to_string(),
                row_key:              "alice".to_string(),
                operation:            "insert".to_string(),
                row_data:             Some(serde_json::json!({ "hp": 100 })),
                row_id:               1,
                shard_id:             0,
                payload_arc:          None,
                counter_add_amount:   0,
                counter_add_timestamp: 0,
            }],
            timestamp_ms: 0,
        };
        let entry = Entry::<TypeConfig> {
            log_id:  make_log_id(1, 1),
            payload: EntryPayload::Normal(req),
        };
        let resps = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(resps[0].applied, 1);

        // Verify the row landed in the TableStore.
        let inner = sm.inner.read();
        let row = inner.tables.get_row("players", "alice").unwrap();
        assert!(row.is_some());
    }

    #[tokio::test]
    async fn test_snapshot_roundtrip() {
        let mut sm = make_sm();
        // Write a row first.
        {
            let inner = sm.inner.read();
            inner.tables.set_row(
                "counters".to_string(),
                "score".to_string(),
                serde_json::json!({ "value": 99 }),
            ).unwrap();
        }

        // Build a snapshot.
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let snap_meta = snap.meta.clone();

        // Install the snapshot into a fresh state machine.
        let mut sm2 = make_sm();
        sm2.install_snapshot(&snap_meta, snap.snapshot).await.unwrap();

        // The row should be present in sm2.
        let inner = sm2.inner.read();
        assert!(inner.tables.get_row("counters", "score").unwrap().is_some());
    }

    #[tokio::test]
    async fn test_begin_receiving_snapshot_returns_empty_cursor() {
        let mut sm = make_sm();
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.get_ref().is_empty());
    }
}
