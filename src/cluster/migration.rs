// ============================================================================
// src/cluster/migration.rs — Streaming key migration for cluster rebalancing
//
// When a new cluster joins, it claims ~1/(N+1) of the existing keys.
// The migration coordinator:
//   1. Computes which (table, key) pairs move to the new cluster.
//   2. Streams them in batches to the new cluster via POST /cluster/receive.
//   3. Deletes them locally after confirmed transfer.
//   4. Tracks progress at GET /cluster/migration-status.
//
// Zero-downtime: during migration both old and new cluster serve reads.
// Writes are routed to the new owner immediately after the ring update.
// ============================================================================

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cluster::{ClusterBus, NodeInfo};
use crate::cluster::ring::SharedRing;
use crate::table::TableStore;
use crate::error::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Status types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MigrationStatus {
    Idle,
    Running,
    Complete,
    Failed(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationState {
    pub status:          MigrationStatus,
    pub target_cluster:  String,
    pub total_keys:      usize,
    pub migrated_keys:   usize,
    pub failed_keys:     usize,
    pub started_at_secs: u64,
    pub finished_at_secs: Option<u64>,
}

impl MigrationState {
    fn idle() -> Self {
        MigrationState {
            status:           MigrationStatus::Idle,
            target_cluster:   String::new(),
            total_keys:       0,
            migrated_keys:    0,
            failed_keys:      0,
            started_at_secs:  0,
            finished_at_secs: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire types (sent to receiving cluster)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
pub struct MigrateRow {
    pub table: String,
    pub key:   String,
    pub data:  Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MigrateBatch {
    pub source_cluster: String,
    pub rows:           Vec<MigrateRow>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MigrateBatchResult {
    pub accepted: usize,
    pub rejected: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// MigrationCoordinator
// ─────────────────────────────────────────────────────────────────────────────

pub struct MigrationCoordinator {
    state: Arc<Mutex<MigrationState>>,
}

impl MigrationCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(MigrationCoordinator {
            state: Arc::new(Mutex::new(MigrationState::idle())),
        })
    }

    /// Current state snapshot (for /cluster/migration-status).
    pub fn status(&self) -> MigrationState {
        self.state.lock().map(|s| s.clone()).unwrap_or_else(|_| MigrationState::idle())
    }

    /// Start a rebalancing migration in the background.
    ///
    /// - `new_cluster_id`  — the ID of the cluster joining the ring.
    /// - `new_node`        — the metrics HTTP URL of that cluster.
    /// - `ring`            — the shared ring (will be updated inside this call).
    /// - `tables`          — local TableStore (keys to scan + delete after send).
    /// - `bus`             — cluster bus for HTTP access.
    /// - `my_cluster_id`   — this node's cluster ID (used in batch source field).
    /// - `batch_size`      — rows per HTTP batch (default 200).
    pub fn start_rebalance(
        self: &Arc<Self>,
        new_cluster_id:  String,
        new_node:        NodeInfo,
        ring:            Arc<SharedRing>,
        tables:          Arc<TableStore>,
        bus:             Arc<ClusterBus>,
        my_cluster_id:   String,
        batch_size:      usize,
    ) {
        // Guard: only one migration at a time.
        {
            let mut st = self.state.lock().unwrap();
            if st.status == MigrationStatus::Running {
                log::warn!("[migration] already running, ignoring new start request");
                return;
            }
            *st = MigrationState {
                status:           MigrationStatus::Running,
                target_cluster:   new_cluster_id.clone(),
                total_keys:       0,
                migrated_keys:    0,
                failed_keys:      0,
                started_at_secs:  now_secs(),
                finished_at_secs: None,
            };
        }

        let coord = Arc::clone(self);
        let batch_size = batch_size.max(1);

        std::thread::spawn(move || {
            let result = run_migration(
                &coord,
                &new_cluster_id,
                &new_node,
                &ring,
                &tables,
                &bus,
                &my_cluster_id,
                batch_size,
            );

            let mut st = coord.state.lock().unwrap();
            st.finished_at_secs = Some(now_secs());
            match result {
                Ok(()) => st.status = MigrationStatus::Complete,
                Err(e) => {
                    log::error!("[migration] failed: {e}");
                    st.status = MigrationStatus::Failed(e.to_string());
                }
            }
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core migration logic (runs in its own OS thread)
// ─────────────────────────────────────────────────────────────────────────────

fn run_migration(
    coord:          &Arc<MigrationCoordinator>,
    new_cluster_id: &str,
    new_node:       &NodeInfo,
    ring:           &Arc<SharedRing>,
    tables:         &Arc<TableStore>,
    bus:            &Arc<ClusterBus>,
    my_cluster_id:  &str,
    batch_size:     usize,
) -> Result<()> {
    // 1. Collect all local (table, key) pairs.
    let table_names = tables.list_tables();
    let mut all_keys: Vec<(String, String)> = Vec::new();
    for table in &table_names {
        if let Ok(rows) = tables.list_rows_with_keys(table) {
            for (key, _) in rows {
                all_keys.push((table.clone(), key));
            }
        }
    }

    // 2. Compute which keys should migrate to the new cluster.
    //    "Should migrate" = new owner (ring WITH new cluster) is new_cluster_id
    //    AND old owner was this node.
    let flat_keys: Vec<String> = all_keys.iter()
        .map(|(t, k)| format!("{t}:{k}"))
        .collect();

    let migrating_flat = ring.keys_migrating_to(new_cluster_id, &flat_keys);
    let total = migrating_flat.len();

    {
        let mut st = coord.state.lock().unwrap();
        st.total_keys = total;
    }

    if total == 0 {
        log::info!("[migration] no keys to migrate to {new_cluster_id}");
        // Still add the cluster to the ring so future keys route there.
        ring.add_cluster(new_cluster_id);
        return Ok(());
    }

    log::info!("[migration] migrating {total} keys to {new_cluster_id}");

    // 3. Add new cluster to ring NOW (new writes go there, reads still work from old).
    ring.add_cluster(new_cluster_id);

    // 4. Stream rows in batches.
    let client = bus.http_client();
    let receive_url = format!("{}/cluster/receive", new_node.metrics_url);
    let mut migrated = 0usize;
    let mut failed   = 0usize;

    // Parse the flat composite keys back to (table, key).
    let migrate_pairs: Vec<(String, String)> = migrating_flat.into_iter()
        .filter_map(|(flat, _)| {
            let mut parts = flat.splitn(2, ':');
            let t = parts.next()?.to_owned();
            let k = parts.next()?.to_owned();
            Some((t, k))
        })
        .collect();

    for chunk in migrate_pairs.chunks(batch_size) {
        let mut batch_rows: Vec<MigrateRow> = Vec::with_capacity(chunk.len());
        for (table, key) in chunk {
            if let Ok(Some(data)) = tables.get_row(table, key) {
                batch_rows.push(MigrateRow { table: table.clone(), key: key.clone(), data });
            }
        }
        if batch_rows.is_empty() { continue; }

        let batch_len = batch_rows.len();
        let batch = MigrateBatch {
            source_cluster: my_cluster_id.to_owned(),
            rows: batch_rows,
        };

        // Send to new cluster.
        let resp = client.post(&receive_url).json(&batch).send();
        match resp {
            Ok(r) if r.status().is_success() => {
                // Delete locally only after confirmed receipt.
                for (table, key) in &chunk[..batch_len.min(chunk.len())] {
                    let _ = tables.delete_row(table, key);
                }
                migrated += batch_len;
                log::debug!("[migration] batch of {batch_len} sent, total migrated={migrated}");
            }
            Ok(r) => {
                let status = r.status();
                log::warn!("[migration] batch rejected by target: {status}");
                failed += batch_len;
            }
            Err(e) => {
                log::warn!("[migration] batch HTTP error: {e}");
                failed += batch_len;
            }
        }

        // Update progress.
        let mut st = coord.state.lock().unwrap();
        st.migrated_keys = migrated;
        st.failed_keys   = failed;
    }

    log::info!("[migration] done: {migrated} migrated, {failed} failed");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handler helper: receive a batch of rows from a peer
// ─────────────────────────────────────────────────────────────────────────────

/// Called from the HTTP handler for `POST /cluster/receive`.
/// Writes all rows into the local TableStore.
pub fn apply_received_batch(batch: &MigrateBatch, tables: &Arc<TableStore>) -> MigrateBatchResult {
    let mut accepted = 0;
    let mut rejected = 0;
    for row in &batch.rows {
        match tables.set_row(row.table.clone(), row.key.clone(), row.data.clone()) {
            Ok(_) => accepted += 1,
            Err(e) => {
                log::warn!("[migration] failed to store {}/{}: {e}", row.table, row.key);
                rejected += 1;
            }
        }
    }
    MigrateBatchResult { accepted, rejected }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_starts_idle() {
        let coord = MigrationCoordinator::new();
        let st = coord.status();
        assert_eq!(st.status, MigrationStatus::Idle);
        assert_eq!(st.total_keys, 0);
    }

    #[test]
    fn migration_status_serializes() {
        let st = MigrationState {
            status:           MigrationStatus::Running,
            target_cluster:   "asia".to_owned(),
            total_keys:       500,
            migrated_keys:    200,
            failed_keys:      0,
            started_at_secs:  1_700_000_000,
            finished_at_secs: None,
        };
        let json = serde_json::to_string(&st).unwrap();
        assert!(json.contains("Running"));
        assert!(json.contains("asia"));
        assert!(json.contains("500"));
    }

    #[test]
    fn migrate_batch_roundtrip() {
        let batch = MigrateBatch {
            source_cluster: "africa".to_owned(),
            rows: vec![
                MigrateRow {
                    table: "players".to_owned(),
                    key:   "alice".to_owned(),
                    data:  serde_json::json!({ "hp": 100, "alive": true }),
                },
            ],
        };
        let json = serde_json::to_string(&batch).unwrap();
        let decoded: MigrateBatch = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(decoded.rows[0].key, "alice");
    }
}
