// ============================================================================
// persistence/mod.rs — sled-backed disk persistence for NeonDB rows
//
// Provides a write-through persistence layer on top of the in-memory
// TableStore.  Every committed RowDelta is written to a sled database so
// that data survives clean server restarts without requiring a full WAL
// replay from scratch.
//
// ## Startup sequence
//
//   1. PersistenceEngine::open(path) — opens / creates the sled directory.
//   2. pe.load_all(&tables)          — restores all rows to the TableStore.
//                                      Returns (row_count, last_seq) so the
//                                      caller knows which WAL entries are new.
//   3. Normal WAL replay for entries after last_seq.
//
// ## On each committed batch
//
//   pe.persist_deltas(&deltas, seq)  — writes the batch atomically.
//
// ## Error policy
//
//   Persistence errors are NON-FATAL.  The WAL still covers crash recovery
//   for sub-second windows.  Log and continue.
//
// ## Key encoding
//
//   Row key  : "table_name\x00row_key"     (byte format: table + NUL + key)
//   Meta key : "\xff\x00last_seq"           (prefix 0xFF avoids all user keys)
// ============================================================================

use crate::error::{NeonDBError, Result};
use crate::table::{RowDelta, TableStore};
use std::path::Path;

const META_LAST_SEQ_KEY: &[u8] = b"\xff\x00last_seq";

// ── PersistenceEngine ─────────────────────────────────────────────────────────

pub struct PersistenceEngine {
    db: sled::Db,
}

impl PersistenceEngine {
    /// Open (or create) the sled database at the given directory path.
    pub fn open(path: &Path) -> Result<Self> {
        let db = sled::open(path).map_err(|e| {
            NeonDBError::StorageError(format!("open sled at {:?}: {}", path, e))
        })?;
        Ok(PersistenceEngine { db })
    }

    /// Load every persisted row into `tables`.
    ///
    /// Returns `(rows_loaded, last_seq_number)`.  If the database is empty,
    /// returns `(0, 0)`.
    pub fn load_all(&self, tables: &TableStore) -> Result<(usize, u64)> {
        // Retrieve last persisted WAL sequence number.
        let last_seq: u64 = match self.db.get(META_LAST_SEQ_KEY).map_err(|e| {
            NeonDBError::StorageError(format!("sled get last_seq: {}", e))
        })? {
            Some(bytes) if bytes.len() == 8 => {
                let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0u8; 8]);
                u64::from_le_bytes(arr)
            }
            _ => 0,
        };

        let mut count = 0usize;

        for result in self.db.iter() {
            let (k, v) = result.map_err(|e| {
                NeonDBError::StorageError(format!("sled iter: {}", e))
            })?;

            // Skip metadata keys (start with 0xFF).
            if k.first() == Some(&0xFF) {
                continue;
            }

            // Parse composite key: "table_name\x00row_key"
            let key_bytes: &[u8] = &k;
            let null_pos = match key_bytes.iter().position(|&b| b == 0x00) {
                Some(p) => p,
                None => continue,
            };
            let table_name = match std::str::from_utf8(&key_bytes[..null_pos]) {
                Ok(s) if !s.is_empty() => s,
                _ => continue,
            };
            let row_key = match std::str::from_utf8(&key_bytes[null_pos + 1..]) {
                Ok(s) if !s.is_empty() => s,
                _ => continue,
            };

            // Deserialize JSON and insert into TableStore.
            match serde_json::from_slice::<serde_json::Value>(&v) {
                Ok(value) => {
                    if let Err(e) = tables.set_row(
                        table_name.to_string(),
                        row_key.to_string(),
                        value,
                    ) {
                        log::warn!(
                            "[persist] Failed to restore {}/{}: {}",
                            table_name,
                            row_key,
                            e
                        );
                    } else {
                        count += 1;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[persist] JSON decode error for {}/{}: {}",
                        table_name,
                        row_key,
                        e
                    );
                }
            }
        }

        Ok((count, last_seq))
    }

    /// Atomically persist a committed batch of deltas.
    ///
    /// `seq` is the WAL sequence number of this batch so `load_all` knows
    /// which WAL entries to skip on the next startup.
    pub fn persist_deltas(&self, deltas: &[RowDelta], seq: u64) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }

        let mut batch = sled::Batch::default();

        for delta in deltas {
            let composite = format!("{}\x00{}", delta.table_name, delta.row_key);
            let key_bytes = composite.as_bytes();

            match delta.operation.as_str() {
                "insert" | "update" | "counter_add" => {
                    if let Some(value) = delta.row_data_value() {
                        match serde_json::to_vec(&value) {
                            Ok(bytes) => {
                                batch.insert(key_bytes, bytes.as_slice());
                            }
                            Err(e) => {
                                log::warn!(
                                    "[persist] JSON encode error for {}: {}",
                                    composite, e
                                );
                            }
                        }
                    }
                }
                "delete" => {
                    batch.remove(key_bytes);
                }
                other => {
                    log::warn!("[persist] Unknown operation '{}', skipping", other);
                }
            }
        }

        // Update the last sequence number in the same batch.
        batch.insert(META_LAST_SEQ_KEY, &seq.to_le_bytes());

        self.db.apply_batch(batch).map_err(|e| {
            NeonDBError::StorageError(format!("sled apply_batch: {}", e))
        })?;

        // sled flushes to disk automatically in the background.
        // We only need an explicit flush for hard-durability guarantees
        // after a clean shutdown; the WAL covers crash recovery in between.

        Ok(())
    }
}
