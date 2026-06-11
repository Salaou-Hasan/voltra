// ============================================================================
// NeonDB Table Engine — High-throughput rewrite
//
// Session 4  — CPU-aware DashMap shard count
// Session 7  — Serializable isolation (TODO-001) + atomicity on panic (TODO-002)
// Session 8  — main.rs wiring fix for TODO-003
// Session 9  — Fixed isolation test to use ReducerContext
// Session 10 — Root-cause fix for lost-update bug in isolation test.
//
//   THE BUG (sessions 7-9 misdiagnosis):
//     apply_delta_batch() acquires row locks only during the write phase.
//     But ReducerContext reads the counter BEFORE commit() is called, i.e.
//     before the lock is held.  Two threads both read old_value=N, both
//     stage new_value=N+1, then commit sequentially — second write clobbers
//     the first.  The lock serialises the writes but not the read-modify-write
//     cycle as a whole.
//
//   THE FIX:
//     Add a new RowDelta operation: "counter_add".  Instead of staging the
//     absolute new counter value, ReducerContext::set_counter() now stages
//     a delta amount (+N).  apply_delta_batch() handles "counter_add" by
//     re-reading the CURRENT committed value under the row lock and adding
//     the delta atomically.  The read is now inside the lock window.
//
//     The "insert"/"update" absolute-write operations remain unchanged for
//     all other use-cases (set_row, WAL replay, direct tests).
//
//   PITFALL: "counter_add" deltas do NOT carry a pre-computed row_data.
//     row_data is set to None on the staged delta and filled in by
//     apply_delta_batch() after the locked re-read+add.  Callers that
//     inspect delta.row_data before commit() will see None — this is
//     intentional and documented.
// ============================================================================

pub mod eviction;
pub mod dispatcher;

pub use eviction::{EvictionPolicy, LruTracker};
pub use dispatcher::{LobbyDispatcher, parse_lobby_key};

use crate::error::{NeonDBError, Result};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Compute the optimal DashMap inner-shard count for the current machine.
fn optimal_row_shard_count() -> usize {
    let cpus = num_cpus::get();
    let target = (cpus * 4).next_power_of_two();
    target.max(16)
}

// ── Blob-size guard ──────────────────────────────────────────────────────────
//
// Bounds the maximum size of a single blob written through `BlobStore::store_blob`.
// A misbehaving reducer (or a malicious client) could otherwise stage a multi-GB
// inventory array and balloon both the on-disk blob file and the in-memory copy.
//
// The limit is a global process-wide AtomicUsize so it can be configured at
// startup from Config (see `Config::apply_global_limits`) without threading
// through every TableStore method.
//
// Default: 16 MiB.
const DEFAULT_MAX_BLOB_SIZE_BYTES: usize = 16 * 1024 * 1024;
static MAX_BLOB_SIZE_BYTES: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_BLOB_SIZE_BYTES);

/// Set the maximum permitted blob size (in bytes) for all subsequent
/// `BlobStore::store_blob` calls in this process.  Typically called once
/// from `main()` via `Config::apply_global_limits`.
pub fn set_max_blob_size(bytes: usize) {
    MAX_BLOB_SIZE_BYTES.store(bytes.max(1), Ordering::Relaxed);
}

/// Current maximum blob size in bytes.
pub fn max_blob_size() -> usize {
    MAX_BLOB_SIZE_BYTES.load(Ordering::Relaxed)
}

// ── Public types ─────────────────────────────────────────────────────────────

pub type RowId = u32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Counter {
    pub id: RowId,
    pub name: String,
    pub value: i32,
    pub last_modified: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Player {
    pub id: RowId,
    pub name: String,
    pub level: i32,
    pub last_modified: i64,
    pub blob_offset: Option<u64>,
}

/// Lightweight delta carrying a shared reference to the serialised payload.
/// Cloning is O(1) — Arc refcount bump only.
///
/// ## Operations
/// - `"insert"` / `"update"` — write `row_data` as the new absolute row value.
/// - `"delete"` — remove the row; `row_data` is None.
/// - `"counter_add"` — atomically add `counter_add_amount` to the named
///   counter under the row lock.  `row_data` is None before commit and is
///   filled in by `apply_delta_batch()`.  This is the ONLY operation that
///   performs a locked read-modify-write, which is what makes concurrent
///   increments correct.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowDelta {
    pub table_name: String,
    pub operation: String,
    pub row_key: String,
    pub row_id: RowId,
    pub shard_id: u32,
    #[serde(skip)]
    pub payload_arc: Option<Arc<Bytes>>,
    pub row_data: Option<Value>,
    /// Non-zero only for `"counter_add"` operations.
    /// Stores the signed integer amount to add to the counter's current value.
    #[serde(default)]
    pub counter_add_amount: i32,
    /// Timestamp for `"counter_add"` — used to set `last_modified`.
    #[serde(default)]
    pub counter_add_timestamp: i64,
}

impl RowDelta {
    pub fn row_data_value(&self) -> Option<Value> {
        if let Some(arc) = &self.payload_arc {
            serde_json::from_slice(arc).ok()
        } else {
            self.row_data.clone()
        }
    }
}

// ── Internal row representation ───────────────────────────────────────────────
//
// `data` is stored as plain `Bytes` (not `Arc<Bytes>`).  `Bytes` already carries
// an internal Arc for O(1) clones; wrapping in another Arc was double-boxing —
// two heap allocations per row for no benefit.  Removing the outer Arc saves
// ~16 bytes per live row (the Arc header) and one heap allocation per insert.
#[derive(Clone, Debug)]
struct StoredRow {
    row_id: RowId,
    shard_id: u32,
    data: Bytes,
    blob_offset: Option<u64>,
    /// Optimistic-concurrency version: bumped on every write. Reducers record
    /// the version they read; commit aborts (and retries) if a concurrent
    /// write bumped it — eliminates silent lost updates in read-modify-write.
    version: u64,
}

// ── Blob store ────────────────────────────────────────────────────────────────

struct BlobStore {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
    next_offset: u64,
}

impl BlobStore {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let next_offset = file.seek(SeekFrom::End(0))?;
        Ok(BlobStore {
            path,
            file,
            next_offset,
        })
    }

    fn store_blob(&mut self, payload: &[u8]) -> Result<u64> {
        let max = MAX_BLOB_SIZE_BYTES.load(Ordering::Relaxed);
        if payload.len() > max {
            return Err(NeonDBError::invalid_argument(format!(
                "Blob size {} exceeds max {}",
                payload.len(),
                max
            )));
        }
        let offset = self.next_offset;
        let len = payload.len() as u64;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(payload)?;
        self.file.flush()?;
        self.next_offset += 8 + len;
        Ok(offset)
    }

    fn load_blob(&mut self, offset: u64) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0u8; 8];
        self.file.read_exact(&mut len_buf)?;
        let len = u64::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

// ── Secondary field index ─────────────────────────────────────────────────────

/// A single-field secondary index: maps serialised field values to the set of
/// row keys that carry that value.
///
/// The inner `DashMap<String, ()>` is used as a concurrent set — presence of
/// a key means the row exists; there is no meaningful value.
struct FieldIndex {
    /// `field_value_as_string → DashMap<row_key, ()>`
    buckets: DashMap<String, DashMap<String, ()>>,
}

impl FieldIndex {
    fn new() -> Self {
        FieldIndex {
            buckets: DashMap::with_capacity_and_shard_amount(64, 16),
        }
    }

    /// Add `row_key` to the bucket for `field_value`.
    fn insert(&self, field_value: &str, row_key: &str) {
        self.buckets
            .entry(field_value.to_string())
            .or_insert_with(|| DashMap::with_capacity_and_shard_amount(16, 4))
            .insert(row_key.to_string(), ());
    }

    /// Remove `row_key` from the bucket for `field_value`.
    /// Drops the bucket entirely if it becomes empty.
    fn remove(&self, field_value: &str, row_key: &str) {
        if let Some(bucket) = self.buckets.get(field_value) {
            bucket.remove(row_key);
        }
        // Drop the outer bucket if it is now empty.
        self.buckets
            .remove_if(field_value, |_, bucket| bucket.is_empty());
    }

    /// Return all row keys whose indexed field equals `field_value`.
    fn lookup(&self, field_value: &str) -> Vec<String> {
        match self.buckets.get(field_value) {
            None => vec![],
            Some(bucket) => bucket.iter().map(|e| e.key().clone()).collect(),
        }
    }
}

// ── Per-table shard ───────────────────────────────────────────────────────────

/// Number of fixed lock slots per table.  Two distinct keys may share a slot
/// (false contention) but this eliminates one heap allocation + DashMap entry
/// per row — saving ~130 bytes/row at 3M-row scale.
const LOCK_SLOTS: usize = 512;

/// FNV-1a 64-bit hash of `key` → `[0, LOCK_SLOTS)`.
fn slot_for_key(key: &str) -> usize {
    let mut h: u64 = 14_695_981_039_346_656_037;
    for b in key.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(1_099_511_628_211);
    }
    (h as usize) % LOCK_SLOTS
}

struct Table {
    rows: DashMap<String, StoredRow>,
    /// Fixed-size mutex pool — no per-row allocation.
    row_locks: Box<[Mutex<()>]>,
    /// Secondary field indexes: indexed_field_name → FieldIndex.
    field_indexes: DashMap<String, Arc<FieldIndex>>,
}

impl Table {
    fn new() -> Self {
        let shards = optimal_row_shard_count();
        let row_locks = (0..LOCK_SLOTS)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Table {
            rows: DashMap::with_capacity_and_shard_amount(256, shards),
            row_locks,
            field_indexes: DashMap::new(),
        }
    }
}

// ── Row encode / decode helpers ───────────────────────────────────────────────
//
// All in-memory row data is stored as zstd-compressed MsgPack.
// This cuts per-row storage from ~80 bytes (JSON) to ~15-25 bytes (compressed
// MsgPack), saving ~200 MB at 3M-row scale vs. the previous JSON format.
//
// Only these two functions touch the wire format — all callers go through them.

/// Encode a `serde_json::Value` to MsgPack bytes for in-memory storage.
///
/// Storage format: 1-byte tag + payload.
///   0x00 + raw MsgPack  — for small rows (< ZSTD_THRESHOLD bytes after MsgPack)
///   0x01 + zstd(MsgPack) — for large rows (≥ ZSTD_THRESHOLD bytes)
///
/// This hybrid ensures the hot path (typical game rows, ~30-80 bytes MsgPack)
/// pays zero compression overhead while large rows (inventory arrays, leaderboards)
/// still get compressed.  Both encode and decode are ~2× faster than JSON at the
/// small-row level, and ~10× smaller than JSON at the large-row level.
const ZSTD_THRESHOLD: usize = 256;

fn encode_row(value: &Value) -> Result<Bytes> {
    let mp = rmp_serde::to_vec_named(value)
        .map_err(|e| NeonDBError::SerializationError(format!("Row encode: {}", e)))?;
    if mp.len() < ZSTD_THRESHOLD {
        // Small row: tag 0x00 + raw MsgPack
        let mut buf = Vec::with_capacity(1 + mp.len());
        buf.push(0x00);
        buf.extend_from_slice(&mp);
        Ok(Bytes::from(buf))
    } else {
        // Large row: tag 0x01 + zstd-compressed MsgPack
        let compressed = zstd::encode_all(mp.as_slice(), 1)
            .map_err(|e| NeonDBError::SerializationError(format!("Row compress: {}", e)))?;
        let mut buf = Vec::with_capacity(1 + compressed.len());
        buf.push(0x01);
        buf.extend_from_slice(&compressed);
        Ok(Bytes::from(buf))
    }
}

/// Decode tagged MsgPack bytes back to a `serde_json::Value`.
fn decode_row_bytes(data: &[u8]) -> Result<Value> {
    let (tag, payload) = data.split_first().ok_or_else(|| {
        NeonDBError::SerializationError("Row decode: empty data".to_string())
    })?;
    match tag {
        0x00 => {
            // Raw MsgPack
            rmp_serde::from_slice(payload)
                .map_err(|e| NeonDBError::SerializationError(format!("Row decode: {}", e)))
        }
        0x01 => {
            // zstd-compressed MsgPack
            let decompressed = zstd::decode_all(payload)
                .map_err(|e| NeonDBError::SerializationError(format!("Row decompress: {}", e)))?;
            rmp_serde::from_slice(&decompressed)
                .map_err(|e| NeonDBError::SerializationError(format!("Row decode: {}", e)))
        }
        _ => Err(NeonDBError::SerializationError(format!(
            "Row decode: unknown tag 0x{:02x}", tag
        ))),
    }
}

// ── TableStore ────────────────────────────────────────────────────────────────

pub struct TableStore {
    tables: DashMap<String, Arc<Table>>,
    blob_store: RwLock<BlobStore>,
    next_row_id: AtomicU32,
    pub shard_id: u32,
    pub shard_count: u32,
    /// Active eviction policy. Checked after every insert/update inside
    /// `apply_delta_batch`. Defaults to `None` (no eviction).
    eviction_policy: EvictionPolicy,
    /// LRU access tracker. `Some` when policy != `None`; `None` when policy
    /// is `None` so there is zero overhead in the common case.
    lru: Option<Arc<LruTracker>>,
}

impl TableStore {
    pub fn new() -> Self {
        Self::with_eviction(EvictionPolicy::None)
    }

    /// Create a `TableStore` with a specific eviction policy.
    ///
    /// Use `EvictionPolicy::None` (or `TableStore::new()`) for unlimited storage.
    /// Use `EvictionPolicy::LruRowCap { max_rows_per_table }` to cap per-table
    /// row counts and silently evict the least-recently-used rows when exceeded.
    /// Use `EvictionPolicy::LruByteCap { max_bytes_total }` to evict based on
    /// estimated total byte usage.
    pub fn with_eviction(policy: EvictionPolicy) -> Self {
        let data_dir = std::env::var("NEONDB_BLOB_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("neondb_blobs"));
        let blob_path = data_dir.join("blobs.bin");
        let blob_store = BlobStore::open(blob_path).expect("Failed to open blob store");

        let lru = match &policy {
            EvictionPolicy::None => None,
            _ => Some(Arc::new(LruTracker::new())),
        };

        TableStore {
            tables: DashMap::with_capacity_and_shard_amount(64, 32),
            blob_store: RwLock::new(blob_store),
            next_row_id: AtomicU32::new(1),
            shard_id: 0,
            shard_count: 1,
            eviction_policy: policy,
            lru,
        }
    }

    pub fn set_shard(&mut self, shard_id: u32, shard_count: u32) {
        self.shard_id = shard_id;
        self.shard_count = shard_count.max(1);
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    fn alloc_row_id(&self) -> RowId {
        self.next_row_id.fetch_add(1, Ordering::Relaxed)
    }

    fn get_or_create_table(&self, name: &str) -> Arc<Table> {
        if let Some(t) = self.tables.get(name) {
            return t.clone();
        }
        self.tables
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Table::new()))
            .clone()
    }

    fn row_matches_shard(&self, shard_id: u32) -> bool {
        self.shard_count <= 1 || shard_id == self.shard_id
    }

    fn decode_row(row: &StoredRow) -> Result<Value> {
        decode_row_bytes(&row.data)
    }

    fn load_blob_into_value(&self, value: &mut Value, offset: u64) -> Result<()> {
        let blob = self.blob_store.write().load_blob(offset)?;
        let inventory: Value = serde_json::from_slice(&blob)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert("inventory".to_string(), inventory);
        }
        Ok(())
    }

    // ── Public read API ──────────────────────────────────────────────────────

    pub fn get_row(&self, table_name: &str, key: &str) -> Result<Option<Value>> {
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        let row = match table.rows.get(key) {
            Some(r) => r.clone(),
            None => return Ok(None),
        };
        if !self.row_matches_shard(row.shard_id) {
            return Ok(None);
        }
        let mut value = Self::decode_row(&row)?;
        if let Some(offset) = row.blob_offset {
            self.load_blob_into_value(&mut value, offset)?;
        }
        Ok(Some(value))
    }

    /// Row fetch that also returns its OCC version (single entry read — the
    /// value and version are guaranteed consistent). Missing rows read as
    /// version 0, so "read nothing, then someone inserted" also conflicts.
    pub fn get_row_with_version(&self, table_name: &str, key: &str) -> Result<Option<(Value, u64)>> {
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        let row = match table.rows.get(key) {
            Some(r) => r.clone(),
            None => return Ok(None),
        };
        if !self.row_matches_shard(row.shard_id) {
            return Ok(None);
        }
        let mut value = Self::decode_row(&row)?;
        if let Some(offset) = row.blob_offset {
            self.load_blob_into_value(&mut value, offset)?;
        }
        Ok(Some((value, row.version)))
    }

    /// Current OCC version of a row (0 = absent).
    pub fn row_version(&self, table_name: &str, key: &str) -> u64 {
        self.tables
            .get(table_name)
            .and_then(|t| t.rows.get(key).map(|r| r.version))
            .unwrap_or(0)
    }

    /// RLS-aware row fetch.
    ///
    /// Reads the row as normal, then evaluates the table's RLS policy.
    /// Returns `Ok(None)` if the policy denies the access — this intentionally
    /// avoids leaking row *existence* to unauthorized callers.
    ///
    /// Pass `schema: None` to skip policy evaluation (equivalent to `get_row`).
    pub fn get_row_rls(
        &self,
        table: &str,
        key: &str,
        caller_id: &str,
        caller_role: &str,
        schema: Option<&crate::schema::SchemaRegistry>,
    ) -> crate::error::Result<Option<Value>> {
        let value = self.get_row(table, key)?;

        // If no schema registry is provided there is no policy to evaluate.
        let schema = match schema {
            Some(s) => s,
            None => return Ok(value),
        };

        // If the table has no registered schema, default to Public (allow all).
        let table_schema = match schema.get(table) {
            Some(ts) => ts,
            None => return Ok(value),
        };

        // Evaluate the policy against the *current stored* row (may be None for
        // new-insert callers, but here we are only reading so value==None means
        // the row doesn't exist — return None regardless of policy).
        if !crate::schema::rls_check(&table_schema.rls, value.as_ref(), caller_id, caller_role) {
            return Ok(None);
        }

        Ok(value)
    }

    pub fn list_rows(&self, table_name: &str) -> Result<Vec<Value>> {
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return Ok(vec![]),
        };
        let mut rows = Vec::with_capacity(table.rows.len());
        for entry in table.rows.iter() {
            let row = entry.value();
            if !self.row_matches_shard(row.shard_id) {
                continue;
            }
            let mut value = Self::decode_row(row)?;
            if let Some(offset) = row.blob_offset {
                self.load_blob_into_value(&mut value, offset)?;
            }
            rows.push(value);
        }
        Ok(rows)
    }

    // ── Internal single-row write (lock must already be held by caller) ──────

    fn write_row_unlocked(&self, table_name: &str, key: &str, value: Value) -> Result<RowDelta> {
        let table = self.get_or_create_table(table_name);

        let (operation, row_id, version) = if let Some(existing) = table.rows.get(key) {
            ("update".to_string(), existing.row_id, existing.version + 1)
        } else {
            ("insert".to_string(), self.alloc_row_id(), 1)
        };

        // Capture the old row data for index maintenance (before overwriting).
        let old_row_value: Option<Value> = if operation == "update" {
            table
                .rows
                .get(key)
                .and_then(|r| decode_row_bytes(&r.data).ok())
        } else {
            None
        };

        let (final_value, blob_offset) = self.prepare_value(table_name, key, value)?;

        let stored = StoredRow {
            row_id,
            shard_id: self.shard_id,
            data: encode_row(&final_value)?,
            blob_offset,
            version,
        };
        table.rows.insert(key.to_string(), stored);

        // ── Maintain secondary indexes ─────────────────────────────────────────
        for idx_entry in table.field_indexes.iter() {
            let field = idx_entry.key();
            let idx = idx_entry.value().clone();
            // Remove old field value from index (update case).
            if let Some(old_val) = &old_row_value {
                if let Some(old_fv) = old_val.get(field).and_then(value_to_index_key) {
                    idx.remove(&old_fv, key);
                }
            }
            // Add new field value to index.
            if let Some(new_fv) = final_value.get(field).and_then(value_to_index_key) {
                idx.insert(&new_fv, key);
            }
        }

        Ok(RowDelta {
            table_name: table_name.to_string(),
            operation,
            row_key: key.to_string(),
            row_id,
            shard_id: self.shard_id,
            payload_arc: None,
            row_data: Some(final_value),
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        })
    }

    fn delete_row_unlocked(&self, table_name: &str, key: &str) -> Result<RowDelta> {
        let row_id = self
            .tables
            .get(table_name)
            .and_then(|t| t.rows.get(key).map(|r| r.row_id))
            .unwrap_or(0);
        if let Some(table) = self.tables.get(table_name) {
            // Remove from secondary indexes before deleting the row.
            if let Some(old_row) = table.rows.get(key) {
                if let Ok(old_val) = decode_row_bytes(&old_row.data) {
                    for idx_entry in table.field_indexes.iter() {
                        let field = idx_entry.key();
                        let idx = idx_entry.value().clone();
                        if let Some(fv) = old_val.get(field).and_then(value_to_index_key) {
                            idx.remove(&fv, key);
                        }
                    }
                }
            }
            table.rows.remove(key);
        }
        Ok(RowDelta {
            table_name: table_name.to_string(),
            operation: "delete".to_string(),
            row_key: key.to_string(),
            row_id,
            shard_id: self.shard_id,
            payload_arc: None,
            row_data: None,
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        })
    }

    // ── Public write API (single-writer / convenience / tests) ───────────────

    pub fn set_row(&self, table_name: String, key: String, value: Value) -> Result<RowDelta> {
        self.write_row_unlocked(&table_name, &key, value)
    }

    pub fn delete_row(&self, table_name: &str, key: &str) -> Result<RowDelta> {
        self.delete_row_unlocked(table_name, key)
    }

    // ── ATOMIC BATCH COMMIT ──────────────────────────────────────────────────
    //
    // TODO-001: Serializable isolation for concurrent reducers.
    //   Row locks are acquired in sorted (table_name, row_key) order before
    //   any writes so two concurrent commits on the same row serialize.
    //
    // TODO-002: Atomicity on panic.
    //   catch_unwind in main.rs prevents commit() from being called on a
    //   panicking reducer.  If apply_delta_batch itself hits an error mid-
    //   batch, all already-applied rows are rolled back.
    //
    // "counter_add" (Session 10 fix):
    //   For counter_add deltas the committed value is re-read UNDER THE LOCK
    //   and the amount is added to it.  This makes the full read-modify-write
    //   cycle atomic — the read is no longer outside the lock window.
    pub fn apply_delta_batch(&self, deltas: &[RowDelta]) -> Result<Vec<RowDelta>> {
        self.apply_delta_batch_versioned(deltas, &[])
    }

    /// Like `apply_delta_batch`, but with optimistic-concurrency validation:
    /// `read_versions` lists `(table, key, version_seen)` for every row the
    /// transaction read. Inside the row locks, any read row that this batch
    /// ALSO WRITES must still be at its seen version — otherwise the whole
    /// batch aborts with `NeonDBError::TxnConflict` (first-committer-wins;
    /// the caller re-executes the reducer against fresh state).
    pub fn apply_delta_batch_versioned(
        &self,
        deltas: &[RowDelta],
        read_versions: &[(String, String, u64)],
    ) -> Result<Vec<RowDelta>> {
        if deltas.is_empty() {
            return Ok(vec![]);
        }

        // ── 1. Collect and sort lock keys ────────────────────────────────────
        let mut lock_keys: Vec<(String, String)> = deltas
            .iter()
            .filter(|d| self.shard_count <= 1 || d.shard_id == self.shard_id)
            .map(|d| (d.table_name.clone(), d.row_key.clone()))
            .collect();
        lock_keys.sort_unstable();
        lock_keys.dedup();

        // ── 2. Acquire all row locks in sorted order ─────────────────────────
        // Collect (Arc<Table>, slot_index) so the table stays alive while
        // guards are held.  Sort by (table-ptr, slot) and dedup before locking
        // to prevent deadlocks when two batches touch overlapping key sets.
        let mut table_slots: Vec<(Arc<Table>, usize)> = lock_keys
            .iter()
            .map(|(table_name, key)| {
                let table = self.get_or_create_table(table_name);
                let slot = slot_for_key(key);
                (table, slot)
            })
            .collect();

        table_slots.sort_unstable_by(|(t1, s1), (t2, s2)| {
            let p1 = Arc::as_ptr(t1) as usize;
            let p2 = Arc::as_ptr(t2) as usize;
            p1.cmp(&p2).then(s1.cmp(s2))
        });
        table_slots.dedup_by(|(t1, s1), (t2, s2)| Arc::ptr_eq(t1, t2) && *s1 == *s2);

        let _guards: Vec<_> = table_slots
            .iter()
            .map(|(t, slot)| t.row_locks[*slot].lock().expect("Row lock poisoned"))
            .collect();

        // ── 2b. OCC validation (inside the locks) ────────────────────────────
        // Lost-update guard: every row this txn read AND writes must still be
        // at the version it read. `lock_keys` is the sorted written-key set.
        for (t, k, seen) in read_versions {
            let written = lock_keys
                .binary_search_by(|(lt, lk)| (lt.as_str(), lk.as_str()).cmp(&(t.as_str(), k.as_str())))
                .is_ok();
            if written {
                let current = self.row_version(t, k);
                if current != *seen {
                    return Err(NeonDBError::TxnConflict(format!(
                        "{t}/{k}: read v{seen}, now v{current}"
                    )));
                }
            }
        }

        // ── 3. Apply each delta, rolling back on error ───────────────────────
        let mut applied: Vec<(String, String, Option<StoredRow>)> = Vec::new();
        let mut committed_deltas: Vec<RowDelta> = Vec::with_capacity(deltas.len());

        for delta in deltas {
            if self.shard_count > 1 && delta.shard_id != self.shard_id {
                continue;
            }

            let result: Result<RowDelta> = match delta.operation.as_str() {
                "insert" | "update" => {
                    let value = delta.row_data_value().ok_or_else(|| {
                        NeonDBError::table_error("insert/update delta missing row_data")
                    })?;
                    let old = self
                        .tables
                        .get(&delta.table_name)
                        .and_then(|t| t.rows.get(&delta.row_key).map(|r| r.clone()));
                    applied.push((delta.table_name.clone(), delta.row_key.clone(), old));
                    self.write_row_unlocked(&delta.table_name, &delta.row_key, value)
                }
                "delete" => {
                    let old = self
                        .tables
                        .get(&delta.table_name)
                        .and_then(|t| t.rows.get(&delta.row_key).map(|r| r.clone()));
                    applied.push((delta.table_name.clone(), delta.row_key.clone(), old));
                    self.delete_row_unlocked(&delta.table_name, &delta.row_key)
                }
                // ── counter_add: locked read-modify-write ────────────────────
                // Re-read current committed value INSIDE the lock, add the
                // staged amount, write the result.  This is what makes
                // concurrent increments correct — the read is no longer a
                // data race outside the lock window.
                "counter_add" => {
                    let name = &delta.row_key;
                    let current_val = self.get_counter(name)?.map(|c| c.value).unwrap_or(0);
                    let new_val = current_val + delta.counter_add_amount;

                    // Preserve existing row_id if the row already exists.
                    let row_id = self
                        .get_counter(name)?
                        .map(|c| c.id)
                        .unwrap_or_else(|| self.alloc_row_id());

                    let counter = Counter {
                        id: row_id,
                        name: name.clone(),
                        value: new_val,
                        last_modified: delta.counter_add_timestamp,
                    };
                    let value = serde_json::to_value(counter)
                        .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

                    let old = self
                        .tables
                        .get(&delta.table_name)
                        .and_then(|t| t.rows.get(&delta.row_key).map(|r| r.clone()));
                    applied.push((delta.table_name.clone(), delta.row_key.clone(), old));
                    self.write_row_unlocked(&delta.table_name, name, value)
                }
                other => Err(NeonDBError::table_error(format!(
                    "Unknown operation: {}",
                    other
                ))),
            };

            match result {
                Ok(committed) => {
                    // ── LRU tracking (touch on insert/update, remove on delete) ─
                    if let Some(lru) = &self.lru {
                        match committed.operation.as_str() {
                            "insert" | "update" => {
                                lru.touch(&committed.table_name, &committed.row_key);
                            }
                            "delete" => {
                                lru.remove(&committed.table_name, &committed.row_key);
                            }
                            _ => {}
                        }
                    }
                    committed_deltas.push(committed);
                }
                Err(e) => {
                    // Rollback all already-applied rows in reverse order.
                    for (tbl, key, old_row) in applied.into_iter().rev() {
                        if let Some(table) = self.tables.get(&tbl) {
                            match old_row {
                                Some(row) => {
                                    table.rows.insert(key, row);
                                }
                                None => {
                                    table.rows.remove(&key);
                                }
                            }
                        }
                    }
                    return Err(e);
                }
            }
        }

        // ── 4. Eviction (runs AFTER all row locks are released) ──────────────
        //
        // IMPORTANT: _guards holds per-row Mutex locks. We must drop them before
        // running eviction to avoid deadlock (eviction may need to re-acquire
        // locks via write_row_unlocked or delete_row_unlocked on the same keys).
        // Dropping _guards here releases all locks before eviction proceeds.
        drop(_guards);

        if let Some(lru) = &self.lru {
            match &self.eviction_policy {
                EvictionPolicy::LruRowCap { max_rows_per_table } => {
                    // Collect distinct table names that were touched in this batch.
                    let mut touched_tables: Vec<String> = committed_deltas
                        .iter()
                        .filter(|d| matches!(d.operation.as_str(), "insert" | "update"))
                        .map(|d| d.table_name.clone())
                        .collect();
                    touched_tables.sort_unstable();
                    touched_tables.dedup();

                    for table_name in touched_tables {
                        let current_count = self
                            .tables
                            .get(&table_name)
                            .map(|t| t.rows.len())
                            .unwrap_or(0);

                        if current_count > *max_rows_per_table {
                            let excess = current_count - max_rows_per_table;
                            let to_evict = lru.evict_oldest(&table_name, excess);
                            for (_tbl, key) in &to_evict {
                                if let Some(table) = self.tables.get(&table_name) {
                                    table.rows.remove(key);
                                }
                                lru.remove(&table_name, key);
                            }
                        }
                    }
                }
                EvictionPolicy::LruByteCap { max_bytes_total } => {
                    // Estimate total bytes across all tables.
                    let total_bytes: usize = self
                        .tables
                        .iter()
                        .map(|t| t.value().rows.iter().map(|r| r.value().data.len()).sum::<usize>())
                        .sum();

                    if total_bytes > *max_bytes_total {
                        // Evict from every table that has rows, oldest first across the whole store.
                        let all_tables: Vec<String> = self
                            .tables
                            .iter()
                            .map(|e| e.key().clone())
                            .collect();

                        // Simple heuristic: try to free ~10% of rows from the largest tables.
                        for table_name in all_tables {
                            let count = self
                                .tables
                                .get(&table_name)
                                .map(|t| t.rows.len())
                                .unwrap_or(0);
                            if count == 0 {
                                continue;
                            }
                            // Evict 10% of each table's rows (minimum 1) until under cap.
                            let evict_n = (count / 10).max(1);
                            let to_evict = lru.evict_oldest(&table_name, evict_n);
                            for (_tbl, key) in &to_evict {
                                if let Some(table) = self.tables.get(&table_name) {
                                    table.rows.remove(key);
                                }
                                lru.remove(&table_name, key);
                            }
                        }
                    }
                }
                EvictionPolicy::None => {
                    // No eviction. This branch is unreachable when lru is Some,
                    // but match is exhaustive so we handle it for safety.
                }
            }
        }

        Ok(committed_deltas)
    }

    /// Legacy single-delta path — used by WAL replay and convenience tests.
    pub fn apply_delta(&self, delta: &RowDelta) -> Result<()> {
        self.apply_delta_batch(std::slice::from_ref(delta))
            .map(|_| ())
    }

    // ── Blob helpers ─────────────────────────────────────────────────────────

    fn should_store_blob(value: &Value) -> bool {
        if let Some(obj) = value.as_object() {
            if let Some(inventory) = obj.get("inventory") {
                return inventory.is_array() && !inventory.as_array().unwrap().is_empty();
            }
        }
        false
    }

    fn prepare_value(
        &self,
        table_name: &str,
        key: &str,
        mut value: Value,
    ) -> Result<(Value, Option<u64>)> {
        let mut blob_offset = None;
        if Self::should_store_blob(&value) {
            if let Some(inventory) = value.get_mut("inventory") {
                let bytes = serde_json::to_vec(inventory)?;
                let offset = self.blob_store.write().store_blob(&bytes)?;
                *inventory = Value::Null;
                blob_offset = Some(offset);
            }
        }
        if table_name == "players" {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("shard_id".to_string(), Value::Number(self.shard_id.into()));
            }
        }
        if let Some(obj) = value.as_object_mut() {
            obj.insert("row_key".to_string(), Value::String(key.to_string()));
        }
        Ok((value, blob_offset))
    }

    // ── Counter convenience layer ─────────────────────────────────────────────

    pub fn get_counter(&self, name: &str) -> Result<Option<Counter>> {
        if let Some(value) = self.get_row("counters", name)? {
            let counter: Counter = serde_json::from_value(value)
                .map_err(|e| NeonDBError::SerializationError(format!("Counter decode: {}", e)))?;
            Ok(Some(counter))
        } else {
            Ok(None)
        }
    }

    pub fn list_counters(&self) -> Result<Vec<Counter>> {
        let values = self.list_rows("counters")?;
        values
            .into_iter()
            .map(|v| {
                serde_json::from_value(v)
                    .map_err(|e| NeonDBError::SerializationError(format!("Counter decode: {}", e)))
            })
            .collect()
    }

    pub fn set_counter(&self, name: String, value: i32, last_modified: i64) -> Result<RowDelta> {
        let existing = self.get_counter(&name)?;
        let row_id = existing
            .map(|c| c.id)
            .unwrap_or_else(|| self.alloc_row_id());
        let counter = Counter {
            id: row_id,
            name: name.clone(),
            value,
            last_modified,
        };
        self.set_row("counters".to_string(), name, serde_json::to_value(counter)?)
    }

    pub fn delete_counter(&self, name: &str) -> Result<RowDelta> {
        self.delete_row("counters", name)
    }

    // ── Secondary index API ──────────────────────────────────────────────────

    /// Register a hash index on `field` for `table_name`.
    ///
    /// If the table already contains rows, the index is built immediately by
    /// scanning all existing rows.  Idempotent: calling twice for the same
    /// (table, field) pair is a no-op.
    pub fn create_index(&self, table_name: &str, field: &str) -> Result<()> {
        let table = self.get_or_create_table(table_name);

        // Idempotent — if index already exists, nothing to do.
        if table.field_indexes.contains_key(field) {
            return Ok(());
        }

        let idx = Arc::new(FieldIndex::new());

        // Back-fill: index all rows that already exist.
        for entry in table.rows.iter() {
            let row_key = entry.key();
            let row = entry.value();
            if let Ok(value) = decode_row_bytes(&row.data) {
                if let Some(fv) = value.get(field).and_then(value_to_index_key) {
                    idx.insert(&fv, row_key);
                }
            }
        }

        table.field_indexes.insert(field.to_string(), idx);
        Ok(())
    }

    /// Drop the secondary index on `field` for `table_name`.
    /// No-op if no such index exists.
    pub fn drop_index(&self, table_name: &str, field: &str) {
        if let Some(table) = self.tables.get(table_name) {
            table.field_indexes.remove(field);
        }
    }

    /// Return the row keys of all rows in `table_name` where `field == value`.
    ///
    /// Returns `None` if no index exists for this field — the caller should
    /// fall back to a full scan.  Returns `Some(Vec)` (possibly empty) when
    /// the index is present.
    pub fn index_lookup(
        &self,
        table_name: &str,
        field: &str,
        value: &Value,
    ) -> Option<Vec<String>> {
        let table = self.tables.get(table_name)?;
        let idx = table.field_indexes.get(field)?;
        let fv = value_to_index_key(value)?;
        Some(idx.lookup(&fv))
    }

    /// List all fields that have a registered secondary index for `table_name`.
    pub fn list_indexes(&self, table_name: &str) -> Vec<String> {
        match self.tables.get(table_name) {
            None => vec![],
            Some(t) => t.field_indexes.iter().map(|e| e.key().clone()).collect(),
        }
    }

    // ── Snapshot helpers ─────────────────────────────────────────────────────────

    /// Return all table names currently in the store.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|e| e.key().clone()).collect()
    }

    // ── Migration tracking helpers ───────────────────────────────────────────────
    //
    // Migrations record themselves in the `__migrations` system table. The
    // row key is the migration filename; the value is
    // `{"applied_at": <unix_nanos>, "version": <u64>}`. These helpers are
    // intentionally thin wrappers around set_row / list_rows_with_keys so
    // they go through the same write path as everything else.

    /// Returns the set of migration filenames that have already been applied.
    /// Returns an empty vec if the `__migrations` table does not exist yet.
    pub fn applied_migration_versions(&self) -> Result<Vec<String>> {
        let rows = self.list_rows_with_keys("__migrations")?;
        Ok(rows.into_iter().map(|(k, _)| k).collect())
    }

    /// Records a migration as applied. `filename` is the migration file's
    /// basename (e.g. `001_add_score.toml`); `version` is the numeric version
    /// declared inside the file.
    pub fn mark_migration_applied(&self, filename: String, version: u64) -> Result<()> {
        let now_nanos: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        self.set_row(
            "__migrations".to_string(),
            filename,
            serde_json::json!({
                "applied_at": now_nanos as u64,
                "version": version,
            }),
        )?;
        Ok(())
    }

    /// Return all rows in `table_name` as (row_key, decoded_value) pairs.
    /// Includes blob data if present. Respects shard filter.
    pub fn list_rows_with_keys(&self, table_name: &str) -> Result<Vec<(String, Value)>> {
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return Ok(vec![]),
        };
        let mut rows = Vec::with_capacity(table.rows.len());
        for entry in table.rows.iter() {
            let key = entry.key().clone();
            let row = entry.value();
            if !self.row_matches_shard(row.shard_id) {
                continue;
            }
            let mut value = Self::decode_row(row)?;
            if let Some(offset) = row.blob_offset {
                self.load_blob_into_value(&mut value, offset)?;
            }
            rows.push((key, value));
        }
        Ok(rows)
    }

    /// Return the current value of the next-row-ID counter.
    /// Used by snapshots to preserve ID continuity across restarts.
    pub fn current_next_row_id(&self) -> u32 {
        self.next_row_id.load(Ordering::SeqCst)
    }

    /// Overwrite the next-row-ID counter.
    /// Called after restoring a snapshot to prevent ID collisions with
    /// rows whose IDs are embedded in WAL entries that post-date the snapshot.
    pub fn set_next_row_id(&self, next_id: u32) {
        self.next_row_id.store(next_id, Ordering::SeqCst);
    }

    // ── Columnar read API ─────────────────────────────────────────────────────
    //
    // These methods provide column-oriented access patterns on top of the
    // existing row-oriented DashMap storage.  They are useful for:
    //   - Analytics queries (count by status, distinct values, etc.)
    //   - Subscription filter back-testing without full row decode
    //   - Aggregations in JS/WASM reducers via host functions

    /// Return the value of `field` for every row in `table_name` that has it,
    /// as a list of `(row_key, field_value)` pairs sorted by `row_key`.
    ///
    /// Much cheaper than `list_rows()` when you only need one field per row —
    /// the JSON decode is limited to a single key extraction.
    pub fn scan_column(&self, table_name: &str, field: &str) -> Vec<(String, Value)> {
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return vec![],
        };
        let mut result = Vec::with_capacity(table.rows.len());
        for entry in table.rows.iter() {
            let row = entry.value();
            if !self.row_matches_shard(row.shard_id) {
                continue;
            }
            if let Ok(val) = decode_row_bytes(&row.data) {
                if let Some(obj) = val.as_object() {
                    if let Some(v) = obj.get(field) {
                        result.push((entry.key().clone(), v.clone()));
                    }
                }
            }
        }
        result.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        result
    }

    /// Count rows in `table_name` grouped by the value of `field`.
    ///
    /// Returns a `HashMap<field_value_as_string, count>`.
    /// Rows that don't have `field` are not counted.
    pub fn count_by_field(
        &self,
        table_name: &str,
        field: &str,
    ) -> std::collections::HashMap<String, usize> {
        use std::collections::HashMap;
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return HashMap::new(),
        };
        let mut counts: HashMap<String, usize> = HashMap::new();
        for entry in table.rows.iter() {
            let row = entry.value();
            if !self.row_matches_shard(row.shard_id) {
                continue;
            }
            if let Ok(val) = decode_row_bytes(&row.data) {
                if let Some(obj) = val.as_object() {
                    if let Some(v) = obj.get(field) {
                        let key = value_to_index_key(v).unwrap_or_else(|| "null".to_string());
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }
        counts
    }

    /// Return all distinct values of `field` across all rows in `table_name`,
    /// sorted by their string representation.
    pub fn distinct_field_values(&self, table_name: &str, field: &str) -> Vec<Value> {
        use std::collections::BTreeSet;
        let table = match self.tables.get(table_name) {
            Some(t) => t.clone(),
            None => return vec![],
        };
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut values: Vec<Value> = Vec::new();
        for entry in table.rows.iter() {
            let row = entry.value();
            if !self.row_matches_shard(row.shard_id) {
                continue;
            }
            if let Ok(val) = decode_row_bytes(&row.data) {
                if let Some(obj) = val.as_object() {
                    if let Some(v) = obj.get(field) {
                        let key = value_to_index_key(v).unwrap_or_else(|| "null".to_string());
                        if seen.insert(key) {
                            values.push(v.clone());
                        }
                    }
                }
            }
        }
        values
    }

    /// Return the count of rows in `table_name` that have a specific value
    /// for `field`.  Uses the secondary index if registered (O(1)); falls back
    /// to a full scan otherwise (O(n)).
    pub fn count_matching(&self, table_name: &str, field: &str, value: &Value) -> usize {
        // Fast path: use secondary index if available.
        if let Some(keys) = self.index_lookup(table_name, field, value) {
            return keys.len();
        }
        // Slow path: linear scan.
        let serialized = value_to_index_key(value).unwrap_or_else(|| "null".to_string());
        self.scan_column(table_name, field)
            .iter()
            .filter(|(_, v)| value_to_index_key(v).as_deref() == Some(serialized.as_str()))
            .count()
    }

    /// Return the total number of rows across all tables in this store.
    pub fn total_row_count(&self) -> usize {
        self.tables.iter().map(|t| t.value().rows.len()).sum()
    }

    // ── Snapshot helpers ──────────────────────────────────────────────────────

    /// Return all rows in `table_name` as a `HashMap<key, value>`.
    /// Used by the Raft snapshot builder.
    pub fn get_all_rows(
        &self,
        table_name: &str,
    ) -> std::collections::HashMap<String, Value> {
        match self.list_rows_with_keys(table_name) {
            Ok(pairs) => pairs.into_iter().collect(),
            Err(_) => std::collections::HashMap::new(),
        }
    }

    /// Return all counter values as a `HashMap<name, value>`.
    /// Used by the Raft snapshot builder.
    pub fn get_all_counters_map(&self) -> std::collections::HashMap<String, i32> {
        match self.list_counters() {
            Ok(counters) => counters.into_iter().map(|c| (c.name, c.value)).collect(),
            Err(_) => std::collections::HashMap::new(),
        }
    }

    /// Remove all rows from all tables.
    /// Called when installing a Raft snapshot to reset state before reload.
    pub fn clear_all(&self) {
        self.tables.clear();
    }
}

/// Convert a JSON Value to the canonical string key used inside a FieldIndex.
/// Returns `None` for `Null` (nulls are not indexed).
fn value_to_index_key(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        other => Some(serde_json::to_string(other).unwrap_or_default()),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Arc<TableStore> {
        Arc::new(TableStore::new())
    }

    #[test]
    fn test_insert_and_get() {
        let s = store();
        s.set_counter("foo".to_string(), 42, 1000).unwrap();
        let c = s.get_counter("foo").unwrap();
        assert_eq!(
            c,
            Some(Counter {
                id: c.as_ref().unwrap().id,
                name: "foo".to_string(),
                value: 42,
                last_modified: 1000,
            })
        );
    }

    #[test]
    fn test_update() {
        let s = store();
        s.set_counter("foo".to_string(), 42, 1000).unwrap();
        s.set_counter("foo".to_string(), 50, 2000).unwrap();
        assert_eq!(s.get_counter("foo").unwrap().unwrap().value, 50);
    }

    #[test]
    fn test_delete() {
        let s = store();
        s.set_counter("foo".to_string(), 42, 1000).unwrap();
        s.delete_counter("foo").unwrap();
        assert_eq!(s.get_counter("foo").unwrap(), None);
    }

    #[test]
    fn test_apply_delta() {
        let s = store();
        let delta = RowDelta {
            table_name: "counters".to_string(),
            operation: "insert".to_string(),
            row_key: "foo".to_string(),
            row_id: 1,
            shard_id: 0,
            payload_arc: None,
            row_data: Some(serde_json::json!({
                "id": 1, "name": "foo", "value": 100, "last_modified": 5000
            })),
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        };
        s.apply_delta(&delta).unwrap();
        assert_eq!(s.get_counter("foo").unwrap().unwrap().value, 100);
    }

    #[test]
    fn test_concurrent_writes_different_tables() {
        use std::thread;
        let s = Arc::new(TableStore::new());
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let s = s.clone();
                thread::spawn(move || {
                    let table = format!("table_{}", i);
                    for j in 0..100 {
                        s.set_row(
                            table.clone(),
                            format!("k{}", j),
                            serde_json::json!({"v": j}),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for i in 0..8 {
            let rows = s.list_rows(&format!("table_{}", i)).unwrap();
            assert_eq!(rows.len(), 100);
        }
    }

    #[test]
    fn test_arc_bytes_delta_payload() {
        // write_row_unlocked no longer carries payload_arc; row_data is the source.
        let s = store();
        let delta = s.set_counter("x".to_string(), 7, 0).unwrap();
        assert!(delta.row_data.is_some());
        let val = delta.row_data.as_ref().unwrap();
        assert_eq!(val["value"], serde_json::json!(7));
    }

    #[test]
    fn test_optimal_shard_count_is_power_of_two() {
        let count = optimal_row_shard_count();
        assert!(count >= 16);
        assert_eq!(count & (count - 1), 0, "shard count must be a power of two");
    }

    // ── TODO-001: Serializable isolation ─────────────────────────────────────
    //
    // Two threads each do 500 increments on the SAME counter via ReducerContext.
    // Final value must be exactly 1000 — no lost updates.
    //
    // WHY THIS NOW PASSES (Session 10):
    //   ReducerContext::set_counter() stages a "counter_add" delta (the amount
    //   +1) instead of an absolute value.  apply_delta_batch() re-reads the
    //   current committed value under the row lock and adds the staged amount.
    //   The full read-modify-write cycle is atomic: no thread can read a stale
    //   value because the read happens inside the lock, not before it.
    #[test]
    fn test_serializable_isolation_no_lost_updates() {
        use crate::reducer::context::{increment_reducer, ReducerContext};
        use std::thread;

        let tables = Arc::new(TableStore::new());
        tables.set_counter("shared".to_string(), 0, 0).unwrap();

        let iters = 500usize;
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let tables = tables.clone();
                thread::spawn(move || {
                    for _ in 0..iters {
                        let mut ctx = ReducerContext::new(tables.clone(), 0);
                        increment_reducer(&mut ctx, "shared".to_string(), 1).unwrap();
                        ctx.commit().unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let final_val = tables.get_counter("shared").unwrap().unwrap().value;
        assert_eq!(
            final_val,
            (iters * 2) as i32,
            "Expected {} but got {} — lost update detected",
            iters * 2,
            final_val
        );
    }

    // ── TODO-002: Atomicity — failed batch must not partially apply ───────────
    #[test]
    fn test_atomic_batch_rollback_on_error() {
        let s = store();

        let deltas = vec![
            RowDelta {
                table_name: "counters".to_string(),
                operation: "insert".to_string(),
                row_key: "alpha".to_string(),
                row_id: 1,
                shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({
                    "id": 1, "name": "alpha", "value": 99, "last_modified": 0
                })),
                counter_add_amount: 0,
                counter_add_timestamp: 0,
            },
            RowDelta {
                table_name: "counters".to_string(),
                operation: "INVALID_OP".to_string(),
                row_key: "beta".to_string(),
                row_id: 2,
                shard_id: 0,
                payload_arc: None,
                row_data: None,
                counter_add_amount: 0,
                counter_add_timestamp: 0,
            },
        ];

        let result = s.apply_delta_batch(&deltas);
        assert!(result.is_err(), "Batch with invalid op should fail");

        assert!(
            s.get_counter("alpha").unwrap().is_none(),
            "Rolled-back row must not be visible after failed batch"
        );
        assert!(
            s.get_counter("beta").unwrap().is_none(),
            "Row after error point must never have been applied"
        );
    }

    // ── counter_add: verify locked RMW produces correct result ───────────────
    #[test]
    fn test_counter_add_delta_atomicity() {
        let s = store();
        s.set_counter("pts".to_string(), 10, 0).unwrap();

        let delta = RowDelta {
            table_name: "counters".to_string(),
            operation: "counter_add".to_string(),
            row_key: "pts".to_string(),
            row_id: 0,
            shard_id: 0,
            payload_arc: None,
            row_data: None,
            counter_add_amount: 5,
            counter_add_timestamp: 1000,
        };
        s.apply_delta_batch(&[delta]).unwrap();
        assert_eq!(s.get_counter("pts").unwrap().unwrap().value, 15);
    }

    // ── Secondary index tests ────────────────────────────────────────────────

    #[test]
    fn test_index_lookup_basic() {
        let ts = Arc::new(TableStore::new());
        ts.create_index("players", "status").unwrap();

        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "active", "level": 5}),
        )
        .unwrap();
        ts.set_row(
            "players".to_string(),
            "p2".to_string(),
            serde_json::json!({"status": "inactive", "level": 3}),
        )
        .unwrap();
        ts.set_row(
            "players".to_string(),
            "p3".to_string(),
            serde_json::json!({"status": "active", "level": 10}),
        )
        .unwrap();

        let active = ts
            .index_lookup("players", "status", &serde_json::json!("active"))
            .expect("index should exist");
        assert_eq!(active.len(), 2, "two active players");
        assert!(active.contains(&"p1".to_string()));
        assert!(active.contains(&"p3".to_string()));

        let inactive = ts
            .index_lookup("players", "status", &serde_json::json!("inactive"))
            .expect("index should exist");
        assert_eq!(inactive.len(), 1);
        assert!(inactive.contains(&"p2".to_string()));
    }

    #[test]
    fn test_index_returns_none_without_index() {
        let ts = Arc::new(TableStore::new());
        ts.set_row(
            "items".to_string(),
            "i1".to_string(),
            serde_json::json!({"rarity": "common"}),
        )
        .unwrap();

        // No index created → should return None
        let result = ts.index_lookup("items", "rarity", &serde_json::json!("common"));
        assert!(
            result.is_none(),
            "should return None without a registered index"
        );
    }

    #[test]
    fn test_index_maintained_on_update() {
        let ts = Arc::new(TableStore::new());
        ts.create_index("players", "status").unwrap();

        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();

        // Update: change status from active → inactive
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "inactive"}),
        )
        .unwrap();

        let active = ts
            .index_lookup("players", "status", &serde_json::json!("active"))
            .unwrap();
        assert!(active.is_empty(), "old bucket should be empty after update");

        let inactive = ts
            .index_lookup("players", "status", &serde_json::json!("inactive"))
            .unwrap();
        assert_eq!(inactive.len(), 1);
        assert!(inactive.contains(&"p1".to_string()));
    }

    #[test]
    fn test_index_maintained_on_delete() {
        let ts = Arc::new(TableStore::new());
        ts.create_index("players", "status").unwrap();

        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();

        ts.delete_row("players", "p1").unwrap();

        let active = ts
            .index_lookup("players", "status", &serde_json::json!("active"))
            .unwrap();
        assert!(
            active.is_empty(),
            "deleted row should be removed from index"
        );
    }

    // ── Columnar API tests ──────────────────────────────────────────────────

    #[test]
    fn test_scan_column_basic() {
        let ts = Arc::new(TableStore::new());
        ts.set_row(
            "items".to_string(),
            "i1".to_string(),
            serde_json::json!({"rarity": "common", "power": 5}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i2".to_string(),
            serde_json::json!({"rarity": "rare", "power": 20}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i3".to_string(),
            serde_json::json!({"rarity": "common", "power": 8}),
        )
        .unwrap();

        let col = ts.scan_column("items", "rarity");
        assert_eq!(col.len(), 3);
        // Sorted by row_key: i1, i2, i3
        assert_eq!(col[0], ("i1".to_string(), serde_json::json!("common")));
        assert_eq!(col[1], ("i2".to_string(), serde_json::json!("rare")));
    }

    #[test]
    fn test_count_by_field() {
        let ts = Arc::new(TableStore::new());
        ts.set_row(
            "items".to_string(),
            "i1".to_string(),
            serde_json::json!({"rarity": "common"}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i2".to_string(),
            serde_json::json!({"rarity": "rare"}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i3".to_string(),
            serde_json::json!({"rarity": "common"}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i4".to_string(),
            serde_json::json!({"no_rarity": true}),
        )
        .unwrap();

        let counts = ts.count_by_field("items", "rarity");
        assert_eq!(counts.get("common"), Some(&2));
        assert_eq!(counts.get("rare"), Some(&1));
        assert_eq!(counts.get("epic"), None);
        // i4 has no 'rarity' field — not counted
        assert_eq!(counts.values().sum::<usize>(), 3);
    }

    #[test]
    fn test_distinct_field_values() {
        let ts = Arc::new(TableStore::new());
        ts.set_row(
            "items".to_string(),
            "i1".to_string(),
            serde_json::json!({"tier": 1}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i2".to_string(),
            serde_json::json!({"tier": 3}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i3".to_string(),
            serde_json::json!({"tier": 1}),
        )
        .unwrap();
        ts.set_row(
            "items".to_string(),
            "i4".to_string(),
            serde_json::json!({"tier": 2}),
        )
        .unwrap();

        let vals = ts.distinct_field_values("items", "tier");
        assert_eq!(vals.len(), 3, "should have 3 distinct tiers: 1, 2, 3");
    }

    #[test]
    fn test_count_matching_uses_index() {
        let ts = Arc::new(TableStore::new());
        ts.create_index("players", "status").unwrap();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();
        ts.set_row(
            "players".to_string(),
            "p2".to_string(),
            serde_json::json!({"status": "inactive"}),
        )
        .unwrap();
        ts.set_row(
            "players".to_string(),
            "p3".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();

        let n = ts.count_matching("players", "status", &serde_json::json!("active"));
        assert_eq!(n, 2);
    }

    #[test]
    fn test_total_row_count() {
        let ts = Arc::new(TableStore::new());
        ts.set_counter("a".to_string(), 1, 0).unwrap();
        ts.set_counter("b".to_string(), 2, 0).unwrap();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({}),
        )
        .unwrap();

        assert_eq!(ts.total_row_count(), 3);
    }

    #[test]
    fn test_blob_size_limit_rejects_oversized_payload() {
        // Sequence the test against any other tests that mutate the global limit
        // by snapshotting and restoring it.
        let original = max_blob_size();
        set_max_blob_size(1024);

        let ts = Arc::new(TableStore::new());
        // Build an inventory value larger than 1024 bytes when JSON-encoded.
        let big_items: Vec<serde_json::Value> = (0..256)
            .map(|i| serde_json::json!({"id": i, "name": format!("item_{}", i), "padding": "xxxxxxxxxxxxxxxxxxxxxxxx"}))
            .collect();
        let row = serde_json::json!({"inventory": big_items, "hp": 100});

        let result = ts.set_row("players".to_string(), "alice".to_string(), row);
        assert!(
            result.is_err(),
            "store_blob must reject payloads larger than the configured max"
        );
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(
            err_msg.contains("exceeds max") || err_msg.contains("Blob size"),
            "error should mention the size limit, got: {}",
            err_msg
        );

        // Restore for any subsequent tests.
        set_max_blob_size(original);
    }

    #[test]
    fn test_create_index_backfills_existing_rows() {
        let ts = Arc::new(TableStore::new());
        // Insert rows BEFORE creating the index
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();
        ts.set_row(
            "players".to_string(),
            "p2".to_string(),
            serde_json::json!({"status": "active"}),
        )
        .unwrap();

        // Create index after rows exist
        ts.create_index("players", "status").unwrap();

        let active = ts
            .index_lookup("players", "status", &serde_json::json!("active"))
            .expect("index should exist");
        assert_eq!(
            active.len(),
            2,
            "back-fill should index both pre-existing rows"
        );
    }

    // ── Eviction integration tests ──────────────────────────────────────────

    // 1. LruRowCap evicts the oldest row when the cap is exceeded.
    #[test]
    fn test_lru_row_cap_evicts_oldest_row() {
        let ts = Arc::new(TableStore::with_eviction(EvictionPolicy::LruRowCap {
            max_rows_per_table: 3,
        }));

        // Insert 3 rows — within cap, none should be evicted.
        let deltas = vec![
            RowDelta {
                table_name: "items".to_string(),
                operation: "insert".to_string(),
                row_key: "r1".to_string(),
                row_id: 1, shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"v": 1})),
                counter_add_amount: 0, counter_add_timestamp: 0,
            },
            RowDelta {
                table_name: "items".to_string(),
                operation: "insert".to_string(),
                row_key: "r2".to_string(),
                row_id: 2, shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"v": 2})),
                counter_add_amount: 0, counter_add_timestamp: 0,
            },
            RowDelta {
                table_name: "items".to_string(),
                operation: "insert".to_string(),
                row_key: "r3".to_string(),
                row_id: 3, shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"v": 3})),
                counter_add_amount: 0, counter_add_timestamp: 0,
            },
        ];
        ts.apply_delta_batch(&deltas).unwrap();
        assert_eq!(ts.list_rows("items").unwrap().len(), 3);

        // Insert a 4th row — should evict the LRU (r1, the oldest).
        std::thread::sleep(std::time::Duration::from_millis(5));
        let delta4 = RowDelta {
            table_name: "items".to_string(),
            operation: "insert".to_string(),
            row_key: "r4".to_string(),
            row_id: 4, shard_id: 0,
            payload_arc: None,
            row_data: Some(serde_json::json!({"v": 4})),
            counter_add_amount: 0, counter_add_timestamp: 0,
        };
        ts.apply_delta_batch(&[delta4]).unwrap();

        // Still at most cap rows.
        let count = ts.list_rows("items").unwrap().len();
        assert!(count <= 3, "expected <= 3 rows after eviction, got {}", count);
        // r4 (just inserted) must be present.
        assert!(
            ts.get_row("items", "r4").unwrap().is_some(),
            "newly inserted row must not be evicted"
        );
    }

    // 2. LruRowCap does NOT evict when under the cap.
    #[test]
    fn test_lru_row_cap_no_eviction_under_cap() {
        let ts = Arc::new(TableStore::with_eviction(EvictionPolicy::LruRowCap {
            max_rows_per_table: 10,
        }));
        for i in 0..5 {
            let delta = RowDelta {
                table_name: "t".to_string(),
                operation: "insert".to_string(),
                row_key: format!("k{}", i),
                row_id: i as u32, shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"i": i})),
                counter_add_amount: 0, counter_add_timestamp: 0,
            };
            ts.apply_delta_batch(&[delta]).unwrap();
        }
        assert_eq!(ts.list_rows("t").unwrap().len(), 5, "no eviction below cap");
    }

    // 3. None policy never evicts, rows accumulate freely.
    #[test]
    fn test_none_policy_no_eviction() {
        let ts = Arc::new(TableStore::new()); // default = None
        for i in 0..20 {
            let delta = RowDelta {
                table_name: "t".to_string(),
                operation: "insert".to_string(),
                row_key: format!("k{}", i),
                row_id: i as u32, shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"i": i})),
                counter_add_amount: 0, counter_add_timestamp: 0,
            };
            ts.apply_delta_batch(&[delta]).unwrap();
        }
        assert_eq!(ts.list_rows("t").unwrap().len(), 20, "None policy must not evict");
    }

    // 4. LruByteCap constructor stores the cap correctly.
    #[test]
    fn test_lru_byte_cap_construction() {
        let ts = TableStore::with_eviction(EvictionPolicy::LruByteCap {
            max_bytes_total: 1_000_000,
        });
        if let EvictionPolicy::LruByteCap { max_bytes_total } = &ts.eviction_policy {
            assert_eq!(*max_bytes_total, 1_000_000);
        } else {
            panic!("Expected LruByteCap policy");
        }
    }

    // 5. Deleting a row removes it from the LRU tracker.
    #[test]
    fn test_delete_removes_lru_entry() {
        let ts = Arc::new(TableStore::with_eviction(EvictionPolicy::LruRowCap {
            max_rows_per_table: 100,
        }));

        // Insert then delete a row.
        let insert_delta = RowDelta {
            table_name: "t".to_string(),
            operation: "insert".to_string(),
            row_key: "key1".to_string(),
            row_id: 1, shard_id: 0,
            payload_arc: None,
            row_data: Some(serde_json::json!({"x": 1})),
            counter_add_amount: 0, counter_add_timestamp: 0,
        };
        ts.apply_delta_batch(&[insert_delta]).unwrap();

        let delete_delta = RowDelta {
            table_name: "t".to_string(),
            operation: "delete".to_string(),
            row_key: "key1".to_string(),
            row_id: 1, shard_id: 0,
            payload_arc: None,
            row_data: None,
            counter_add_amount: 0, counter_add_timestamp: 0,
        };
        ts.apply_delta_batch(&[delete_delta]).unwrap();

        // The LRU tracker should have no entry for key1 after deletion.
        if let Some(lru) = &ts.lru {
            let evictable = lru.evict_oldest("t", 10);
            for (_, key) in evictable {
                assert_ne!(key, "key1", "deleted key must not appear in LRU tracker");
            }
        }
    }
}
