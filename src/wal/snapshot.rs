//! Snapshot subsystem for Voltra.
//!
//! A snapshot serialises every row of every table in a `TableStore` into a
//! single MessagePack file at path `<dir>/voltra_snapshot_<seq>.bin`.  On
//! startup the server loads the most-recent valid snapshot and replays only
//! the WAL entries whose sequence number is *greater than* `last_sequence`.
//!
//! ## Atomicity guarantee
//! Snapshots are written to a `.tmp` file first, fsynced, then renamed.  A
//! crash mid-write leaves a stale `.tmp` that is simply ignored on the next
//! startup.  A good snapshot is never overwritten by a partial one.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;
use crate::table::TableStore;

// ── On-disk structures ────────────────────────────────────────────────────────

/// Header stored at the beginning of every snapshot file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Format version — currently always `1`.
    pub version: u32,
    /// The highest WAL sequence number whose effects are captured here.
    /// On recovery, replay only entries with `sequence_number > last_sequence`.
    pub last_sequence: u64,
    /// Unix timestamp (nanoseconds) when the snapshot was taken.
    pub timestamp: u64,
    /// Total row count across all tables (informational only).
    pub row_count: u64,
    /// Value of `TableStore::next_row_id` at snapshot time.
    /// Restored on load to prevent future rows getting IDs that collide with
    /// IDs already embedded in post-snapshot WAL entries.
    pub next_row_id: u32,
}

/// Complete on-disk snapshot: metadata + all table rows.
#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    meta: SnapshotMeta,
    /// `table_name → [(row_key, decoded_row_value)]`
    tables: HashMap<String, Vec<(String, Value)>>,
}

// ── File-name helpers ─────────────────────────────────────────────────────────

/// Return the canonical path for a snapshot at the given sequence number.
pub fn snapshot_path(dir: &Path, last_seq: u64) -> PathBuf {
    dir.join(format!("voltra_snapshot_{}.bin", last_seq))
}

fn parse_snapshot_seq(name: &str) -> Option<u64> {
    name.strip_prefix("voltra_snapshot_")
        .and_then(|s| s.strip_suffix(".bin"))
        .and_then(|s| s.parse().ok())
}

/// Scan `dir` for `voltra_snapshot_*.bin` files and return the path and
/// `last_sequence` of the most recent one.  Returns `None` if the directory
/// does not exist or contains no snapshot files.
pub fn find_latest_snapshot(dir: &Path) -> Option<(PathBuf, u64)> {
    let entries = fs::read_dir(dir).ok()?;
    let mut best: Option<(PathBuf, u64)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(seq) = parse_snapshot_seq(&name_str) {
            if best.as_ref().is_none_or(|(_, s)| seq > *s) {
                best = Some((entry.path(), seq));
            }
        }
    }
    best
}

// ── Save ──────────────────────────────────────────────────────────────────────

/// Atomically write a snapshot of `tables` to `dir/voltra_snapshot_{last_seq}.bin`.
///
/// The write sequence is:
/// 1. Encode to MessagePack in memory.
/// 2. Write to `<dir>/voltra_snapshot_{last_seq}.bin.tmp`.
/// 3. `fsync` the tmp file.
/// 4. `rename` tmp → final path (atomic on POSIX; best-effort on Windows).
pub fn save_snapshot(tables: &TableStore, dir: &Path, last_seq: u64, timestamp: u64) -> Result<()> {
    let mut all_tables: HashMap<String, Vec<(String, Value)>> = HashMap::new();
    let mut row_count = 0u64;

    for table_name in tables.list_tables() {
        let rows = tables.list_rows_with_keys(&table_name)?;
        row_count += rows.len() as u64;
        if !rows.is_empty() {
            all_tables.insert(table_name, rows);
        }
    }

    let snap = SnapshotFile {
        meta: SnapshotMeta {
            version: 1,
            last_sequence: last_seq,
            timestamp,
            row_count,
            next_row_id: tables.current_next_row_id(),
        },
        tables: all_tables,
    };

    fs::create_dir_all(dir)?;
    let final_path = snapshot_path(dir, last_seq);
    let tmp_path = dir.join(format!("voltra_snapshot_{}.bin.tmp", last_seq));

    {
        // Stream the MessagePack encoding straight to a buffered file writer
        // instead of materializing the entire encoded blob in a Vec<u8> first.
        // `rmp_serde::encode::write` uses the same default Serializer as
        // `to_vec`, so the on-disk bytes are identical and `from_slice` in
        // load_snapshot reads them unchanged — this only removes the second
        // full-dataset-sized copy that previously sat in memory alongside the
        // decoded `snap` during the write, halving the snapshot memory spike.
        let file = fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
        rmp_serde::encode::write(&mut writer, &snap)?;
        writer.flush()?;
        // Recover the inner File to fsync it (BufWriter::into_inner flushes too).
        let file = writer.into_inner().map_err(|e| {
            crate::error::VoltraError::wal_error(format!("snapshot buffer flush failed: {e}"))
        })?;
        file.sync_all()?;
    }

    // Drop the decoded snapshot as soon as it is on disk so the allocator can
    // reclaim it promptly rather than holding it until function return.
    let table_count = snap.tables.len();
    drop(snap);

    fs::rename(&tmp_path, &final_path)?;

    log::info!(
        "Snapshot saved: {} rows, {} tables, seq={} → {:?}",
        row_count,
        table_count,
        last_seq,
        final_path
    );

    Ok(())
}

// ── Load ──────────────────────────────────────────────────────────────────────

/// Load a snapshot from `path` and bulk-restore all rows into `tables`.
///
/// Returns the [`SnapshotMeta`] so the caller can determine which WAL entries
/// to skip during recovery (`sequence_number <= meta.last_sequence`).
///
/// The `TableStore::next_row_id` counter is restored from the snapshot so
/// future row allocations do not collide with IDs embedded in post-snapshot
/// WAL entries.
pub fn load_snapshot(path: &Path, tables: &TableStore) -> Result<SnapshotMeta> {
    let data = fs::read(path)?;
    let snap: SnapshotFile = rmp_serde::from_slice(&data)?;

    let meta = snap.meta.clone();

    for (table_name, rows) in snap.tables {
        for (row_key, row_data) in rows {
            tables.set_row(table_name.clone(), row_key, row_data)?;
        }
    }

    // Restore the ID counter so new rows don't collide with IDs already used
    // in post-snapshot WAL entries.
    tables.set_next_row_id(meta.next_row_id);

    log::info!(
        "Snapshot loaded: {} rows from {:?}, last_seq={}",
        meta.row_count,
        path,
        meta.last_sequence
    );

    Ok(meta)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_dir(suffix: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("voltra_snap_{}", suffix));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("alpha".to_string(), 42, 0).unwrap();
        tables.set_counter("beta".to_string(), 99, 0).unwrap();
        tables
            .set_row(
                "players".to_string(),
                "hero_1".to_string(),
                serde_json::json!({"name": "Alice", "level": 10}),
            )
            .unwrap();

        let dir = temp_dir("roundtrip");
        save_snapshot(&tables, &dir, 100, 12345).unwrap();

        let snap_path = snapshot_path(&dir, 100);
        assert!(snap_path.exists(), "snapshot file must exist after save");

        // Restore into a fresh store
        let tables2 = Arc::new(TableStore::new());
        let meta = load_snapshot(&snap_path, &tables2).unwrap();
        assert_eq!(meta.last_sequence, 100);
        assert_eq!(meta.version, 1);

        let alpha = tables2.get_counter("alpha").unwrap().unwrap();
        assert_eq!(alpha.value, 42);
        let beta = tables2.get_counter("beta").unwrap().unwrap();
        assert_eq!(beta.value, 99);

        let player = tables2.get_row("players", "hero_1").unwrap();
        assert!(
            player.is_some(),
            "hero_1 should survive snapshot round-trip"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_latest_snapshot_picks_highest_seq() {
        let dir = temp_dir("find_latest");
        fs::create_dir_all(&dir).unwrap();

        // Create fake (empty) snapshot files at various sequence numbers
        fs::write(dir.join("voltra_snapshot_10.bin"), b"x").unwrap();
        fs::write(dir.join("voltra_snapshot_200.bin"), b"x").unwrap();
        fs::write(dir.join("voltra_snapshot_50.bin"), b"x").unwrap();
        fs::write(dir.join("other_file.txt"), b"x").unwrap();

        let found = find_latest_snapshot(&dir);
        assert!(found.is_some());
        assert_eq!(found.unwrap().1, 200, "should return highest seq");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_find_latest_snapshot_returns_none_when_empty() {
        let dir = temp_dir("find_empty");
        fs::create_dir_all(&dir).unwrap();
        assert!(find_latest_snapshot(&dir).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_snapshot_preserves_next_row_id() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("x".to_string(), 1, 0).unwrap();
        tables.set_counter("y".to_string(), 2, 0).unwrap();
        let id_before = tables.current_next_row_id();

        let dir = temp_dir("next_row_id");
        save_snapshot(&tables, &dir, 7, 0).unwrap();

        let tables2 = Arc::new(TableStore::new());
        let meta = load_snapshot(&snapshot_path(&dir, 7), &tables2).unwrap();
        assert_eq!(meta.next_row_id, id_before);
        assert_eq!(tables2.current_next_row_id(), id_before);

        let _ = fs::remove_dir_all(&dir);
    }
}
