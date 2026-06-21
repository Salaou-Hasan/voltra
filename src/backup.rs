// ============================================================================
// backup.rs — Automated backups, rotation, restore, and point-in-time recovery
//
// BACKUP LAYOUT:
//   <backup_dir>/
//     backup_<unix_secs>_<seq>/
//       snapshot_<seq>.bin      (full TableStore snapshot via save_snapshot)
//       voltra.wal              (copy of the live WAL at backup time)
//       backup.json             (metadata: timestamp, seq, row_count)
//
// OPERATIONS:
//   backup_now()       — snapshot + WAL copy into a timestamped directory
//   rotate_backups()   — delete oldest backups beyond `keep`
//   list_backups()     — enumerate backups sorted newest-first
//   restore_to_dirs()  — copy a backup's snapshot + WAL back into the live
//                        data dirs (server must be stopped); optional
//                        --until-ts cutoff rewrites the WAL with only entries
//                        at or before the given unix-nanos timestamp (PITR)
//
// The background task in main.rs calls backup_now + rotate_backups every
// VOLTRA_BACKUP_INTERVAL_SECS when VOLTRA_BACKUP_DIR is configured.
// ============================================================================

use crate::error::{VoltraError, Result};
use crate::table::TableStore;
use crate::wal::{
    snapshot::{find_latest_snapshot, save_snapshot},
    WalEntry, WalReader, WalWriter,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Metadata stored alongside each backup.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BackupMeta {
    pub created_unix_secs: u64,
    pub last_seq: u64,
    pub row_count: usize,
    pub wal_bytes: u64,
    pub voltra_version: String,
}

/// Take a full backup: snapshot the TableStore + copy the WAL file.
/// Returns the backup directory path.
pub fn backup_now(
    tables: &TableStore,
    wal_path: &Path,
    backup_dir: &Path,
    last_seq: u64,
) -> Result<PathBuf> {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let dest = backup_dir.join(format!("backup_{}_{}", now_secs, last_seq));
    fs::create_dir_all(&dest)?;

    // 1. Snapshot directly into the backup directory.
    save_snapshot(tables, &dest, last_seq, now_nanos)?;

    // 2. Copy the WAL (entries after the snapshot seq let restore catch up
    //    to the exact backup moment, and PITR can cut within them).
    let mut wal_bytes = 0u64;
    if wal_path.exists() {
        let wal_dest = dest.join("voltra.wal");
        fs::copy(wal_path, &wal_dest)?;
        wal_bytes = fs::metadata(&wal_dest).map(|m| m.len()).unwrap_or(0);
    }

    // 3. Metadata.
    let meta = BackupMeta {
        created_unix_secs: now_secs,
        last_seq,
        row_count: tables.total_row_count(),
        wal_bytes,
        voltra_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    fs::write(dest.join("backup.json"), serde_json::to_string_pretty(&meta)?)?;

    log::info!(
        "[backup] Wrote backup to {:?} (seq={}, rows={}, wal={}B)",
        dest, last_seq, meta.row_count, wal_bytes
    );
    Ok(dest)
}

/// List backups in `backup_dir`, newest first.  Returns (path, created_secs, seq).
pub fn list_backups(backup_dir: &Path) -> Vec<(PathBuf, u64, u64)> {
    let mut found = Vec::new();
    let Ok(rd) = fs::read_dir(backup_dir) else { return found; };
    for entry in rd.flatten() {
        let path = entry.path();
        if !path.is_dir() { continue; }
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix("backup_") else { continue; };
        let parts: Vec<&str> = rest.splitn(2, '_').collect();
        if parts.len() != 2 { continue; }
        let (Ok(ts), Ok(seq)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) else { continue; };
        found.push((path, ts, seq));
    }
    // Newest first (by timestamp, then seq).
    found.sort_by(|a, b| (b.1, b.2).cmp(&(a.1, a.2)));
    found
}

/// Delete the oldest backups beyond `keep`.  Returns how many were removed.
pub fn rotate_backups(backup_dir: &Path, keep: usize) -> Result<usize> {
    let backups = list_backups(backup_dir);
    let mut removed = 0usize;
    for (path, _, _) in backups.into_iter().skip(keep.max(1)) {
        match fs::remove_dir_all(&path) {
            Ok(()) => {
                log::info!("[backup] Rotated out old backup {:?}", path);
                removed += 1;
            }
            Err(e) => log::warn!("[backup] Could not remove {:?}: {}", path, e),
        }
    }
    Ok(removed)
}

/// Restore a backup into live data directories.  The server must be STOPPED.
///
/// Copies the snapshot into `snapshot_dir` and the WAL into `wal_path`.
/// If `until_ts_nanos` is given, the restored WAL is rewritten to contain
/// only entries with `timestamp <= until_ts_nanos` (point-in-time recovery).
///
/// Returns (snapshot_seq, wal_entries_restored).
pub fn restore_to_dirs(
    backup_path: &Path,
    wal_path: &Path,
    snapshot_dir: &Path,
    until_ts_nanos: Option<u64>,
) -> Result<(u64, usize)> {
    if !backup_path.is_dir() {
        return Err(VoltraError::StorageError(format!(
            "Backup directory not found: {:?}", backup_path
        )));
    }

    // 1. Locate the snapshot inside the backup.
    let (snap_src, snap_seq) = find_latest_snapshot(backup_path)
        .ok_or_else(|| VoltraError::StorageError(format!(
            "No snapshot file inside backup {:?}", backup_path
        )))?;

    // 2. Copy snapshot into the live snapshot dir.
    fs::create_dir_all(snapshot_dir)?;
    let snap_dest = snapshot_dir.join(
        snap_src.file_name().ok_or_else(|| VoltraError::StorageError("Bad snapshot filename".into()))?
    );
    fs::copy(&snap_src, &snap_dest)?;

    // 3. Restore the WAL — full copy, or PITR-filtered rewrite.
    let wal_src = backup_path.join("voltra.wal");
    let mut restored_entries = 0usize;
    if let Some(parent) = wal_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if wal_src.exists() {
        match until_ts_nanos {
            None => {
                fs::copy(&wal_src, wal_path)?;
                // Count entries for the report.
                if let Ok(mut r) = WalReader::open(wal_path) {
                    restored_entries = r.read_all_entries().map(|e| e.len()).unwrap_or(0);
                }
            }
            Some(cutoff) => {
                let mut reader = WalReader::open(&wal_src)?;
                let entries: Vec<WalEntry> = reader
                    .read_all_entries()?
                    .into_iter()
                    .filter(|e| e.header.timestamp <= cutoff)
                    .collect();
                // Rewrite the live WAL with only the surviving entries.
                let _ = fs::remove_file(wal_path);
                let mut writer = WalWriter::open(wal_path)?;
                for entry in &entries {
                    writer.append(entry)?;
                }
                writer.fsync()?;
                restored_entries = entries.len();
                log::info!(
                    "[restore] PITR cutoff {}: kept {} WAL entries", cutoff, restored_entries
                );
            }
        }
    } else {
        // No WAL in the backup — make sure a stale live WAL doesn't replay
        // writes from after the backup point.
        let _ = fs::remove_file(wal_path);
    }

    log::info!(
        "[restore] Restored snapshot seq={} + {} WAL entries from {:?}",
        snap_seq, restored_entries, backup_path
    );
    Ok((snap_seq, restored_entries))
}

/// Read a backup's metadata file, if present.
pub fn read_meta(backup_path: &Path) -> Option<BackupMeta> {
    let raw = fs::read_to_string(backup_path.join("backup.json")).ok()?;
    serde_json::from_str(&raw).ok()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::snapshot::load_snapshot;

    fn unique_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "{}_{}_{}", name, std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn test_backup_and_restore_roundtrip() {
        let backup_dir = unique_dir("test_bk_root");
        let restore_dir = unique_dir("test_bk_restore");
        let wal_path = backup_dir.join("live.wal");

        // Source data: 2 rows + a WAL with one entry.
        let tables = TableStore::new();
        tables.set_row("players".into(), "alice".into(), serde_json::json!({"hp": 100})).unwrap();
        tables.set_row("players".into(), "bob".into(),   serde_json::json!({"hp": 80})).unwrap();
        {
            let mut w = WalWriter::open(&wal_path).unwrap();
            w.append(&WalEntry::new(5000, 10, "noop".into(), vec![], vec![])).unwrap();
            w.fsync().unwrap();
        }

        let dest = backup_now(&tables, &wal_path, &backup_dir, 9).unwrap();
        assert!(dest.join("backup.json").exists());
        assert!(dest.join("voltra.wal").exists());

        // Restore into fresh dirs.
        let new_wal  = restore_dir.join("voltra.wal");
        let new_snap = restore_dir.join("snapshots");
        let (seq, wal_n) = restore_to_dirs(&dest, &new_wal, &new_snap, None).unwrap();
        assert_eq!(seq, 9);
        assert_eq!(wal_n, 1);

        // Load the restored snapshot into a fresh store and verify data.
        let fresh = TableStore::new();
        let (snap_path, _) = find_latest_snapshot(&new_snap).unwrap();
        load_snapshot(&snap_path, &fresh).unwrap();
        let alice = fresh.get_row("players", "alice").unwrap().unwrap();
        assert_eq!(alice["hp"], 100);

        fs::remove_dir_all(&backup_dir).ok();
        fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn test_pitr_cutoff_filters_wal() {
        let backup_dir = unique_dir("test_bk_pitr");
        let restore_dir = unique_dir("test_bk_pitr_restore");
        let wal_path = backup_dir.join("live.wal");

        let tables = TableStore::new();
        tables.set_row("t".into(), "k".into(), serde_json::json!({"v": 1})).unwrap();
        {
            let mut w = WalWriter::open(&wal_path).unwrap();
            w.append(&WalEntry::new(1_000, 1, "a".into(), vec![], vec![])).unwrap();
            w.append(&WalEntry::new(2_000, 2, "b".into(), vec![], vec![])).unwrap();
            w.append(&WalEntry::new(3_000, 3, "c".into(), vec![], vec![])).unwrap();
            w.fsync().unwrap();
        }

        let dest = backup_now(&tables, &wal_path, &backup_dir, 3).unwrap();
        let new_wal  = restore_dir.join("voltra.wal");
        let new_snap = restore_dir.join("snapshots");

        // Cut at ts=2000: entries 1 and 2 survive, 3 is dropped.
        let (_, wal_n) = restore_to_dirs(&dest, &new_wal, &new_snap, Some(2_000)).unwrap();
        assert_eq!(wal_n, 2);

        let mut r = WalReader::open(&new_wal).unwrap();
        let entries = r.read_all_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].header.sequence_number, 2);

        fs::remove_dir_all(&backup_dir).ok();
        fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn test_rotation_keeps_newest() {
        let backup_dir = unique_dir("test_bk_rotate");
        let tables = TableStore::new();
        tables.set_row("t".into(), "k".into(), serde_json::json!({"v": 1})).unwrap();
        let wal_path = backup_dir.join("live.wal");

        // Create 4 backups with distinct (ts, seq) — seq increments guarantee
        // unique directory names even within the same second.
        for seq in 1..=4u64 {
            backup_now(&tables, &wal_path, &backup_dir, seq).unwrap();
        }
        assert_eq!(list_backups(&backup_dir).len(), 4);

        let removed = rotate_backups(&backup_dir, 2).unwrap();
        assert_eq!(removed, 2);
        let remaining = list_backups(&backup_dir);
        assert_eq!(remaining.len(), 2);
        // Newest first — highest seq retained.
        assert_eq!(remaining[0].2, 4);
        assert_eq!(remaining[1].2, 3);

        fs::remove_dir_all(&backup_dir).ok();
    }

    #[test]
    fn test_restore_missing_backup_errors() {
        let bogus = std::env::temp_dir().join("definitely_not_a_backup_xyz");
        let res = restore_to_dirs(&bogus, Path::new("w"), Path::new("s"), None);
        assert!(res.is_err());
    }

    #[test]
    fn test_read_meta() {
        let backup_dir = unique_dir("test_bk_meta");
        let tables = TableStore::new();
        tables.set_row("t".into(), "k".into(), serde_json::json!({"v": 1})).unwrap();
        let dest = backup_now(&tables, &backup_dir.join("live.wal"), &backup_dir, 42).unwrap();
        let meta = read_meta(&dest).expect("meta should exist");
        assert_eq!(meta.last_seq, 42);
        assert_eq!(meta.row_count, 1);
        fs::remove_dir_all(&backup_dir).ok();
    }
}
