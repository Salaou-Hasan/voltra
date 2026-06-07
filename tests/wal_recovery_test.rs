// ============================================================================
// WAL recovery unit-level tests (Session 39)
//
// These tests exercise the `wal::*` types directly — no server, no socket, no
// async runtime.  They cover:
//
//   1. Write 10 entries, read all back, verify count + checksums.
//   2. Write 10 entries, corrupt the last entry's checksum byte, confirm
//      `verify_checksum()` returns false on it (the recovery layer drops it).
//   3. Write 5 entries, truncate the file mid-entry, confirm read_all_entries
//      returns the 5 fully-written entries without panicking.
//   4. Snapshot at seq=5 + WAL entries through seq=15, replay only seq > 5 and
//      confirm only entries 6..=15 land in a fresh TableStore.
//
// The tests do NOT spawn a real server — they exercise the persistence layer in
// isolation, which is exactly what `tests/integration.rs` cannot do safely (the
// real-server integration tests are owned by Session 37/38 work).
// ============================================================================

use neondb::table::{RowDelta, TableStore};
use neondb::wal::{
    snapshot::{load_snapshot, save_snapshot, snapshot_path},
    WalEntry, WalReader, WalWriter,
};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn unique_tmp(label: &str) -> PathBuf {
    // Disambiguate per-test so parallel `cargo test` runs don't stomp on each
    // other's files.  Combines process id with a nanosecond timestamp.
    let pid = std::process::id();
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("neondb_wal_recovery_{}_{}_{}", label, pid, ns))
}

fn delta_for(table: &str, key: &str, value: serde_json::Value) -> RowDelta {
    RowDelta {
        table_name: table.to_string(),
        operation: "insert".to_string(),
        row_key: key.to_string(),
        row_id: 0,
        shard_id: 0,
        payload_arc: None,
        row_data: Some(value),
        counter_add_amount: 0,
        counter_add_timestamp: 0,
    }
}

fn write_entries(path: &PathBuf, count: u64) {
    let mut writer = WalWriter::open(path).expect("open writer");
    for seq in 1..=count {
        let delta = delta_for(
            "players",
            &format!("p{}", seq),
            json!({ "name": format!("player_{}", seq), "score": (seq as i64) * 10 }),
        );
        let entry = WalEntry::new(
            (1_000_000 + seq) as u64,
            seq,
            "increment".to_string(),
            vec![1, 2, 3],
            vec![delta],
        );
        writer.append(&entry).expect("append");
    }
    writer.fsync().expect("fsync");
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[test]
fn write_then_read_all_entries_roundtrip() {
    let dir = unique_tmp("roundtrip");
    fs::create_dir_all(&dir).unwrap();
    let wal = dir.join("neondb.wal");

    write_entries(&wal, 10);

    let mut reader = WalReader::open(&wal).expect("open reader");
    let entries = reader.read_all_entries().expect("read");

    assert_eq!(entries.len(), 10, "all 10 entries must be readable");
    for (i, e) in entries.iter().enumerate() {
        let expected_seq = (i as u64) + 1;
        assert_eq!(e.header.sequence_number, expected_seq);
        assert!(
            e.verify_checksum(),
            "entry seq={} should have a valid checksum",
            expected_seq
        );
        assert_eq!(e.payload.reducer_id, "increment");
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn corrupted_last_entry_checksum_fails_verification() {
    let dir = unique_tmp("corrupt");
    fs::create_dir_all(&dir).unwrap();
    let wal = dir.join("neondb.wal");

    write_entries(&wal, 10);

    // Flip the LAST byte of the file.  Because each WAL entry ends with its
    // serialised payload (the checksum lives in the header at the START of the
    // entry, but the message-pack-encoded entry will end inside the payload),
    // mutating the last byte mangles the encoded data.  When the reader
    // attempts to deserialize the final entry it will either fail to decode
    // OR produce a struct whose recomputed checksum no longer matches the
    // stored header.checksum.  Either way the recovery layer drops the entry.
    {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal)
            .unwrap();
        let end = f.seek(SeekFrom::End(0)).unwrap();
        assert!(end > 0);
        f.seek(SeekFrom::End(-1)).unwrap();
        let mut buf = [0u8; 1];
        // Read current value, then write a different one so this works
        // regardless of what the encoding produced.
        use std::io::Read;
        f.read_exact(&mut buf).unwrap();
        f.seek(SeekFrom::End(-1)).unwrap();
        let new_byte = buf[0].wrapping_add(0xFF);
        f.write_all(&[new_byte]).unwrap();
        f.flush().unwrap();
    }

    let mut reader = WalReader::open(&wal).expect("open reader");
    let entries = reader.read_all_entries().expect("read after corrupt");

    // Either decoder bailed early (returns fewer entries) OR the last entry
    // decoded but its checksum no longer matches.  Both are acceptable
    // outcomes — the recovery layer treats an invalid-checksum entry as
    // discardable, identical to a torn write.
    if entries.len() == 10 {
        let last = entries.last().unwrap();
        assert!(
            !last.verify_checksum(),
            "corrupted final entry must fail verify_checksum()"
        );
    } else {
        assert!(
            entries.len() < 10,
            "corruption should reduce readable entries; got {}",
            entries.len()
        );
        // All surviving entries must still verify.
        for e in &entries {
            assert!(e.verify_checksum(), "surviving entries must verify");
        }
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn truncated_mid_entry_recovers_completed_entries() {
    let dir = unique_tmp("truncate");
    fs::create_dir_all(&dir).unwrap();
    let wal = dir.join("neondb.wal");

    write_entries(&wal, 5);

    // Append 12 bytes of garbage — enough to look like a length prefix + the
    // start of an entry, but not enough to satisfy the length.  The reader
    // should stop cleanly after the 5 valid entries.
    {
        let mut f = OpenOptions::new().append(true).open(&wal).unwrap();
        // 4-byte length prefix that claims a huge message follows
        f.write_all(&(10_000u32).to_le_bytes()).unwrap();
        // 8 bytes of garbage that obviously do not form a 10_000-byte payload
        f.write_all(&[0xAA; 8]).unwrap();
        f.flush().unwrap();
    }

    let mut reader = WalReader::open(&wal).expect("open reader");
    let entries = reader.read_all_entries().expect("read should not panic");

    assert_eq!(
        entries.len(),
        5,
        "torn tail must be ignored; expected 5 entries, got {}",
        entries.len()
    );
    for (i, e) in entries.iter().enumerate() {
        assert!(e.verify_checksum(), "entry {} checksum", i);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_plus_wal_replay_applies_only_postsnapshot_entries() {
    let dir = unique_tmp("snapwal");
    fs::create_dir_all(&dir).unwrap();

    // Build a TableStore representing the state captured by the snapshot.
    let tables_at_snapshot = Arc::new(TableStore::new());
    for seq in 1..=5u64 {
        tables_at_snapshot
            .set_row(
                "players".to_string(),
                format!("p{}", seq),
                json!({ "name": format!("player_{}", seq), "score": (seq as i64) * 10 }),
            )
            .unwrap();
    }
    let snapshot_seq = 5u64;
    save_snapshot(&tables_at_snapshot, &dir, snapshot_seq, 99_999).unwrap();

    // Now write a WAL containing entries 1..=15 — we expect replay to skip
    // 1..=5 (already in the snapshot) and apply only 6..=15.
    let wal = dir.join("neondb.wal");
    write_entries(&wal, 15);

    // Recovery: fresh store, load snapshot, then replay WAL entries with
    // sequence_number > snapshot.last_sequence.
    let recovered = Arc::new(TableStore::new());
    let meta =
        load_snapshot(&snapshot_path(&dir, snapshot_seq), &recovered).expect("load snapshot");
    assert_eq!(meta.last_sequence, snapshot_seq);

    let mut reader = WalReader::open(&wal).unwrap();
    let entries = reader.read_all_entries().unwrap();
    assert_eq!(entries.len(), 15);

    let mut applied_post_snapshot = 0u64;
    for entry in &entries {
        if entry.header.sequence_number <= meta.last_sequence {
            // Skipped — already in the snapshot.
            continue;
        }
        // Apply the entry's deltas to the recovered store.
        recovered
            .apply_delta_batch(&entry.payload.deltas)
            .expect("apply delta");
        applied_post_snapshot += 1;
    }

    assert_eq!(
        applied_post_snapshot, 10,
        "expected to apply 10 entries (seq 6..=15); applied {}",
        applied_post_snapshot
    );

    // Sanity: all 15 rows should exist in the recovered store (1..=5 came
    // from the snapshot, 6..=15 from WAL replay).
    for seq in 1..=15u64 {
        let row = recovered.get_row("players", &format!("p{}", seq)).unwrap();
        assert!(
            row.is_some(),
            "row p{} should exist after recovery",
            seq
        );
    }

    let _ = fs::remove_dir_all(&dir);
}
