//! Append-only file persistence for the MVCC engine.
//!
//! AOF record framing:  `[len: u32 LE][crc32: u32 LE][payload: rmp(AofRecord)]`
//! Snapshot framing:    `[MAGIC "NSNP1"][rmp(SnapFile)]` written to a tmp file
//! and atomically renamed.
//!
//! Replay tolerates a torn tail (crash mid-write): the first record that fails
//! length/CRC/decode validation ends the replay.

use super::{Datum, FsyncPolicy, WriteOp};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

const SNAP_MAGIC: &[u8; 5] = b"NSNP1";
/// Sanity cap on a single AOF record (64 MiB) — protects replay from a
/// corrupt length prefix claiming gigabytes.
const MAX_RECORD: u32 = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub struct AofRecord {
    pub ts: u64,
    pub ops: Vec<WriteOp>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SnapEntry {
    pub ns: u32,
    pub key: Bytes,
    pub value: Datum,
    pub expires_at_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct SnapFile {
    ts: u64,
    entries: Vec<SnapEntry>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Writer
// ─────────────────────────────────────────────────────────────────────────────

pub struct AofWriter {
    w: BufWriter<File>,
    policy: FsyncPolicy,
    last_sync: Instant,
    dirty: bool,
}

impl AofWriter {
    /// Open for appending (creates the file if missing).
    pub fn open(path: &Path, policy: FsyncPolicy) -> std::io::Result<Self> {
        let f = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { w: BufWriter::with_capacity(256 * 1024, f), policy, last_sync: Instant::now(), dirty: false })
    }

    /// Truncate and start fresh (after a snapshot SAVE).
    pub fn truncate(path: &Path, policy: FsyncPolicy) -> std::io::Result<Self> {
        let f = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        Ok(Self { w: BufWriter::with_capacity(256 * 1024, f), policy, last_sync: Instant::now(), dirty: false })
    }

    /// Append a group of records and flush to the OS (one syscall per group).
    pub fn append_records(&mut self, records: &[AofRecord]) {
        for rec in records {
            let payload = match rmp_serde::to_vec(rec) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("[mvcc-aof] encode failed (record dropped): {e}");
                    continue;
                }
            };
            let crc = crc32fast::hash(&payload);
            let _ = self.w.write_all(&(payload.len() as u32).to_le_bytes());
            let _ = self.w.write_all(&crc.to_le_bytes());
            let _ = self.w.write_all(&payload);
        }
        let _ = self.w.flush();
        self.dirty = true;
    }

    /// Unconditional fsync.
    pub fn sync(&mut self) {
        let _ = self.w.flush();
        if let Err(e) = self.w.get_ref().sync_data() {
            log::error!("[mvcc-aof] fsync failed: {e}");
        }
        self.last_sync = Instant::now();
        self.dirty = false;
    }

    /// Policy-driven fsync: EverySec syncs at most once per second.
    pub fn maybe_sync(&mut self) {
        if !self.dirty {
            return;
        }
        match self.policy {
            FsyncPolicy::Always => self.sync(),
            FsyncPolicy::EverySec => {
                if self.last_sync.elapsed().as_millis() >= 1000 {
                    self.sync();
                }
            }
            FsyncPolicy::No => {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Replay
// ─────────────────────────────────────────────────────────────────────────────

/// Replay every valid record in the AOF, stopping at the first torn/corrupt
/// record. A missing file is an empty AOF.
pub fn replay(path: &Path, mut f: impl FnMut(AofRecord)) -> std::io::Result<()> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut r = BufReader::with_capacity(256 * 1024, file);
    let mut header = [0u8; 8];
    while r.read_exact(&mut header).is_ok() {
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len == 0 || len > MAX_RECORD {
            log::warn!("[mvcc-aof] invalid record length {len}; stopping replay");
            break;
        }
        let mut payload = vec![0u8; len as usize];
        if r.read_exact(&mut payload).is_err() {
            log::warn!("[mvcc-aof] torn record at tail; stopping replay");
            break;
        }
        if crc32fast::hash(&payload) != crc {
            log::warn!("[mvcc-aof] CRC mismatch; stopping replay");
            break;
        }
        match rmp_serde::from_slice::<AofRecord>(&payload) {
            Ok(rec) => f(rec),
            Err(e) => {
                log::warn!("[mvcc-aof] decode failed ({e}); stopping replay");
                break;
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot
// ─────────────────────────────────────────────────────────────────────────────

/// Write a point-in-time snapshot atomically (tmp file + rename).
pub fn save_snapshot(path: &Path, ts: u64, entries: &[SnapEntry]) -> std::io::Result<()> {
    let tmp = path.with_extension("snap.tmp");
    {
        let f = File::create(&tmp)?;
        let mut w = BufWriter::with_capacity(1024 * 1024, f);
        w.write_all(SNAP_MAGIC)?;
        let body = rmp_serde::to_vec(&SnapFile { ts, entries: entries.to_vec() })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        w.write_all(&body)?;
        w.flush()?;
        w.get_ref().sync_data()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load a snapshot. Returns None if the file is missing or unreadable
/// (a corrupt snapshot is treated as absent — the AOF is still replayed).
pub fn load_snapshot(path: &Path) -> std::io::Result<Option<(u64, Vec<SnapEntry>)>> {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut magic = [0u8; 5];
    if f.read_exact(&mut magic).is_err() || &magic != SNAP_MAGIC {
        log::warn!("[mvcc-snap] bad magic; ignoring snapshot");
        return Ok(None);
    }
    let mut body = Vec::new();
    f.read_to_end(&mut body)?;
    match rmp_serde::from_slice::<SnapFile>(&body) {
        Ok(s) => Ok(Some((s.ts, s.entries))),
        Err(e) => {
            log::warn!("[mvcc-snap] decode failed ({e}); ignoring snapshot");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::Datum;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn aof_roundtrip_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("neondb_aof_unit_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.aof");
        let _ = std::fs::remove_file(&path);

        {
            let mut w = AofWriter::open(&path, FsyncPolicy::Always).unwrap();
            w.append_records(&[
                AofRecord {
                    ts: 1,
                    ops: vec![WriteOp::Put { ns: 0, key: b("a"), value: Datum::Str(b("1")), expires_at_ms: None }],
                },
                AofRecord { ts: 2, ops: vec![WriteOp::Del { ns: 0, key: b("a") }] },
            ]);
            w.sync();
        }
        // Simulate a torn tail: append garbage half-record.
        {
            use std::io::Write as _;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[9, 0, 0, 0, 1, 2]).unwrap();
        }

        let mut seen = Vec::new();
        replay(&path, |rec| seen.push(rec.ts)).unwrap();
        assert_eq!(seen, vec![1, 2]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_roundtrip() {
        let dir = std::env::temp_dir().join(format!("neondb_snap_unit_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.snap");

        save_snapshot(
            &path,
            42,
            &[SnapEntry { ns: 3, key: b("k"), value: Datum::Str(b("v")), expires_at_ms: Some(99) }],
        )
        .unwrap();
        let (ts, entries) = load_snapshot(&path).unwrap().unwrap();
        assert_eq!(ts, 42);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ns, 3);
        assert_eq!(entries[0].expires_at_ms, Some(99));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
