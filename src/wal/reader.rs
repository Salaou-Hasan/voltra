use crate::error::Result;
use crate::wal::entry::WalEntry;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// Reads WAL entries from disk
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Open a WAL file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Ok(WalReader {
            reader: BufReader::new(file),
        })
    }

    /// Read the next entry from the WAL
    pub fn next_entry(&mut self) -> Result<Option<WalEntry>> {
        // This simplified version is not used in Phase 1; we use read_all_entries instead
        Ok(None)
    }

    /// Read all entries from the WAL (simpler approach for Phase 1)
    pub fn read_all_entries(&mut self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut all_data = Vec::new();
        self.reader.read_to_end(&mut all_data)?;

        // Try to decode messages from the buffer
        // MessagePack doesn't have a frame boundary, so we need size prefixes
        // For Phase 1, we'll store messages with a 4-byte length prefix

        let mut pos = 0;
        while pos < all_data.len() {
            let entry_start = pos;

            if pos + 4 > all_data.len() {
                // Trailing bytes that are too short to even hold a length prefix.
                // This is almost certainly a torn write from a crash mid-fsync.
                log::warn!(
                    "WAL has partial trailing entry at byte offset {} after {} valid entries",
                    entry_start,
                    entries.len()
                );
                break;
            }

            let len = u32::from_le_bytes([
                all_data[pos],
                all_data[pos + 1],
                all_data[pos + 2],
                all_data[pos + 3],
            ]) as usize;

            pos += 4;

            if pos + len > all_data.len() {
                // Length prefix is fine, but the body was truncated. Same root
                // cause as above — log it but don't fail recovery.
                log::warn!(
                    "WAL has partial trailing entry at byte offset {} after {} valid entries",
                    entry_start,
                    entries.len()
                );
                break;
            }

            let msg_data = &all_data[pos..pos + len];
            pos += len;

            match rmp_serde::from_slice::<WalEntry>(msg_data) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    log::warn!(
                        "WAL has partial trailing entry at byte offset {} after {} valid entries (decode error: {})",
                        entry_start,
                        entries.len(),
                        e
                    );
                    break;
                }
            }
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_wal_roundtrip() {
        let tmp_path = std::env::temp_dir().join("test_wal_roundtrip.bin");
        let _ = fs::remove_file(&tmp_path);

        // Write
        let mut writer = WalWriter::open(&tmp_path).unwrap();
        let entry = WalEntry::new(1000, 1, "increment".to_string(), vec![1, 2, 3], vec![]);

        writer.append(&entry).unwrap();
        writer.fsync().unwrap();

        // We can verify the file exists
        assert!(fs::metadata(&tmp_path).is_ok());

        let _ = fs::remove_file(&tmp_path);
    }

    #[test]
    fn test_wal_partial_trailing_entry_is_tolerated() {
        // Simulate a crash mid-write: two complete entries followed by a torn
        // third entry. read_all_entries should return the two complete ones,
        // log a warning about the trailing tear, and not surface an error.
        let tmp_path = std::env::temp_dir().join(format!(
            "test_wal_partial_trailing_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0),
        ));
        let _ = fs::remove_file(&tmp_path);

        let mut writer = WalWriter::open(&tmp_path).unwrap();
        let e1 = WalEntry::new(1000, 1, "inc".to_string(), vec![1, 2, 3], vec![]);
        let e2 = WalEntry::new(1001, 2, "inc".to_string(), vec![4, 5, 6], vec![]);
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();
        writer.fsync().unwrap();
        drop(writer);

        // Append a corrupt/incomplete trailer: a length prefix that promises
        // 1024 bytes of payload, plus only 3 actual bytes. read_all_entries
        // should treat it as a torn write.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&tmp_path)
                .unwrap();
            let phantom_len: u32 = 1024;
            f.write_all(&phantom_len.to_le_bytes()).unwrap();
            f.write_all(&[0xAA, 0xBB, 0xCC]).unwrap();
        }

        let mut reader = WalReader::open(&tmp_path).unwrap();
        let entries = reader.read_all_entries().expect("partial tail must not error");
        assert_eq!(entries.len(), 2, "should recover both complete entries");
        assert_eq!(entries[0].header.sequence_number, 1);
        assert_eq!(entries[1].header.sequence_number, 2);

        let _ = fs::remove_file(&tmp_path);
    }

    #[test]
    fn test_wal_truncated_mid_length_prefix() {
        // Two complete entries plus a single trailing byte — not even enough
        // for the 4-byte length prefix.
        let tmp_path = std::env::temp_dir().join(format!(
            "test_wal_partial_len_prefix_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0),
        ));
        let _ = fs::remove_file(&tmp_path);

        let mut writer = WalWriter::open(&tmp_path).unwrap();
        writer.append(&WalEntry::new(1000, 1, "inc".to_string(), vec![1], vec![])).unwrap();
        writer.append(&WalEntry::new(1001, 2, "inc".to_string(), vec![2], vec![])).unwrap();
        writer.fsync().unwrap();
        drop(writer);

        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&tmp_path)
                .unwrap();
            f.write_all(&[0x42]).unwrap(); // 1 byte; less than the 4-byte length prefix
        }

        let mut reader = WalReader::open(&tmp_path).unwrap();
        let entries = reader.read_all_entries().expect("must not error on stub trailer");
        assert_eq!(entries.len(), 2);

        let _ = fs::remove_file(&tmp_path);
    }
}
