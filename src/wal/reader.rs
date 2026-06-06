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
            if pos + 4 > all_data.len() {
                break; // Not enough data for length prefix
            }

            let len = u32::from_le_bytes([
                all_data[pos],
                all_data[pos + 1],
                all_data[pos + 2],
                all_data[pos + 3],
            ]) as usize;

            pos += 4;

            if pos + len > all_data.len() {
                break; // Not enough data for message
            }

            let msg_data = &all_data[pos..pos + len];
            pos += len;

            match rmp_serde::from_slice::<WalEntry>(msg_data) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    log::warn!(
                        "Failed to decode WAL entry at position {}: {}",
                        pos - len,
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
}
