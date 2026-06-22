use crate::error::Result;
use crate::wal::entry::WalEntry;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

/// Writes WAL entries to disk
pub struct WalWriter {
    file: BufWriter<File>,
    entry_count: u64,
}

impl WalWriter {
    /// Open or create a WAL file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path_ref)?;

        Ok(WalWriter {
            file: BufWriter::new(file),
            entry_count: 0,
        })
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let encoded = rmp_serde::to_vec(entry)?;

        // Write length prefix (4 bytes, little-endian)
        let len = encoded.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;

        // Write the encoded data
        self.file.write_all(&encoded)?;
        self.entry_count += 1;
        Ok(())
    }

    /// Flush and fsync the WAL to disk
    pub fn fsync(&mut self) -> Result<()> {
        self.file.flush()?;
        let _ = self.file.get_mut().sync_all();
        Ok(())
    }

    /// Get the number of entries written
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_writer_append() {
        let tmp_path = std::env::temp_dir().join("test_wal.bin");
        let _ = std::fs::remove_file(&tmp_path); // Clean up if exists

        let mut writer = WalWriter::open(&tmp_path).unwrap();
        let entry = WalEntry::new(1000, 1, "increment".to_string(), vec![1, 2, 3], vec![]);

        writer.append(&entry).unwrap();
        writer.fsync().unwrap();

        assert_eq!(writer.entry_count(), 1);

        // Verify file exists
        assert!(std::fs::metadata(&tmp_path).is_ok());

        let _ = std::fs::remove_file(&tmp_path);
    }
}
