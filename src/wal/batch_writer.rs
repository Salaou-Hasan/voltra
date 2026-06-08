use crate::error::{NeonDBError, Result};
use crate::wal::entry::WalEntry;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

/// Command sent to the background flusher thread
enum FlushCommand {
    Append(Vec<u8>, u64),
    /// Rotate the WAL: flush pending writes, rename current file to `.old`,
    /// and open a fresh WAL file. The u64 is the snapshot sequence number
    /// (informational — logged for diagnostics).
    Truncate(u64),
    Shutdown,
}

/// Batched WAL writer that accumulates entries and flushes them in batches.
pub struct BatchedWalWriter {
    sender: mpsc::SyncSender<FlushCommand>,
    flusher_thread: Option<thread::JoinHandle<()>>,
    /// Current WAL file size in bytes, updated by the flusher thread after each flush.
    file_size: Arc<AtomicU64>,
}

impl BatchedWalWriter {
    pub fn open<P: AsRef<Path>>(
        path: P,
        flush_interval_ms: u32,
        batch_size: usize,
        unsafe_no_fsync: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Seed the file_size with the existing file's size (if any).
        let initial_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let file_size = Arc::new(AtomicU64::new(initial_size));
        let file_size_writer = file_size.clone();

        let (sender, receiver) = mpsc::sync_channel(batch_size * 2);
        let flusher_thread = thread::spawn(move || {
            background_flusher(path, receiver, flush_interval_ms, batch_size, unsafe_no_fsync, file_size_writer);
        });

        Ok(BatchedWalWriter {
            sender,
            flusher_thread: Some(flusher_thread),
            file_size,
        })
    }

    pub fn append(&self, entry: &WalEntry, sequence_number: u64) -> Result<()> {
        let encoded = rmp_serde::to_vec(entry)?;
        self.sender
            .try_send(FlushCommand::Append(encoded, sequence_number))
            .map_err(|e| NeonDBError::WalError(format!("WAL channel error: {}", e)))
    }

    /// Rotate the WAL file after a snapshot has been confirmed at `sequence`.
    ///
    /// The flusher thread will:
    /// 1. Flush any pending writes to the current file.
    /// 2. Close the current WAL file.
    /// 3. Rename it to `neondb.wal.old` (overwriting any previous `.old`).
    /// 4. Open a fresh `neondb.wal` and continue writing there.
    ///
    /// This is NOT in-place truncation — it is a rotate-and-start-fresh approach.
    /// The `.old` file can be deleted by an operator or a separate cleanup pass.
    pub fn truncate_before(&self, sequence: u64) -> Result<()> {
        self.sender
            .try_send(FlushCommand::Truncate(sequence))
            .map_err(|e| NeonDBError::WalError(format!("WAL truncate channel error: {}", e)))
    }

    /// Return the current WAL file size in bytes.
    pub fn wal_file_size_bytes(&self) -> u64 {
        self.file_size.load(Ordering::Relaxed)
    }

    pub fn shutdown(mut self) -> Result<()> {
        let _ = self.sender.send(FlushCommand::Shutdown);
        if let Some(thread) = self.flusher_thread.take() {
            thread.join().ok();
        }
        Ok(())
    }
}

impl Drop for BatchedWalWriter {
    fn drop(&mut self) {
        let _ = self.sender.send(FlushCommand::Shutdown);
        if let Some(thread) = self.flusher_thread.take() {
            thread.join().ok();
        }
    }
}

fn background_flusher(
    path: PathBuf,
    receiver: mpsc::Receiver<FlushCommand>,
    flush_interval_ms: u32,
    _batch_size: usize,
    unsafe_no_fsync: bool,
    file_size: Arc<AtomicU64>,
) {
    let mut file = match open_wal_file(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to open WAL file for batch writer: {}", e);
            return;
        }
    };

    let mut buffer = Vec::with_capacity(1024 * 1024);
    let flush_interval = Duration::from_millis(flush_interval_ms as u64);
    let mut last_flush = std::time::Instant::now();
    let mut entry_count = 0u64;
    let mut current_file_size: u64 = file_size.load(Ordering::Relaxed);

    loop {
        match receiver.recv_timeout(flush_interval) {
            Ok(FlushCommand::Append(encoded, _seq_num)) => {
                let len = encoded.len() as u32;
                buffer.extend_from_slice(&len.to_le_bytes());
                buffer.extend_from_slice(&encoded);
                entry_count += 1;

                if buffer.len() > 512 * 1024 {
                    let bytes_written = buffer.len() as u64;
                    if let Err(e) = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync) {
                        log::error!("WAL flush error: {}", e);
                    } else {
                        current_file_size += bytes_written;
                        file_size.store(current_file_size, Ordering::Relaxed);
                    }
                    last_flush = std::time::Instant::now();
                }
            }
            Ok(FlushCommand::Truncate(sequence)) => {
                // Flush any pending data first
                if !buffer.is_empty() {
                    if let Err(e) = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync) {
                        log::error!("WAL flush error during truncate: {}", e);
                    }
                }
                // Close current file (drop it)
                drop(file);

                // Rename current WAL to .old
                let old_path = path.with_extension("wal.old");
                if let Err(e) = std::fs::rename(&path, &old_path) {
                    log::warn!("WAL rotate: rename to .old failed: {} (continuing with fresh file)", e);
                } else {
                    log::info!("WAL rotated at snapshot seq={}: {} -> {}", sequence, path.display(), old_path.display());
                }

                // Open a fresh WAL file
                file = match open_wal_file(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("WAL rotate: failed to open new file after rotation: {}", e);
                        // Attempt to recover: try re-opening the old file
                        if old_path.exists() {
                            let _ = std::fs::rename(&old_path, &path);
                        }
                        match open_wal_file(&path) {
                            Ok(f) => f,
                            Err(e2) => {
                                log::error!("WAL rotate: FATAL — cannot re-open WAL: {}", e2);
                                return;
                            }
                        }
                    }
                };

                // Reset file size tracking
                current_file_size = 0;
                file_size.store(0, Ordering::Relaxed);
            }
            Ok(FlushCommand::Shutdown) => {
                if !buffer.is_empty() {
                    let _ = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync);
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() && last_flush.elapsed() >= flush_interval {
                    let bytes_written = buffer.len() as u64;
                    if let Err(e) = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync) {
                        log::error!("WAL flush error: {}", e);
                    } else {
                        current_file_size += bytes_written;
                        file_size.store(current_file_size, Ordering::Relaxed);
                    }
                    last_flush = std::time::Instant::now();
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if !buffer.is_empty() {
                    let _ = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync);
                }
                break;
            }
        }
    }

    log::info!("WAL batch writer flushed {} entries to disk", entry_count);
}

fn flush_to_disk(file: &mut File, buffer: &mut Vec<u8>, unsafe_no_fsync: bool) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    file.write_all(buffer)?;
    file.flush()?;
    if !unsafe_no_fsync {
        file.sync_all()?;
    }
    buffer.clear();
    Ok(())
}

fn open_wal_file(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true).custom_flags(libc::O_DIRECT);
        if let Ok(f) = options.open(path) {
            return Ok(f);
        }
        log::warn!("O_DIRECT open failed, falling back to normal WAL file access");
    }

    OpenOptions::new().create(true).append(true).read(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batched_wal_writer() {
        let tmp_path = std::env::temp_dir().join("test_batched_wal.bin");
        let _ = std::fs::remove_file(&tmp_path);

        let writer = BatchedWalWriter::open(&tmp_path, 10, 1000, true).unwrap();

        for i in 0..100 {
            let entry = WalEntry::new(
                1000 + i,
                i as u64,
                "increment".to_string(),
                vec![1, 2, 3],
                vec![],
            );
            writer.append(&entry, i as u64).unwrap();
        }

        writer.shutdown().unwrap();

        let metadata = std::fs::metadata(&tmp_path).unwrap();
        assert!(metadata.len() > 0);

        let _ = std::fs::remove_file(&tmp_path);
    }

    #[test]
    fn test_truncate_rotates_wal_file() {
        let tmp_dir = std::env::temp_dir().join("neondb_test_truncate");
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).unwrap();

        let wal_path = tmp_dir.join("neondb.wal");
        let old_path = tmp_dir.join("neondb.wal.old");

        let writer = BatchedWalWriter::open(&wal_path, 5, 1000, true).unwrap();

        // Append 5 entries
        for i in 0..5 {
            let entry = WalEntry::new(
                1000 + i,
                i as u64,
                "test_reducer".to_string(),
                vec![1, 2, 3],
                vec![],
            );
            writer.append(&entry, i as u64).unwrap();
        }

        // Give the flusher time to write (flush_interval_ms=5)
        std::thread::sleep(Duration::from_millis(50));

        // Verify WAL file exists and has data
        assert!(wal_path.exists(), "WAL file should exist before truncate");
        let size_before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_before > 0, "WAL file should have data before truncate");

        // Trigger rotation
        writer.truncate_before(4).unwrap();

        // Give the flusher time to process the truncate command
        std::thread::sleep(Duration::from_millis(100));

        // Verify old file was created and new WAL is empty/fresh
        assert!(old_path.exists(), "Old WAL file should exist after rotation");
        assert!(wal_path.exists(), "Fresh WAL file should exist after rotation");

        let old_size = std::fs::metadata(&old_path).unwrap().len();
        assert!(old_size > 0, "Old WAL file should contain the original data");

        let new_size = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(new_size, 0, "Fresh WAL file should be empty");

        writer.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_wal_file_size_increases_on_append() {
        let tmp_dir = std::env::temp_dir().join("neondb_test_filesize");
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).unwrap();

        let wal_path = tmp_dir.join("neondb.wal");
        let writer = BatchedWalWriter::open(&wal_path, 5, 1000, true).unwrap();

        // Initial size should be 0
        assert_eq!(writer.wal_file_size_bytes(), 0, "Fresh WAL should report 0 bytes");

        // Append entries
        for i in 0..10 {
            let entry = WalEntry::new(
                2000 + i,
                i as u64,
                "size_test".to_string(),
                vec![10, 20, 30, 40, 50],
                vec![],
            );
            writer.append(&entry, i as u64).unwrap();
        }

        // Give the flusher time to flush (flush_interval_ms=5)
        std::thread::sleep(Duration::from_millis(50));

        let size_after = writer.wal_file_size_bytes();
        assert!(size_after > 0, "WAL file size should increase after appending entries (got {})", size_after);

        // Append more entries and verify size continues growing
        for i in 10..20 {
            let entry = WalEntry::new(
                3000 + i,
                i as u64,
                "size_test".to_string(),
                vec![10, 20, 30, 40, 50],
                vec![],
            );
            writer.append(&entry, i as u64).unwrap();
        }

        std::thread::sleep(Duration::from_millis(50));

        let size_final = writer.wal_file_size_bytes();
        assert!(size_final > size_after, "WAL file size should keep growing (got {} then {})", size_after, size_final);

        writer.shutdown().unwrap();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
