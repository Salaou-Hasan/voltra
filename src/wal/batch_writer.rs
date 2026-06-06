use crate::error::{NeonDBError, Result};
use crate::wal::entry::WalEntry;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Command sent to the background flusher thread
enum FlushCommand {
    Append(Vec<u8>, u64),
    Shutdown,
}

/// Batched WAL writer that accumulates entries and flushes them in batches.
pub struct BatchedWalWriter {
    sender: mpsc::SyncSender<FlushCommand>,
    flusher_thread: Option<thread::JoinHandle<()>>,
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

        let (sender, receiver) = mpsc::sync_channel(batch_size * 2);
        let flusher_thread = thread::spawn(move || {
            background_flusher(path, receiver, flush_interval_ms, batch_size, unsafe_no_fsync);
        });

        Ok(BatchedWalWriter {
            sender,
            flusher_thread: Some(flusher_thread),
        })
    }

    pub fn append(&self, entry: &WalEntry, sequence_number: u64) -> Result<()> {
        let encoded = rmp_serde::to_vec(entry)?;
        self.sender
            .try_send(FlushCommand::Append(encoded, sequence_number))
            .map_err(|e| NeonDBError::WalError(format!("WAL channel error: {}", e)))
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

    loop {
        match receiver.recv_timeout(flush_interval) {
            Ok(FlushCommand::Append(encoded, _seq_num)) => {
                let len = encoded.len() as u32;
                buffer.extend_from_slice(&len.to_le_bytes());
                buffer.extend_from_slice(&encoded);
                entry_count += 1;

                if buffer.len() > 512 * 1024 {
                    if let Err(e) = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync) {
                        log::error!("WAL flush error: {}", e);
                    }
                    last_flush = std::time::Instant::now();
                }
            }
            Ok(FlushCommand::Shutdown) => {
                if !buffer.is_empty() {
                    let _ = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync);
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() && last_flush.elapsed() >= flush_interval {
                    if let Err(e) = flush_to_disk(&mut file, &mut buffer, unsafe_no_fsync) {
                        log::error!("WAL flush error: {}", e);
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
}
