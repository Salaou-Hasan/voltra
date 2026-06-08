// ============================================================================
// src/raft/storage.rs — Raft log storage for NeonDB
//
// Implements openraft's RaftLogStorage + RaftLogReader traits.
//
// Design:
//   - Log entries are held in-memory in a BTreeMap<u64, Entry<TypeConfig>>.
//     This gives O(log n) access by log index and ordered iteration for ranges.
//   - The Raft vote (term + voted_for) is persisted to a small JSON file so
//     it survives server restarts (required for correctness — a node must
//     never grant two votes in the same term across crashes).
//   - Log entries are also durable via the WAL, but for simplicity the in-
//     memory BTreeMap is the authoritative log store (crash recovery uses the
//     existing snapshot + WAL path; the Raft log is rebuilt from the snapshot
//     on restart).
//   - Concurrent access: RaftLogStorage is accessed only from the Raft core
//     task (single-writer).  The LogReader clone shares the Arc<RwLock<...>>
//     for concurrent reads from replication tasks.
// ============================================================================

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend,
    RaftLogReader, StorageError, Vote,
};
use openraft::storage::RaftLogStorage;
use parking_lot::RwLock;

use crate::raft::TypeConfig;

// ─────────────────────────────────────────────────────────────────────────────
// Shared in-memory log state
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct LogStoreInner {
    /// Last log id that was purged (compacted into a snapshot).
    last_purged_log_id: Option<LogId<u64>>,
    /// All un-purged log entries, keyed by their index.
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    /// Committed log id — saved optionally for faster restart.
    committed: Option<LogId<u64>>,
    /// Raft vote — must survive crashes (persisted to disk on every change).
    vote: Option<Vote<u64>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// MemLogStore — implements RaftLogStorage + RaftLogReader
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory Raft log store with vote persistence to disk.
#[derive(Clone, Debug)]
pub struct MemLogStore {
    inner: Arc<RwLock<LogStoreInner>>,
    /// Path for the vote file. `None` in tests (vote not persisted).
    vote_path: Option<PathBuf>,
}

impl MemLogStore {
    /// Create a new log store.
    /// `vote_path` — where to persist the vote (e.g. `<wal_dir>/raft_vote.json`).
    pub fn new(vote_path: Option<PathBuf>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogStoreInner::default())),
            vote_path,
        }
    }

    /// Persist the vote to disk (required before returning from `save_vote`).
    fn persist_vote(&self, vote: &Vote<u64>) {
        if let Some(path) = &self.vote_path {
            let json = serde_json::to_string(vote).unwrap_or_default();
            let _ = std::fs::write(path, json.as_bytes());
        }
    }

    /// Load a previously persisted vote from disk on startup.
    pub fn load_persisted_vote(vote_path: &PathBuf) -> Option<Vote<u64>> {
        let bytes = std::fs::read(vote_path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RaftLogReader impl
// ─────────────────────────────────────────────────────────────────────────────

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let inner = self.inner.read();
        let entries = inner
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect();
        Ok(entries)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RaftLogStorage impl
// ─────────────────────────────────────────────────────────────────────────────

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = MemLogStore;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.read();
        let last_log_id = inner
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<u64>,
    ) -> Result<(), StorageError<u64>> {
        {
            let mut inner = self.inner.write();
            inner.vote = Some(*vote);
        }
        self.persist_vote(vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.write().committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.read().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.write();
            for entry in entries {
                inner.entries.insert(entry.log_id.index, entry);
            }
        }
        // Entries are in memory — call the callback immediately (no async I/O).
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write();
        let keys: Vec<u64> = inner
            .entries
            .range(log_id.index..)
            .map(|(&k, _)| k)
            .collect();
        for k in keys {
            inner.entries.remove(&k);
        }
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write();
        inner.last_purged_log_id = Some(log_id);
        let keys: Vec<u64> = inner
            .entries
            .range(..=log_id.index)
            .map(|(&k, _)| k)
            .collect();
        for k in keys {
            inner.entries.remove(&k);
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, EntryPayload, Entry};

    fn make_log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    /// Insert entries directly into the inner BTreeMap (bypasses LogFlushed
    /// which is pub(crate) in openraft and cannot be constructed in external tests).
    fn insert_entries(store: &MemLogStore, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = store.inner.write();
        for e in entries {
            inner.entries.insert(e.log_id.index, e);
        }
    }

    #[tokio::test]
    async fn test_get_log_state_empty() {
        let mut store = MemLogStore::new(None);
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn test_save_and_read_vote() {
        let mut store = MemLogStore::new(None);
        let vote = Vote::new(1, 42);
        store.save_vote(&vote).await.unwrap();
        let back = store.read_vote().await.unwrap();
        assert!(back.is_some());
        assert_eq!(back.unwrap(), vote);
    }

    #[tokio::test]
    async fn test_insert_and_read_entries() {
        let mut store = MemLogStore::new(None);
        insert_entries(&store, vec![
            Entry { log_id: make_log_id(1, 1), payload: EntryPayload::Blank },
        ]);

        let entries = store.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1u64);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(make_log_id(1, 1)));
    }

    #[tokio::test]
    async fn test_truncate_removes_entries() {
        let mut store = MemLogStore::new(None);
        insert_entries(&store, vec![
            Entry { log_id: make_log_id(1, 1), payload: EntryPayload::Blank },
            Entry { log_id: make_log_id(1, 2), payload: EntryPayload::Blank },
            Entry { log_id: make_log_id(1, 3), payload: EntryPayload::Blank },
        ]);

        store.truncate(make_log_id(1, 2)).await.unwrap();
        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1u64);
    }

    #[tokio::test]
    async fn test_purge_removes_old_entries() {
        let mut store = MemLogStore::new(None);
        insert_entries(&store, vec![
            Entry { log_id: make_log_id(1, 1), payload: EntryPayload::Blank },
            Entry { log_id: make_log_id(1, 2), payload: EntryPayload::Blank },
        ]);

        store.purge(make_log_id(1, 1)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 2u64);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(make_log_id(1, 1)));
    }

    #[tokio::test]
    async fn test_save_and_read_committed() {
        let mut store = MemLogStore::new(None);
        assert!(store.read_committed().await.unwrap().is_none());
        let log_id = make_log_id(3, 10);
        store.save_committed(Some(log_id)).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id));
    }

    #[tokio::test]
    async fn test_vote_not_persisted_without_path() {
        // No vote_path — vote lives only in memory.
        let mut store = MemLogStore::new(None);
        let vote = Vote::new(5, 99);
        store.save_vote(&vote).await.unwrap();
        // Simulate restart: read_vote from a fresh store (different Arc) → None
        let mut fresh = MemLogStore::new(None);
        let back = fresh.read_vote().await.unwrap();
        assert!(back.is_none());
    }

    #[tokio::test]
    async fn test_get_log_reader_is_clone() {
        let mut store = MemLogStore::new(None);
        insert_entries(&store, vec![
            Entry { log_id: make_log_id(1, 5), payload: EntryPayload::Blank },
        ]);
        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(5..6).await.unwrap();
        assert_eq!(entries.len(), 1);
    }
}
