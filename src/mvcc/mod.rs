//! MVCC engine — multi-version concurrency control store.
//!
//! Architecture (Redis-style single writer + PostgreSQL-style snapshot isolation):
//!
//! ```text
//!   readers (any thread) ──► DashMap<NsKey, Chain>  version chains, lock-free reads
//!                                  ▲
//!   writers (all)  ──► kanal ──► single sequencer OS thread ──► AOF (group commit)
//! ```
//!
//! * Every committed write creates a new `Version` with a monotonically increasing
//!   `commit_ts`. Readers pick the newest version with `commit_ts <= read_ts` —
//!   readers never block writers, writers never block readers.
//! * All mutations flow through ONE sequencer thread: zero lock contention,
//!   linearizable read-modify-write (Redis `INCR` semantics for free).
//! * Transactions pin a snapshot (`read_ts`) and commit with first-committer-wins
//!   conflict detection (PostgreSQL snapshot-isolation semantics).
//! * Durability: append-only file (AOF) of committed effects, group-committed,
//!   plus point-in-time snapshots (`SAVE`). Boot = load snapshot + replay AOF.

pub mod aof;

use bytes::Bytes;
use dashmap::DashMap;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

pub type TxnId = u64;

/// Redis logical databases 0-15 map to namespaces 0-15.
pub const NS_REDIS_BASE: u32 = 0;
/// PostgreSQL catalog (table definitions, sequences).
pub const NS_PG_CATALOG: u32 = 64;
/// PostgreSQL user tables are assigned namespaces starting here.
pub const NS_PG_BASE: u32 = 65;

// ─────────────────────────────────────────────────────────────────────────────
// Value universe
// ─────────────────────────────────────────────────────────────────────────────

/// SQL scalar value for PostgreSQL rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }
}

/// Sorted set: dual index — member→score plus score-ordered set for range queries.
/// Both sides are persistent (im) structures, so cloning a ZSet is O(1).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ZSet {
    pub by_member: im::HashMap<Bytes, f64>,
    pub by_score: im::OrdSet<(OrderedFloat<f64>, Bytes)>,
}

impl ZSet {
    pub fn len(&self) -> usize {
        self.by_member.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_member.is_empty()
    }
    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.by_member.get(member).copied()
    }
    /// Insert or update a member. Returns the previous score if present.
    pub fn insert(&mut self, member: Bytes, score: f64) -> Option<f64> {
        let old = self.by_member.insert(member.clone(), score);
        if let Some(o) = old {
            self.by_score.remove(&(OrderedFloat(o), member.clone()));
        }
        self.by_score.insert((OrderedFloat(score), member));
        old
    }
    /// Remove a member. Returns its score if it existed.
    pub fn remove(&mut self, member: &Bytes) -> Option<f64> {
        if let Some(score) = self.by_member.remove(member) {
            self.by_score.remove(&(OrderedFloat(score), member.clone()));
            Some(score)
        } else {
            None
        }
    }
}

/// The value universe shared by the Redis and PostgreSQL layers.
/// All container variants use persistent (im) collections: cloning any Datum
/// is O(1), which is what makes per-write version creation cheap.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Datum {
    /// Redis string (also used for bitmaps / integers stored as text).
    Str(Bytes),
    /// Redis hash.
    Hash(im::HashMap<Bytes, Bytes>),
    /// Redis list.
    List(im::Vector<Bytes>),
    /// Redis set.
    Set(im::HashSet<Bytes>),
    /// Redis sorted set.
    ZSet(ZSet),
    /// PostgreSQL row: column name → scalar.
    Row(im::HashMap<String, Scalar>),
}

impl Datum {
    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Str(_) => "string",
            Datum::Hash(_) => "hash",
            Datum::List(_) => "list",
            Datum::Set(_) => "set",
            Datum::ZSet(_) => "zset",
            Datum::Row(_) => "row",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Keys, versions, chains
// ─────────────────────────────────────────────────────────────────────────────

/// Namespaced key: (namespace, raw key bytes).
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct NsKey {
    pub ns: u32,
    pub key: Bytes,
}

impl NsKey {
    pub fn new(ns: u32, key: impl Into<Bytes>) -> Self {
        Self { ns, key: key.into() }
    }
}

/// One committed version of a key. `value: None` is a tombstone (deletion).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
    pub commit_ts: TxnId,
    pub value: Option<Datum>,
    /// Wall-clock expiry in epoch milliseconds (Redis TTL).
    pub expires_at_ms: Option<u64>,
}

/// Version chain, newest first. Most keys have 1-2 live versions, so the
/// SmallVec keeps them inline without a heap allocation.
#[derive(Default, Debug)]
pub struct Chain {
    pub versions: SmallVec<[Version; 2]>,
}

impl Chain {
    /// Newest version visible at `ts` (commit_ts <= ts).
    pub fn visible_at(&self, ts: TxnId) -> Option<&Version> {
        self.versions.iter().find(|v| v.commit_ts <= ts)
    }
    pub fn head_ts(&self) -> TxnId {
        self.versions.first().map(|v| v.commit_ts).unwrap_or(0)
    }
    /// Drop versions that no active snapshot can see: once a version with
    /// commit_ts <= min_active exists, everything older is dead.
    pub fn prune(&mut self, min_active: TxnId) {
        if let Some(idx) = self.versions.iter().position(|v| v.commit_ts <= min_active) {
            self.versions.truncate(idx + 1);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Write effects (the unit of AOF persistence and replication)
// ─────────────────────────────────────────────────────────────────────────────

/// A deterministic write effect. Commands are resolved to effects BEFORE
/// logging, so AOF replay never re-runs nondeterministic logic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteOp {
    Put {
        ns: u32,
        key: Bytes,
        value: Datum,
        expires_at_ms: Option<u64>,
    },
    Del {
        ns: u32,
        key: Bytes,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitError {
    /// First-committer-wins: another transaction committed a conflicting key
    /// after this transaction's snapshot was taken.
    Conflict,
    /// The store is shutting down.
    Closed,
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Conflict => write!(f, "serialization failure: concurrent update"),
            CommitError::Closed => write!(f, "store closed"),
        }
    }
}

/// Transactional commit request (PostgreSQL path, Redis WATCH/EXEC path).
pub struct CommitRequest {
    /// Snapshot the transaction read at.
    pub read_ts: TxnId,
    /// Effects to apply atomically.
    pub writes: Vec<WriteOp>,
    /// Keys that must not have been committed past `read_ts`
    /// (written keys for SI, plus WATCHed keys for Redis).
    pub conflict_keys: Vec<NsKey>,
    pub resp: oneshot::Sender<Result<TxnId, CommitError>>,
}

/// Callback run after the batch reaches the durability point.
pub type AfterCommit = Box<dyn FnOnce() + Send>;

/// A unit of work for the sequencer thread.
pub enum Batch {
    /// Run a closure with linearizable read-modify-write access (Redis writes).
    Apply(Box<dyn FnOnce(&mut Writer) -> AfterCommit + Send>),
    /// Transactional commit with conflict detection.
    Commit(CommitRequest),
    /// Write a point-in-time snapshot and truncate the AOF.
    Save(oneshot::Sender<std::io::Result<()>>),
    /// Barrier: resolves after everything queued before it is applied + durable.
    Barrier(oneshot::Sender<()>),
}

// ─────────────────────────────────────────────────────────────────────────────
// Writer — the sequencer-side mutation handle
// ─────────────────────────────────────────────────────────────────────────────

/// Handle given to `Batch::Apply` closures. Reads see latest committed state
/// plus this batch's own staged writes (read-your-writes inside MULTI/EXEC).
pub struct Writer<'a> {
    inner: &'a Inner,
    staged: Vec<WriteOp>,
    overlay: HashMap<NsKey, Option<(Datum, Option<u64>)>>,
    now_ms: u64,
}

impl<'a> Writer<'a> {
    fn new(inner: &'a Inner) -> Self {
        Self {
            inner,
            staged: Vec::new(),
            overlay: HashMap::new(),
            now_ms: now_ms(),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Latest committed value (linearizable read), with lazy-expiry semantics.
    /// Expired keys read as None and are auto-staged for deletion.
    pub fn get(&mut self, ns: u32, key: &Bytes) -> Option<Datum> {
        let nk = NsKey { ns, key: key.clone() };
        if let Some(staged) = self.overlay.get(&nk) {
            return staged.as_ref().and_then(|(d, exp)| {
                if exp.map(|e| e <= self.now_ms).unwrap_or(false) {
                    None
                } else {
                    Some(d.clone())
                }
            });
        }
        match self.inner.chains.get(&nk) {
            Some(chain) => match chain.versions.first() {
                Some(v) => {
                    v.value.as_ref()?;
                    if v.expires_at_ms.map(|e| e <= self.now_ms).unwrap_or(false) {
                        drop(chain);
                        // Lazy expiry: reap on touch, exactly like Redis.
                        self.del(ns, key.clone());
                        return None;
                    }
                    v.value.clone()
                }
                None => None,
            },
            None => None,
        }
    }

    /// Commit timestamp of the newest committed version of a key
    /// (0 = never written). Used for WATCH conflict detection.
    pub fn head_ts(&self, ns: u32, key: &Bytes) -> TxnId {
        let nk = NsKey { ns, key: key.clone() };
        self.inner.chains.get(&nk).map(|c| c.head_ts()).unwrap_or(0)
    }

    /// Approximate live-key count of a namespace (ignores this batch's staged writes).
    pub fn dbsize(&self, ns: u32) -> u64 {
        self.inner
            .counts
            .get(&ns)
            .map(|c| c.load(Ordering::Relaxed).max(0) as u64)
            .unwrap_or(0)
    }

    /// Current TTL of a key in epoch ms (None = no expiry set).
    pub fn get_expiry(&self, ns: u32, key: &Bytes) -> Option<u64> {
        let nk = NsKey { ns, key: key.clone() };
        if let Some(staged) = self.overlay.get(&nk) {
            return staged.as_ref().and_then(|(_, e)| *e);
        }
        self.inner
            .chains
            .get(&nk)
            .and_then(|c| c.versions.first().and_then(|v| v.expires_at_ms))
    }

    pub fn exists(&mut self, ns: u32, key: &Bytes) -> bool {
        self.get(ns, key).is_some()
    }

    /// Stage a write. Becomes visible to subsequent `get`s in this batch.
    pub fn put(&mut self, ns: u32, key: Bytes, value: Datum, expires_at_ms: Option<u64>) {
        let nk = NsKey { ns, key: key.clone() };
        self.overlay.insert(nk, Some((value.clone(), expires_at_ms)));
        self.staged.push(WriteOp::Put { ns, key, value, expires_at_ms });
    }

    /// Stage a deletion. Returns true if the key existed (visible) beforehand.
    pub fn del(&mut self, ns: u32, key: Bytes) -> bool {
        let existed = {
            let nk = NsKey { ns, key: key.clone() };
            match self.overlay.get(&nk) {
                Some(v) => v.is_some(),
                None => self
                    .inner
                    .chains
                    .get(&nk)
                    .and_then(|c| c.versions.first().map(|v| {
                        v.value.is_some()
                            && !v.expires_at_ms.map(|e| e <= self.now_ms).unwrap_or(false)
                    }))
                    .unwrap_or(false),
            }
        };
        let nk = NsKey { ns, key: key.clone() };
        self.overlay.insert(nk, None);
        self.staged.push(WriteOp::Del { ns, key });
        existed
    }

    /// Visible keys in a namespace (latest committed + staged overlay).
    /// Used by FLUSHDB / KEYS executed inside the sequencer.
    pub fn live_keys(&self, ns: u32) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = Vec::new();
        for entry in self.inner.chains.iter() {
            if entry.key().ns != ns {
                continue;
            }
            if self.overlay.contains_key(entry.key()) {
                continue; // handled below from overlay
            }
            if let Some(v) = entry.value().versions.first() {
                if v.value.is_some()
                    && !v.expires_at_ms.map(|e| e <= self.now_ms).unwrap_or(false)
                {
                    out.push(entry.key().key.clone());
                }
            }
        }
        for (nk, staged) in &self.overlay {
            if nk.ns == ns && staged.is_some() {
                out.push(nk.key.clone());
            }
        }
        out
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Store
// ─────────────────────────────────────────────────────────────────────────────

pub struct Inner {
    chains: DashMap<NsKey, Chain>,
    /// Last committed transaction id. Readers snapshot this.
    last_commit: AtomicU64,
    /// Live (non-tombstone) key count per namespace, for DBSIZE.
    counts: DashMap<u32, AtomicI64>,
    /// keys with a TTL → expiry epoch ms (active-expiry index).
    ttl_index: DashMap<NsKey, u64>,
    /// Pinned snapshot read timestamps (refcounted) — GC floor.
    active_snapshots: Mutex<BTreeMap<TxnId, usize>>,
    shutdown: AtomicBool,
}

impl Inner {
    fn min_active_snapshot(&self) -> TxnId {
        let snaps = self.active_snapshots.lock();
        snaps
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.last_commit.load(Ordering::Acquire))
    }

    fn bump_count(&self, ns: u32, delta: i64) {
        self.counts
            .entry(ns)
            .or_insert_with(|| AtomicI64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }
}

/// RAII pin on a snapshot timestamp. While alive, GC will not remove versions
/// this snapshot can see.
pub struct SnapshotGuard {
    inner: Arc<Inner>,
    pub ts: TxnId,
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let mut snaps = self.inner.active_snapshots.lock();
        if let Some(cnt) = snaps.get_mut(&self.ts) {
            *cnt -= 1;
            if *cnt == 0 {
                snaps.remove(&self.ts);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every group commit (safest, slowest).
    Always,
    /// fsync at most once per second (Redis default).
    EverySec,
    /// never fsync explicitly — leave it to the OS.
    No,
}

#[derive(Clone, Debug)]
pub struct MvccConfig {
    /// Directory for `mvcc.aof` / `mvcc.snap`. None = pure in-memory.
    pub data_dir: Option<PathBuf>,
    pub fsync: FsyncPolicy,
}

impl Default for MvccConfig {
    fn default() -> Self {
        Self { data_dir: None, fsync: FsyncPolicy::EverySec }
    }
}

/// The MVCC store handle. Cheap to clone; all clones share state.
#[derive(Clone)]
pub struct MvccStore {
    inner: Arc<Inner>,
    tx: kanal::AsyncSender<Batch>,
}

impl MvccStore {
    /// Open a store. Loads snapshot + replays AOF when `data_dir` is set,
    /// then spawns the sequencer and GC threads.
    pub fn open(cfg: MvccConfig) -> std::io::Result<Self> {
        let inner = Arc::new(Inner {
            chains: DashMap::new(),
            last_commit: AtomicU64::new(0),
            counts: DashMap::new(),
            ttl_index: DashMap::new(),
            active_snapshots: Mutex::new(BTreeMap::new()),
            shutdown: AtomicBool::new(false),
        });

        // ── Recovery: snapshot, then AOF replay ────────────────────────────
        let mut aof_writer = None;
        if let Some(dir) = &cfg.data_dir {
            std::fs::create_dir_all(dir)?;
            let snap_path = dir.join("mvcc.snap");
            let aof_path = dir.join("mvcc.aof");

            if let Some((snap_ts, entries)) = aof::load_snapshot(&snap_path)? {
                for e in entries {
                    let nk = NsKey { ns: e.ns, key: e.key };
                    inner.bump_count(nk.ns, 1);
                    if let Some(exp) = e.expires_at_ms {
                        inner.ttl_index.insert(nk.clone(), exp);
                    }
                    inner.chains.insert(
                        nk,
                        Chain {
                            versions: smallvec::smallvec![Version {
                                commit_ts: snap_ts,
                                value: Some(e.value),
                                expires_at_ms: e.expires_at_ms,
                            }],
                        },
                    );
                }
                inner.last_commit.store(snap_ts, Ordering::Release);
            }

            let mut max_ts = inner.last_commit.load(Ordering::Acquire);
            let base_ts = max_ts;
            aof::replay(&aof_path, |rec| {
                if rec.ts <= base_ts {
                    return; // already covered by the snapshot
                }
                apply_ops(&inner, &rec.ops, rec.ts, rec.ts);
                if rec.ts > max_ts {
                    max_ts = rec.ts;
                }
            })?;
            inner.last_commit.store(max_ts, Ordering::Release);

            aof_writer = Some(aof::AofWriter::open(&aof_path, cfg.fsync)?);
        }

        // ── Sequencer thread ────────────────────────────────────────────────
        let (tx, rx) = kanal::bounded::<Batch>(65_536);
        {
            let inner = inner.clone();
            let data_dir = cfg.data_dir.clone();
            let fsync = cfg.fsync;
            std::thread::Builder::new()
                .name("mvcc-sequencer".into())
                .spawn(move || sequencer_loop(inner, rx, aof_writer, data_dir, fsync))
                .expect("spawn mvcc sequencer");
        }

        // ── GC / active-expiry thread ───────────────────────────────────────
        {
            let inner = inner.clone();
            let gc_tx = tx.clone();
            std::thread::Builder::new()
                .name("mvcc-gc".into())
                .spawn(move || gc_loop(inner, gc_tx))
                .expect("spawn mvcc gc");
        }

        Ok(Self { inner, tx: tx.to_async() })
    }

    /// In-memory store for tests.
    pub fn open_memory() -> Self {
        Self::open(MvccConfig::default()).expect("in-memory store cannot fail")
    }

    /// Signal background threads to exit (tests / graceful shutdown).
    pub fn close(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
    }

    // ── Read path (lock-free, any thread) ───────────────────────────────────

    pub fn current_ts(&self) -> TxnId {
        self.inner.last_commit.load(Ordering::Acquire)
    }

    /// Pin a snapshot for a transaction. Reads via `get_at(.., guard.ts)`.
    pub fn pin_snapshot(&self) -> SnapshotGuard {
        let ts = self.current_ts();
        *self.inner.active_snapshots.lock().entry(ts).or_insert(0) += 1;
        SnapshotGuard { inner: self.inner.clone(), ts }
    }

    /// Read a key as of snapshot `ts`. Expired keys read as None.
    pub fn get_at(&self, ns: u32, key: &Bytes, ts: TxnId) -> Option<Datum> {
        let nk = NsKey { ns, key: key.clone() };
        let chain = self.inner.chains.get(&nk)?;
        let v = chain.visible_at(ts)?;
        if v.expires_at_ms.map(|e| e <= now_ms()).unwrap_or(false) {
            return None;
        }
        v.value.clone()
    }

    /// Read at the latest committed snapshot.
    pub fn get(&self, ns: u32, key: &Bytes) -> Option<Datum> {
        self.get_at(ns, key, self.current_ts())
    }

    /// Expiry (epoch ms) of a live key at the latest snapshot.
    pub fn get_expiry(&self, ns: u32, key: &Bytes) -> Option<u64> {
        let nk = NsKey { ns, key: key.clone() };
        let chain = self.inner.chains.get(&nk)?;
        let v = chain.visible_at(self.current_ts())?;
        v.value.as_ref()?;
        v.expires_at_ms
    }

    /// Approximate live-key count for a namespace (DBSIZE).
    pub fn ns_len(&self, ns: u32) -> u64 {
        self.inner
            .counts
            .get(&ns)
            .map(|c| c.load(Ordering::Relaxed).max(0) as u64)
            .unwrap_or(0)
    }

    /// Number of keys with a TTL in a namespace (INFO keyspace `expires=`).
    pub fn ttl_count(&self, ns: u32) -> u64 {
        self.inner.ttl_index.iter().filter(|e| e.key().ns == ns).count() as u64
    }

    /// Visit every visible key/value in a namespace as of `ts`.
    pub fn for_each_visible(&self, ns: u32, ts: TxnId, mut f: impl FnMut(&Bytes, &Datum)) {
        let now = now_ms();
        for entry in self.inner.chains.iter() {
            if entry.key().ns != ns {
                continue;
            }
            if let Some(v) = entry.value().visible_at(ts) {
                if let Some(d) = &v.value {
                    if !v.expires_at_ms.map(|e| e <= now).unwrap_or(false) {
                        f(&entry.key().key, d);
                    }
                }
            }
        }
    }

    /// All visible keys of a namespace, sorted (stable SCAN cursors).
    pub fn visible_keys_sorted(&self, ns: u32, ts: TxnId) -> Vec<Bytes> {
        let mut keys = Vec::new();
        self.for_each_visible(ns, ts, |k, _| keys.push(k.clone()));
        keys.sort();
        keys
    }

    // ── Write path (funnels into the sequencer) ─────────────────────────────

    /// Run a closure on the sequencer with linearizable RMW access.
    /// The closure's `AfterCommit` runs after the durability point.
    pub async fn apply<F>(&self, f: F) -> Result<(), CommitError>
    where
        F: FnOnce(&mut Writer) -> AfterCommit + Send + 'static,
    {
        self.tx
            .send(Batch::Apply(Box::new(f)))
            .await
            .map_err(|_| CommitError::Closed)
    }

    /// Transactional commit with first-committer-wins conflict detection.
    pub async fn commit(
        &self,
        read_ts: TxnId,
        writes: Vec<WriteOp>,
        conflict_keys: Vec<NsKey>,
    ) -> Result<TxnId, CommitError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Batch::Commit(CommitRequest { read_ts, writes, conflict_keys, resp: resp_tx }))
            .await
            .map_err(|_| CommitError::Closed)?;
        resp_rx.await.map_err(|_| CommitError::Closed)?
    }

    /// Point-in-time snapshot + AOF truncation (Redis SAVE).
    pub async fn save(&self) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Batch::Save(tx))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "store closed"))?;
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "store closed"))?
    }

    /// Wait until everything queued before this call is applied and durable.
    pub async fn barrier(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Batch::Barrier(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Sequencer
// ─────────────────────────────────────────────────────────────────────────────

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Apply effects at `ts`: push versions, maintain counts + TTL index, prune.
fn apply_ops(inner: &Inner, ops: &[WriteOp], ts: TxnId, min_active: TxnId) {
    for op in ops {
        match op {
            WriteOp::Put { ns, key, value, expires_at_ms } => {
                let nk = NsKey { ns: *ns, key: key.clone() };
                let mut chain = inner.chains.entry(nk.clone()).or_default();
                let was_live = chain.versions.first().map(|v| v.value.is_some()).unwrap_or(false);
                chain.versions.insert(
                    0,
                    Version { commit_ts: ts, value: Some(value.clone()), expires_at_ms: *expires_at_ms },
                );
                chain.prune(min_active);
                drop(chain);
                if !was_live {
                    inner.bump_count(*ns, 1);
                }
                match expires_at_ms {
                    Some(e) => {
                        inner.ttl_index.insert(nk, *e);
                    }
                    None => {
                        inner.ttl_index.remove(&nk);
                    }
                }
            }
            WriteOp::Del { ns, key } => {
                let nk = NsKey { ns: *ns, key: key.clone() };
                let mut chain = inner.chains.entry(nk.clone()).or_default();
                let was_live = chain.versions.first().map(|v| v.value.is_some()).unwrap_or(false);
                chain.versions.insert(0, Version { commit_ts: ts, value: None, expires_at_ms: None });
                chain.prune(min_active);
                drop(chain);
                if was_live {
                    inner.bump_count(*ns, -1);
                }
                inner.ttl_index.remove(&nk);
            }
        }
    }
}

fn sequencer_loop(
    inner: Arc<Inner>,
    rx: kanal::Receiver<Batch>,
    mut aof: Option<aof::AofWriter>,
    data_dir: Option<PathBuf>,
    fsync: FsyncPolicy,
) {
    let mut after_commit: Vec<AfterCommit> = Vec::new();
    loop {
        let first = match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(b) => b,
            Err(kanal::ReceiveErrorTimeout::Timeout) => {
                if inner.shutdown.load(Ordering::Acquire) {
                    break;
                }
                if let Some(a) = aof.as_mut() {
                    a.maybe_sync();
                }
                continue;
            }
            Err(_) => break, // channel closed
        };

        let mut batch: Vec<Batch> = Vec::with_capacity(64);
        batch.push(first);
        while batch.len() < 512 {
            match rx.try_recv() {
                Ok(Some(b)) => batch.push(b),
                _ => break,
            }
        }

        let min_active = inner.min_active_snapshot();
        let mut records: Vec<aof::AofRecord> = Vec::new();
        let mut save_req: Option<oneshot::Sender<std::io::Result<()>>> = None;

        for b in batch {
            match b {
                Batch::Apply(f) => {
                    let mut w = Writer::new(&inner);
                    let after = f(&mut w);
                    let ops = w.staged;
                    if !ops.is_empty() {
                        let ts = inner.last_commit.load(Ordering::Acquire) + 1;
                        apply_ops(&inner, &ops, ts, min_active);
                        inner.last_commit.store(ts, Ordering::Release);
                        records.push(aof::AofRecord { ts, ops });
                    }
                    after_commit.push(after);
                }
                Batch::Commit(req) => {
                    let conflict = req.conflict_keys.iter().any(|nk| {
                        inner.chains.get(nk).map(|c| c.head_ts()).unwrap_or(0) > req.read_ts
                    });
                    if conflict {
                        let _ = req.resp.send(Err(CommitError::Conflict));
                        continue;
                    }
                    let ts = inner.last_commit.load(Ordering::Acquire) + 1;
                    if !req.writes.is_empty() {
                        apply_ops(&inner, &req.writes, ts, min_active);
                        inner.last_commit.store(ts, Ordering::Release);
                        records.push(aof::AofRecord { ts, ops: req.writes });
                    }
                    let resp = req.resp;
                    after_commit.push(Box::new(move || {
                        let _ = resp.send(Ok(ts));
                    }));
                }
                Batch::Save(tx) => save_req = Some(tx),
                Batch::Barrier(tx) => {
                    after_commit.push(Box::new(move || {
                        let _ = tx.send(());
                    }));
                }
            }
        }

        // Group commit: one write + one (policy-driven) fsync for the batch.
        if let Some(a) = aof.as_mut() {
            if !records.is_empty() {
                a.append_records(&records);
            }
            if fsync == FsyncPolicy::Always && !records.is_empty() {
                a.sync();
            } else {
                a.maybe_sync();
            }
        }

        for f in after_commit.drain(..) {
            f();
        }

        // SAVE: write snapshot of live heads, then truncate the AOF.
        if let Some(tx) = save_req.take() {
            let result = do_save(&inner, &data_dir, &mut aof, fsync);
            let _ = tx.send(result);
        }

        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
    }
    if let Some(a) = aof.as_mut() {
        a.sync();
    }
}

fn do_save(
    inner: &Inner,
    data_dir: &Option<PathBuf>,
    aof: &mut Option<aof::AofWriter>,
    fsync: FsyncPolicy,
) -> std::io::Result<()> {
    let Some(dir) = data_dir else {
        return Ok(()); // in-memory store: SAVE is a no-op
    };
    let snap_ts = inner.last_commit.load(Ordering::Acquire);
    let now = now_ms();
    let mut entries = Vec::new();
    for entry in inner.chains.iter() {
        if let Some(v) = entry.value().versions.first() {
            if let Some(d) = &v.value {
                if !v.expires_at_ms.map(|e| e <= now).unwrap_or(false) {
                    entries.push(aof::SnapEntry {
                        ns: entry.key().ns,
                        key: entry.key().key.clone(),
                        value: d.clone(),
                        expires_at_ms: v.expires_at_ms,
                    });
                }
            }
        }
    }
    aof::save_snapshot(&dir.join("mvcc.snap"), snap_ts, &entries)?;
    // Snapshot covers everything ≤ snap_ts; start a fresh AOF.
    // (Replay skips records with ts ≤ snapshot ts, so a crash between the
    //  rename and the truncation is still correct.)
    *aof = Some(aof::AofWriter::truncate(&dir.join("mvcc.aof"), fsync)?);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// GC + active expiry
// ─────────────────────────────────────────────────────────────────────────────

fn gc_loop(inner: Arc<Inner>, tx: kanal::Sender<Batch>) {
    loop {
        for _ in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            if inner.shutdown.load(Ordering::Acquire) {
                return;
            }

            // Active expiry every 100ms: reap up to 1000 expired keys through
            // the sequencer so the deletions are versioned + AOF-durable.
            let now = now_ms();
            let mut expired: Vec<NsKey> = Vec::new();
            for e in inner.ttl_index.iter() {
                if *e.value() <= now {
                    expired.push(e.key().clone());
                    if expired.len() >= 1000 {
                        break;
                    }
                }
            }
            if !expired.is_empty() {
                let _ = tx.send(Batch::Apply(Box::new(move |w| {
                    for nk in expired {
                        // Re-check under the sequencer: a writer may have
                        // refreshed the TTL since we sampled it.
                        if w.get_expiry(nk.ns, &nk.key).map(|e| e <= w.now_ms()).unwrap_or(false) {
                            w.del(nk.ns, nk.key);
                        }
                    }
                    Box::new(|| {})
                })));
            }
        }

        // Full GC sweep every ~5s: prune dead versions, drop dead chains.
        let min_active = inner.min_active_snapshot();
        inner.chains.retain(|_, chain| {
            chain.prune(min_active);
            match chain.versions.first() {
                // Tombstone no snapshot can distinguish from absence → drop chain.
                Some(head) => !(head.value.is_none() && head.commit_ts <= min_active),
                None => false,
            }
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }
    fn sval(s: &str) -> Datum {
        Datum::Str(b(s))
    }
    fn as_str(d: &Datum) -> &Bytes {
        match d {
            Datum::Str(s) => s,
            _ => panic!("expected Str"),
        }
    }

    async fn put(store: &MvccStore, ns: u32, key: &str, val: &str) {
        let (k, v) = (b(key), sval(val));
        store
            .apply(move |w| {
                w.put(ns, k, v, None);
                Box::new(|| {})
            })
            .await
            .unwrap();
        store.barrier().await;
    }

    #[tokio::test]
    async fn snapshot_isolation_basic() {
        let store = MvccStore::open_memory();
        put(&store, 0, "k", "v1").await;
        let snap = store.pin_snapshot();
        put(&store, 0, "k", "v2").await;

        // New reads see v2, the pinned snapshot still sees v1.
        assert_eq!(as_str(&store.get(0, &b("k")).unwrap()), &b("v2"));
        assert_eq!(as_str(&store.get_at(0, &b("k"), snap.ts).unwrap()), &b("v1"));
        store.close();
    }

    #[tokio::test]
    async fn tombstone_hides_key() {
        let store = MvccStore::open_memory();
        put(&store, 0, "k", "v1").await;
        let snap = store.pin_snapshot();
        store
            .apply(|w| {
                w.del(0, b("k"));
                Box::new(|| {})
            })
            .await
            .unwrap();
        store.barrier().await;

        assert!(store.get(0, &b("k")).is_none());
        // Snapshot taken before the delete still sees the row.
        assert!(store.get_at(0, &b("k"), snap.ts).is_some());
        store.close();
    }

    #[tokio::test]
    async fn commit_conflict_first_committer_wins() {
        let store = MvccStore::open_memory();
        put(&store, 0, "k", "base").await;

        let read_ts = store.current_ts();
        // A concurrent writer lands after our snapshot…
        put(&store, 0, "k", "intruder").await;

        // …so our commit on the same key must abort.
        let err = store
            .commit(
                read_ts,
                vec![WriteOp::Put { ns: 0, key: b("k"), value: sval("mine"), expires_at_ms: None }],
                vec![NsKey::new(0, b("k"))],
            )
            .await
            .unwrap_err();
        assert_eq!(err, CommitError::Conflict);
        assert_eq!(as_str(&store.get(0, &b("k")).unwrap()), &b("intruder"));
        store.close();
    }

    #[tokio::test]
    async fn commit_without_conflict_applies() {
        let store = MvccStore::open_memory();
        let read_ts = store.current_ts();
        let ts = store
            .commit(
                read_ts,
                vec![WriteOp::Put { ns: 0, key: b("a"), value: sval("1"), expires_at_ms: None }],
                vec![NsKey::new(0, b("a"))],
            )
            .await
            .unwrap();
        assert!(ts > read_ts);
        assert_eq!(as_str(&store.get(0, &b("a")).unwrap()), &b("1"));
        store.close();
    }

    #[tokio::test]
    async fn lazy_expiry_reads_none() {
        let store = MvccStore::open_memory();
        let exp = now_ms().saturating_sub(10); // already expired
        store
            .apply(move |w| {
                w.put(0, b("gone"), sval("x"), Some(exp));
                Box::new(|| {})
            })
            .await
            .unwrap();
        store.barrier().await;
        assert!(store.get(0, &b("gone")).is_none());
        store.close();
    }

    #[tokio::test]
    async fn active_expiry_reaps_key() {
        let store = MvccStore::open_memory();
        let exp = now_ms() + 50;
        store
            .apply(move |w| {
                w.put(0, b("ttl"), sval("x"), Some(exp));
                Box::new(|| {})
            })
            .await
            .unwrap();
        store.barrier().await;
        assert!(store.get(0, &b("ttl")).is_some());

        // GC expiry pass runs every 100ms; give it a few cycles.
        tokio::time::sleep(Duration::from_millis(400)).await;
        store.barrier().await;
        assert!(store.get(0, &b("ttl")).is_none());
        store.close();
    }

    #[tokio::test]
    async fn ns_len_tracks_live_keys() {
        let store = MvccStore::open_memory();
        put(&store, 3, "a", "1").await;
        put(&store, 3, "b", "2").await;
        put(&store, 3, "a", "1b").await; // overwrite: count unchanged
        assert_eq!(store.ns_len(3), 2);
        store
            .apply(|w| {
                w.del(3, b("a"));
                Box::new(|| {})
            })
            .await
            .unwrap();
        store.barrier().await;
        assert_eq!(store.ns_len(3), 1);
        store.close();
    }

    #[tokio::test]
    async fn writer_read_your_writes() {
        let store = MvccStore::open_memory();
        let (tx, rx) = oneshot::channel::<bool>();
        store
            .apply(move |w| {
                w.put(0, b("x"), sval("staged"), None);
                let visible = w
                    .get(0, &b("x"))
                    .map(|d| as_str(&d) == &b("staged"))
                    .unwrap_or(false);
                Box::new(move || {
                    let _ = tx.send(visible);
                })
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap());
        store.close();
    }

    #[tokio::test]
    async fn gc_prunes_old_versions() {
        let store = MvccStore::open_memory();
        for i in 0..10 {
            put(&store, 0, "hot", &format!("v{i}")).await;
        }
        // No pinned snapshots → chain should prune to 1 version on next write.
        put(&store, 0, "hot", "final").await;
        let nk = NsKey::new(0, b("hot"));
        let len = store.inner.chains.get(&nk).unwrap().versions.len();
        assert!(len <= 2, "expected pruned chain, got {len} versions");
        store.close();
    }

    #[tokio::test]
    async fn aof_roundtrip_recovers_state() {
        let dir = std::env::temp_dir().join(format!("voltra_mvcc_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let store = MvccStore::open(MvccConfig {
                data_dir: Some(dir.clone()),
                fsync: FsyncPolicy::Always,
            })
            .unwrap();
            put(&store, 0, "persist", "yes").await;
            put(&store, 1, "other_db", "42").await;
            store
                .apply(|w| {
                    w.put(0, b("temp"), sval("x"), None);
                    w.del(0, b("temp"));
                    Box::new(|| {})
                })
                .await
                .unwrap();
            store.barrier().await;
            store.close();
            tokio::time::sleep(Duration::from_millis(250)).await; // let threads exit
        }

        let store = MvccStore::open(MvccConfig {
            data_dir: Some(dir.clone()),
            fsync: FsyncPolicy::Always,
        })
        .unwrap();
        assert_eq!(as_str(&store.get(0, &b("persist")).unwrap()), &b("yes"));
        assert_eq!(as_str(&store.get(1, &b("other_db")).unwrap()), &b("42"));
        assert!(store.get(0, &b("temp")).is_none());
        assert_eq!(store.ns_len(0), 1);
        store.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn save_snapshot_and_recover() {
        let dir = std::env::temp_dir().join(format!("voltra_mvcc_snap_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let store = MvccStore::open(MvccConfig {
                data_dir: Some(dir.clone()),
                fsync: FsyncPolicy::Always,
            })
            .unwrap();
            put(&store, 0, "a", "1").await;
            put(&store, 0, "b", "2").await;
            store.save().await.unwrap();
            put(&store, 0, "c", "3").await; // lands in the fresh AOF
            store.barrier().await;
            store.close();
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let store = MvccStore::open(MvccConfig {
            data_dir: Some(dir.clone()),
            fsync: FsyncPolicy::Always,
        })
        .unwrap();
        assert_eq!(as_str(&store.get(0, &b("a")).unwrap()), &b("1"));
        assert_eq!(as_str(&store.get(0, &b("b")).unwrap()), &b("2"));
        assert_eq!(as_str(&store.get(0, &b("c")).unwrap()), &b("3"));
        store.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn zset_dual_index_consistent() {
        let mut z = ZSet::default();
        z.insert(b("alice"), 10.0);
        z.insert(b("bob"), 5.0);
        z.insert(b("alice"), 20.0); // re-score must remove the old score entry
        assert_eq!(z.len(), 2);
        assert_eq!(z.score(&b("alice")), Some(20.0));
        let ordered: Vec<_> = z.by_score.iter().map(|(s, m)| (s.0, m.clone())).collect();
        assert_eq!(ordered, vec![(5.0, b("bob")), (20.0, b("alice"))]);
        z.remove(&b("alice"));
        assert_eq!(z.by_score.len(), 1);
    }
}
