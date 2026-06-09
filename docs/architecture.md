# Architecture

---

## Full Stack Diagram

```
  Client (WebSocket)
       |
       | MessagePack frames
       v
  ┌────────────────────────────────────┐
  │  WebSocket Listener (tokio-tungstenite) │
  │  - Auth: Bearer token              │
  │  - Role parsing: key:role          │
  │  - Permissions check               │
  │  - Per-client outbound mpsc channel│
  └──────────────┬─────────────────────┘
                 |  kanal async channel
                 | (PendingCall)
                 v
  ┌────────────────────────────────────┐
  │  Worker Pool (N Tokio blocking threads)│
  │  - Deserialize args                │
  │  - Lookup reducer in registry      │
  │  - Run reducer in ReducerContext   │
  │  - Drain deltas → Raft             │
  └──────────────┬─────────────────────┘
                 |  RaftRequest (deltas)
                 v
  ┌────────────────────────────────────┐
  │  Raft Consensus (openraft 0.9)     │
  │  - Leader election                 │
  │  - Quorum replication              │
  │  - Log storage (MemLogStore)       │
  └──────────────┬─────────────────────┘
                 |  apply()
                 v
  ┌────────────────────────────────────┐
  │  State Machine (NeonStateMachine)  │
  │  - apply_delta_batch → TableStore  │
  │  - publish_deltas → SubscriptionMgr│
  └──────────────┬─────────────────────┘
       |                   |
       v                   v
  ┌──────────┐   ┌──────────────────────┐
  │TableStore│   │SubscriptionManager   │
  │(DashMap) │   │- reverse index       │
  │          │   │- fan-out Arc<Bytes>  │
  └──────────┘   └──────────────────────┘
       |
       v
  ┌──────────┐
  │  WAL     │
  │(BatchedWal│
  │Writer)   │
  └──────────┘
```

---

## Components

### TableStore

All game data lives in an in-memory `DashMap<table_name, DashMap<row_key, StoredRow>>`. Reads are entirely lock-free — any number of threads can read simultaneously with no contention. Writes go through `apply_delta_batch()`, which acquires per-row write locks in sorted key order to provide serializable isolation without deadlock.

Key properties:

- DashMap shard count scales with CPU count: `max(16, next_pow2(cpus * 4))`.
- Large values (inventory arrays, etc.) are offloaded to `BlobStore` — a memory-mapped file backed by `parking_lot::RwLock`.
- `RowDelta` carries an `Arc<Bytes>` payload, so cloning a delta for fan-out is O(1).
- HLC timestamps (`hlc_ts: u64`) are stored on every row. When a cluster delta arrives with a non-zero `hlc_ts`, `apply_delta_batch` skips the write if the stored row's timestamp is newer (last-write-wins).

### Write-Ahead Log (WAL)

Every committed write is appended to the WAL before the response is sent to the client. The WAL is an append-only binary file: each entry has a header (magic, length, CRC32) followed by a MessagePack-encoded payload.

Two write modes:

- **Synchronous** (`WalWriter`): every write calls `fsync`. Safest, slowest.
- **Batched** (`BatchedWalWriter`): entries are buffered and flushed every N entries or every M milliseconds, whichever comes first. Much higher throughput at the cost of a small window of data loss on crash.

On startup, `WalReader` replays all entries after the most recent snapshot to reconstruct in-memory state. Snapshots are atomic (`write to temp file` then `fsync + rename`) and store a JSON dump of all rows and counters.

### Reducer Runtimes

A reducer is a named function that receives a `ReducerContext` (read/write access to the TableStore) and a MessagePack-encoded args payload. It returns a MessagePack-encoded result or an error. All writes staged during execution are committed atomically or rolled back entirely.

Three runtimes are supported:

**Native Rust**: compiled into the server binary. Zero overhead — a function call. Used for built-in reducers like `increment`.

**JavaScript (Boa 0.19)**: a pure-Rust JS engine with no C++ dependencies. JS reducers live as `.js` files in the `modules/` directory and are loaded at startup. Access to the database is via the `__neondb_*` host API (see [docs/reducers.md](reducers.md)). JS reducers are semi-trusted: the engine enforces a wall-clock timeout but does not cap JS heap memory (use the WASM backend for untrusted code).

**WASM (Wasmtime 21, Cranelift JIT)**: `.wasm` or `.wat` files in `modules/`. Wasmtime enforces a hard memory limit via `ResourceLimiter`. 10–50x faster than the Boa interpreter for compute-heavy logic. The `neondb build` command compiles `.js` files to WASM using javy.

### Subscriptions

`SubscriptionManager` maintains two concurrent indexes:

- `clients: DashMap<subscription_id, SubscriptionEntry>` — the full predicate + callback for each subscription.
- `table_index: DashMap<table_name, Vec<subscription_id>>` — reverse lookup: given a table that just changed, find affected subscriptions in O(matching) time.

When a delta is published:

1. Look up `table_index[table_name]` to get affected subscription IDs.
2. For each ID, evaluate the predicate against the new row data.
3. If the predicate matches, encode a `SubscriptionDiff` once as `Arc<Bytes>` and clone the `Arc` to each matching subscriber's outbound channel — zero re-encodings.

**Initial state sync**: when a client subscribes, `subscribe_with_snapshot()` immediately delivers all existing matching rows as `initial_snapshot` frames. The snapshot is sorted (ORDER BY) and truncated (LIMIT N) before delivery.

**Two-frame protocol**: with `two_frame_protocol = true`, a delta is encoded as two frames: a routing header (list of subscription IDs) and a shared body (the actual row data). This reduces encoding work when many subscriptions match the same delta.

### Raft Consensus

NeonDB embeds an openraft-based Raft node. In single-node mode it auto-bootstraps as a one-member cluster. In multi-node mode, nodes are added as learners first, then promoted to voters via `POST /raft/change-membership`.

Every reducer write goes through Raft: the worker drains staged deltas and submits them as a `RaftRequest` via `client_write()`. The entry is replicated to a quorum of nodes, then the state machine's `apply()` is called on every node — applying to the TableStore and fanning out subscriptions. This guarantees every node sees every write in the same order.

Followers that receive a reducer call from a client check the Raft leader status and forward the call to the current leader via `POST /cluster/call`.

### HLC / CRDT

Every `RowDelta` carries an HLC timestamp (`hlc_ts: u64`) packed as 48-bit wall-clock milliseconds + 16-bit logical counter. `apply_delta_batch` implements last-write-wins: if the incoming delta's `hlc_ts` is older than the stored row's `hlc_ts`, the write is silently skipped. This resolves conflicts on cross-shard or cross-cluster fanout without coordination.

---

## Data Flow: Single Reducer Call

1. Client sends `ClientMessage::ReducerCall` (MessagePack).
2. WebSocket handler authenticates, checks permissions, enqueues `PendingCall` to the kanal channel.
3. A worker thread picks up the call, builds a `ReducerContext`, runs the reducer.
4. Reducer stages writes via `ctx.set_row()` / `ctx.set_counter()`.
5. Worker calls `ctx.drain_pending_deltas()` and submits them to `raft.client_write()`.
6. Raft replicates the entry to a quorum of nodes.
7. State machine `apply()` calls `apply_delta_batch()` on the TableStore and `publish_deltas()` on the SubscriptionManager.
8. Subscription fan-out encodes `SubscriptionDiff` once, fans out `Arc<Bytes>` to all matching subscribers.
9. WAL entry appended after `client_write` succeeds.
10. `ReducerResponse` sent back to the client.

---

## Performance Notes

Benchmark results on Ryzen 7 / 32 GB / NVMe:

| Scenario | Throughput |
|---|---|
| In-process engine, no network | ~2.9 M ops/s |
| Parallel engine, 24 threads | ~1.65 M TPS |
| Single-thread engine | ~297 K TPS |
| WebSocket round-trip, 10 clients | ~40 K TPS |

The WebSocket round-trip number is bounded by network RTT and per-connection serialization. The in-process number reflects the raw TableStore throughput with no network overhead.

---

## Related docs

- [docs/reducers.md](reducers.md) — writing reducers
- [docs/protocol.md](protocol.md) — wire protocol
- [docs/cluster.md](cluster.md) — Raft and clustering
