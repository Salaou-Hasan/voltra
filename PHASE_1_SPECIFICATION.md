# Voltra – Phase 1: Core Engine Skeleton – Technical Specification

**Document Version**: 1.0  
**Date**: 2026-06-05  
**Status**: Awaiting "Execute Phase 1" confirmation  
**Approved Design Decisions**:
- V8 for TypeScript reducers ✓
- Both WASM + native .so for Rust reducers (user selects) ✓
- Custom DSL for subscriptions ✓
- Row-oriented in-memory tables ✓
- Multi-stage Docker build ✓
- MessagePack for WAL ✓

---

## Table of Contents

1. [Overview & Goals](#overview--goals)
2. [Architecture – Single-Threaded Loop](#architecture--single-threaded-loop)
3. [In-Memory Table Structure](#in-memory-table-structure)
4. [Write-Ahead Log (WAL) Format](#write-ahead-log-wal-format)
5. [Core Data Structures & Interfaces](#core-data-structures--interfaces)
6. [Execution Flow Diagrams](#execution-flow-diagrams)
7. [Error Handling & Panic Safety](#error-handling--panic-safety)
8. [Testing Strategy](#testing-strategy)
9. [Performance Targets & Profiling](#performance-targets--profiling)
10. [Build, Run & Dev Workflow](#build-run--dev-workflow)

---

## Overview & Goals

### Phase 1 Deliverable
A **runnable `voltra-server` binary** that:
- Listens for WebSocket connections on `0.0.0.0:8000`
- Accepts one hardcoded reducer: `increment(name: String, delta: i32) -> (new_value: i32, timestamp: i64)`
- Maintains an in-memory table of counters: `{String -> i32}`
- Logs every reducer call to a WAL on disk
- On restart, replays the entire WAL to recover state
- Serves ~1000 increment calls/second with <10ms p99 latency
- **No panics** on invalid input, client disconnects, or partial WAL writes

### Non-Goals (for Phase 1)
- V8 integration (Phase 2)
- Subscriptions (Phase 3)
- Production deployment (Phase 5)
- Custom reducers (Phase 2)

### Critical Constraints
1. **Single-threaded reducer engine**: All reducer calls execute sequentially on one OS thread.
2. **WebSocket listener on separate thread**: Accept connections without blocking the reducer loop.
3. **Determinism**: Reducer must always produce the same output given the same input (no time, no randomness, no network calls).
4. **Durability**: Every WAL entry must be fsync'd before responding to client (or fsync at configurable interval).
5. **No external dependencies on reducer logic**: Reducers see a deterministic context; they cannot access system state beyond the database tables.

---

## Architecture – Single-Threaded Loop

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Voltra Server (Rust)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  main()                                                              │
│  ├─ Load config (port, WAL path, fsync interval)                    │
│  ├─ Load/create schema                                              │
│  ├─ Replay WAL to rebuild in-memory tables                          │
│  │  └─ For each WAL entry: execute reducer, apply delta             │
│  ├─ Spawn: WebSocket Listener Task (tokio)                          │
│  │  └─ Accept connections → queue ReducerCall to channel            │
│  └─ Main Loop (blocking, single OS thread)                          │
│     ├─ for each ReducerCall from channel:                           │
│     │   ├─ Execute reducer (in-process or WASM)                     │
│     │   ├─ Compute row deltas                                       │
│     │   ├─ Write to WAL                                             │
│     │   ├─ Apply delta to in-memory tables                          │
│     │   ├─ Send response to client                                  │
│     │   └─ (In Phase 3: compute subscription diffs, push updates)   │
│     └─ (Optional: periodically fsync WAL if not per-call)           │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Thread Model

```
┌──────────────────────────────────────────────┐
│           Tokio Runtime (async)               │
│                                               │
│  ┌────────────────────────────────┐           │
│  │   WebSocket Listener Task      │           │
│  │  (tokio::spawn, async)         │           │
│  │                                │           │
│  │  • tokio::net::TcpListener      │           │
│  │  • tungstenite for WS protocol  │           │
│  │  • On new client:               │           │
│  │    - create ClientConn          │           │
│  │    - spawn read_task per client │           │
│  │    - on message: queue to       │           │
│  │      mpsc::channel              │           │
│  └────────────────────────────────┘           │
│         │                                      │
│         └─── mpsc channel ───────────────────┐│
│                                              ││
└──────────────────────────────────────────────┘│
                                                │
┌───────────────────────────────────────────────┘
│
│  Main Thread (blocking, std::sync)
│
│  ┌────────────────────────────────────────┐
│  │   Reducer Engine Loop                  │
│  │   (single-threaded, blocking)          │
│  │                                        │
│  │   while let Ok(call) = rx.recv() {    │
│  │     execute_reducer(call)              │
│  │     log_to_wal(delta)                  │
│  │     apply_to_tables(delta)             │
│  │     respond_to_client()                │
│  │   }                                    │
│  │                                        │
│  │   Tables: Arc<Mutex<TableStore>>       │
│  │   WAL: Arc<Mutex<WalWriter>>           │
│  │                                        │
│  └────────────────────────────────────────┘
│
└────────────────────────────────────────────
```

### Key Design Decisions

**Why tokio async for listener, blocking loop for reducer?**
- **Listener is I/O-bound**: Accepts many connections, reads frames. Async is natural.
- **Reducer is CPU-bound**: Executes deterministic logic, writes to WAL, updates tables. Blocking is simpler, no context-switching overhead.
- **Communication**: mpsc channel (thread-safe queue) bridges the two worlds.

**Why no async in the reducer loop?**
- Complexity: async Rust is hard to get right.
- Determinism: We must guarantee that the reducer produces the same output every time. Async can introduce timing-dependent behavior.
- Performance: For single-threaded, blocking is actually faster (no runtime overhead).

---

## In-Memory Table Structure

### Data Model

**Phase 1 uses a single hardcoded table: `counters`**

```
Table: counters
├─ Columns:
│  ├─ name: String (primary key)
│  ├─ value: i32
│  └─ last_modified: i64 (Unix timestamp at time of log; deterministic)
└─ Storage: HashMap<String, Row>
```

**Expandable to generic tables in Phase 2** (but Phase 1 is hardcoded for simplicity).

### Row Structure

```rust
// Phase 1: Hardcoded
#[derive(Clone, Debug)]
pub struct Counter {
    pub name: String,
    pub value: i32,
    pub last_modified: i64,  // Set by reducer at time of execution
}

// Generic (Phase 2+):
#[derive(Clone, Debug)]
pub struct Row {
    pub id: u64,
    pub columns: Vec<Value>,  // Ordered by schema
    pub version: u64,         // For CDC / incremental updates
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    I32(i32),
    I64(i64),
    U64(u64),
    String(String),
    Bool(bool),
    Bytes(Vec<u8>),
}
```

### Storage Layout

```rust
// Arc<Mutex<...>> so that multiple tasks can read/write safely
pub struct TableStore {
    pub counters: HashMap<String, Counter>,
    // Phase 2: pub tables: HashMap<String, Table> // generic
}

// Global state
pub static TABLES: Lazy<Arc<Mutex<TableStore>>> = Lazy::new(|| {
    Arc::new(Mutex::new(TableStore {
        counters: HashMap::new(),
    }))
});
```

**Access Pattern (in reducer loop)**:
```rust
{
    let mut tables = TABLES.lock().unwrap();
    tables.counters.insert(name, Counter { name, value, last_modified });
}
```

### Why Row-Oriented?

- **Simplicity**: Each row is a single object, easy to serialize/deserialize.
- **Update-heavy workload**: Games typically do full-row updates (e.g., player position, health), not column-selective updates.
- **Column-oriented trade-off**: Would require more complex delta computation in Phase 3. Defer to Phase 6 optimization.

---

## Write-Ahead Log (WAL) Format

### Purpose
- **Durability**: Every reducer call is logged before the response is sent to the client.
- **Recovery**: On restart, replay the WAL to rebuild in-memory state.
- **Append-only**: Never overwrite or delete entries. New database snapshots can be created by taking a logical snapshot + new WAL suffix.

### Physical Format: MessagePack

**Why MessagePack?**
- Compact binary encoding (smaller than JSON, readable with tools like `msgpackc` CLI).
- Built-in version tolerance (can add fields later).
- Fast encode/decode (< 1µs per entry on modern CPU).

### WAL Entry Structure

Each entry is a MessagePack array: `[header, payload]`

#### Header (Common to all entries)

```rust
#[derive(Serialize, Deserialize)]
pub struct WalHeader {
    pub version: u32,              // Format version (currently 1)
    pub entry_type: u8,            // 1=ReducerCall, 2=Snapshot (future)
    pub timestamp: u64,            // Unix nanos at time of call (for determinism)
    pub sequence_number: u64,      // Monotonic counter (recovery aid)
    pub checksum: u32,             // CRC32 of payload (corruption detection)
}
```

#### Payload: ReducerCall (entry_type = 1)

```rust
#[derive(Serialize, Deserialize)]
pub struct ReducerCallEntry {
    pub reducer_id: String,        // e.g., "increment"
    pub args: Vec<u8>,             // Serialized args (MessagePack)
    pub delta: Vec<RowDelta>,      // Which rows changed and how
}

#[derive(Serialize, Deserialize)]
pub struct RowDelta {
    pub table_name: String,        // "counters"
    pub operation: String,         // "insert", "update", "delete"
    pub row_key: String,           // Primary key (for phase 1: counter name)
    pub row_data: Counter,         // Full row data (for insert/update)
}
```

### Physical Layout on Disk

```
[WalHeader (msg)] [ReducerCallEntry (msg)] [WalHeader (msg)] [ReducerCallEntry (msg)] ...
```

Each entry is **self-contained** and **independently decodable**. No fixed record size.

### Durability Options (Configurable)

**Option 1: Per-call fsync** (default, safest)
```rust
wal_writer.append(entry)?;
wal_writer.fsync()?;  // Block until written to disk
respond_to_client(Ok(result));
```
- **Trade-off**: ~100 fsync/sec max on typical SSD (1000 reducers/sec → 10 batches/sec).
- **Safety**: Zero data loss on crash.

**Option 2: Batch fsync** (configurable via `FSYNC_INTERVAL_MS`)
```rust
wal_writer.append(entry)?;
// fsync only every 100ms (or N entries)
if should_fsync(entry_count, elapsed_ms) {
    wal_writer.fsync()?;
}
respond_to_client(Ok(result));
```
- **Trade-off**: Slight data loss risk (last few entries might not be on disk if crash), but higher throughput.
- **Safety**: Configurable risk.

**Phase 1 default: Per-call fsync** (conservative, simplest to test).

### WAL Recovery (Replay)

```rust
pub fn replay_wal(wal_path: &Path) -> Result<TableStore> {
    let mut reader = WalReader::open(wal_path)?;
    let mut tables = TableStore::new();
    let mut sequence_number = 0;

    while let Some(entry) = reader.next_entry()? {
        // Validate header
        assert_eq!(entry.header.version, 1);
        assert_eq!(entry.header.sequence_number, sequence_number);
        sequence_number += 1;

        // Re-execute reducer (deterministic, same args → same output)
        // For Phase 1: just apply delta directly
        for delta in entry.deltas {
            tables.apply_delta(delta)?;
        }
    }

    Ok(tables)
}
```

**Important**: The WAL stores **the delta (row changes)**, not the reducer invocation. This is faster to replay and avoids re-running reducer code (which might be non-deterministic in edge cases).

---

## Core Data Structures & Interfaces

### ReducerCall (Wire Protocol)

Clients send this over WebSocket (serialized as MessagePack):

```rust
#[derive(Serialize, Deserialize)]
pub struct ReducerCall {
    pub call_id: u64,              // Client-provided, returned in response
    pub reducer_name: String,      // "increment"
    pub args: Vec<u8>,             // Serialized args (MessagePack)
}
```

### ReducerResponse (Wire Protocol)

Server sends back:

```rust
#[derive(Serialize, Deserialize)]
pub struct ReducerResponse {
    pub call_id: u64,              // Echo from ReducerCall
    pub success: bool,             // true if reducer ran; false if error
    pub result: Option<Vec<u8>>,   // Serialized return value (MessagePack)
    pub error: Option<String>,     // Error message if success=false
}
```

### ReducerContext (In-Process API)

The reducer function (hardcoded in Phase 1) receives this:

```rust
pub struct ReducerContext<'a> {
    pub tables: &'a Arc<Mutex<TableStore>>,
    pub timestamp: u64,            // When this call started (deterministic)
}

impl ReducerContext<'_> {
    // Read operations
    pub fn get_counter(&self, name: &str) -> Result<Option<Counter>> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.counters.get(name).cloned())
    }

    pub fn list_counters(&self) -> Result<Vec<Counter>> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.counters.values().cloned().collect())
    }

    // Write operation
    pub fn set_counter(&self, name: String, value: i32) -> Result<()> {
        let mut tables = self.tables.lock().unwrap();
        tables.counters.insert(name, Counter {
            name,
            value,
            last_modified: self.timestamp,
        });
        Ok(())
    }
}
```

### Reducer Function Signature (Phase 1 Hardcoded)

```rust
fn increment(ctx: &ReducerContext, name: String, delta: i32) -> Result<IncrementResult> {
    let current = ctx.get_counter(&name)?
        .unwrap_or(Counter {
            name: name.clone(),
            value: 0,
            last_modified: ctx.timestamp,
        });

    let new_value = current.value + delta;

    ctx.set_counter(name, new_value)?;

    Ok(IncrementResult {
        new_value,
        timestamp: ctx.timestamp,
    })
}

#[derive(Serialize, Deserialize)]
pub struct IncrementResult {
    pub new_value: i32,
    pub timestamp: i64,
}
```

---

## Execution Flow Diagrams

### Write Path (Client calls `increment`)

```
Client (WebSocket)
  │
  ├─ send: ReducerCall { call_id: 1, reducer_name: "increment", args: [name: "foo", delta: 5] }
  │
  ▼
Listener Task (async, tokio)
  │
  ├─ parse MessagePack into ReducerCall
  ├─ create: PendingCall { call_id: 1, response_tx: channel }
  ├─ send to REDUCER_QUEUE (mpsc)
  │
  ▼
Reducer Engine Loop (main thread, blocking)
  │
  ├─ rx.recv() → PendingCall { call_id: 1, ... }
  ├─ TIMESTAMP_FOR_CALL = system_time_nanos() (set once at start)
  ├─ ctx = ReducerContext { tables: &TABLES, timestamp: TIMESTAMP_FOR_CALL }
  ├─ result = increment(ctx, "foo", 5)
  │  └─ read current value of "foo" from TABLES (0 if not exists)
  │  └─ compute new_value = 0 + 5 = 5
  │  └─ call ctx.set_counter("foo", 5)
  │     └─ acquire TABLES lock
  │     └─ insert Counter { name: "foo", value: 5, last_modified: TIMESTAMP_FOR_CALL }
  │     └─ release lock
  │  └─ return IncrementResult { new_value: 5, timestamp: TIMESTAMP_FOR_CALL }
  │
  ├─ build RowDelta { table: "counters", op: "update", key: "foo", data: Counter { ... } }
  ├─ build WalEntry { header: { version: 1, entry_type: 1, timestamp: TIMESTAMP_FOR_CALL, seq: 42, checksum: ... }, deltas: [...] }
  │
  ├─ WAL_WRITER.append(WalEntry)
  ├─ WAL_WRITER.fsync()  // Wait for disk
  │
  ├─ serialize result to MessagePack
  ├─ response_tx.send(ReducerResponse { call_id: 1, success: true, result: [...], error: None })
  │
  ▼
Listener Task (receives from response_tx)
  │
  ├─ client connection has stored response_tx
  ├─ send WebSocket frame to client
  │
  ▼
Client
  │
  └─ receive ReducerResponse { call_id: 1, success: true, result: { new_value: 5, timestamp: ... } }
```

### Recovery Path (Server Restart)

```
Server starts
  │
  ├─ load config
  ├─ WAL_PATH = "/data/wal/voltra.wal" (or env var)
  │
  ├─ TABLES = Arc<Mutex::new(TableStore::new())>
  │
  ├─ WalReader::open(WAL_PATH)?
  │
  ├─ for each WalEntry in WAL:
  │  ├─ validate header (version, checksum)
  │  ├─ for each RowDelta in entry:
  │  │  ├─ match delta.operation:
  │  │  │  ├─ "insert" | "update" → tables.insert(key, data)
  │  │  │  ├─ "delete" → tables.remove(key)
  │  │  │  └─ unknown → return Err
  │  │  └─ (no need to re-run reducer logic, WAL has the result)
  │
  ├─ listen for WebSocket connections (same as before)
  │
  └─ DONE: TABLES are now in same state as before crash
```

---

## Error Handling & Panic Safety

### Principle: **The database must never panic.**

All errors must be:
1. **Logged** (to stderr or log file)
2. **Returned** to the client (in ReducerResponse)
3. **Recovered** gracefully (no state corruption, no partial updates)

### Error Cases & Handling

| Error | Cause | Handling |
|-------|-------|----------|
| **Reducer returns Err** | User code error (e.g., invalid input) | Return ReducerResponse { success: false, error: "..." } to client. Do NOT log to WAL. |
| **WAL write fails** | Disk full, permission denied, etc. | Log to stderr. Return ReducerResponse { success: false, error: "WAL write failed: ..." }. Do NOT apply delta to tables. |
| **Duplicate ReducerCall** | Network retry (same call_id sent twice) | Idempotent: if call_id already seen, return cached response. (Phase 2 feature) |
| **Client disconnects mid-call** | Network error | Finish executing reducer. If client reconnected or not, log entry to WAL regardless. |
| **Corruption in WAL (bad checksum)** | Disk corruption or truncated file | Log error, stop replay at that point, start with recovered state. (Phase 6: implement snapshot to skip replay) |
| **Overflow in arithmetic** | e.g., i32 + i32 overflows | Rust's checked_add prevents panic. Return error to reducer. Reducer decides what to do. |

### Panic Safety

**What must NOT panic:**
- Receiving a malformed ReducerCall
- Client sending invalid args (wrong type, too large)
- Disk I/O errors
- Lock contention / deadlock

**Implementation:**
- Use `.expect()` / `.unwrap()` ONLY in tests and known-safe paths.
- Use `.?` operator to propagate errors.
- Use `Result<T, E>` for all fallible operations.
- Use `panic!()` only for impossible states (e.g., internal consistency checks that should never fire).

---

## Testing Strategy

### Unit Tests (Phase 1)

**File: `voltra-server/tests/unit.rs`**

```rust
#[test]
fn test_increment_reducer() {
    let ctx = ReducerContext { tables: &TABLES, timestamp: 1000 };
    let result = increment(&ctx, "foo".to_string(), 5).unwrap();
    assert_eq!(result.new_value, 5);

    // Call again, should accumulate
    let result2 = increment(&ctx, "foo".to_string(), 3).unwrap();
    assert_eq!(result2.new_value, 8);
}

#[test]
fn test_wal_roundtrip() {
    // Write entry, read it back, verify identical
    let entry = WalEntry { ... };
    let encoded = entry.serialize()?;
    let decoded: WalEntry = WalEntry::deserialize(&encoded)?;
    assert_eq!(entry, decoded);
}

#[test]
fn test_wal_recovery() {
    // Create in-memory WAL, insert entries, replay, verify tables match
    let wal = WalWriter::in_memory();
    wal.append(entry1)?;
    wal.append(entry2)?;

    let tables = replay_wal(wal)?;
    assert_eq!(tables.counters.get("foo").unwrap().value, 42);
}
```

### Integration Tests (Phase 1)

**File: `voltra-server/tests/integration.rs`**

```rust
#[tokio::test]
async fn test_end_to_end_single_client() {
    // Start server
    let server_handle = tokio::spawn(run_server());

    // Connect client
    let client = connect_ws("ws://localhost:8000").await?;

    // Send reducer call
    let call = ReducerCall { call_id: 1, reducer_name: "increment".into(), args: [...] };
    client.send(serialize(call))?;

    // Receive response
    let response = client.recv().await?;
    assert_eq!(response.success, true);
    assert_eq!(response.call_id, 1);
}

#[tokio::test]
async fn test_crash_recovery() {
    // Start server, call increment, kill server, restart
    let server_handle = tokio::spawn(run_server());

    let client = connect_ws("ws://localhost:8000").await?;
    let call = ReducerCall { call_id: 1, ... };
    client.send(serialize(call))?;
    let response = client.recv().await?;
    assert_eq!(response.success, true);

    // Kill server (drop handle)
    drop(server_handle);

    // Restart server
    let server_handle = tokio::spawn(run_server());

    // Connect new client
    let client2 = connect_ws("ws://localhost:8000").await?;
    let call2 = ReducerCall { call_id: 2, reducer_name: "list".into(), ... };
    client2.send(serialize(call2))?;
    let response2 = client2.recv().await?;

    // Verify state was recovered (counter "foo" still has value 5)
    assert_eq!(response2.success, true);
}

#[tokio::test]
async fn test_multiple_concurrent_clients() {
    // 10 clients, each sends 100 increment calls (same counter "foo")
    // Verify final value = 10 * 100 = 1000
    let server_handle = tokio::spawn(run_server());

    let mut clients = vec![];
    for i in 0..10 {
        let client = connect_ws("ws://localhost:8000").await?;
        clients.push(tokio::spawn(async move {
            for j in 0..100 {
                let call = ReducerCall { call_id: i * 100 + j, ... };
                client.send(serialize(call))?;
                let response = client.recv().await?;
                assert_eq!(response.success, true);
            }
        }));
    }

    futures::future::join_all(clients).await;

    // Final check: value should be 1000
    let client = connect_ws("ws://localhost:8000").await?;
    let call = ReducerCall { call_id: 9999, reducer_name: "get".into(), args: [name: "foo"] };
    client.send(serialize(call))?;
    let response = client.recv().await?;
    let result: GetResult = deserialize(response.result)?;
    assert_eq!(result.value, 1000);
}
```

### Manual Testing

**Provided script: `voltra-server/test_client.py`**

```python
#!/usr/bin/env python3
import websocket
import json
import time
import sys

def test_increment():
    ws = websocket.create_connection("ws://localhost:8000")
    
    for i in range(10):
        call = {
            "call_id": i,
            "reducer_name": "increment",
            "args": {"name": "counter_1", "delta": 1}
        }
        ws.send(json.dumps(call))
        response = json.loads(ws.recv())
        print(f"Call {i}: success={response['success']}, result={response.get('result')}")
    
    ws.close()

if __name__ == "__main__":
    test_increment()
```

### Performance Baseline (Phase 1)

**Benchmark: `voltra-server/benches/throughput.rs`**

```rust
#[bench]
fn bench_increment_tps(b: &mut Bencher) {
    let ctx = ReducerContext { ... };
    b.iter(|| {
        increment(&ctx, "foo".into(), 1).unwrap()
    });
}
```

**Target**: >10,000 increments/second on a modern CPU.

---

## Performance Targets & Profiling

### Phase 1 Baseline Goals

| Metric | Target | Notes |
|--------|--------|-------|
| **Throughput (TPS)** | ≥ 1,000 incr/sec | Single client, simple hardware. |
| **Latency (p99)** | ≤ 10ms | End-to-end from client send to response recv. |
| **Memory (per table)** | < 1MB for 10k rows | HashMap overhead is small. |
| **WAL disk usage** | < 1KB per entry | MessagePack is compact. |
| **Startup time** | < 1s for empty WAL, < 5s for 10 GB WAL | Baseline; optimize in Phase 6. |

### Profiling Tools

**CPU Profiling** (Phase 1 post-implementation):
```bash
cargo build --release
perf record -F 99 ./target/release/voltra-server
perf report
```

**Memory Profiling** (Phase 1 post-implementation):
```bash
valgrind --leak-check=full ./target/release/voltra-server
```

**Benchmark harness** (Phase 4+):
```bash
cargo bench --release
```

---

## Build, Run & Dev Workflow

### Folder Structure (Phase 1)

```
Voltra/
├── voltra-server/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs                 # Entry point
│   │   ├── lib.rs
│   │   ├── config.rs               # Load config from env/TOML
│   │   ├── wal/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs           # WalWriter
│   │   │   ├── reader.rs           # WalReader
│   │   │   └── entry.rs            # WalEntry struct
│   │   ├── table/
│   │   │   ├── mod.rs
│   │   │   ├── store.rs            # TableStore
│   │   │   └── row.rs              # Row, Counter, Value enums
│   │   ├── reducer/
│   │   │   ├── mod.rs
│   │   │   ├── context.rs          # ReducerContext
│   │   │   └── increment.rs        # Phase 1: hardcoded increment reducer
│   │   ├── network/
│   │   │   ├── mod.rs
│   │   │   ├── websocket.rs        # tokio + tungstenite listener
│   │   │   ├── protocol.rs         # MessagePack encode/decode
│   │   │   └── message.rs          # ReducerCall, ReducerResponse
│   │   └── error.rs                # VoltraError, Result<T>
│   ├── tests/
│   │   ├── unit.rs
│   │   ├── integration.rs
│   │   └── crash_recovery.rs
│   ├── benches/
│   │   └── throughput.rs
│   ├── Dockerfile                  # Multi-stage (Phase 5)
│   ├── .cargo/config.toml          # Compiler flags for perf
│   ├── test_client.py              # Manual test script
│   └── README.md
│
├── docker-compose.yml
└── Cargo.workspace.toml
```

### Build Instructions

```bash
# Build in debug mode
cd voltra-server
cargo build

# Build in release mode (optimized)
cargo build --release

# Run tests
cargo test

# Run with custom WAL path
VOLTRA_WAL_PATH=/tmp/wal.bin cargo run --release

# Run benchmarks
cargo bench --release
```

### Environment Variables (Phase 1)

| Var | Default | Purpose |
|-----|---------|---------|
| `VOLTRA_PORT` | `8000` | WebSocket listen port |
| `VOLTRA_HOST` | `0.0.0.0` | Listen address |
| `VOLTRA_WAL_PATH` | `/tmp/voltra.wal` | Path to WAL file |
| `VOLTRA_FSYNC_INTERVAL_MS` | `0` (per-call) | Batch fsync interval (0 = per-call) |
| `RUST_LOG` | `info` | Log level (debug, info, warn, error) |

### Running Locally

```bash
# Terminal 1: Start server
cd voltra-server
RUST_LOG=debug cargo run --release

# Terminal 2: Run test script
python3 test_client.py

# Terminal 3: Monitor WAL growth
watch -n 1 'ls -lah /tmp/voltra.wal'
```

### Docker Build (Phase 5, but include Dockerfile now for reference)

```dockerfile
# Multi-stage build
FROM rust:1.75 AS builder

WORKDIR /app
COPY voltra-server /app

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/voltra-server /usr/local/bin/

EXPOSE 8000

ENTRYPOINT ["voltra-server"]
```

### Example: Manual Test with `websocat`

```bash
# Install websocat (Rust tool for WebSocket testing)
cargo install websocat

# In one terminal
cargo run --release

# In another
echo '{"call_id": 1, "reducer_name": "increment", "args": {"name": "foo", "delta": 5}}' | websocat ws://localhost:8000
```

---

## Summary: What Phase 1 Delivers

### Artifacts

1. ✅ **`voltra-server` binary**: ~10 MB (Rust compiled), runs standalone
2. ✅ **WAL file** (`/tmp/voltra.wal`): Append-only, MessagePack-encoded entries
3. ✅ **Unit + integration tests**: >70% code coverage
4. ✅ **README.md**: Build, run, test instructions
5. ✅ **Benchmark harness**: TPS & latency baseline

### Success Criteria

- [ ] Server listens on `:8000` for WebSocket connections
- [ ] Single `increment` reducer can execute 1000+ calls/second
- [ ] p99 latency < 10ms
- [ ] Crash recovery: WAL replayed correctly, state restored
- [ ] No panics on invalid input or network errors
- [ ] All tests pass with `cargo test`
- [ ] Code compiles without warnings

### Known Limitations (Intentional for Phase 1)

- ❌ No custom reducers (Phase 2)
- ❌ No subscriptions (Phase 3)
- ❌ No TypeScript/V8 (Phase 2)
- ❌ No WASM (Phase 2)
- ❌ No Docker image (Phase 5)
- ❌ No CLI (Phase 5)
- ❌ Only hardcoded "increment" reducer

---

## Next Steps

This Phase 1 specification is **complete and awaiting your approval**.

**To proceed, please confirm:**
> **Execute Phase 1: I'm ready for you to begin implementation.**

Once you give the go-ahead, I will:
1. Create the Rust project structure with Cargo.toml
2. Implement all modules (WAL, tables, reducer, network, error handling)
3. Write unit + integration tests
4. Run benchmarks and report baseline metrics
5. Provide a working binary and detailed testing instructions
6. Ask for confirmation before moving to Phase 2

---

**Awaiting your confirmation to begin Phase 1 implementation.**
