# CLAUDE.md — NeonDB Agent Handoff Document

Read this before touching any file. It captures the full project state, architecture decisions, every fix applied so far, and what still needs doing.

---

## What This Project Is

NeonDB is a high-throughput, self-hosted game-backend database written in Rust. It speaks WebSocket (MessagePack framing), stores data in a lock-free in-memory table engine, logs every write to a WAL, and executes user-supplied logic ("reducers") in three runtimes:

- **Native** — compiled Rust functions, zero overhead
- **JS (Boa 0.19)** — pure-Rust JS engine, no V8 dependency, works on Windows/Linux/macOS
- **WASM (Wasmtime 21)** — `.wasm` / `.wat` modules via Cranelift JIT

The server is a single binary. Clients connect over WebSocket and send MessagePack-encoded `ReducerCall` messages. The server dispatches calls through a `kanal` async channel to N parallel Tokio blocking-thread workers, commits deltas to an in-memory `TableStore` (DashMap-backed), appends a WAL entry, then fans out subscription updates as `Arc<Bytes>` to all subscribed clients.

---

## Project Root

```
C:\Users\King\Desktop\NeonDB
```

Allowed filesystem directories for agents: `C:\Users\King\Desktop` and `C:\Users\King\Documents`.

---

## Directory Map

```
NeonDB/
├── Cargo.toml                  # workspace manifest — single crate "neondb"
├── src/
│   ├── main.rs                 # CLI (init / build / start), server bootstrap
│   ├── lib.rs                  # crate root, re-exports
│   ├── config.rs               # Config struct, from_env(), TOML loading
│   ├── error.rs                # NeonDBError enum, Result alias
│   ├── subscriptions.rs        # SubscriptionManager (DashMap, Arc<Bytes> fan-out)
│   ├── table/
│   │   └── mod.rs              # TableStore, Counter, Player, RowDelta, BlobStore
│   ├── reducer/
│   │   ├── mod.rs              # pub re-exports
│   │   ├── backend.rs          # ReducerBackend trait
│   │   ├── context.rs          # ReducerContext, increment_reducer()
│   │   ├── native.rs           # NativeReducerBackend
│   │   ├── registry.rs         # ReducerRegistry (auto-loads modules/)
│   │   ├── v8.rs               # Boa JS engine backend
│   │   └── wasm.rs             # Wasmtime backend
│   ├── network/
│   │   ├── mod.rs
│   │   ├── message.rs          # ClientMessage, ServerMessage, ReducerResponse
│   │   ├── protocol.rs         # MessagePack encode/decode helpers
│   │   └── websocket.rs        # WebSocket listener, handle_client(), PendingCall
│   └── wal/
│       ├── mod.rs
│       ├── entry.rs            # WalEntry, WalHeader, WalPayload
│       ├── writer.rs           # WalWriter (sync, fsync)
│       ├── batch_writer.rs     # BatchedWalWriter (async batching)
│       └── reader.rs           # WalReader
├── benches/
│   ├── throughput.rs           # criterion bench — Scenario 1/2/3 (see below)
│   └── end_to_end.rs           # criterion bench — full WebSocket round-trip (needs server running)
├── tests/
│   └── integration.rs          # tokio integration tests
├── modules/
│   ├── increment_js.js         # sample JS reducer
│   └── increment_wasm.wat      # sample WAT reducer
└── mygame/                     # sample project directory (neondb init output)
```

---

## Architecture — Key Design Decisions

### TableStore (src/table/mod.rs)
- Internally concurrent via `DashMap` — **no Mutex wrapper needed** at the call site.
- Always passed as `Arc<TableStore>` (never `Arc<Mutex<TableStore>>`).
- `RowDelta` carries an `Arc<Bytes>` payload — cloning a delta is O(1).
- BlobStore handles large inventory arrays; small rows live inline in DashMap.
- Inner-row DashMap shard count = `max(16, next_pow2(cpus * 4))` — CPU-aware, near-zero contention under full parallelism.
- **Per-row write locks** (`row_locks: DashMap<String, Arc<Mutex<()>>>`) — acquired in sorted key order inside `apply_delta_batch()` for serializable isolation. Reads are still entirely lock-free.

### ReducerContext (src/reducer/context.rs)
- Constructor: `ReducerContext::new(Arc<TableStore>, timestamp: u64)` — **no Mutex argument**.
- Staged writes go into `pending_deltas: Vec<RowDelta>`.
- `commit()` calls `apply_delta_batch()` — the single atomic commit entry point. All deltas land or none do.
- `rollback()` drains `pending_deltas` without touching TableStore. Panicking reducers never reach `commit()` (caught by `catch_unwind` in main.rs), so TableStore is never partially mutated.
- `IncrementResult` is defined here and exported publicly — import from `crate::reducer::context`.

### WebSocket (src/network/websocket.rs)
- `SubscriptionManager` is `Arc<SubscriptionManager>` (no Mutex) — DashMap inside.
- Reducer queue: `kanal::AsyncSender<PendingCall>` — replaces old `SegQueue + sleep(50ms)`.
- Per-client write task owns the sink; all outbound frames funnel through `mpsc::unbounded_channel::<Message>()`.
- Subscription fan-out delivers `Arc<Bytes>` — one encode, zero re-encodes per subscriber.
- `start_listener` now takes `tables: Arc<TableStore>` and passes it to `subscribe_with_snapshot` so new subscribers receive initial state immediately (TODO-003).

### SubscriptionManager (src/subscriptions.rs) — SESSION 5 + SESSION 7
- **Reverse index** (`table_index: DashMap<String, DashMap<ClientId, Vec<String>>>`) — maps each table name directly to the set of (client_id, sub_id) pairs watching it.
- `publish_deltas()` now O(matching_subscribers) not O(all_subscribers).
- **Initial state sync (TODO-003)**: `subscribe_with_snapshot()` accepts `Option<&Arc<TableStore>>`. When `Some`, it immediately snapshots all currently matching rows and delivers them as `"initial_snapshot"` frames before any future deltas. Race-safe: subscription is registered in the index before the snapshot query so no delta can be missed.

### main.rs
- Uses `kanal::unbounded_async()` for the reducer channel.
- Spawns `num_cpus::get()` parallel reducer workers, each calling `tokio::task::spawn_blocking`.
- WAL is `BatchedWalWriter` (async batching, configurable interval + batch size).
- Shutdown: drops `reducer_tx` → workers drain → WAL flushed → listener/metrics notified via `watch::channel`.
- Passes `Arc<TableStore>` to `start_listener` (Session 8 fix for TODO-003 wiring).

### config.rs
- `ConfigFile` / `ConfigProject` structs exist only for TOML deserialization — their fields are marked `#[allow(dead_code)]` intentionally.
- `apply_server_section()` and `apply_env_overrides()` are extracted helpers to eliminate duplication between `from_env()` and `load_from_path()`.

---

## Dependency Notes (Cargo.toml)

| Crate | Version | Notes |
|---|---|---|
| `boa_engine` | 0.19 | `NativeFunction::from_closure` is `unsafe fn` — always call inside `unsafe {}` block |
| `boa_gc` | 0.19 | must match boa_engine |
| `wasmtime` | 21 | `store.add_fuel` → `store.set_fuel`; use `&mut *store` reborrows for typed func calls |
| `kanal` | 0.1 | MPMC async channel |
| `dashmap` | 5.5 | lock-free concurrent HashMap |
| `bytes` | 1.5 | `Arc<Bytes>` zero-copy fan-out |
| `tokio` | 1.35 | full features |
| `parking_lot` | 0.12 | BlobStore RwLock |
| `num_cpus` | 1.16 | worker count |

The `v8` crate (old C++ V8 binding) is **NOT in use** and must NOT be added. The compile.log in the repo root shows an old failed attempt. The JS engine is `boa_engine` only.

---

## Build & Test Commands

```powershell
# Unit tests (run all tests)
cargo test

# Benchmarks — three scenarios, see benches/throughput.rs for full breakdown
cargo bench

# Release build
cargo build --release

# Start server
cargo run --release -- start

# Reset criterion baseline (do this after architecture changes)
Remove-Item -Recurse -Force target\criterion
```

### Why tests show "ignored" under cargo bench

`cargo bench` compiles with `--profile bench` (optimised) and runs benchmark binaries. In bench mode, `#[test]` items are compiled but **skipped** — they show as "ignored". This is correct Rust behaviour. Run `cargo test` to execute all unit tests.

---

## Complete Fix History

### Session 1 (previous agent)

1. `src/main.rs` — rewrote server bootstrap: `Arc<TableStore>` (no Mutex), `kanal` channel, N-worker parallel dispatch, `BatchedWalWriter`, `num_cpus` parallelism.
2. `src/network/websocket.rs` — rewrote: `Arc<SubscriptionManager>` (no Mutex), `kanal::AsyncSender`, dedicated write task, `Arc<Bytes>` fan-out.
3. `src/reducer/context.rs` — new constructor `ReducerContext::new(Arc<TableStore>, u64)` removing Mutex.
4. `src/table/mod.rs` — rewrote with DashMap, `Arc<Bytes>` payloads, atomic row IDs.
5. `src/reducer/wasm.rs` — fixed `store.add_fuel` → `store.set_fuel`, fixed `&mut *store` reborrows.
6. `src/reducer/v8.rs` — wrapped both `NativeFunction::from_closure` calls in `unsafe {}`.
7. `src/reducer/context.rs` — removed unused time imports.
8. `src/table/mod.rs` — removed dead `fn now_nanos()` and its imports.
9. `src/network/websocket.rs` — removed unused `write_tx_err` variable.
10. `benches/throughput.rs` — updated to new `ReducerContext::new` signature.
11. `src/reducer/registry.rs` — updated tests to `Arc<TableStore>` directly.

### Session 2

- `src/reducer/native.rs` — removed unused `NeonDBError` import.

### Session 3

All 6 remaining warnings eliminated:

1. `src/reducer/native.rs` — removed stale `IncrementResult` import reference.
2. `src/config.rs` — `#[allow(dead_code)]` on dead fields; extracted `apply_server_section()` / `apply_env_overrides()` helpers.
3. `src/reducer/registry.rs` — `#[allow(dead_code)]` on `ModuleMetadata.runtime`.
4. `src/reducer/v8.rs` — `#[allow(dead_code)]` on `V8ReducerBackend.timeout_ms`.
5. `src/table/mod.rs` — `#[allow(dead_code)]` on `BlobStore.path`.

### Session 4 — Encode-once fan-out + CPU-aware shards

1. `src/subscriptions.rs` — `publish_deltas()` rewritten: collect matching pairs outside shard locks, sort by sub_id, encode once per unique sub_id, fan out `Arc<Bytes>` pointer clones.
2. `src/table/mod.rs` — `Table::new()` calls `optimal_row_shard_count()` = `max(16, next_pow2(cpus * 4))`.

### Session 5 — Reverse index breakthrough

**File: `src/subscriptions.rs`**

New field `table_index: DashMap<String, DashMap<ClientId, Vec<String>>>` — reverse index from table name to subscriber set. `publish_deltas()` now skips all non-matching clients at O(1) cost. `subscribe()` / `unsubscribe()` / `unregister_client()` keep it consistent. Added 6 correctness tests.

**File: `benches/throughput.rs`**

Expanded to three Criterion groups: `scenario1_pure_engine`, `scenario2_fan_out`, `scenario3_game_genres` matching the benchmark analysis in the screenshots.

### Session 6 — Test failures fixed

**Bug 1 — WAT "import after memory" parse error**

File: `src/reducer/wasm.rs`

The inline WAT string in `test_wasm_host_imports` had `(memory ...)` declared before `(import ...)`. The WebAssembly spec requires all import declarations to appear before any memory, table, or function definitions. Fixed by moving both `(import ...)` lines to the top of the module body.

**Bug 2 — `test_registry_executes_native_increment` returned `Null` for `new_value`**

File: `src/reducer/registry.rs`

Root cause: test encoded args using `rmp_serde::to_vec(&serde_json::json!({"name": "hp", "delta": 10}))` — MessagePack **map format** (string keys) — but `NativeReducerBackend::increment_reducer` expects **array format** (positional fields). Fixed by encoding with the concrete `IncrementArgs` struct directly.

### Session 7 — TODO-001, 002, 003 implemented

**TODO-001: Serializable isolation (src/table/mod.rs)**

Added `row_locks: DashMap<String, Arc<Mutex<()>>>` to each `Table`. `apply_delta_batch()` now acquires all row locks in sorted key order before writing — concurrent reducers touching the same row serialize at commit, not mid-execution. Zero extra cost for reducers touching disjoint rows.

**TODO-002: Atomicity on panic (src/table/mod.rs, src/reducer/context.rs)**

`apply_delta_batch()` is now the sole write entry point. If any delta fails, all previously-applied deltas in the batch are rolled back. Panics in the reducer executor are caught by `catch_unwind` in `main.rs` before `commit()` is called, so `pending_deltas` are simply dropped — TableStore is never partially mutated.

**TODO-003: Initial state sync on subscribe (src/subscriptions.rs, src/network/websocket.rs)**

`subscribe_with_snapshot()` added to `SubscriptionManager`. When called with `Some(&tables)`, it snapshots all currently matching rows and delivers them as `"initial_snapshot"` frames before returning. The subscription is registered in the index *before* the snapshot query to prevent missing deltas. `websocket.rs` updated to pass `&tables` when handling `ClientMessage::Subscribe`.

### Session 8 — TODO-003 wiring fix (src/main.rs)

`start_listener` signature changed in Session 7 to accept `tables: Arc<TableStore>` as a new parameter. `main.rs` was not updated to pass it — this would have caused a compile error. Fixed by passing `tables.clone()` to `start_listener` in the listener spawn block.

**Files changed**: `src/main.rs`

---

## Current Build Status

After Session 8:
- `cargo test` → **51 tests, all pass** (48 from Session 6 + 3 new TODO-003 snapshot tests).
- `cargo build --release` → zero errors, zero warnings.
- `cargo bench` → run to establish updated baselines.

---

## What Remains (Roadmap)

### 1. Two-frame protocol for subscription delivery
Move `subscription_id` out of the serialized `SubscriptionDiff` payload and into a thin routing header. With the two-frame approach:
- Encode body (table, key, op, data) ONCE per delta per table — shared `Arc<Bytes>`.
- Send a tiny 8-byte `sub_id` token frame per subscriber.
- Projected: O(1) encodes instead of O(unique_sub_ids) encodes.
- Requires client-side protocol change (easy — clients already know their sub IDs).

### 2. Columnar Table Storage
Replace per-row `HashMap<String, StoredRow>` with column-oriented arrays + SIMD scans.
File: `src/table/mod.rs`. Expected gain: eliminates hash overhead on reads.

### 3. Wasmtime JIT for JS reducers
Boa 0.19 is an AST interpreter — no JIT. For JS-heavy workloads, compile JS → WASM offline and run via Wasmtime. Keep Boa for dev-mode prototyping.

---

## Common Pitfalls for Future Agents

1. **`NativeFunction::from_closure` is `unsafe`** in Boa 0.19 — always wrap in `unsafe {}` in `src/reducer/v8.rs`.

2. **Never use `Arc<Mutex<TableStore>>`** — `TableStore` is already concurrent via DashMap. Adding a Mutex re-introduces the original bottleneck.

3. **`ReducerContext::new` signature** — `(Arc<TableStore>, u64)` — no Mutex, no third arg.

4. **Wasmtime 21 API** — `store.set_fuel()` not `store.add_fuel()`. Use `&mut *store` (reborrow) to avoid move-after-use on typed func calls.

5. **Never add the `v8` crate** — it panics on Windows (`TypeId` size assertion). The project uses `boa_engine` for JS.

6. **Boa 0.19 string API** — use `.as_string().and_then(|s| s.to_std_string().ok())`. Method `to_std_string_lossy()` does not exist.

7. **`#[allow(dead_code)]` fields** — `ConfigFile.project`, `ConfigProject.name`, `ModuleMetadata.runtime`, `V8ReducerBackend.timeout_ms`, `BlobStore.path` are all intentionally present but currently unused. Do not remove them.

8. **`table_index` must stay consistent with `clients`** — whenever you modify `subscribe()`, `unsubscribe()`, or `unregister_client()` in `subscriptions.rs`, update both `clients` (per-client sub map) and `table_index` (reverse index). Inconsistency causes silent non-delivery or ghost deliveries under concurrent load.

9. **Benchmark group names are stable** — `scenario1_pure_engine`, `scenario2_fan_out`, `scenario3_game_genres`. Do not rename them or Criterion will create duplicate baseline directories.

10. **WAT imports must come first** — In any `.wat` file or inline WAT string, ALL `(import ...)` declarations must appear before any `(memory ...)`, `(table ...)`, or `(func ...)` definitions. This is a hard WebAssembly spec requirement; violation causes a parse error "import after memory".

11. **rmp_serde struct encoding format** — `rmp_serde::to_vec` serializes Rust structs as MessagePack **array format** (positional fields, no keys). `rmp_serde::from_slice::<MyStruct>` expects array format. Never encode test args using `rmp_serde::to_vec(&serde_json::json!({...}))` for structs — that produces **map format** (string keys) which fails to deserialize into a concrete struct. Always encode with the concrete struct type: `rmp_serde::to_vec(&MyStruct { ... })`.

12. **`start_listener` signature** — `(host, port, reducer_tx, subscription_manager, tables, max_connections, api_key, active_connections, shutdown)`. The `tables: Arc<TableStore>` parameter (5th arg) is required for initial-snapshot delivery on subscribe. Always pass `tables.clone()` from `run_server`.

13. **`apply_delta_batch` is the only write path for reducers** — never call `TableStore::set_row` / `set_counter` directly from reducer code. Only `ReducerContext::commit()` → `apply_delta_batch()` gives you atomicity and isolation. The public `set_row`/`set_counter` on `TableStore` are for single-writer tests only.

14. **Row lock ordering** — `apply_delta_batch` acquires per-row locks in sorted (table_name, row_key) order. If you ever add a second batched write path, use the same sort order to avoid deadlock.
