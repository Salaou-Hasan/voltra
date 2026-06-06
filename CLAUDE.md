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

### Session 9 — TODO-006 Snapshots implemented

**TODO-006: Snapshot subsystem (src/wal/snapshot.rs, src/table/mod.rs, src/config.rs, src/main.rs)**

Implemented atomic WAL-scale snapshots to bound startup replay time:

- **`src/wal/snapshot.rs`** — new file. `SnapshotMeta` header struct (`version`, `last_sequence`, `timestamp`, `row_count`, `next_row_id`). `save_snapshot()` serialises all `TableStore` rows to MessagePack and writes atomically via `.tmp` → fsync → rename. `load_snapshot()` bulk-restores rows and resets `next_row_id`. `find_latest_snapshot()` scans a directory for `neondb_snapshot_*.bin` files and returns the highest sequence. 4 new unit tests.
- **`src/table/mod.rs`** — added 4 public helper methods: `list_tables()`, `list_rows_with_keys()`, `current_next_row_id()`, `set_next_row_id()`.
- **`src/config.rs`** — added `snapshot_interval: u64` (default 1 000 000) and `snapshot_dir: PathBuf` (default `temp_dir/neondb_snapshots`). Wired into `ConfigServer` TOML fields and `NEONDB_SNAPSHOT_INTERVAL` / `NEONDB_SNAPSHOT_DIR` env vars.
- **`src/main.rs`** — startup now: (1) loads the latest snapshot from `snapshot_dir` if one exists, (2) replays only WAL entries with `sequence_number > snapshot.last_sequence`, (3) initialises `global_seq` to `max_replayed_seq + 1` (fixes a latent duplicate-seq bug present since session 1). Worker loop triggers a `tokio::task::spawn_blocking` snapshot task every `snapshot_interval` committed transactions. `recover_from_wal` now takes `min_seq: u64` and returns `(usize, u64)`.

### Session 10 — TODO-007 Auth / Identity implemented

**TODO-007: Auth & per-reducer caller identity**

- **`src/reducer/context.rs`** — added `pub caller_id: String` field to `ReducerContext`, default `String::new()`. Worker loop sets it from `PendingCall.caller_id` after construction. Reducers can read `ctx.caller_id` to identify the calling client.
- **`src/network/websocket.rs`** — added `pub caller_id: String` to `PendingCall`. During WebSocket handshake, `handle_client` extracts the `X-NeonDB-Identity` HTTP header value; falls back to TCP peer address if the header is absent. Threaded through both `PendingCall` construction sites (primary `ReducerCall` arm and legacy fallback decoder).
- **`src/main.rs`** — worker loop captures `call.caller_id` before the `spawn_blocking` closure and sets `ctx.caller_id` inside the blocking context.
- **`tests/integration.rs`** — added `spawn_server_with_env()` helper, `bearer_request()` helper (uses `IntoClientRequest` + header mutation for correct WebSocket upgrade headers), and two new integration tests: `integration_api_key_rejects_unauthorized` (verifies no-key and wrong-key connections are rejected; correct key succeeds) and `integration_no_api_key_accepts_all` (verifies open access when no `NEONDB_API_KEY` is set).

**API key auth** (already in place via `NEONDB_API_KEY` env var / `api_key` config field) is enforced at the WebSocket upgrade handshake via `Authorization: Bearer <key>` header.

**Per-connection identity** is derived from `X-NeonDB-Identity` header on the upgrade request, falling back to the TCP peer address string (`"ip:port"`). The value is available to all reducer backends as `ctx.caller_id`.

### Session 11 — TODO-004 Subscription Query Engine

**TODO-004: IN operator + AND compound predicates (src/subscriptions.rs)**

- **`Predicate`** changed from a flat struct to an enum with three variants:
  - `Predicate::Comparison { field, op, value }` — existing single-field comparisons
  - `Predicate::In { field, values }` — new: `WHERE status IN ('active', 'pending')`
  - `Predicate::And(Box<Predicate>, Box<Predicate>)` — new: `WHERE score > 100 AND level > 5`
- **`ComparisonOp::compare(actual, expected)`** — new method that encapsulates the comparison logic (moved out of the old `Predicate::matches`).
- **`Predicate::eval(delta)`** — replaces the old `matches(actual: Option<&Value>)`. Each variant evaluates recursively against the full `RowDelta`, enabling multi-field compound predicates.
- **`SubscriptionFilter::matches`** — simplified to call `predicate.eval(delta)`.
- **`subscribe_with_snapshot`** — switched from `list_rows` to `list_rows_with_keys`; builds a synthetic `RowDelta` per row so `filter.matches()` handles all variants uniformly.
- **Parser** — `parse_predicate` rewritten as a 3-step recursive descent: (1) paren-depth-aware `AND` split, (2) `IN (...)` detection, (3) fall back to `parse_comparison`.
- 6 new unit tests covering: `IN` strings, `IN` numbers, `AND` short-circuit, combined `IN AND comparison`, and two parse-shape tests.

### Session 12 — TODO-009 Secondary Indexes implemented

**TODO-009: B-tree + Hash Indexes on Tables (src/table/mod.rs)**

- **`FieldIndex` struct** — two-level concurrent set: `field_value_as_string → DashMap<row_key, ()>`. All DashMap operations are lock-free; reads add zero contention.
- **`Table.field_indexes`** — `DashMap<String, Arc<FieldIndex>>` per table; indexed fields are registered at runtime via `create_index()`.
- **`TableStore::create_index(table, field)`** — idempotent; back-fills existing rows immediately on registration.
- **`TableStore::drop_index(table, field)`** — removes the index; no-op if absent.
- **`TableStore::index_lookup(table, field, value) -> Option<Vec<String>>`** — returns `None` if no index (caller falls back to scan), or `Some(row_keys)` for O(1) equality lookup.
- **`TableStore::list_indexes(table)`** — list registered indexed fields.
- **Automatic maintenance** — `write_row_unlocked` and `delete_row_unlocked` update all registered indexes on every write/delete; old-value removal + new-value insertion are handled atomically within the row lock window.
- 5 new unit tests: basic lookup, no-index returns None, update maintenance, delete maintenance, back-fill on create.

### Session 13 — TODO-008 Scheduled Reducers implemented

**TODO-008: Scheduled Reducers (src/config.rs, src/main.rs)**

- **`src/config.rs`** — new `ScheduledReducerConfig { reducer, interval_ms, args_json }` struct. Added `scheduled_reducers: Vec<ScheduledReducerConfig>` to `Config` (default empty). Added `[[scheduler]]` TOML table-array schema (`ConfigScheduler`) and `apply_scheduler_section()` helper.
- **`src/main.rs`** — after worker handles are spawned, one async task is created per `[[scheduler]]` entry. Each task:
  - Skips the first tick (so the first real fire happens one full interval after startup)
  - Uses `tokio::time::MissedTickBehavior::Skip` to avoid burst catch-up
  - Encodes `args_json` (if present) to MessagePack before dispatch
  - Enqueues a `PendingCall` with `caller_id = "scheduler"` into the worker queue
  - Awaits the response in a detached inner task and logs failures at WARN level
  - Exits cleanly on shutdown signal
  - Scheduler call IDs use a separate namespace (`u64::MAX / 2` base) to avoid collisions with client call IDs
- Scheduler handles join during graceful shutdown (after workers drain, before WAL close)

Example `neondb.toml` usage:
```toml
[[scheduler]]
reducer = "cleanup_expired"
interval_ms = 60000

[[scheduler]]
reducer = "leaderboard_refresh"
interval_ms = 300000
args_json = "{\"top_n\": 100}"
```

### Session 14 — TODO-015 Standalone Benchmarking Tool

**TODO-015: `neondb-bench` binary (src/bin/neondb_bench.rs, Cargo.toml)**

- **`src/bin/neondb_bench.rs`** — new standalone binary that connects N concurrent WebSocket clients to a running NeonDB server, sends M reducer calls each, records per-call round-trip latencies using an HDR histogram, and prints a Markdown report. CLI flags: `--url`, `--clients`, `--calls`, `--warmup`, `--reducer`, `--counter`, `--delta`, `--api-key`, `--output`, `--timeout-ms`.
- **`Cargo.toml`** — moved `hdrhistogram = "7.5"` from `[dev-dependencies]` to `[dependencies]` (required for the binary target); added `[[bin]] name = "neondb-bench"` entry.
- Output includes: configuration table, total time, TPS, success count, failure count, success rate, and latency percentiles (p50, p75, p90, p95, p99, p99.9, max).

Usage:
```
cargo run --release --bin neondb-bench -- --clients 20 --calls 1000 --output report.md
```

---

### Session 15 — Deployment: Coolify → Dokploy migration

**Deployment platform changed from Coolify to Dokploy.**

- **`COOLIFY_DEPLOYMENT.md`** — deleted.
- **`DOKPLOY_DEPLOYMENT.md`** — new comprehensive Dokploy deployment guide. Covers: Option A (Git-connected build via Dokploy dashboard), Option B (Docker Compose with Traefik labels), environment variables (including new snapshot/auth vars), volume mounts, domain + auto-TLS via Traefik, performance tuning profiles, sharded multi-node topology, monitoring, troubleshooting, and security.
- **`SELF_HOSTED_SETUP.md`** — rewritten for Dokploy (replaces old Coolify/WSL2 guide).
- **`DEPLOYMENT.md`** — Coolify section replaced with Dokploy section; new `### Snapshot Configuration` env var docs added.
- **`PRODUCTION_READY.md`** — all Coolify references updated to Dokploy; test count updated to 79.
- **`PHASE_0_PLANNING.md`** — Coolify → Dokploy throughout.
- **`Dockerfile`** — pinned to `rust:1.78-slim`; added build deps (`pkg-config`, `libssl-dev`); copies `modules/` into runtime image; creates `/data/snapshots`; adds `NEONDB_SNAPSHOT_INTERVAL` and `NEONDB_SNAPSHOT_DIR` env vars; bumps `NEONDB_MAX_CONNECTIONS` to 200.
- **`docker-compose.yml`** — split `neondb-data` into `neondb-wal` + `neondb-snapshots` volumes; adds snapshot env vars; adds commented-out Traefik labels for Dokploy Option B.

---

### Session 16 — TODO-011 TypeScript Client SDK

**TODO-011: `@neondb/client` TypeScript package (neondb-client-ts/)**

- **`neondb-client-ts/src/types.ts`** — `NeonDBClientOptions`, `ReducerResult`, `SubscriptionAck`, `RowDiff`, `SubscriptionRouteData`, `SubscriptionBodyData`, `SubscriptionCallback`, `Subscription`, `RowCache`.
- **`neondb-client-ts/src/protocol.ts`** — MessagePack encode/decode helpers: `encodeReducerCall`, `encodeSubscribe`, `encodeUnsubscribe`, `encodeArgs`, `decodeServerMessage` (handles bare `ReducerResponse` array, `SubscriptionDiff`, `SubscriptionRoute`, `SubscriptionBody`, `SubscriptionAck`, `Error`).
- **`neondb-client-ts/src/client.ts`** — `NeonDBClient` class: async `connect()` / `disconnect()`, `call(reducer, args)`, `subscribe(query, callback)`, local row cache (`getRows` / `getRow`), `onConnected` / `onDisconnected` / `onError` hooks, auto-reconnect with full re-subscription, Node.js API-key auth via `Authorization: Bearer` header (ESM dynamic `import("ws")`).
- **`neondb-client-ts/src/tests/client.test.ts`** — 3 Node.js built-in tests using a mock `WebSocketServer`: auth header test, subscription diff + row cache test, auto-reconnect re-subscription test.
- **`neondb-client-ts/package.json`** — `@neondb/client 0.1.0`, ESM module, `@msgpack/msgpack` dependency, `ws` peer dependency.
- **`neondb-client-ts/README.md`** — full API reference and wire-protocol documentation.

All 3 TypeScript tests pass: `node --test dist/tests/client.test.js`.

---

### Session 17 — TODO-013 Two-Frame Subscription Protocol

**TODO-013: Two-Frame Protocol (src/subscriptions.rs, src/network/message.rs, src/network/websocket.rs)**

The server-side implementation was already complete in a prior session (noted here for clarity):

- **`src/network/message.rs`** — added `SubscriptionRoute { subscription_ids: Vec<String> }` and `SubscriptionBody { table_name, row_key, operation, row_data }` structs, plus `ServerMessage::SubscriptionRoute` and `ServerMessage::SubscriptionBody` enum variants.
- **`src/subscriptions.rs`** — `OutboundFrames` enum (`One(Arc<Bytes>)` or `Two { first, second }`). `SubscriptionManager::new_with_options(two_frame: bool)` constructor. `publish_deltas()` in two-frame mode: encodes body ONCE per delta (O(1)), sends a tiny route frame per client listing all matching subscription IDs (O(clients)). `subscribe_with_snapshot()` delivers initial-snapshot frames in two-frame format. Test: `two_frame_protocol_groups_route_and_body`.
- **`src/network/websocket.rs`** — subscription write task sends `OutboundFrames::Two` as two separate WebSocket binary frames atomically.
- **`src/config.rs`** — added `two_frame_protocol: bool` (default `false`, env: `NEONDB_TWO_FRAME_PROTOCOL=1`).
- **`src/main.rs`** — `SubscriptionManager::new_with_options(config.two_frame_protocol)`.
- **Client-side**: `neondb-client-ts` handles both legacy `SubscriptionDiff` and two-frame `SubscriptionRoute`/`SubscriptionBody` — transparent to the caller.

---

### Session 18 — Project state audit + TypeScript source sync

Discovered that `neondb-client-ts/dist/` was built from a more complete version of the TypeScript source than what existed in `src/`. Synced `src/client.ts` and `src/protocol.ts` to match the compiled dist (the dist was the ground truth):

- **`src/client.ts`** — subscriptions now stored as `Map<id, { query, callback }>` so re-subscription on reconnect works; `openSocket()` is `async` with ESM dynamic import; `handleFrame` handles `SubscriptionRoute` and `SubscriptionBody`; `onmessage` handles both `ArrayBuffer` and `ArrayBufferView` (Node.js Buffer).
- **`src/protocol.ts`** — `decodeServerMessage` handles `SubscriptionRoute` and `SubscriptionBody`; `DecodedMessage` union includes all variants.
- **`src/types.ts`** — added `SubscriptionRouteData` and `SubscriptionBodyData` interfaces.
- Rebuilt with `npm run build` — zero TypeScript errors. All 3 tests still pass.

---

### Session 19 — TODO-012 Rust Client SDK

**TODO-012: `neondb-client` Rust crate (neondb-client-rust/)**

- **`neondb-client-rust/src/lib.rs`** — public re-exports, quick-start doc comment.
- **`neondb-client-rust/src/types.rs`** — wire types (`ReducerCall`, `ReducerResponse`, `ServerMessage`, `ClientMessage`, `SubscriptionRoute`, `SubscriptionBody`) and client API types (`ClientOptions`, `RowDiff`, `RowCache`).
- **`neondb-client-rust/src/protocol.rs`** — `encode_client_message`, `decode_server_frame` (handles bare array `ReducerResponse` + `ServerMessage` enum variants), `encode_args`, `decode_result`.
- **`neondb-client-rust/src/client.rs`** — `NeonDBClient` with `connect()`, `call()`, `subscribe()`, `get_rows()`, `get_row()`, `disconnect()`; background `run_connection` task with `tokio::select!` loop; two-frame protocol (`SubscriptionRoute`/`SubscriptionBody`) support; `Subscription` handle with channel-based diff delivery.
- API key auth via `Authorization: Bearer` header at WebSocket upgrade time.
- Standalone crate — own `Cargo.toml`, NOT a workspace member; builds with zero errors and zero warnings.

---

### Session 20 — TODO-010 Schema Migrations implemented

**TODO-010: Schema Migration Support (src/migrations.rs)**

- **`src/migrations.rs`** — new module. `apply_migrations(dir, tables)` scans `migrations/*.toml` sorted lexicographically, parses each file, and applies steps. Three idempotent operations:
  - `add_field` — adds a field with a default value to rows that are missing it (skips rows that already have the field)
  - `remove_field` — removes a field from rows that have it
  - `rename_field` — renames `old_field` to `new_field` in rows that have the old name
- **`src/lib.rs`** — added `pub mod migrations;`.
- **`src/main.rs`** — migrations applied at startup, after snapshot + WAL recovery, before workers start. Errors are logged as `WARN` (non-fatal, consistent with WAL/snapshot failure handling).
- **`migrations/README.md`** — new directory with format documentation.
- 6 new unit tests: add field, skip existing, remove field, rename field, empty-dir no-op, full TOML file apply.

---

### Session 21 — TODO-005 JS Runtime Improvement

**TODO-005: WASM-first JS loading + `neondb build` command**

Implemented Option A from the TODO: JS reducers now transparently upgrade to Wasmtime JIT when a pre-compiled `.wasm` companion file exists.

- **`src/reducer/registry.rs`** — `register_module()` now checks if a `.wasm` file with the same stem exists alongside a `.js` file. If found, the WASM version is loaded via the existing Wasmtime runtime (Cranelift JIT, ~10–50× faster). Falls back to Boa for `.js` files without a WASM companion.
- **`src/main.rs`** — `Commands::Build { modules_dir }` is now functional: scans `modules/` for `.js` files and invokes `javy compile <file>.js -o <file>.wasm` for each. Prints install instructions if `javy` is not on PATH.

Workflow for production: `neondb build` → produces `.wasm` files → `neondb start` picks them up automatically.

---

### Session 22 — TODO-014 Columnar Table Storage (Practical)

**TODO-014: Columnar read API on TableStore (src/table/mod.rs)**

Added 5 columnar-access methods that provide column-oriented performance patterns on top of the existing row-oriented DashMap storage:

- **`scan_column(table, field)`** — returns `(row_key, field_value)` pairs sorted by key, decoding only the requested field per row (avoids full row decode).
- **`count_by_field(table, field)`** — groups rows by field value → count. Useful for analytics.
- **`distinct_field_values(table, field)`** — returns all unique values of a field (de-duplicated via `BTreeSet`).
- **`count_matching(table, field, value)`** — uses the secondary index (O(1)) if registered; falls back to `scan_column` (O(n)).
- **`total_row_count()`** — sums row counts across all tables.
- 5 new unit tests.

---

### Session 23 — TODO-016 End-to-End Benchmark

**TODO-016: End-to-End WebSocket Benchmark (benches/end_to_end.rs, tests/integration.rs)**

- **`benches/end_to_end.rs`** — fully rewritten. Now auto-spawns the NeonDB release binary on port 19000, waits for it to be ready, runs N concurrent WebSocket clients with warmup + benchmark phases, and prints TPS + latency percentiles (p50/p90/p95/p99/p99.9/max). Respects `WS_URL` env var to connect to an external server instead.
- **`tests/integration.rs`** — added `integration_e2e_throughput_benchmark` test marked `#[ignore]`. Spawns the server, runs 5 clients × 100 calls, asserts 100% success rate and > 100 TPS. Run with: `cargo test -- --include-ignored`.

---

## Current Build Status

After Session 23:
- `cargo test` → **85 unit + 6 integration (1 ignored) = 91 tests, all pass**.
- `cargo build --release` → zero errors, zero warnings.
- `cargo build --bench end_to_end` → end-to-end benchmark binary builds cleanly.
- `cargo build --bin neondb-bench` → standalone benchmark binary builds cleanly.
- `neondb-client-rust/`: `cargo build` → zero errors, zero warnings.
- TypeScript SDK: `node --test neondb-client-ts/dist/tests/client.test.js` → **3 tests pass**.

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
