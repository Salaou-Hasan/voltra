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
│   ├── main.rs                 # CLI (init / build / start), server bootstrap, 4 templates
│   ├── lib.rs                  # crate root, re-exports
│   ├── config.rs               # Config struct, from_env(), TOML loading, PermissionsConfig
│   ├── cli.rs                  # CLI arg parsing, parse_args_json() (PowerShell-safe)
│   ├── error.rs                # NeonDBError enum, Result alias
│   ├── schema.rs               # ColumnType, ColumnDef, TableSchema, SchemaRegistry
│   ├── migrations.rs           # apply_migrations(), add/remove/rename field ops
│   ├── subscriptions.rs        # SubscriptionManager, Predicate (AND/OR/IN/Comparison),
│   │                           #   LIMIT N, ORDER BY field ASC|DESC
│   ├── table/
│   │   └── mod.rs              # TableStore, Counter, Player, RowDelta, BlobStore
│   ├── reducer/
│   │   ├── mod.rs              # pub re-exports
│   │   ├── backend.rs          # ReducerBackend trait
│   │   ├── context.rs          # ReducerContext, increment_reducer(), caller_id, caller_role
│   │   ├── native.rs           # NativeReducerBackend
│   │   ├── registry.rs         # ReducerRegistry (auto-loads modules/)
│   │   ├── v8.rs               # Boa JS engine backend
│   │   └── wasm.rs             # Wasmtime backend
│   ├── network/
│   │   ├── mod.rs
│   │   ├── message.rs          # ClientMessage, ServerMessage, ReducerResponse
│   │   ├── protocol.rs         # MessagePack encode/decode helpers
│   │   └── websocket.rs        # WebSocket listener, handle_client(), PendingCall, permissions check
│   └── wal/
│       ├── mod.rs
│       ├── entry.rs            # WalEntry, WalHeader, WalPayload
│       ├── writer.rs           # WalWriter (sync, fsync)
│       ├── batch_writer.rs     # BatchedWalWriter (async batching)
│       ├── reader.rs           # WalReader
│       └── snapshot.rs         # SnapshotMeta, save_snapshot(), load_snapshot()
├── benches/
│   ├── throughput.rs           # criterion bench — Scenario 1/2/3
│   └── end_to_end.rs           # criterion bench — full WebSocket round-trip
├── tests/
│   └── integration.rs          # tokio integration tests (94 total)
├── modules/
│   ├── increment_js.js         # sample JS reducer
│   └── increment_wasm.wat      # sample WAT reducer
├── migrations/
│   └── README.md               # migration file format docs
├── neondb-client-ts/           # TypeScript client SDK
│   └── src/
│       ├── client.ts           # NeonDBClient — call(), call() w/ optimistic, subscribe()
│       └── types.ts            # OptimisticOptions, OptimisticCache, RowDiff, …
├── neondb-client-rust/         # Rust client SDK
│   └── src/
│       └── client.rs           # NeonDBClient — call(), call_optimistic(), subscribe()
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
- `commit()` calls `apply_delta_batch()` — the single atomic commit entry point.
- `rollback()` drains `pending_deltas` without touching TableStore.
- `pub caller_id: String` — set from `PendingCall.caller_id` in the worker loop.
- `pub caller_role: String` — set from `PendingCall.caller_role` in the worker loop.

### WebSocket (src/network/websocket.rs)
- `SubscriptionManager` is `Arc<SubscriptionManager>` (no Mutex) — DashMap inside.
- Reducer queue: `kanal::AsyncSender<PendingCall>` — replaces old `SegQueue + sleep(50ms)`.
- Per-client write task owns the sink; all outbound frames funnel through `mpsc::unbounded_channel::<Message>()`.
- Subscription fan-out delivers `Arc<Bytes>` — one encode, zero re-encodes per subscriber.
- **Auth**: `Authorization: Bearer <key>` checked at WebSocket upgrade handshake.
- **Role parsing**: `Bearer <key>:<role>` supported — role extracted and placed in `PendingCall.caller_role`.
- **Permissions check**: reducer calls checked against `PermissionsConfig` before dispatch.

### SubscriptionManager (src/subscriptions.rs)
- **Reverse index** (`table_index`) — O(matching_subscribers) publish.
- **Initial state sync**: `subscribe_with_snapshot()` delivers existing rows as `"initial_snapshot"` frames.
- **Predicate tree**: `Predicate::Comparison | In | And | Or` — full boolean expression tree.
- **LIMIT N**: `SubscriptionFilter.limit: Option<usize>` caps the initial snapshot (applied after ORDER BY sort).
- **OR operator**: `WHERE status = 'alive' OR status = 'idle'` fully supported.
- **ORDER BY**: `SubscriptionFilter.order_by: Option<OrderBy>` sorts snapshot rows before delivery; `SortDirection::Asc | Desc`; numbers numeric, strings lexicographic, missing field sorts last.

### config.rs
- `PermissionsConfig` — `HashMap<reducer_name, Vec<role>>`. Loaded from `[permissions]` TOML or `NEONDB_PERMISSIONS` env var (JSON). Used by websocket.rs to enforce per-reducer roles.

### cli.rs
- `parse_args_json()` — PowerShell-safe. Auto-detects bare unquoted words inside `[...]` (e.g. `[general, alice]` from PowerShell quote-stripping) and auto-quotes them to produce valid JSON before parsing.

### TypeScript SDK (neondb-client-ts/src/client.ts)
- **Optimistic updates**: `call(reducer, args, { optimistic: (cache) => newCache })`.
  - Snapshots cache before call, applies speculative state immediately.
  - Rolls back to snapshot on server error; calls `onRollback?` if provided.
  - Also rolls back on timeout or disconnect.
  - `OptimisticCache = Map<tableName, Map<rowKey, rowData>>`.

### Rust SDK (neondb-client-rust/src/client.rs)
- **Optimistic updates**: `call_optimistic(reducer, args, |cache| new_cache).await`.
  - `CacheSnapshot = HashMap<String, HashMap<String, serde_json::Value>>`.
  - Background connection task stores snapshot keyed by call_id; rolls back on `ReducerResponse.success == false`.

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
| `dialoguer` | 0.11 | fuzzy-select feature — interactive init UX |
| `console` | 0.15 | terminal styling for dialoguer |

The `v8` crate (old C++ V8 binding) is **NOT in use** and must NOT be added.

---

## Build & Test Commands

```powershell
# Unit tests (run all tests)
cargo test

# Benchmarks
cargo bench

# Release build
cargo build --release

# Start server
cargo run --release -- start

# Reset criterion baseline (do this after architecture changes)
Remove-Item -Recurse -Force target\criterion
```

---

## Complete Fix History

### Sessions 1–26
(See previous CLAUDE.md for full detail. Summary: TableStore, kanal channel, N-worker dispatch, BatchedWalWriter, snapshots, auth, query engine, indexes, scheduled reducers, TypeScript/Rust SDKs, schema migrations, WASM-first JS, columnar storage, end-to-end bench, templates, typed schema, React hooks.)

### Session 27 — PowerShell Args Fix + TODO-022 partial
- `parse_args_json()` auto-quotes bare words in brackets for PowerShell compatibility.
- `PermissionsConfig`, `caller_role`, and websocket permissions check all wired.

### Session 28 — TODO-022 complete wiring (main.rs)
- `Arc<PermissionsConfig>` passed to `start_listener`.
- `ctx.caller_role` set in worker loop.
- Scheduler `PendingCall` gets `caller_role: "scheduler"`.

### Session 29 — Template system redesign
- `main.rs` completely rebuilt with 4 templates: `rust/basic`, `rust/game-ready`, `rust/chat`, `typescript`.
- `neondb templates` subcommand lists all templates.

### Session 30 — TODO-022 tests + TODO-020 OR/LIMIT
- 3 permissions integration tests added (94 total).
- `Predicate::Or` + `parse_predicate()` OR support, `extract_limit()`, `SubscriptionFilter.limit`.
- 9 new unit tests in subscriptions.rs.

### Session 31 — TODO-020 ORDER BY + TODO-021 Optimistic Updates

**TODO-020 ORDER BY** (`src/subscriptions.rs`):
- `SortDirection { Asc, Desc }` enum.
- `OrderBy { field: String, direction: SortDirection }` struct.
- `SubscriptionFilter.order_by: Option<OrderBy>` field.
- `extract_order_by()` strips `ORDER BY field [ASC|DESC]` from the query before WHERE parsing.
- `subscribe_with_snapshot()` refactored: collect → sort (ORDER BY) → take (LIMIT) → deliver.
- Query clause ordering: `TABLE [WHERE pred] [ORDER BY field [ASC|DESC]] [LIMIT N]`.
- Numbers compared numerically; strings lexicographically; missing field sorts last in both directions.
- 5 new unit tests: `order_by_parses_desc`, `order_by_parses_asc_default`, `order_by_with_where_and_limit`, `order_by_desc_sorts_snapshot_numeric`, `order_by_asc_sorts_snapshot_numeric`, `order_by_desc_combined_with_limit`, `order_by_does_not_affect_live_deltas`.

**TODO-021 Optimistic Updates — TypeScript SDK** (`neondb-client-ts/src/`):
- `types.ts`: added `OptimisticCache`, `OptimisticOptions { optimistic, onRollback? }`.
- `client.ts`: `call()` now accepts optional third `OptimisticOptions` arg.
  - Pre-call: `snapshotCache()` deep-clones rowCache → `rollbackSnapshot`.
  - Applies `optimistic(rollbackSnapshot)` to live cache immediately.
  - On server error: `applyOptimisticCache(rollbackSnapshot)` + `onRollback?()`.
  - On timeout: same rollback.
  - On disconnect: `rejectAllPending()` rolls back all in-flight optimistic calls.
  - `applyOptimisticCache(cache)` and `snapshotCache()` are private helpers.

**TODO-021 Optimistic Updates — Rust SDK** (`neondb-client-rust/src/client.rs`):
- `CacheSnapshot = HashMap<String, HashMap<String, serde_json::Value>>`.
- `snapshot_dashmap_cache()` / `apply_snapshot_to_cache()` helpers.
- `Command::ApplyOptimistic` variant registers the rollback snapshot with the background task.
- `call_optimistic(reducer, args, |cache| new_cache)` — public async method.
  - Applies speculative state before sending the reducer call.
  - Background `dispatch_message()` removes snapshot on success, rolls back on failure.

---

## Current Build Status

After Session 31:
- `cargo test` → **101 tests passing** (7 new ORDER BY unit tests + existing 94).
- `cargo build --release` → zero errors, zero warnings.
- `neondb-client-rust/`: `cargo build` → zero errors, zero warnings.
- TypeScript SDK: `node --test neondb-client-ts/dist/tests/client.test.js` → **3 tests pass**.

---

## Common Pitfalls for Future Agents

1. **`NativeFunction::from_closure` is `unsafe`** in Boa 0.19 — always wrap in `unsafe {}` in `src/reducer/v8.rs`.
2. **Never use `Arc<Mutex<TableStore>>`** — `TableStore` is already concurrent via DashMap.
3. **`ReducerContext::new` signature** — `(Arc<TableStore>, u64)` — no Mutex, no third arg.
4. **Wasmtime 21 API** — `store.set_fuel()` not `store.add_fuel()`. Use `&mut *store` reborrow.
5. **Never add the `v8` crate** — it panics on Windows.
6. **Boa 0.19 string API** — use `.as_string().and_then(|s| s.to_std_string().ok())`.
7. **`#[allow(dead_code)]` fields** — don't remove `ConfigFile.project`, `ConfigProject.name`, `ModuleMetadata.runtime`, `V8ReducerBackend.timeout_ms`, `BlobStore.path`.
8. **`table_index` must stay consistent with `clients`** — update both in subscribe/unsubscribe/unregister.
9. **Benchmark group names are stable** — don't rename `scenario1_pure_engine`, `scenario2_fan_out`, `scenario3_game_genres`.
10. **WAT imports must come first** — all `(import ...)` before any `(memory ...)` / `(func ...)`.
11. **rmp_serde struct encoding** — `rmp_serde::to_vec` on a Rust struct → array format. Never use `serde_json::json!({})` for test args.
12. **`start_listener` signature** — takes `permissions: Arc<PermissionsConfig>` as 9th arg after `active_connections`. Always pass it from `run_server`.
13. **`apply_delta_batch` is the only write path** — never call `set_row`/`set_counter` directly from reducer code.
14. **Row lock ordering** — `apply_delta_batch` acquires locks in sorted (table_name, row_key) order.
15. **PowerShell args auto-quoting** — `parse_args_json()` handles `[general, alice]` → `["general", "alice"]`. Do not remove.
16. **OR vs AND precedence** — in `subscriptions.rs`, OR is parsed first (lower precedence), AND second. `A AND B OR C` = `(A AND B) OR C`. This matches SQL standard.
17. **LIMIT only affects initial snapshot** — `SubscriptionFilter.limit` is checked during `subscribe_with_snapshot()` only.
18. **ORDER BY only affects initial snapshot** — `SubscriptionFilter.order_by` sorts rows before delivery; live deltas are never reordered.
19. **ORDER BY before LIMIT** — `extract_order_by()` strips ORDER BY first; LIMIT truncation happens after sorting.
20. **Template slash paths** — template names contain `/` (e.g. `rust/basic`). The `--template` flag accepts them as-is.
21. **Optimistic cache in TS SDK** — `snapshotCache()` deep-clones (new Map per table). `applyOptimisticCache()` clears then rebuilds. Both are private. Never mutate the snapshot returned by the `optimistic` callback's input.
22. **Optimistic cache in Rust SDK** — `call_optimistic` sends `Command::ApplyOptimistic` (snapshot registration) then `Command::Call` (network frame). The background task processes them in order; `ApplyOptimistic` always arrives before `Call`'s ReducerResponse.
