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
│   ├── subscriptions.rs        # SubscriptionManager, Predicate (AND/OR/IN/Comparison), LIMIT
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
- **LIMIT N**: `SubscriptionFilter.limit: Option<usize>` caps the initial snapshot. Live deltas are never limited.
- **OR operator**: `WHERE status = 'alive' OR status = 'idle'` fully supported.

### config.rs
- `PermissionsConfig` — `HashMap<reducer_name, Vec<role>>`. Loaded from `[permissions]` TOML or `NEONDB_PERMISSIONS` env var (JSON). Used by websocket.rs to enforce per-reducer roles.

### cli.rs
- `parse_args_json()` — PowerShell-safe. Auto-detects bare unquoted words inside `[...]` (e.g. `[general, alice]` from PowerShell quote-stripping) and auto-quotes them to produce valid JSON before parsing.

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
- Each template: JS reducers, TypeScript client, schema.toml, neondb.toml with permissions, README.
- `rust/game-ready` adds `GENRE_GUIDE.md` explaining how to adapt to any game genre.
- `neondb templates` subcommand lists all templates with categories and descriptions.

### Session 30 — TODO-022 integration tests + TODO-020 OR/LIMIT (partial)

**TODO-022 — 3 integration tests** (`tests/integration.rs`):
- `integration_permissions_unauthorized_call_rejected` — caller with empty role cannot call a restricted reducer.
- `integration_permissions_authorized_call_passes` — caller with `Bearer key:admin` can call an admin-only reducer.
- `integration_permissions_open_reducer_always_allowed` — unrestricted reducers pass regardless of role.
- Total integration tests: **94** (up from 91).

**TODO-020 partial — OR predicate + LIMIT N** (`src/subscriptions.rs`):
- `Predicate::Or(Box<Predicate>, Box<Predicate>)` added to the predicate enum.
- `parse_predicate()` now tries OR first (lowest precedence), then AND, then IN, then comparison.
- Precedence: `AND > OR` — so `A AND B OR C` parses as `(A AND B) OR C`.
- `extract_limit()` strips a trailing `LIMIT N` from the query string.
- `SubscriptionFilter.limit: Option<usize>` — caps initial snapshot delivery only.
- 9 new unit tests covering OR matching, precedence, LIMIT parsing, LIMIT=0, live delta not limited.
- **Remaining for TODO-020**: `ORDER BY field ASC|DESC` on snapshot, `JOIN` across two tables.

---

## Current Build Status

After Session 30:
- `cargo test` → **94 tests passing** (3 new permissions integration tests + 9 new subscriptions unit tests).
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
17. **LIMIT only affects initial snapshot** — `SubscriptionFilter.limit` is checked during `subscribe_with_snapshot()` only. Live deltas from `publish_deltas()` are never limited.
18. **Template slash paths** — template names contain `/` (e.g. `rust/basic`). The `--template` flag accepts them as-is; the `init_project` function dispatches on the full string.
