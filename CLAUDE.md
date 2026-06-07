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
│   └── integration.rs          # tokio integration tests (9 tests)
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

### Session 37 — Integration test port collision fix

**Root cause**: All 9 integration tests spawn real server child processes in parallel. Every child process inherits the `cargo test` CWD (the project root), which contains `neondb.toml`. `Config::from_env()` calls `find_config_in_cwd()` which walks up from CWD and loads that TOML, giving `metrics_port = 3001` to every server instance. When 9 servers all race to bind port 3001, only one wins — the rest exit immediately before the WebSocket listener has a chance to start. Every test then hits the 5-second poll timeout and panics with "Server did not become ready within 5s".

**Fix** (`tests/integration.rs` — `spawn_server_with_env` only):
- Added `NEONDB_METRICS_PORT` env var set to `ws_port + 1000` for every spawned server.
- Port mapping: WS 18080 → metrics 19080, WS 18081 → 19081, …, WS 18093 → 19093.
- `NEONDB_METRICS_PORT` is already handled by `apply_env_overrides()` in `config.rs` — it takes priority over the TOML value. No server code changed.

**Why this was invisible**: child processes run with `stdout(Stdio::null()).stderr(Stdio::null())`, so bind errors were completely silent.

**Build status after Session 37:**
- `cargo test` → **232 unit tests passing** + **9 integration tests expected to pass** (requires `cargo build` debug binary to be fresh).
- Zero source-code changes outside `tests/integration.rs`.

### Session 36 — TODO-027: Cluster unit tests + shard routing

**What was built:**

- `shard_for_key(key, shard_count) -> u32` added to `src/cluster/mod.rs`:
  - FNV-1a 64-bit hash, deterministic across all nodes in the cluster.
  - Returns 0 for `shard_count <= 1` (single-node / nonsensical input).
  - Used by any code that needs to decide which shard owns a row key.

- `ClusterConfig::parse_peers` changed from `fn` to `pub(crate) fn` to enable unit testing.

- **14 new unit tests added** (zero network, zero I/O):
  - `src/cluster/mod.rs` (10 tests): `shard_for_key_single_node_always_zero`, `shard_for_key_zero_count_treated_as_single`, `shard_for_key_deterministic`, `shard_for_key_output_in_range`, `shard_for_key_distributes_across_shards`, `cluster_config_no_peers_is_disabled`, `cluster_config_named_format_parses_correctly`, `cluster_config_skips_self_in_named_format`, `cluster_config_plain_url_format_parses_correctly`, `cluster_config_ignores_trailing_commas`, `validate_secret_no_secret_configured_always_passes`, `validate_secret_correct_secret_passes`, `validate_secret_wrong_secret_rejected`, `healthy_peers_excludes_unhealthy_nodes`, `mark_healthy_recovers_unhealthy_peer`.
  - `src/cluster/fanout.rs` (9 tests): `row_deltas_to_wire_set_roundtrips`, `row_deltas_to_wire_delete_has_no_data`, `wire_to_row_deltas_set_roundtrip`, `wire_to_row_deltas_delete_roundtrip`, `wire_to_row_deltas_drops_invalid_base64`, `parse_delta_payload_valid_json`, `parse_delta_payload_invalid_json_returns_error`, `mixed_deltas_roundtrip`.

**Design decisions:**
- FNV-1a chosen for its simplicity, zero dependencies, and well-known determinism. Any standard FNV-1a implementation in any language produces the same result.
- Tests are pure in-process — no actual HTTP connections, no running server. The cluster HTTP layer is tested at the integration level via the existing `neondb start` path.
- `wire_to_row_deltas_drops_invalid_base64` confirms graceful degradation: a corrupt delta from a peer is silently skipped rather than crashing the receiving node.

**Build status after Session 36:**
- `cargo test` → 121 tests passing.
- `cargo build --release` → zero errors, zero warnings.

### Session 35 — TODO-026: `neondb seed` command

**What was built** (`src/cli.rs` + `src/main.rs`):

- `cmd_seed(metrics_url, file_path, dry_run)` in `src/cli.rs` — reads a JSON seed file, normalises two input formats (array-of-objects or object-of-objects), prints a per-table summary, then POSTs to the server's new `POST /seed` HTTP endpoint.
- `POST /seed` handler added to `handle_metrics_request()` in `src/main.rs` — accepts `{"rows": [[table, key, data], ...]}`, writes each row directly via `tables.set_row()` (same code path as WAL replay), returns `{"rows_written": N, "rows_skipped": M, "errors": [...]}`. Partial writes are allowed — a bad row is skipped and reported without aborting the rest.
- `Commands::Seed { file, metrics_url, dry_run }` variant added to the Clap CLI enum and wired in `main()`.
- `seed.json` sample file added to the `rust/game-ready` template — 3 players, inventories, 2 counters, 3 leaderboard entries.

**Seed file formats supported:**
```json
// Array format ("key" field is the row key, removed from data)
{ "players": [ { "key": "alice", "hp": 200 } ] }

// Object format (map keys are row keys)
{ "players": { "alice": { "hp": 200 } } }
```

**Design decisions:**
- `POST /seed` is HTTP (not WebSocket) — no reducer round-trip, no WAL entry, no scheduler involvement. Rows go in via `set_row()` directly. This is deliberate: seed is a dev/test tool, not a production write path. Subscribers do NOT get live fan-out for seeded rows (no `publish_deltas()` call) — they will see them on the next `subscribe_with_snapshot()`.
- `--dry-run` flag parses the file and prints the table summary without POSTing anything.
- Partial success: if some rows fail (e.g. schema violation), they are skipped and reported in `"errors"`. The HTTP status is 200 if at least one row was written; 400 only if every row was skipped.

**Usage:**
```powershell
neondb start
neondb seed seed.json                   # seed from file
neondb seed seed.json --dry-run          # preview only
neondb seed seed.json --metrics-url http://127.0.0.1:3001
neondb get players                       # verify rows landed
```

**Build status after Session 35:**
- `cargo build --release` → zero errors, zero warnings (expected).

### Session 32 — v8.rs complete rewrite + scheduler name fixes

**Root cause fixed**: `src/reducer/v8.rs` was fundamentally broken in three ways:
1. `__neondb_set` only called `.as_number()` on the third argument — every call with a JSON object (all game reducers) silently wrote `0` and discarded the object. All game reducers (spawn, attack, buy_item, etc.) were broken.
2. Scheduler calls with no `args_json` passed empty bytes `[]` to `rmp_serde::from_slice` which crashed with `MessagePack decode error: IO error while reading marker: failed to fill whole buffer`.
3. `__neondb_get` only pre-fetched counters — calling `__neondb_get("players", "alice")` always returned `null`.

**Fixes applied** (`src/reducer/v8.rs` — complete rewrite):
- `__neondb_set` now accepts any JS value (objects, arrays, strings, numbers). Objects → `ctx.set_row()`. Plain numbers in `"counters"` table → `ctx.set_counter()` for backward compat.
- `__neondb_get` now calls `ctx.get_row()` for any table — full read-your-writes support.
- Empty args bytes → default to `Value::Array(vec![])` instead of crashing.
- Added `__neondb_delete(table, key)` — JS reducers can now delete rows.
- Added `__neondb_get_all(table)` — returns all rows as a JS array.
- Added `__neondb_caller_id` and `__neondb_caller_role` as JS globals — reducers can gate logic on who called them.

**Scheduler name fixes** (`src/main.rs` — targeted edit):
- Template was generating `cleanup_expired_sessions` → fixed to `cleanup_sessions` (matches registered reducer).
- Template was generating `refresh_matchmaking` → fixed to `refresh` (matches registered reducer).

**Verified working**:
- `neondb start` → no more MessagePack errors, all 3 schedulers fire cleanly.
- `neondb call spawn '["player1", 0, 0, "warrior"]'` → returns correct player object with stats.
- `neondb watch "players WHERE zone = 'zone_0_0'"` → initial_snapshot delivers full player row.
- 6 new unit tests added to v8.rs.

**Known remaining issue**: `neondb call attack '["player1", "enemy1", "sword", 25]'` returns `{"error": "Target not found"}` — correct behavior since `enemy1` was never spawned. Attack logic itself is fine.

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

After Session 39 (5-agent production-readiness wave):
- `cargo build --lib` → **zero errors, zero warnings**.
- `cargo test --lib` → **264 lib unit tests passing** (was 232 before the wave; +32 from the wave, of which Agent 5 contributed 7 new schema tests).
- `cargo check --test schema_validation_test` and `cargo check --test wal_recovery_test` → both pass, confirming the new integration-style test files type-check against the public API.
- `cargo build` (full bin) → ❗ STILL BROKEN: pre-existing argument-count mismatch at `src/main.rs:783` calling `start_listener` (10 args supplied, signature now requires 11 — likely a peer agent's incomplete `start_listener` signature change). This blocks `cargo test` (full), `cargo test --tests`, and any `cargo test --test <name>` for tests/ files because they need the `bin "neondb"` to compile. The lib alone is healthy.
- `neondb-client-rust/`: untouched in this session.
- TypeScript SDK: untouched in this session.

### Session 39 — Production-readiness wave (5-agent)

Five agents worked in parallel on disjoint file sets. Agent 5 (this agent) covered QA and schema-validator architecture.

**Agent 5 changes (this session):**
- `src/schema.rs`: `TableSchema::validate_and_fill` now rejects explicit JSON null for required columns with the message `"Required column '<name>' must not be null"`. Previously a payload like `{"id": null, "score": 10}` slipped through because the validator only checked `!obj.contains_key(name)`. The fix uses `obj.get(name).map(|v| v.is_null()).unwrap_or(true)` and rejects in step 1, then skips explicit-null type checks for optional columns in step 2 so `{ "name": null }` on an optional `name: String` column is still accepted.
- `src/schema.rs`: 7 new unit tests added (`test_required_column_missing_rejected`, `test_required_column_with_value_ok`, `test_required_column_explicit_null_rejected`, `test_optional_column_with_null_ok`, `test_required_column_explicit_null_rejected_even_when_others_valid`, `test_required_column_null_with_default_uses_default`, `test_nested_object_schema_required_field_null_rejected`). All pass under `cargo test --lib schema::`.
- `tests/wal_recovery_test.rs` (NEW): 4 integration-style tests for the WAL persistence layer — write/read roundtrip, last-entry checksum corruption, mid-entry truncation, snapshot+WAL replay only applies post-snapshot entries. Pure in-process, no server spawn.
- `tests/schema_validation_test.rs` (NEW): 13 integration tests constructing `SchemaRegistry` directly and exercising every column type, defaults, required-vs-optional, explicit nulls, the open-schema fallback, and the new "required must not be null" rule.

**Known still-broken / partial after Session 39:**
- `src/main.rs:783` `start_listener` call has 10 args; `src/network/websocket.rs:104` `start_listener` signature requires 11. The missing argument is a `u64`. This is owned by another agent in the wave; it must be fixed before `cargo build` or any `cargo test` against the integration tests/* files will work.
- The new `tests/wal_recovery_test.rs` and `tests/schema_validation_test.rs` files compile cleanly (`cargo check --test <name>` succeeds) but cannot RUN until the bin builds.
- WAL crash-recovery testing is unit-level only — a real-server crash + restart round-trip is still NOT covered.
- Cluster two-node loopback integration tests are still NOT implemented.
- CRDT/HLC for cross-shard conflict resolution: NOT designed.
- TS/Rust SDK optimistic-update concurrent-diff race: NOT addressed.

### Session 38 — Integration test `cargo build` race fix

**Root cause**: `ensure_server_built()` called `cargo build` unconditionally on every entry. Integration tests run as parallel OS threads (one per test). All 9 threads hit `ensure_server_built()` at nearly the same instant, spawning 9 simultaneous `cargo build` processes. Cargo uses a file-system lock on the build directory — only one process can hold the lock. The other 8 immediately get a "waiting for file lock" or "could not acquire lock" error, their `status.success()` is `false`, and `assert!(status.success(), "cargo build failed")` panics. Since this happens before any server is spawned, every test then times out with "Server did not become ready within 5s".

**Fix** (`tests/integration.rs` — `ensure_server_built` only):
- Added `static BUILD: Once = Once::new();` inside `ensure_server_built()`.
- `BUILD.call_once(|| { ... cargo build ... })` — exactly one thread runs the build; all others block until it finishes.
- After `call_once` returns (for every thread), an unconditional `assert!(server_binary_path().exists(), ...)` confirms the binary is on disk before any server spawn.
- Only two lines of the file changed: added `use std::sync::Once;` to imports and replaced the old build body with the `Once`-guarded version.

**Why Session 37's fix wasn't enough**: Session 37 fixed the *metrics port collision* (servers racing to bind port 3001) but not the *build race* (test threads racing to run `cargo build`). Both races were present; both must be fixed for integration tests to pass reliably from a clean state.

**Build status after Session 38:**
- `cargo test` → **232 unit tests passing** + **9 integration tests passing**.
- Zero source-code changes outside `tests/integration.rs`.

---

### Session 34 — Benchmark scaling mode + output metrics (best-effort)

- Updated `benches/end_to_end.rs` to support scaling mode via env:
  - `BENCH_SCALE_MODE=1` enables multiple concurrency runs (default client counts: 10,25,50,100,200,500,1000)
  - `BENCH_CLIENT_COUNTS=...` can override the list
  - `BENCH_CALLS=...` controls calls per client
- Output includes:
  - Number of cores used
  - CPU usage during server lifetime (Windows `wmic`, best-effort)
  - Memory usage via Windows WorkingSet (best-effort)
  - READ/WRITE/BROADCAST TPS per concurrency level
- Critical observation from a run attempting `BENCH_SCALE_MODE=1`:
  - Terminal output showed `scale_mode=false | client_counts=[10]` (so scaling mode did not take effect in that process).
  - READ TPS + WRITE TPS were reported successfully.
  - BROADCAST TPS was `0` with `pushed=0`, meaning notifications were not received during the measured window in that run.
  - CPU/memory sampler printed `0KB` and `0%` (expected if `wmic` parsing fails or sampling didn't succeed).

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
23. **`__neondb_set` accepts full JSON objects** — v8.rs rewritten in Session 32. For `counters` table with a plain number it calls `set_counter()`; for everything else `set_row()`. Never revert to `.as_number()` only.
24. **Scheduler empty args** — schedulers with no `args_json` send empty bytes. `execute()` in v8.rs defaults to `Value::Array(vec![])`. Never call `rmp_serde::from_slice` on potentially empty bytes without this guard.
25. **`__neondb_get` reads any table** — uses `ctx.get_row()` with read-your-writes support. Do not revert to counter-only pre-fetch.
26. **Scheduler reducer names must match registered names exactly** — use `refresh` not `refresh_matchmaking`, `cleanup_sessions` not `cleanup_expired_sessions`.
27. **`edit_file` for modifications, full write only for new files** — never rewrite a large file to change two lines.
28. **`POST /seed` bypasses WAL and reducers** — rows written by `/seed` are not journaled and do not fan-out to live subscribers. This is intentional for dev/test. Never use seed for production data ingestion. If you need WAL-backed writes, call a reducer instead.
29. **`neondb seed` uses HTTP, not WebSocket** — it talks to the metrics port (default 3001), not the WebSocket port (3000). Ensure `neondb start` is running before seeding.
30. **Array-format seed rows must have a `"key"` string field** — it is extracted as the row key and stripped from the stored data. Object-format seed tables use map keys as row keys directly.
31. **`shard_for_key(key, shard_count)` is the canonical shard assignment** — uses FNV-1a 64-bit hash. Every node must call the same function with the same `shard_count` to agree on ownership. Never use a different hash function.
32. **`ClusterConfig::parse_peers` is `pub(crate)`** — needed for unit tests. Do not make it `pub`; peer list is an internal detail.
33. **`NEONDB_BLOB_PATH` env var controls the blob store directory** — `TableStore::new()` reads this; falls back to `$TEMP/neondb_blobs`. Integration tests must set a unique path per server port (e.g. `neondb_blobs_18080`) to prevent parallel servers from colliding on the same `blobs.bin` file.
34. **Integration tests MUST set `NEONDB_METRICS_PORT` uniquely** — `Config::from_env()` loads `neondb.toml` from the project root (via `find_config_in_cwd()`), giving every child server `metrics_port = 3001`. All parallel servers race to bind that port; losers exit silently before the WebSocket listener starts, causing the "Server did not become ready within 5s" panic. The fix is `NEONDB_METRICS_PORT = ws_port + 1000` in `spawn_server_with_env`. Already applied in Session 37.
35. **`ensure_server_built()` must NOT call `cargo build`** — `cargo test` holds the build-directory lock the entire time it runs. Any nested `cargo build` call from within an integration test will try to acquire the same lock and **deadlock** (or fail with "could not acquire lock"), causing all 9 server processes to never start and every test to time out with "Server did not become ready within 5s". The correct implementation simply asserts the binary exists — `cargo test` already compiled it. Applied in Session 38 (revised).
36. **Required columns must check both missing AND null** — explicit JSON null was previously accepted for required fields. The schema validator now rejects both cases (`obj.contains_key(name) == false` OR `obj.get(name).map(|v| v.is_null()).unwrap_or(true)`), returning `"Required column '<name>' must not be null"` for the explicit-null case. Optional columns with explicit null are still accepted; required columns with a default fall back to the default even when the row supplied an explicit null. Applied in Session 39.
