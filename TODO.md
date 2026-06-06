# NeonDB — TODO & Roadmap
# Agent Handoff: Gap Analysis vs SpacetimeDB

**Last Updated**: 2026-06-06 (Session 23)
**Current Build**: 91 tests passing, zero warnings, ~2.9M raw TPS (in-process benchmark)

Read CLAUDE.md before touching any file. This document translates the SpacetimeDB gap analysis
into concrete, prioritized tasks for the next agent(s) to execute.

---

## Where We Stand Right Now

Sessions 1–8 built a solid, correct, fast engine:

- Lock-free DashMap-backed TableStore with CPU-aware sharding
- Arc<Bytes> zero-copy subscription fan-out with reverse index (O(matching subscribers))
- BatchedWalWriter (async, configurable batch + fsync interval)
- Three reducer runtimes: Native Rust, Boa JS (0.19), Wasmtime WASM
- **Serializable isolation**: per-row write locks acquired in sorted order inside apply_delta_batch()
- **Atomicity on panic**: catch_unwind before commit(); rollback on batch failure
- **Initial state sync**: subscribe_with_snapshot() delivers existing rows as "initial_snapshot" frames
- 51 unit + integration tests, all passing

---

## CRITICAL GAPS — DONE ✅

### TODO-001 — Serializable Isolation ✅ DONE (Session 7)
Per-row Mutex inside each Table. `apply_delta_batch()` acquires all locks in sorted (table, key)
order. Concurrent reducers touching the same row serialize at commit. Zero cost for disjoint rows.
Test: `test_serializable_isolation_no_lost_updates` — 2 threads × 500 increments = 1000 exact.

### TODO-002 — Atomicity on Panic ✅ DONE (Session 7)
`apply_delta_batch()` is the sole write entry point. Failed batches roll back all applied deltas.
Panics are caught by `catch_unwind` in main.rs before `commit()` — TableStore never partially mutated.
Test: `test_atomic_batch_rollback_on_error`.

### TODO-003 — Initial State Sync on Subscribe ✅ DONE (Session 7+8)
`subscribe_with_snapshot()` on SubscriptionManager snapshots all matching rows and delivers them
as "initial_snapshot" frames before returning. Registered in index BEFORE snapshot to avoid missing
concurrent deltas. `websocket.rs` passes `&tables` on subscribe. `main.rs` passes `tables.clone()`
to `start_listener` (Session 8 wiring fix).
Tests: `initial_snapshot_delivered_on_subscribe`, `initial_snapshot_respects_predicate`,
`subscribe_without_tables_sends_no_snapshot`.

---

## HIGH PRIORITY (Next to tackle)

### TODO-006 — Snapshots (WAL-only recovery gets slow at scale)
**Status**: ✅ DONE (Session 9)
**SpacetimeDB has**: Automatic snapshot every 1M transactions. On restart, loads latest snapshot
then replays only the WAL suffix after it.
**We have**: WAL replay from the beginning. Fine for dev; slow at production scale
(10GB WAL at 300K TPS = ~33 seconds replay, and it only grows).

**What to build**:
- Snapshot format: serialize all TableStore contents to a single MessagePack file at a
  stable path (e.g., `neondb_snapshot_{seq}.bin`).
- Trigger: every N transactions (configurable via `NEONDB_SNAPSHOT_INTERVAL`, default 1_000_000).
- Recovery: on startup, find the latest valid snapshot file, load it, then replay only
  WAL entries with sequence_number > snapshot.last_sequence.
- Atomic snapshot: write to a temp file, fsync, rename — never leave a partial snapshot.

**Files to modify**: `src/wal/`, `src/table/mod.rs`, `src/main.rs`
**Tests to add**: Crash after snapshot + N more writes → recovery restores all N+snapshot rows.

---

### TODO-007 — Auth / Identity
**Status**: ✅ DONE (Session 10)
**SpacetimeDB has**: OIDC, per-reducer identity.
**We have**: No authentication. Any client can call any reducer.

**Minimum viable auth for production**:
- API key auth: clients send `Authorization: Bearer <key>` in the WebSocket upgrade HTTP headers.
- Server reads `NEONDB_API_KEY` from env; if set, reject connections without a matching key.
- Per-reducer identity: expose `caller_id: String` in ReducerContext, derived from the API key
  or a client-provided identity token.

**Files to modify**: `src/network/websocket.rs`, `src/reducer/context.rs`, `src/config.rs`

---

### TODO-004 — Subscription Query Engine (SQL-style predicates)
**Status**: ✅ DONE (Session 11) — single comparison, IN operator, AND compound predicates
**SpacetimeDB has**: Type-safe query builder; SQL-based subscription queries; supports JOINs,
`IN`, multi-column predicates, and incremental eval_incr() for delta evaluation.
**We have**: Simple predicate parser. No `IN`, no JOINs, no multi-column WHERE.

**What to build** (in priority order):
1. Add `IN` operator: `WHERE status IN ('active', 'pending')`
2. Add multi-column predicates: `WHERE score > 100 AND level > 5`
3. Incremental delta evaluation: when a delta arrives, evaluate the subscription predicate
   against ONLY the changed rows (not re-scan everything). This is what SpacetimeDB calls
   `eval_incr` and is critical for scale.

**Files to modify**: `src/subscriptions.rs` (predicate parser and matcher)
**Tests to add**: IN operator test, multi-column WHERE test, delta-only evaluation test.

---

### TODO-005 — Replace Boa with V8 or Wasmtime for JS Reducers
**Status**: ✅ DONE (Session 21) — WASM-first loading (js → wasm auto-upgrade); neondb build invokes javy compiler; Boa kept for dev prototyping
**SpacetimeDB TypeScript**: 303K TPS (full JIT via V8 threading improvements).
**We have**: Boa 0.19 — AST interpreter, NO JIT. JS reducers are 10–50x slower than theirs
for compute-heavy logic.

**Options** (pick one):
- **Option A (Recommended)**: Compile JS/TS reducers to WASM offline, run them through
  the existing Wasmtime path. Keep Boa only for dev-mode prototyping.
- **Option B**: Integrate rusty_v8 / Deno Core for a full JIT JS path.

**CRITICAL PITFALL**: Never add the `v8` crate (C++ binding) — it panics on Windows.

---

## MEDIUM PRIORITY (Production Readiness)

### TODO-009 — B-tree + Hash Indexes on Tables
**Status**: ✅ DONE (Session 12) — lock-free DashMap two-level set per field, O(1) lookup, auto-maintained on write/delete
Range queries or non-PK lookups require full table scan. Add secondary BTreeMap index per
column. Must be done AFTER TODO-001 (isolation) — already done, so this is unblocked.

**Files to modify**: `src/table/mod.rs`

---

### TODO-008 — Scheduled Reducers
**Status**: ✅ DONE (Session 13) — [[scheduler]] TOML config, one async task per entry, MissedTickBehavior::Skip, args_json support, graceful shutdown
Add `[scheduler]` section to config: `{ reducer: "cleanup_expired", interval_ms: 60000 }`.
Spawn a scheduler task that fires `PendingCall` into the reducer queue on interval.

**Files to modify**: `src/main.rs`, `src/config.rs`, `src/reducer/registry.rs`

---

### TODO-010 — Schema Migration Support
**Status**: ✅ DONE (Session 20) — migrations/*.toml files; add_field / remove_field / rename_field; idempotent; applied after WAL replay; 6 unit tests
Migration file format with ordered `.migration.toml` files. Phase 1: add/rename/drop columns.

---

## LOW PRIORITY (Client SDK)

### TODO-011 — TypeScript Client SDK
**Status**: ✅ DONE (Session 16) — NeonDBClient class, local row cache, auto-reconnect, API key auth, full two-frame + legacy protocol support, 3 unit tests
**Planned location**: `neondb-client-ts/`
`NeonDBClient` class, local row cache, React hooks, MessagePack protocol.

### TODO-012 — Rust Client SDK
**Status**: ✅ DONE (Session 19) — neondb-client-rust/ crate: NeonDBClient, call(), subscribe() channels, two-frame protocol, API key auth, row cache
**Planned location**: `neondb-client-rust/`

### TODO-013 — Two-Frame Protocol for Subscription Delivery
**Status**: ✅ DONE (Session 17) — server encodes body ONCE per delta, route frame per client; opt-in via NEONDB_TWO_FRAME_PROTOCOL=1; client handles both protocols transparently
Encode delta body ONCE, send tiny 8-byte sub_id token frame per subscriber. Breaking protocol
change — coordinate with client SDK work (TODO-011).

**Files to modify**: `src/subscriptions.rs`, `src/network/websocket.rs`

### TODO-014 — Columnar Table Storage (Performance)
**Status**: ✅ DONE (Session 22) — scan_column, count_by_field, distinct_field_values, count_matching (index-accelerated), total_row_count; 5 unit tests
Replace per-row HashMap with column-oriented arrays + SIMD scans. Do AFTER indexes (TODO-009).

---

## BENCHMARKING & TOOLING

### TODO-015 — Standalone Benchmarking Tool (Phase 6 Deliverable)
**Status**: ✅ DONE (Session 14) — src/bin/neondb_bench.rs: N clients, M calls, HDR histogram, p50/p95/p99, Markdown report, --output flag, --api-key support
Standalone `neondb-bench` Rust binary: N concurrent WebSocket clients, M calls each, p50/p95/p99
latency + TPS, markdown report. Required by PHASE_0_PLANNING.md Phase 6 acceptance criteria.

### TODO-016 — End-to-End Benchmark (WebSocket round-trip)
**Status**: ✅ DONE (Session 23) — end_to_end.rs auto-spawns server; #[ignore] integration_e2e_throughput_benchmark; WS_URL override for external server
`benches/end_to_end.rs` exists. Add a test harness that starts the server in a background thread.

---

## NEW GAPS — Added Session 25 (vs SpacetimeDB feature parity)

### TODO-018 — Typed Schema System
**Status**: ✅ DONE (Session 26)
**SpacetimeDB has**: Strongly-typed tables defined in code — columns have explicit types (`u64`, `String`, `bool`), primary keys declared, server enforces them, client bindings generated from schema.
**We have**: Schema-free tables — rows are raw JSON blobs (`HashMap<String, Value>`). Anything goes in, no validation, no type enforcement.

**What to build**:
- `schema.toml` file format: define tables with named columns + types (`u64`, `i32`, `String`, `bool`, `f64`, `bytes`)
- Primary key declaration per table
- Server validates row writes against schema on every `set_row` call
- `neondb init` scaffolds a starter `schema.toml` alongside `neondb.toml`
- Schema loaded at startup, stored in `TableStore`
- Migration support already exists (migrations/*.toml) — wire schema changes through it

**Files to create/modify**: `src/schema.rs` (new), `src/table/mod.rs`, `src/main.rs`, `src/cli.rs`
**Tests to add**: type enforcement rejects wrong types, primary key uniqueness, schema load from file.

---

### TODO-019 — React Hooks in TypeScript SDK
**Status**: ✅ DONE (Session 26)
**SpacetimeDB has**: `useSpacetimeDBQuery()`, `useReducer()` — type-safe React hooks that auto-subscribe and re-render on data changes.
**We have**: Bare `NeonDBClient` class. Developers must wire subscriptions to React state manually.

**What to build**:
- `neondb-client-ts/src/hooks.ts` — `useNeonDBQuery(query)`, `useNeonDBReducer(name)` hooks
- `useNeonDBQuery(query)` returns `{ rows, loading, error }` and re-renders when matching rows change
- `useNeonDBReducer(name)` returns `[call, { loading, error }]` — fires the reducer and tracks status
- `NeonDBProvider` context component wraps the app with a shared client instance
- Works with React 18 (hooks + concurrent mode safe)

**Files to create**: `neondb-client-ts/src/hooks.ts`, `neondb-client-ts/src/context.ts`
**Tests to add**: hook re-renders on subscription diff, reducer call updates loading state.

---

### TODO-020 — OR / JOIN / ORDER BY / LIMIT in Subscription Queries
**Status**: ❌ NOT STARTED
**SpacetimeDB has**: Full SQL-style subscription queries including `OR`, `JOIN` across tables, `ORDER BY`, `LIMIT`.
**We have**: `WHERE field op value`, `IN (...)`, `AND` — no `OR`, no joins, no ordering, no limits.

**What to build** (in priority order):
1. `OR` operator: `WHERE status = 'active' OR level > 10`
2. `LIMIT N`: cap the number of rows delivered in initial snapshot and diffs
3. `ORDER BY field ASC|DESC`: sort initial snapshot rows (not applicable to live diffs)
4. `JOIN` across two tables: `players JOIN scores ON players.id = scores.player_id` (hardest)

**Files to modify**: `src/subscriptions.rs` (predicate parser + matcher)
**Tests to add**: OR short-circuit, LIMIT on snapshot, ORDER BY sort order.

---

### TODO-021 — Optimistic Updates in Client SDKs
**Status**: ❌ NOT STARTED
**SpacetimeDB has**: Client immediately updates local cache before server confirms, then reconciles on server response. UI feels instant.
**We have**: Both SDKs wait for the server ack before updating cache. Adds round-trip latency to every UI interaction.

**What to build**:
- `call(reducer, args, { optimistic: (cache) => newCache })` API in both SDKs
- Client applies the optimistic function to local cache immediately
- On server success: reconcile with real server data (usually a no-op)
- On server error: roll back optimistic change, expose error to caller
- Works transparently with `useNeonDBQuery` (TODO-019) for instant UI updates

**Files to modify**: `neondb-client-ts/src/client.ts`, `neondb-client-rust/src/client.rs`
**Tests to add**: optimistic apply + rollback on error, reconcile on success.

---

### TODO-022 — Per-Reducer Permissions / Role-Based Auth
**Status**: ❌ NOT STARTED
**SpacetimeDB has**: OIDC tokens, per-user identity, per-reducer access control.
**We have**: Single global API key — any authenticated client can call any reducer.

**What to build**:
- `[permissions]` section in `neondb.toml`: map reducer names to required roles
- Roles assigned to connections at handshake via JWT or extended Bearer token (`Bearer <key>:<role>`)
- Server checks role before dispatching reducer call; rejects with 403-equivalent error if unauthorized
- `ctx.caller_role: String` available inside reducers (alongside existing `ctx.caller_id`)
- Example: `admin` role can call `delete_player`, `user` role can only call `increment`

**Files to modify**: `src/config.rs`, `src/network/websocket.rs`, `src/reducer/context.rs`, `src/main.rs`
**Tests to add**: unauthorized call rejected, authorized call passes, role available in ctx.

---

### TODO-023 — Project Templates
**Status**: ✅ DONE (Session 26)
**SpacetimeDB has**: Starter templates for common game patterns (chat, MMO movement, leaderboard, turn-based game).
**We have**: `neondb init <name>` creates a minimal project with one sample JS reducer. No domain-specific templates.

**What to build**:
- `neondb init <name> --template <template>` flag
- Built-in templates:
  - `blank` (default) — current behavior: neondb.toml + hello.js
  - `chat` — rooms table, messages table, send_message reducer, join_room reducer, TS client example
  - `leaderboard` — scores table, submit_score reducer, get_top_n reducer, scheduled reset reducer
  - `mmo` — players table, move reducer, attack reducer, subscription by zone/area
  - `turn-based` — games table, players table, make_move reducer with turn validation
- Each template ships as an embedded directory in the binary (use `include_dir!` macro or hand-coded strings)
- `neondb init my-game --template chat` scaffolds the full working example
- `neondb templates` CLI command lists available templates with descriptions

**Files to modify**: `src/main.rs` (init_project + new templates subcommand)
**New dependency**: `include_dir` crate for embedding template files in the binary
**Tests to add**: each template scaffolds without error, server starts from scaffolded dir.

---

### TODO-024 — C# / Unity SDK
**Status**: ❌ NOT STARTED — LOW PRIORITY
**SpacetimeDB has**: Full C# SDK targeting Unity — their primary game dev market.
**We have**: TypeScript and Rust SDKs only.

**What to build**:
- `neondb-client-csharp/` — standalone C# library targeting .NET Standard 2.1 (Unity compatible)
- `NeonDBClient` class: `Connect()`, `Call()`, `Subscribe()`, `Disconnect()`
- MessagePack encode/decode (use `MessagePack-CSharp` library)
- WebSocket via `ClientWebSocket` (.NET built-in)
- Local row cache as `Dictionary<string, Dictionary<string, object>>`
- Unity-friendly: no `async/await` in hot path, uses callbacks + `UnityMainThreadDispatcher`

**Files to create**: `neondb-client-csharp/` directory, full SDK
**Note**: Tackle after TODO-019 (React hooks) since C# follows the same SDK patterns.

---

## DEPLOYMENT

### TODO-017 — Dokploy Deployment
**Status**: ✅ DOCKER FILES UPDATED — needs live deployment test
Files: `Dockerfile`, `docker-compose.yml`, `DEPLOYMENT.md`, `DOKPLOY_DEPLOYMENT.md`, `SELF_HOSTED_SETUP.md`
Remaining: push to Git repo, connect to Dokploy, deploy image on Linux VPS, run test client from outside container. Deployment guide: DOKPLOY_DEPLOYMENT.md.

---

## EXECUTION ORDER (Recommended for Next Agent)

```
── DONE ──────────────────────────────────────────────────────────
 1. TODO-001  Serializable isolation          ✅ DONE (Session 7)
 2. TODO-002  Atomicity on panic              ✅ DONE (Session 7)
 3. TODO-003  Initial state sync              ✅ DONE (Session 7+8)
 4. TODO-006  Snapshots                       ✅ DONE (Session 9)
 5. TODO-007  Auth (API key + caller_id)      ✅ DONE (Session 10)
 6. TODO-004  Subscription query engine       ✅ DONE (Session 11)
 7. TODO-009  Indexes                         ✅ DONE (Session 12)
 8. TODO-008  Scheduled reducers              ✅ DONE (Session 13)
 9. TODO-015  Benchmarking tool               ✅ DONE (Session 14)
10. TODO-011  TypeScript SDK                  ✅ DONE (Session 16)
11. TODO-013  Two-frame protocol              ✅ DONE (Session 17)
12. TODO-012  Rust SDK                        ✅ DONE (Session 19)
13. TODO-010  Schema migrations               ✅ DONE (Session 20)
14. TODO-005  JS runtime (WASM-first)         ✅ DONE (Session 21)
15. TODO-014  Columnar storage API            ✅ DONE (Session 22)
16. TODO-016  End-to-end bench                ✅ DONE (Session 23)

── REMAINING ─────────────────────────────────────────────────────
17. TODO-017  Dokploy live deploy             ← ship it (your task, not code)
18. TODO-023  Project templates               ✅ DONE (Session 26)
19. TODO-018  Typed schema system             ✅ DONE (Session 26)
20. TODO-019  React hooks (TS SDK)            ✅ DONE (Session 26)
21. TODO-022  Role-based auth / permissions   ← production security
22. TODO-020  OR / JOIN / LIMIT queries       ← query completeness
23. TODO-021  Optimistic updates (SDKs)       ← UX quality
24. TODO-024  C# / Unity SDK                  ← low priority, big effort
```

---

## AGENT PITFALL REMINDERS

- `NativeFunction::from_closure` in Boa 0.19 is `unsafe fn` — always wrap in `unsafe {}` in `src/reducer/v8.rs`.
- NEVER use `Arc<Mutex<TableStore>>` — TableStore is concurrent via DashMap. Mutex re-introduces the bottleneck.
- `ReducerContext::new` signature is `(Arc<TableStore>, u64)` — no Mutex, no third arg.
- Wasmtime 21: use `store.set_fuel()` not `store.add_fuel()`. Use `&mut *store` reborrow.
- NEVER add the `v8` crate (C++ binding) — it panics on Windows. Use `boa_engine` for JS or `rusty_v8`/`deno_core` for V8.
- `rmp_serde::to_vec` on a struct → array format. `rmp_serde::to_vec` on `serde_json::json!({})` → map format. ALWAYS encode test args with the concrete struct.
- WAT imports must come BEFORE memory/func definitions. Hard WebAssembly spec requirement.
- `table_index` in SubscriptionManager must stay consistent with `clients` — update BOTH in subscribe/unsubscribe/unregister_client.
- `start_listener` takes `tables: Arc<TableStore>` as 5th argument — always pass it from `run_server`.
- `apply_delta_batch` is the ONLY write path for reducer commits — never bypass it with direct `set_row`/`set_counter` calls from reducer logic.

---

## QUICK REFERENCE: CURRENT BENCHMARK NUMBERS

| Scenario | NeonDB | SpacetimeDB (May 2026) | Status |
|---|---|---|---|
| Raw TPS (in-process) | ~2.9M ops/sec | — | ✅ Engine is fast |
| No-commit (reducer only) | 391K TPS | — | ✅ |
| Full-cycle (single thread) | 297K TPS | 265K Rust / 303K TypeScript | ✅ Competitive |
| Aggregate (24 threads) | 1.65M TPS | Not published | ✅ Ahead |
| JS reducer TPS | Wasmtime JIT via javy compile; Boa for dev | 303K TypeScript (V8 JIT) | 🔶 Improved (javy path) |
| ACID / isolation | ✅ Serializable (row locks) | Full serializable | ✅ Done |
| Atomicity on panic | ✅ apply_delta_batch rollback | Full atomicity | ✅ Done |
| Initial state sync | ✅ initial_snapshot frames | ✅ | ✅ Done |
| Snapshots | Every 1M tx, atomic write | Every 1M transactions | ✅ Done |
| Client SDKs | TypeScript SDK (@neondb/client) | C#, C++, TypeScript, Rust | 🔶 Partial |
| Auth | API key (Bearer token) + per-reducer caller_id | OIDC per-reducer | ✅ Done |
| Scheduled reducers | Every N ms, args_json, graceful shutdown | ✅ | ✅ Done |
| Indexes | Lock-free hash index, O(1) lookup, auto-maintained | B-tree + hash | ✅ Done |
| Schema migrations | migrations/*.toml, idempotent, 3 ops | ✅ | ✅ Done |

**TL;DR**: Correctness layer is now solid (isolation, atomicity, initial sync). The remaining gaps
are JS runtime (performance parity) and client SDKs (developer experience). Production readiness features are complete.
