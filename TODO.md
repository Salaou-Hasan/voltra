# NeonDB — TODO & Roadmap
# Agent Handoff: Gap Analysis vs SpacetimeDB

**Last Updated**: 2026-06-06 (Session 27)
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

### TODO-007 — Auth / Identity
**Status**: ✅ DONE (Session 10)

### TODO-004 — Subscription Query Engine (SQL-style predicates)
**Status**: ✅ DONE (Session 11) — single comparison, IN operator, AND compound predicates

### TODO-005 — Replace Boa with V8 or Wasmtime for JS Reducers
**Status**: ✅ DONE (Session 21) — WASM-first loading (js → wasm auto-upgrade); neondb build invokes javy compiler; Boa kept for dev prototyping

---

## MEDIUM PRIORITY (Production Readiness)

### TODO-009 — B-tree + Hash Indexes on Tables
**Status**: ✅ DONE (Session 12) — lock-free DashMap two-level set per field, O(1) lookup, auto-maintained on write/delete

### TODO-008 — Scheduled Reducers
**Status**: ✅ DONE (Session 13) — [[scheduler]] TOML config, one async task per entry, MissedTickBehavior::Skip, args_json support, graceful shutdown

### TODO-010 — Schema Migration Support
**Status**: ✅ DONE (Session 20) — migrations/*.toml files; add_field / remove_field / rename_field; idempotent; applied after WAL replay; 6 unit tests

---

## LOW PRIORITY (Client SDK)

### TODO-011 — TypeScript Client SDK
**Status**: ✅ DONE (Session 16)

### TODO-012 — Rust Client SDK
**Status**: ✅ DONE (Session 19)

### TODO-013 — Two-Frame Protocol for Subscription Delivery
**Status**: ✅ DONE (Session 17)

### TODO-014 — Columnar Table Storage (Performance)
**Status**: ✅ DONE (Session 22)

---

## BENCHMARKING & TOOLING

### TODO-015 — Standalone Benchmarking Tool
**Status**: ✅ DONE (Session 14)

### TODO-016 — End-to-End Benchmark (WebSocket round-trip)
**Status**: ✅ DONE (Session 23)

---

## NEW GAPS — Added Session 25 (vs SpacetimeDB feature parity)

### TODO-018 — Typed Schema System
**Status**: ✅ DONE (Session 26)

### TODO-019 — React Hooks in TypeScript SDK
**Status**: ✅ DONE (Session 26)

### TODO-023 — Project Templates
**Status**: ✅ DONE (Session 26)

---

## Session 27 Fixes

### PowerShell Args Parsing — FIXED (Session 27)
**Problem**: PowerShell strips quotes from args like `'["general", "alice"]'`, delivering
`[general, alice]` — bare unquoted words. The old parser couldn't handle this.

**Fix** (`src/cli.rs`): `parse_args_json()` now detects bare unquoted words inside `[...]`
and auto-quotes them before JSON parsing. E.g. `[general, alice]` → `["general", "alice"]`.
This means `neondb call join_room '["general", "alice"]'` works correctly in PowerShell.

---

## REMAINING TASKS

### TODO-022 — Per-Reducer Permissions / Role-Based Auth
**Status**: 🔄 IN PROGRESS (Session 27 — files written, main.rs wiring pending)
**SpacetimeDB has**: OIDC tokens, per-user identity, per-reducer access control.
**We have**: Single global API key — any authenticated client can call any reducer.

**What was done in Session 27**:
- `src/config.rs` — `PermissionsConfig` struct: `HashMap<reducer_name, Vec<role>>`. Loaded from
  `[permissions]` TOML section and `NEONDB_PERMISSIONS` env var (JSON fallback).
- `src/reducer/context.rs` — added `pub caller_role: String` field alongside existing `caller_id`.
- `src/network/websocket.rs` — parses `Bearer <key>:<role>` at handshake (backward-compatible —
  bare `<key>` still works, role defaults to `""`). Enforces permissions on every reducer call;
  rejects unauthorized calls with error response.
- `src/main.rs` — **NEEDS WIRING**: pass `Arc<PermissionsConfig>` to `start_listener`, set
  `ctx.caller_role` in worker loop, add `caller_role: "scheduler"` to scheduler `PendingCall`.

**What remains for TODO-022**:
1. Wire `PermissionsConfig` into `main.rs`: import, build `Arc<PermissionsConfig>`, pass to
   `start_listener`, set `ctx.caller_role` in the worker loop.
2. Add `caller_role: "scheduler"` to `PendingCall` in scheduler task.
3. Add 3 tests: unauthorized call rejected, authorized call passes, role visible in ctx.
4. Update `neondb.toml` example with `[permissions]` section.

**Files to modify**: `src/main.rs` (primary remaining work)

**Design**:
- `[permissions]` section in `neondb.toml` — maps reducer names to required roles
- Bearer token extended format: `Bearer <key>:<role>` (backward compatible)
- `ctx.caller_role: String` in ReducerContext
- Server rejects calls where caller's role isn't in the allowed list

---

### TODO-020 — OR / JOIN / ORDER BY / LIMIT in Subscription Queries
**Status**: ❌ NOT STARTED
**SpacetimeDB has**: Full SQL-style subscription queries including `OR`, `JOIN` across tables,
`ORDER BY`, `LIMIT`.
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
**SpacetimeDB has**: Client immediately updates local cache before server confirms, then
reconciles on server response.
**We have**: Both SDKs wait for server ack before updating cache.

**What to build**:
- `call(reducer, args, { optimistic: (cache) => newCache })` API in both SDKs
- On server success: reconcile with real server data
- On server error: roll back optimistic change, expose error to caller

**Files to modify**: `neondb-client-ts/src/client.ts`, `neondb-client-rust/src/client.rs`

---

### TODO-024 — C# / Unity SDK
**Status**: ❌ NOT STARTED — LOW PRIORITY

---

## DEPLOYMENT

### TODO-017 — Dokploy Deployment
**Status**: ✅ DOCKER FILES UPDATED — needs live deployment test
Remaining: push to Git repo, connect to Dokploy, deploy image on Linux VPS, run test client from outside container.

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
17. TODO-023  Project templates               ✅ DONE (Session 26)
18. TODO-018  Typed schema system             ✅ DONE (Session 26)
19. TODO-019  React hooks (TS SDK)            ✅ DONE (Session 26)

── REMAINING ─────────────────────────────────────────────────────
20. TODO-022  Role-based auth / permissions   ← 🔄 IN PROGRESS — wire main.rs + 3 tests
21. TODO-017  Dokploy live deploy             ← ship it (your task, not code)
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
- NEVER add the `v8` crate (C++ binding) — it panics on Windows. Use `boa_engine` for JS.
- `rmp_serde::to_vec` on a struct → array format. `rmp_serde::to_vec` on `serde_json::json!({})` → map format. ALWAYS encode test args with the concrete struct.
- WAT imports must come BEFORE memory/func definitions. Hard WebAssembly spec requirement.
- `table_index` in SubscriptionManager must stay consistent with `clients` — update BOTH in subscribe/unsubscribe/unregister_client.
- `start_listener` takes `tables: Arc<TableStore>` as 5th argument — always pass it from `run_server`.
- `apply_delta_batch` is the ONLY write path for reducer commits — never bypass it with direct `set_row`/`set_counter` calls from reducer logic.
- PowerShell strips single-quotes AND inner double-quotes from JSON args. `parse_args_json()` in cli.rs handles this via auto-quoting bare words — do not remove that logic.

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
| Client SDKs | TypeScript + Rust SDKs | C#, C++, TypeScript, Rust | 🔶 Partial |
| Auth | API key (Bearer token) + per-reducer caller_id | OIDC per-reducer | ✅ Done |
| Role-based auth | 🔄 In progress (main.rs wiring remaining) | Per-reducer OIDC roles | 🔶 Partial |
| Scheduled reducers | Every N ms, args_json, graceful shutdown | ✅ | ✅ Done |
| Indexes | Lock-free hash index, O(1) lookup, auto-maintained | B-tree + hash | ✅ Done |
| Schema migrations | migrations/*.toml, idempotent, 3 ops | ✅ | ✅ Done |
