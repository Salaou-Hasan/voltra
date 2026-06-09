# NeonDB — TODO & Roadmap
# Agent Handoff: Gap Analysis vs SpacetimeDB

**Last Updated**: 2026-06-09 (Session 44)
**Current Build**: 471 tests passing (465 lib + 6 Raft), zero warnings, ~2.9M raw TPS (in-process benchmark)

Read CLAUDE.md before touching any file. This document translates the SpacetimeDB gap analysis
into concrete, prioritized tasks for the next agent(s) to execute.

---

## 🎯 THE GOAL (Session 44 — set by project owner)

**Make NeonDB a single-node, production-ready game backend at full feature + performance parity
with SpacetimeDB — then make it the easiest such database in the world to build real games and
apps on.**

Three pillars, in order:

1. **PERFORMANCE — multi-language reducers at near-native speed, on all cores.**
   - Today: Native Rust, Boa JS, Wasmtime WASM.
   - Add **C# reducers** and **Go reducers** that run *side by side* with native Rust at close-to-native
     throughput. The mechanism is **WASM compilation** (C# → .NET 8 WASI, Go → TinyGo), executed in
     the existing Wasmtime backend which already runs across the full Tokio worker pool (`num_cpus`).
     Parallelism comes from NeonDB's worker dispatch, NOT from the language runtime — so a cheap,
     re-entrant per-call WASM module rides every core for free.
   - Why not embed the real Go runtime / .NET CLR? Go's scheduler assumes it owns the process; the CLR
     is a heavyweight GC'd dependency that fights the DB for memory. WASM gets ~1.5–3× of native with
     none of that fragility. This is the realistic "very close to Rust" path.

2. **PRODUCTION-READY — implement every remaining hardening item.** Bounded reducer queue with
   backpressure, real queue-depth metric, WAL crash-recovery integration test, SDK optimistic-update
   race fix, `neondb migrate` CLI, benchmark scaling-mode fix. (Full list: TODO-035…TODO-041 below.)

3. **EASE OF USE — make building apps/games trivial.** The DX wave (TODO-027…TODO-031): `#[reducer]`
   macros, rewritten templates, `neondb generate` typed-client codegen, engine templates
   (Unity / Unreal / Godot / Web / GammaRay), `GET /schema`.

### Explicitly DEFERRED (project owner decision, Session 44)
- **Cluster + distribution + Raft consensus — REMOVED for now.** The cluster system is causing
  active trouble and will be revisited later. The legacy `src/cluster/` module and the `src/raft/`
  consensus layer are being unwired from the write path and removed from the build (TODO-034). The
  single-node write path reverts to direct `ctx.commit()` + `publish_deltas()` + WAL append — which
  is *faster* than routing every write through consensus. **The removed code is preserved in git**
  (tag `pre-cluster-removal` + the Session 40–43 commits) for clean resurrection later.

### Execution model (how the waves run)
More agents ≠ faster on a shared codebase — every agent that edits `main.rs`/`Cargo.toml`/`lib.rs`
collides with every other. Work is sequenced into waves so the conflict-prone foundation lands
first, then disjoint work parallelises:
- **Wave 0 (solo, foundation):** TODO-034 remove cluster/raft + TODO-035 bounded queue. Touches the
  hot shared files; must land before any agent branches.
- **Wave 1 (parallel, disjoint):** TODO-032 C# backend, TODO-033 Go backend, TODO-036 SDK race fix,
  TODO-037 WAL crash test, TODO-038 benchmark fix, TODO-039 `neondb migrate`, TODO-040 queue metric.
- **Wave 2 (parallel, disjoint):** DX foundation — TODO-027 macros, TODO-031 `/schema`.
- **Wave 3 (parallel, disjoint):** TODO-028 template rewrite, TODO-029 codegen, TODO-030 engine templates.

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

#### TODO-016b — Verify end_to_end benchmark scaling mode + metrics output (env-driven)
**Status**: ❗ PARTIALLY VALIDATED
- `benches/end_to_end.rs` supports scaling via:
  - `BENCH_SCALE_MODE=1`
  - `BENCH_CLIENT_COUNTS=10,25,50,100,200,500,1000` (default list)
  - `BENCH_CALLS=<calls per client>`
- Ensure runtime actually reports `scale_mode=true` and iterates over requested concurrency levels (100–1000 must be exercised).
- Confirm required query/subscription strings are used under load:
  - Read SQL: `SELECT * FROM players WHERE zone = 'north' LIMIT 1`
  - Broadcast subscription: `players WHERE zone = 'north'`
- Confirm output includes (per concurrency level):
  - CPU usage during benchmark (avg/peak normalized/core; Windows `wmic`, best-effort)
  - Memory usage (WorkingSet avg/peak in KB; best-effort)
  - Number of cores used
  - READ/WRITE/BROADCAST TPS
- Known observation to address:
  - A run attempting `BENCH_SCALE_MODE=1` printed `scale_mode=false` and used only `client_counts=[10]`.
  - In that same run, `BROADCAST TPS=0` (`pushed=0`).
  - CPU/memory samples printed `0KB/0%` (sampling likely failed or parsing didn’t yield values).

---

## NEW GAPS — Added Session 25 (vs SpacetimeDB feature parity)

### TODO-018 — Typed Schema System
**Status**: ✅ DONE (Session 26)

### TODO-019 — React Hooks in TypeScript SDK
**Status**: ✅ DONE (Session 26)

### TODO-023 — Project Templates
**Status**: ✅ DONE (Session 26)

---

## Session 27–32 Fixes

### PowerShell Args Parsing — FIXED (Session 27)
`parse_args_json()` auto-quotes bare words. `[general, alice]` → `["general", "alice"]`.

### TODO-022 Wiring — FIXED (Session 28)
`Arc<PermissionsConfig>` threaded through `main.rs`. `ctx.caller_role` set in worker loop.

### Template System — FIXED (Session 29)
4 templates: `rust/basic`, `rust/game-ready`, `rust/chat`, `typescript`. `neondb templates` subcommand.

### Query Completeness (OR / LIMIT / ORDER BY) — FIXED (Session 30–31)
`Predicate::Or`, `LIMIT N`, `ORDER BY field ASC|DESC` all implemented in `subscriptions.rs`.

### Optimistic Updates — FIXED (Session 31)
Both TypeScript and Rust SDKs support optimistic updates with automatic rollback.

### v8.rs Complete Rewrite — FIXED (Session 32)
- `__neondb_set` accepts full JSON objects (was number-only — broke all game reducers).
- `__neondb_get` reads any table (was counter-only).
- Empty scheduler args no longer crash with MessagePack decode error.
- Scheduler reducer names corrected (`refresh`, `cleanup_sessions`).
- Added `__neondb_delete`, `__neondb_get_all`, `__neondb_caller_id`, `__neondb_caller_role`.

### Known Non-Bug (Session 32)
`neondb call attack '["player1", "enemy1", "sword", 25]'` → `{"error": "Target not found"}` is **correct** — `enemy1` was never spawned. The attack reducer itself works. See TODO-025.

---

## REMAINING TASKS

### TODO-022 — Per-Reducer Permissions / Role-Based Auth
**Status**: ✅ DONE (Sessions 27–30)
**SpacetimeDB has**: OIDC tokens, per-user identity, per-reducer access control.
**We have**: Single global API key — any authenticated client can call any reducer.

**Completed**:
- `src/config.rs` — `PermissionsConfig` struct, loaded from `[permissions]` TOML / env var.
- `src/reducer/context.rs` — `pub caller_role: String` field.
- `src/network/websocket.rs` — `Bearer <key>:<role>` parsing; per-reducer enforcement.
- `src/main.rs` — `Arc<PermissionsConfig>` passed to `start_listener`; `ctx.caller_role` set in worker loop; schedulers get `caller_role: "scheduler"`.
- 3 integration tests: unauthorized call rejected, authorized call passes, role visible in ctx.

---

### TODO-020 — OR / JOIN / ORDER BY / LIMIT in Subscription Queries
**Status**: ✅ DONE (Sessions 30–31) — OR, LIMIT, ORDER BY complete; JOIN not built (low value)
**SpacetimeDB has**: Full SQL-style subscription queries including `OR`, `JOIN` across tables,
`ORDER BY`, `LIMIT`.
**We have**: `WHERE field op value`, `IN (...)`, `AND` — no `OR`, no joins, no ordering, no limits.

**Completed**: `Predicate::Or`, `extract_order_by()`, `SubscriptionFilter.limit`, `SubscriptionFilter.order_by`. 14 new unit tests added. `JOIN` deferred (no current demand).

---

### TODO-021 — Optimistic Updates in Client SDKs
**Status**: ✅ DONE (Session 31)
- TypeScript SDK: `call(reducer, args, { optimistic, onRollback? })` with deep-clone snapshot + rollback on error/timeout/disconnect.
- Rust SDK: `call_optimistic(reducer, args, |cache| new_cache)` with `Command::ApplyOptimistic` background rollback.

---

### TODO-024 — C# / Unity SDK
**Status**: ❌ NOT STARTED — LOW PRIORITY

---

## DEVELOPER EXPERIENCE — Make NeonDB Easy to Write

The goal: a developer opening NeonDB for the first time should be able to write a reducer in
5 minutes without reading docs. Rust remains the runtime. The complexity is hidden behind macros
and generated code, not removed.

---

### TODO-027 — `neondb-macros` Proc Macro Crate (Python-like Reducer Syntax)

**Status**: ✅ DONE (Session 44 Wave 2)
**Priority**: HIGH — this unblocks TODO-028, TODO-029, and all future templates
**Effort**: 1–2 weeks

**Problem:**
Writing a reducer today requires understanding `serde_json::Value`, manual arg extraction,
error conversion, the `execute()` signature, and explicit `ctx.commit()`. New developers
bounce off this immediately.

**Goal:**
A developer writes only business logic. The macro handles everything else.

**Before (current template):**
```rust
pub fn execute(&self, ctx: &mut ReducerContext, args: serde_json::Value)
    -> Result<serde_json::Value>
{
    let name = args[0].as_str()
        .ok_or_else(|| NeonDBError::invalid_argument("name required"))?;
    let delta = args[1].as_i64()
        .ok_or_else(|| NeonDBError::invalid_argument("delta required"))? as i32;

    let current = ctx.get_counter("stats", name).unwrap_or(0);
    ctx.set_counter("stats", name, current + delta);
    ctx.commit()?;

    Ok(serde_json::json!({ "new_value": current + delta }))
}
```

**After (with `#[reducer]` macro):**
```rust
#[reducer]
fn increment(ctx, name: String, delta: i32) {
    let current = ctx.get("stats", &name).unwrap_or(0i32);
    ctx.set("stats", &name, current + delta);
    ret!({ "new_value": current + delta })
}
```

**What the macro generates invisibly:**
- The `execute(&self, ctx: &mut ReducerContext, args: serde_json::Value) -> Result<serde_json::Value>` wrapper
- Argument extraction from `args[0]`, `args[1]`, ... with type coercion and clean error messages
- `ctx.commit()` call at the end of the function body (before return)
- `Ok(...)` wrapping of the return value
- The `NativeReducerBackend` struct and `impl ReducerBackend` boilerplate
- Auto-registration with the `ReducerRegistry` via an `inventory` or `linkme` static initialiser

**`ret!` macro:**
```rust
// Instead of: Ok(serde_json::json!({ "key": value }))
ret!({ "key": value })

// Instead of: Ok(serde_json::Value::Null)
ret!()
```

**`#[table]` macro for schema definition:**
```rust
// Instead of manually building ColumnDef vectors in neondb.toml:
#[table]
struct Player {
    #[key]
    id: String,
    hp: i32,
    x: f32,
    y: f32,
    class: String,
    alive: bool,
    #[default = 0]
    score: i32,
    #[optional]
    guild: Option<String>,
}
```

The macro auto-registers the table with `SchemaRegistry` at startup and generates typed
`ctx.get::<Player>(table, key)` and `ctx.set::<Player>(table, key, value)` methods.

**`ctx` ergonomics improvements (no macro required, just API):**
```rust
// Current:
ctx.set_row("players", "alice", serde_json::json!({ "hp": 100 }));
ctx.get_row("players", "alice");
ctx.delete_row("players", "alice");
ctx.set_counter("stats", "kills", 5);

// New (unified, shorter):
ctx.set("players", "alice", player!{ hp: 100, x: 0.0, y: 0.0 });
ctx.get("players", "alice");         // returns Option<serde_json::Value>
ctx.delete("players", "alice");
ctx.set("stats", "kills", 5i32);     // detects scalar → counter path
```

**New crate structure:**
```
neondb-macros/          ← new crate
├── Cargo.toml          (proc-macro = true)
└── src/
    ├── lib.rs          (exports: reducer, table, ret)
    ├── reducer.rs      (proc macro implementation)
    └── table.rs        (proc macro implementation)
```

Add to workspace `Cargo.toml`:
```toml
[workspace]
members = [".", "neondb-macros", "neondb-client-ts", "neondb-client-rust"]
```

**Files to create/modify:**
- `neondb-macros/` — new crate (create from scratch)
- `src/reducer/context.rs` — add unified `ctx.set()` / `ctx.get()` / `ctx.delete()` shortcuts
- `src/reducer/native.rs` — update `NativeReducerBackend` to work with macro output
- `src/reducer/registry.rs` — support static auto-registration if using `inventory` crate
- `Cargo.toml` — add `neondb-macros` workspace member, add `inventory = "0.3"` dep

**Tests to write:**
- `#[reducer]` macro expands correctly (use `cargo expand` / trybuild)
- Argument type mismatch returns clean error, not panic
- `#[table]` macro registers schema in SchemaRegistry on startup
- `ctx.set()` routes to `set_counter` for scalars, `set_row` for objects
- Round-trip: write with macro reducer, read back with typed `ctx.get::<Player>()`

---

### TODO-028 — Rewrite All Existing Templates Using `#[reducer]` Syntax

**Status**: ❌ NOT STARTED (depends on TODO-027)
**Priority**: HIGH
**Effort**: 3–4 days

**Problem:**
The four current templates (`rust/basic`, `rust/game-ready`, `rust/chat`, `typescript`)
were written before the macro system and use verbose boilerplate. They teach new developers
the hard way to write reducers instead of the easy way.

**Goal:**
Every template file a developer opens looks like simple, readable logic. Zero boilerplate visible.

**Templates to rewrite:**

**`rust/basic` — before:**
```rust
// modules/increment.rs
pub struct IncrementReducer;
impl ReducerBackend for IncrementReducer {
    fn execute(&self, ctx: &mut ReducerContext, args: serde_json::Value)
        -> Result<serde_json::Value>
    {
        let name = args[0].as_str().ok_or_else(|| ...)?;
        let delta = args[1].as_i64().ok_or_else(|| ...)? as i32;
        let current = ctx.get_counter("stats", name).unwrap_or(0);
        ctx.set_counter("stats", name, current + delta);
        ctx.commit()?;
        Ok(serde_json::json!({ "new_value": current + delta }))
    }
}
```

**`rust/basic` — after:**
```rust
#[reducer]
fn increment(ctx, name: String, delta: i32) {
    let current = ctx.get("stats", &name).unwrap_or(0i32);
    ctx.set("stats", &name, current + delta);
    ret!({ "new_value": current + delta })
}
```

**`rust/game-ready` — after:**
```rust
#[reducer]
fn spawn_player(ctx, id: String, x: f32, y: f32, class: String) {
    ctx.set("players", &id, {
        "hp": 100, "max_hp": 100,
        "x": x, "y": y,
        "class": class,
        "alive": true,
        "score": 0,
    });
    ret!({ "spawned": id })
}

#[reducer]
fn attack(ctx, attacker_id: String, target_id: String, damage: i32) {
    let mut target = ctx.get("players", &target_id)?;
    let new_hp = (target["hp"].as_i64().unwrap_or(0) - damage as i64).max(0);
    target["hp"] = new_hp.into();
    target["alive"] = (new_hp > 0).into();
    ctx.set("players", &target_id, target);
    ret!({ "damage_dealt": damage, "target_hp": new_hp })
}
```

**`rust/chat` — after:**
```rust
#[reducer]
fn send_message(ctx, room: String, author: String, text: String) {
    let msg_id = format!("{}-{}", room, ctx.timestamp());
    ctx.set("messages", &msg_id, {
        "room": room,
        "author": author,
        "text": text,
        "ts": ctx.timestamp(),
    });
    ret!({ "message_id": msg_id })
}
```

**Files to modify:**
- `src/main.rs` — update all 4 template string constants
- All template strings that contain reducer module source code

---

### TODO-029 — `neondb generate` Code Generator Command

**Status**: ❌ NOT STARTED (depends on TODO-027)
**Priority**: HIGH
**Effort**: 1 week

**Problem:**
When a developer writes a reducer with `#[reducer] fn spawn_player(ctx, id: String, x: f32, ...)`,
their Unity/Godot/Unreal client has to call it with raw strings:
```csharp
await db.Call("spawn_player", new object[] { "player1", 0f, 0f, "warrior" });
```
No autocomplete, no type checking, typos cause runtime errors.

**Goal:**
Run one command after changing server code → all client SDKs get typed, autocomplete-ready wrappers.

**Command:**
```
neondb generate
neondb generate --lang csharp --out ../MyUnityGame/Assets/NeonDB/Generated/
neondb generate --lang gdscript --out ../MyGodotGame/addons/neondb/generated/
neondb generate --lang cpp --out ../MyUEGame/Plugins/NeonDB/Source/Generated/
neondb generate --lang typescript --out ../web-client/src/generated/
```

**What it reads:**
- `#[table] struct Player { ... }` definitions → generates typed row structs in target language
- `#[reducer] fn spawn_player(ctx, id: String, x: f32, ...)` → generates typed caller methods

**What it generates per target:**

C# (Unity):
```csharp
// Generated/NeonDBReducers.cs — DO NOT EDIT, run `neondb generate` to update
public static class Reducers {
    public static Task SpawnPlayer(this NeonDBClient db,
        string id, float x, float y, string playerClass)
        => db.Call("spawn_player", id, x, y, playerClass);

    public static Task Attack(this NeonDBClient db,
        string attackerId, string targetId, int damage)
        => db.Call("attack", attackerId, targetId, damage);
}

// Generated/NeonDBTables.cs
[Serializable]
public class Player {
    public string Id;
    public int Hp;
    public float X;
    public float Y;
    public string Class;
    public bool Alive;
    public int Score;
}
```

GDScript (Godot):
```gdscript
# generated/reducers.gd — DO NOT EDIT
class_name NeonDBReducers

static func spawn_player(db, id: String, x: float, y: float, player_class: String):
    return await db.call_reducer("spawn_player", [id, x, y, player_class])

static func attack(db, attacker_id: String, target_id: String, damage: int):
    return await db.call_reducer("attack", [attacker_id, target_id, damage])
```

TypeScript:
```typescript
// generated/reducers.ts — DO NOT EDIT
export const Reducers = {
  spawnPlayer: (db: NeonDBClient, id: string, x: number, y: number, playerClass: string) =>
    db.call("spawn_player", [id, x, y, playerClass]),

  attack: (db: NeonDBClient, attackerId: string, targetId: string, damage: number) =>
    db.call("attack", [attackerId, targetId, damage]),
};
```

**How the generator reads the schema:**
- Parse `#[table]` macro attributes from `reducers/*.rs` using `syn` crate (same crate proc macros use)
- Parse `#[reducer]` function signatures from `reducers/*.rs`
- Alternatively: emit a JSON schema file at `neondb start` time (`GET /schema` endpoint) and have
  the generator read that — no Rust parsing required in the generator itself

**Files to create/modify:**
- `src/cli.rs` — add `Commands::Generate { lang, out_dir }` variant
- `src/main.rs` — add `Commands::Generate` arm calling `cmd_generate()`
- `src/codegen/mod.rs` — new module: schema reader + per-language emitters
- `src/codegen/csharp.rs` — C# emitter
- `src/codegen/gdscript.rs` — GDScript emitter
- `src/codegen/typescript.rs` — TypeScript emitter
- `src/codegen/cpp.rs` — C++ / Unreal emitter
- `GET /schema` endpoint in `handle_metrics_request` — returns JSON describing all tables + reducers

---

### TODO-030 — Engine-Specific `neondb init` Templates

**Status**: ❌ NOT STARTED (depends on TODO-027, TODO-028, TODO-029)
**Priority**: MEDIUM
**Effort**: varies per engine (see breakdown below)

**Problem:**
`neondb init` currently creates a server-only project. A game developer using Unity still has
to manually wire up a WebSocket client, handle MessagePack, parse subscription frames, and
figure out how to map server rows to Unity GameObjects. This is days of work before they write
a single line of game logic.

**Goal:**
`neondb init mygame --engine unity` produces a complete, runnable starting point:
a NeonDB server project (simplified Rust reducers) AND a ready-to-import client package
for that engine, with typed generated code already in place.

**New CLI flag:**
```
neondb init <name> --engine <unity|unreal|godot|web|custom>
neondb templates   # lists all available templates including engine templates
```

**Output structure (example: Unity):**
```
mygame/
├── server/
│   ├── Cargo.toml
│   ├── neondb.toml
│   └── reducers/
│       ├── player.rs     #[reducer] fn spawn_player(...)
│       ├── combat.rs     #[reducer] fn attack(...)
│       └── world.rs      #[reducer] fn move_player(...)
│
└── client-unity/         ← paste into Assets/ folder
    └── NeonDB/
        ├── NeonDBClient.cs
        ├── TableWatcher.cs       (MonoBehaviour for live updates)
        ├── MessagePackHelper.cs
        ├── Generated/
        │   ├── NeonDBReducers.cs (typed reducer calls)
        │   ├── NeonDBTables.cs   (typed row structs)
        │   └── README.md
        └── package.json          (UPM package manifest)
```

---

**Engine breakdown and effort:**

#### Unity (C#) — `--engine unity`
**Effort**: 2–3 weeks
**Client tech**: WebSocket via `NativeWebSocket` (Unity-compatible), `MessagePack-CSharp` for encoding
**Key challenge**: Unity's main thread requirement — all GameObject operations must run on main thread;
subscription callbacks must `Dispatch` to main thread via a `SynchronizationContext` or `Queue<Action>`
**What the template includes**:
- `NeonDBClient.cs` — Connect, Subscribe, CallReducer, Disconnect, auto-reconnect
- `TableWatcher.cs` — MonoBehaviour that syncs a NeonDB table to a `Dictionary<string, T>`
  and fires `OnInsert`, `OnUpdate`, `OnDelete` Unity events
- `NeonDBManager.cs` — singleton MonoBehaviour that holds the client, survives scene loads
- Sample scene: a `PlayerManager.cs` that calls `spawn_player` on Start and moves players around
- Generated typed wrappers from the server schema

#### Unreal Engine (C++) — `--engine unreal`
**Effort**: 3–5 weeks (most complex)
**Client tech**: Unreal's built-in `IWebSocket` module, custom MessagePack encoder (header-only `msgpack-c`)
**Key challenge**: Unreal build system (`.Build.cs`), USTRUCT/UFUNCTION reflection macros, packaging
**What the template includes**:
- `UNeonDBSubsystem.h/.cpp` — `UGameInstanceSubsystem` for lifecycle management
- `UNeonDBClient.h/.cpp` — Connect, Subscribe, CallReducer as `UFUNCTION(BlueprintCallable)`
- `FNeonDBTableDiff` — USTRUCT with `TArray<FJsonObject>` Inserted/Updated/Deleted
- Blueprint-callable reducer wrappers (from generated code)
- `.uplugin` manifest + `.Build.cs` with module dependencies
- Sample `APlayerSpawner.cpp` demonstrating spawn + movement sync

#### Godot (GDScript + C#) — `--engine godot`
**Effort**: 1–2 weeks
**Client tech**: Godot 4 built-in `WebSocketPeer`, GDScript MessagePack encoder (pure GDScript, ~100 lines)
**Key challenge**: Godot addon structure, autoload singletons, signal-based update delivery
**What the template includes**:
- `addons/neondb/neondb.gd` — autoload singleton (add to Project Settings → Autoload)
- `addons/neondb/neondb_client.gd` — WebSocket connection, MessagePack encode/decode
- `addons/neondb/table_watcher.gd` — Node that watches one table and emits signals on changes
- `addons/neondb/plugin.cfg` — addon manifest
- Generated `generated/reducers.gd` and `generated/tables.gd`
- Sample scene: `examples/player_demo/player_manager.gd`

#### Web / TypeScript — `--engine web`
**Effort**: 2–3 days (SDK already exists, just needs a project template)
**Client tech**: Existing `neondb-client-ts` SDK
**What the template includes**:
- Vite + React starter project
- Pre-wired `NeonDBProvider` React context
- `useTable("players")` hook returning live-synced data
- Generated `src/generated/reducers.ts` and `src/generated/tables.ts`
- Example `PlayerList.tsx` component showing a live player list

#### GammaRay / Custom Engine — `--engine custom`
**Effort**: 1–2 weeks
**Client tech**: C header + Rust FFI shared library (`.dll`/`.so`)
**What the template includes**:
- `neondb.h` — pure C API: `neondb_connect`, `neondb_call`, `neondb_subscribe`, `neondb_disconnect`
- `libneondb.dll` / `libneondb.so` built from a Rust FFI crate (`neondb-ffi/`)
- Python binding (`neondb.py`) wrapping the C API via `ctypes` — for Pygame / Ren'Py / custom Python engines
- Lua binding (`neondb.lua`) wrapping the C API via `ffi` — for Love2D / custom Lua engines
- A plain C example (`example.c`) demonstrating connect + call + subscribe

---

**Files to create/modify for TODO-030:**
- `src/main.rs` — add engine template constants (server-side reducer files) + new `init --engine` branch
- `src/cli.rs` — add `--engine` flag to `Commands::Init`
- `neondb-ffi/` — new Rust crate for C FFI bindings (GammaRay target)
- `templates/unity/` — C# source files for the Unity package
- `templates/unreal/` — C++ source files for the UE plugin
- `templates/godot/` — GDScript source files for the Godot addon
- `templates/web/` — React/TypeScript starter project files

**Build order within TODO-030:**
1. Web template first (trivial, reuses existing TS SDK)
2. Godot (simplest engine integration, fast to validate)
3. GammaRay / custom C API (unblocks Lua, Python, and any future engine)
4. Unity (largest audience, validates the C# SDK)
5. Unreal last (most complex build system, smallest indie audience)

---

### TODO-031 — `GET /schema` Endpoint (Machine-Readable Schema)

**Status**: ❌ NOT STARTED (needed by TODO-029 generator)
**Priority**: MEDIUM
**Effort**: 2–3 days

**Problem:**
`neondb generate` needs to know the current tables and reducer signatures without parsing Rust
source files. A running server already has this information in `SchemaRegistry` and
`ReducerRegistry`.

**Goal:**
```
GET http://localhost:3001/schema
```
Returns:
```json
{
  "tables": {
    "players": {
      "columns": [
        { "name": "id",    "type": "String",  "required": true,  "key": true },
        { "name": "hp",    "type": "Int32",   "required": true,  "default": 100 },
        { "name": "x",     "type": "Float32", "required": true },
        { "name": "alive", "type": "Bool",    "required": true }
      ],
      "rls": "Public"
    }
  },
  "reducers": {
    "spawn_player": {
      "args": [
        { "name": "id",    "type": "String" },
        { "name": "x",     "type": "Float32" },
        { "name": "y",     "type": "Float32" },
        { "name": "class", "type": "String" }
      ]
    },
    "attack": {
      "args": [
        { "name": "attacker_id", "type": "String" },
        { "name": "target_id",   "type": "String" },
        { "name": "damage",      "type": "Int32" }
      ]
    }
  },
  "version": "1.0.0"
}
```

**Files to modify:**
- `src/main.rs` — add `(&Method::GET, "/schema")` arm in `handle_metrics_request`
- `src/reducer/registry.rs` — add `list_reducer_signatures()` method
- `src/schema.rs` — `SchemaRegistry` already has `get_schema()`, just serialize it

---

## PILLAR 1 — MULTI-LANGUAGE REDUCERS (C# + Go via WASM)

The thesis: NeonDB already dispatches every reducer call across N Tokio worker threads
(`num_cpus`). The Wasmtime backend (Cranelift JIT) is already parallel-safe — a compiled module is
shared `Arc`, each call gets a fresh `Store`. So adding C# and Go is NOT about adding new threading;
it is about **adding two new compile targets that produce `.wasm` modules the existing backend loads.**
This is how they run "side by side with native Rust" at near-native speed on all cores.

---

### TODO-032 — C# Reducer Authoring Path (C# → WASM)

**Status**: ✅ DONE (Session 44)
**Priority**: HIGH (Pillar 1)
**Effort**: 1–2 weeks
**Wave**: 1 (disjoint — touches `neondb build` + new template dir, minimal main.rs overlap)

**Goal:** A game developer writes a reducer in C#, runs `neondb build`, and NeonDB loads the
resulting `.wasm` and executes it through the existing Wasmtime backend.

**Developer experience:**
```csharp
// reducers/Combat.cs
using NeonDB;

public static class Combat {
    [Reducer]
    public static void Attack(ReducerContext ctx, string attackerId, string targetId, int damage) {
        var target = ctx.Get("players", targetId);
        int hp = Math.Max(0, target["hp"].AsInt() - damage);
        target["hp"] = hp;
        target["alive"] = hp > 0;
        ctx.Set("players", targetId, target);
    }
}
```
```powershell
neondb build          # detects C# project, runs dotnet publish --os wasi → attack.wasm
neondb start          # Wasmtime loads attack.wasm like any other module
```

**How it compiles:**
- .NET 8 ships a WASI workload: `dotnet workload install wasi-experimental` (or .NET 9 `wasi-wasm`).
- `dotnet publish -c Release -r wasi-wasm` produces a `.wasm` module.
- The module imports NeonDB host functions (`__neondb_get`, `__neondb_set`, `__neondb_delete`,
  `__neondb_get_all`, plus `__neondb_caller_id` / `__neondb_caller_role`) — the SAME host ABI the
  WAT/JS-via-javy modules already use (see `src/reducer/wasm.rs`).
- A small **`NeonDB` C# host-binding package** (provided in the template) wraps those imports into the
  ergonomic `ReducerContext` API shown above.

**What must be built:**
1. `src/reducer/wasm.rs` — verify the host-function ABI is documented and stable; add any missing
   imports C# needs (likely none — the ABI is language-agnostic). Confirm `.wasm` produced by dotnet
   loads (it is plain wasm32; Wasmtime 21 handles it).
2. `src/main.rs` `cmd_build()` — detect a C# reducer project (`*.csproj` in `reducers/`) and invoke
   `dotnet publish -c Release -r wasi-wasm -o modules/`. Copy the `.wasm` into `modules/`.
3. `templates/csharp-reducers/` — a `.csproj`, the `NeonDB` host-binding `.cs` file, and a sample
   `Combat.cs` reducer.
4. `neondb init <name> --reducer-lang csharp` — scaffolds the above.
5. Docs: `docs/reducers-csharp.md`.

**Acceptance test:** Build the sample C# reducer to wasm, load it, call `attack`, verify the row
mutates and the result returns. Add an integration test that skips gracefully if `dotnet` is not
installed (CI may not have the WASI workload).

**Pitfalls:**
- .NET WASI output can be large (several MB) and may need `<InvariantGlobalization>true` and trimming
  to stay small. Document the recommended `.csproj` settings in the template.
- The host ABI passes data as MessagePack or JSON bytes through linear memory — match exactly what
  `src/reducer/wasm.rs` expects (length-prefixed pointer convention). Read that file first.

---

### TODO-033 — Go Reducer Authoring Path (Go → WASM via TinyGo)

**Status**: ✅ DONE (Session 44)
**Priority**: HIGH (Pillar 1)
**Effort**: 1–2 weeks
**Wave**: 1 (disjoint — parallel with TODO-032, separate template dir)

**Goal:** Same as TODO-032 but for Go. Write a reducer in Go, `neondb build` compiles it to `.wasm`
via TinyGo, the Wasmtime backend runs it.

**Developer experience:**
```go
// reducers/combat.go
package main

import "neondb"

//export attack
func Attack(ctx *neondb.Context, attackerId, targetId string, damage int32) {
    target := ctx.Get("players", targetId)
    hp := target.Int("hp") - damage
    if hp < 0 { hp = 0 }
    target.Set("hp", hp)
    target.Set("alive", hp > 0)
    ctx.Set("players", targetId, target)
}

func main() {}  // required by TinyGo wasm target
```
```powershell
neondb build          # detects Go project, runs: tinygo build -o modules/combat.wasm -target wasi
neondb start
```

**How it compiles:**
- **TinyGo** (not standard `go build`) — standard Go's wasm output drags in the full Go runtime/GC
  and is huge + slow to start. TinyGo produces compact, fast wasm32-wasi modules ideal for per-call
  execution.
- `tinygo build -o combat.wasm -target wasi ./reducers`
- Imports the same NeonDB host-function ABI as C#/WAT/JS.
- A small **`neondb` Go host-binding package** (provided in template) wraps the imports.

**What must be built:**
1. `src/main.rs` `cmd_build()` — detect a Go reducer project (`go.mod` + `*.go` in `reducers/`) and
   invoke `tinygo build -target wasi`. (Coordinate with TODO-032 — both edit `cmd_build`; one PR adds
   a `detect_reducer_lang()` dispatcher both hook into, to avoid a main.rs conflict.)
2. `templates/go-reducers/` — `go.mod`, the `neondb` host-binding package, sample `combat.go`.
3. `neondb init <name> --reducer-lang go`.
4. Docs: `docs/reducers-go.md`.

**Acceptance test:** Build sample Go reducer to wasm via TinyGo, load, call, verify mutation.
Skip gracefully if `tinygo` is not on PATH.

**Pitfalls:**
- TinyGo's `wasi` target export convention uses `//export name`. The exported function name must match
  the registered reducer name.
- TinyGo has a partial stdlib — document which packages are unavailable in the template README.
- Standard `go build -target=wasm` will NOT work well here — the template and build command must use
  TinyGo specifically.

---

## PILLAR 2 — PRODUCTION HARDENING (remaining items)

### TODO-034 — Remove Cluster + Raft (revert to single-node write path)

**Status**: ❌ NOT STARTED
**Priority**: CRITICAL — Wave 0, blocks everything (foundation)
**Effort**: 1 day
**Wave**: 0 (SOLO — touches main.rs/lib.rs/Cargo.toml; must land before any agent branches)

**Why:** The cluster system is causing active trouble and is being deferred. Routing every write
through Raft consensus adds latency even on a single node. Reverting to direct commit is simpler
AND faster.

**Steps:**
1. **Tag for recovery first:** `git tag pre-cluster-removal` so the Raft + cluster code (Sessions
   36, 40–43) is trivially recoverable later.
2. `src/lib.rs` — remove `pub mod cluster;` and `pub mod raft;`.
3. `src/main.rs` — revert the worker write path (lines ~1183–1249): replace
   `ctx.drain_pending_deltas()` + `raft_w.client_write(...)` with the single-node path:
   `let deltas = ctx.commit()?;` then `subscription_manager.publish_deltas(&deltas);` then
   `wal_w.append(&entry, seq_num)`. (This is the path that existed pre-Session-40 — see git history.)
4. `src/main.rs` — delete the cluster bus setup (`ClusterBus::new`, gossip/fanout task spawns,
   ~lines 904–1028), the Raft node init block (~lines 1049–1100), and all `/cluster/*` + `/raft/*`
   route arms in `handle_metrics_request` (~lines 1782–2014).
5. `start_metrics_server` / `handle_metrics_request` — drop the `cluster_bus`, `raft`, `raft_node_id`,
   `raft_node_addr` parameters.
6. `Cargo.toml` — remove `openraft`, `anyerror` (Raft-only deps). Keep `base64` (used elsewhere).
7. Delete `tests/raft_consensus_test.rs` and `tests/cluster_integration_test.rs`.
8. `src/config.rs` — remove cluster/shard/raft config fields if unused elsewhere.
9. `git rm -r src/cluster src/raft`.

**Acceptance:** `cargo build` clean, `cargo test --lib` green (expect ~465 minus any cluster-only lib
tests), single-node `neondb start` + `neondb call` round-trips a write through commit→publish→WAL.

---

### TODO-035 — Bounded Reducer Queue + Backpressure

**Status**: ❌ NOT STARTED
**Priority**: HIGH (Pillar 2)
**Effort**: 1 day
**Wave**: 0 (SOLO — edits the same main.rs region as TODO-034; do them together)

**Problem:** `let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();` — under
overload the queue grows without limit, exhausting memory.

**Fix:**
- `kanal::bounded_async::<PendingCall>(cap)` where `cap` is configurable
  (`NEONDB_REDUCER_QUEUE_CAP`, default e.g. 16_384).
- In `websocket.rs`, when `reducer_tx.try_send()` fails with `Full`, return a `ReducerResponse::error`
  ("server overloaded, retry") instead of awaiting indefinitely — fail fast, shed load.
- Wire the chosen depth into TODO-040's metric.

---

### TODO-036 — SDK Optimistic-Update Concurrent-Diff Race Fix

**Status**: ❌ NOT STARTED
**Priority**: HIGH (Pillar 2 — correctness bug)
**Effort**: 3–4 days
**Wave**: 1 (FULLY DISJOINT — only touches the two SDK files, zero server overlap)

**Problem:** Two overlapping `call_optimistic()` calls. The server's diff for call #1 arrives AFTER
call #2's speculative state is applied; rolling back #1 clobbers #2's state.

**Fix (both SDKs):**
- Track optimistic layers as an ordered stack keyed by call_id, not a single snapshot.
- On resolve/rollback of one call, recompute the live cache as: base server state + replay of all
  still-pending optimistic layers in order (skip/insert the resolving one). This makes rollback
  order-independent.
- `neondb-client-ts/src/client.ts` and `neondb-client-rust/src/client.rs`.
- Add a reproducer test: fire two overlapping optimistic calls, resolve #1 with a conflicting diff,
  assert #2's speculative state survives.

---

### TODO-037 — Real-Server WAL Crash-Recovery Integration Test

**Status**: ❌ NOT STARTED
**Priority**: MEDIUM (Pillar 2)
**Effort**: 2–3 days
**Wave**: 1 (DISJOINT — new test file only)

**Problem:** WAL recovery is unit-tested but no test starts a real `neondb start`, kills it
mid-write, restarts, and verifies state end-to-end.

**Fix:** New `tests/crash_recovery_test.rs` using the existing `spawn_server_with_env` harness
(Sessions 37–38). Write N rows via reducer calls, `kill()` the process (no graceful shutdown),
restart on the same WAL/snapshot dir, verify all committed rows are present and no torn writes.

---

### TODO-038 — Benchmark Scaling-Mode Fix (TODO-016b carryover)

**Status**: ❌ NOT STARTED
**Priority**: LOW (Pillar 2)
**Effort**: 1–2 days
**Wave**: 1 (DISJOINT — `benches/end_to_end.rs` only)

**Problem:** `BENCH_SCALE_MODE=1` was observed to print `scale_mode=false` and `BROADCAST TPS=0`.
**Fix:** Debug the env parsing so scale mode actually engages and iterates 10→1000 client counts;
fix the broadcast counter so `pushed > 0` is measured. Fix the `wmic` CPU/mem sampler or replace it.

---

### TODO-039 — `neondb migrate` CLI Command

**Status**: ❌ NOT STARTED
**Priority**: MEDIUM (Pillar 2)
**Effort**: 2–3 days
**Wave**: 1 (mostly disjoint — adds a cli.rs variant + main.rs arm; coordinate light main.rs touch)

**Problem:** `apply_migrations()` exists in `src/migrations.rs` but there is no CLI command to run
`migrations/*.toml` against a running server.

**Fix:** `neondb migrate [--dry-run]` — reads `migrations/`, POSTs to a new `POST /migrate` admin
endpoint (like `neondb seed` does for `/seed`), applies add/remove/rename field ops, reports a
per-migration summary.

---

### TODO-040 — Real `reducer_queue_depth` Metric

**Status**: ❌ NOT STARTED
**Priority**: LOW (Pillar 2)
**Effort**: 0.5 day
**Wave**: 1 (DISJOINT once TODO-035 lands — reads the channel len)

**Problem:** `/healthz` hardcodes `"reducer_queue_depth": 0`.
**Fix:** After TODO-035 makes the queue bounded, expose `reducer_rx.len()` via a shared atomic or
the kanal channel's `len()`; surface it in `/healthz` and as a Prometheus gauge.

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
20. TODO-022  Role-based auth / permissions   ✅ DONE (Sessions 27–30)
21. TODO-020  OR / ORDER BY / LIMIT queries   ✅ DONE (Sessions 30–31)
22. TODO-021  Optimistic updates (SDKs)       ✅ DONE (Session 31)
23. TODO-017  Dokploy live deploy             ← ship it (your task, not code)
24. TODO-024  C# / Unity SDK                  ← low priority, big effort

── NEW GAPS (Session 32+) ────────────────────────────────────────
25. TODO-025  Enemy/NPC spawning system       ✅ DONE (Session 33)
26. TODO-026  CLI `neondb seed` command       ✅ DONE (Session 35)

── DEVELOPER EXPERIENCE (Session 43+) ──────────────────────────
27. TODO-027  neondb-macros proc macro crate  ✅ DONE (Session 44 Wave 2)
28. TODO-028  Rewrite templates w/ #[reducer] ❌ NOT STARTED  ← Wave 3 (needs 027)
29. TODO-029  neondb generate code generator  ❌ NOT STARTED  ← Wave 3 (needs 027,031)
30. TODO-030  Engine-specific init templates  ❌ NOT STARTED  ← Wave 3 (needs 028,029)
              (Unity / Unreal / Godot / Web / GammaRay)
31. TODO-031  GET /schema endpoint            ✅ DONE  (Session 44 Wave 1)

── SESSION 44 — PARITY + PERFORMANCE PUSH ──────────────────────
   GOAL: single-node SpacetimeDB parity, then easiest game DB to build on.

   WAVE 0 (SOLO — foundation, conflict-prone shared files):
34. TODO-034  Remove cluster + Raft           ✅ DONE  (Session 44 Wave 0)
35. TODO-035  Bounded reducer queue           ✅ DONE  (Session 44 Wave 0)

   WAVE 1 (PARALLEL — disjoint file sets, after Wave 0 lands):
32. TODO-032  C# reducers (C# → WASM)          ✅ DONE  (Session 44 Wave 1)
33. TODO-033  Go reducers (Go → WASM/TinyGo)   ✅ DONE  (Session 44 Wave 1)
36. TODO-036  SDK optimistic race fix          ✅ DONE  (Session 44 Wave 1)
37. TODO-037  WAL crash-recovery test          ✅ DONE  (Session 44 Wave 1)
38. TODO-038  Benchmark scaling fix            ✅ DONE  (Session 44 Wave 1)
39. TODO-039  neondb migrate CLI               ✅ DONE  (Session 44 Wave 1)
40. TODO-040  Real queue-depth metric          ✅ DONE  (Session 44 Wave 1)

   WAVE 2 + 3: DX (TODO-027…031) per dependency order above.

   DEFERRED: cluster, distribution, Raft, C#/Unity native SDK (TODO-024), Dokploy live deploy.
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
| Role-based auth | ✅ Done (Sessions 27–30) | Per-reducer OIDC roles | ✅ Done |
| Scheduled reducers | Every N ms, args_json, graceful shutdown | ✅ | ✅ Done |
| Indexes | Lock-free hash index, O(1) lookup, auto-maintained | B-tree + hash | ✅ Done |
| Schema migrations | migrations/*.toml, idempotent, 3 ops | ✅ | ✅ Done |

---

## Remaining for Production (post-Session-39 wave)

The 5-agent Session-39 wave landed schema hardening, new WAL unit tests, and integration-style
schema tests, but the following gaps remain before NeonDB is production-ready. They are listed
in roughly decreasing order of severity.

### Build & Test Plumbing (BLOCKING)
- **`cargo build` (bin) is broken.** `src/main.rs:783` calls `start_listener` with 10 arguments
  but `src/network/websocket.rs:104` now requires 11. The missing argument is a `u64`.  This
  blocks every `cargo test` invocation that needs the bin (including the new
  `tests/wal_recovery_test.rs` and `tests/schema_validation_test.rs` files Agent 5 wrote — they
  type-check fine via `cargo check --test <name>` but cannot RUN until the bin compiles).
  Fix is owned by whichever agent introduced the new `start_listener` parameter.

### Data Reliability
- **Full WAL crash recovery on a REAL server.** Session 39 added unit-level WAL recovery tests
  (`tests/wal_recovery_test.rs`) covering checksum corruption, mid-entry truncation, and
  snapshot+replay. We do NOT yet have a test that starts a real `neondb start` process, kills it
  mid-write, restarts it, and verifies the state matches expectations end-to-end. The integration
  test infrastructure exists (see Sessions 37–38) but no test exercises crash recovery.
- **CRDT / HLC for cross-shard write conflict resolution** (Wave 4 of the original plan). With
  multiple shards there is no causal ordering for concurrent writes that touch the same row on
  different nodes. Either pick a single-writer-per-key strategy or introduce a Hybrid Logical
  Clock + CRDT-merge layer. Not designed.

### Concurrency Bugs
- **TS / Rust SDK optimistic-update concurrent-diff race** (Wave 3 of the original plan).
  When two `call_optimistic()` calls overlap and the server's diff for the FIRST call arrives
  AFTER the optimistic state of the SECOND call has been applied, the rollback path can clobber
  the second call's speculative state. Reproducer + fix needed for both `neondb-client-ts/src/client.ts`
  and `neondb-client-rust/src/client.rs`.

### Cluster
- **Cluster integration tests (two-node loopback).** `src/cluster/mod.rs` has solid unit tests
  for `shard_for_key` and config parsing (Session 36) but no test spins up two nodes on
  loopback, fans out a write, and verifies the peer received it. The cluster HTTP layer is
  effectively untested at the integration level.

### Lower-priority
- C# / Unity client SDK (TODO-024) — explicit deferral.
- Dokploy live deployment validation (TODO-017) — checklist exists, no live run.
- End-to-end benchmark scaling mode (TODO-016b) — `BENCH_SCALE_MODE=1` was observed to print
  `scale_mode=false` in one run; needs reproduction + fix.
