# AGENTS.md — Voltra Agent Handoff Document

Read this before touching any file. It captures the full project state, architecture decisions, every fix applied so far, and what still needs doing.

---

## What This Project Is

Voltra is a high-throughput, self-hosted game-backend database written in Rust. It speaks WebSocket (MessagePack framing), stores data in a lock-free in-memory table engine, logs every write to a WAL, and executes user-supplied logic ("reducers") in three runtimes:

- **Native** — compiled Rust functions, zero overhead
- **JS (Boa 0.19)** — pure-Rust JS engine, no V8 dependency, works on Windows/Linux/macOS
- **WASM (Wasmtime 21)** — `.wasm` / `.wat` modules via Cranelift JIT

The server is a single binary. Clients connect over WebSocket and send MessagePack-encoded `ReducerCall` messages. The server dispatches calls through a `kanal` async channel to N parallel Tokio blocking-thread workers, commits deltas to an in-memory `TableStore` (DashMap-backed), appends a WAL entry, then fans out subscription updates as `Arc<Bytes>` to all subscribed clients.

---

## Project Root

```
C:\Users\King\Desktop\Voltra
```

Allowed filesystem directories for agents: `C:\Users\King\Desktop` and `C:\Users\King\Documents`.

---

## Directory Map

```
Voltra/
├── Cargo.toml                  # workspace manifest — single crate "voltra"
├── src/
│   ├── main.rs                 # CLI (init / build / start), server bootstrap, 4 templates
│   ├── lib.rs                  # crate root, re-exports
│   ├── config.rs               # Config struct, from_env(), TOML loading, PermissionsConfig
│   ├── cli.rs                  # CLI arg parsing, parse_args_json() (PowerShell-safe)
│   ├── error.rs                # VoltraError enum, Result alias
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
├── voltra-client-ts/           # TypeScript client SDK
│   └── src/
│       ├── client.ts           # VoltraClient — call(), call() w/ optimistic, subscribe()
│       └── types.ts            # OptimisticOptions, OptimisticCache, RowDiff, …
├── voltra-client-rust/         # Rust client SDK
│   └── src/
│       └── client.rs           # VoltraClient — call(), call_optimistic(), subscribe()
└── mygame/                     # sample project directory (voltra init output)
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
- `PermissionsConfig` — `HashMap<reducer_name, Vec<role>>`. Loaded from `[permissions]` TOML or `VOLTRA_PERMISSIONS` env var (JSON). Used by websocket.rs to enforce per-reducer roles.

### cli.rs
- `parse_args_json()` — PowerShell-safe. Auto-detects bare unquoted words inside `[...]` (e.g. `[general, alice]` from PowerShell quote-stripping) and auto-quotes them to produce valid JSON before parsing.

### TypeScript SDK (voltra-client-ts/src/client.ts)
- **Optimistic updates**: `call(reducer, args, { optimistic: (cache) => newCache })`.
  - Snapshots cache before call, applies speculative state immediately.
  - Rolls back to snapshot on server error; calls `onRollback?` if provided.
  - Also rolls back on timeout or disconnect.
  - `OptimisticCache = Map<tableName, Map<rowKey, rowData>>`.

### Rust SDK (voltra-client-rust/src/client.rs)
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

### Session 53 — Benchmark harness split (serve/external) + eviction-thrash fix → 15K CCU PASS

**Root causes of the 7K-CCU collapse (diagnosed, both fixed):**
1. **LRU eviction thrash** — sim's `max_rows_per_table = 50_000` sat below the legitimate working set at 7K+ players (`sim_inventory` ≈ 9 rows/player). Every insert past the cap triggered eviction inside the write path: game TPS collapsed 21K → 2.2K. Fixed: cap raised to 2_000_000 (pure OOM safety net). **Diagnostic that proved it wasn't locks:** 7K connections of pure `stress_ping` (zero TableStore work) sustained 41K TPS at the same p50 (134ms) as game@5K — the latency floor was scheduling, the TPS collapse was eviction.
2. **Shared-runtime harness** — bot clients and server ran in ONE process/runtime; at 7K+ connections the tokio scheduler thrashed (14K+ tasks). Fixed by splitting the harness.

**sim.rs changes:**
- `voltra-sim serve [--ws-port 3777] [--metrics-port 3778]` — server-only mode: embedded server + native reducers + minimal HTTP `/healthz` (raw tokio TCP responder, `spawn_health_server`), parks forever.
- `--external` global flag — client-only mode: skips embedded server, connects to `--url`, samples server stats over HTTP (`StatsSource::Remote` / `fetch_health`). `StatsSource` enum replaced `&ServerHandle` through `run_game_sim`/`run_chat_sim`/`sample_loop`/`run_scale`.
- `--id-offset N` global flag — multiple client processes simulate distinct players (offsets 0/5000/10000…). Threaded via `SimConfig.id_offset`.

**Measured results (24-core box, server + clients SHARING the machine):**
| Setup | CCU | TPS | p50 | p99 | Errors |
|---|---|---|---|---|---|
| game, 1 client proc | 5K | 43.3K | 58ms | 172ms | 0.12% |
| game, 1 client proc | 10K | 38.7K | 172ms | 293ms | 0.13% ✅ (was 63% FAIL) |
| game, 3 client procs | **15K** | **46.9K** | 289ms | 386ms | 0.12% ✅ |
| chat, 3 client procs | 15K | 5.6K | ~400ms | 2.2s | ~0.01% ✅ |
| game, 6 client procs | 30K | n/a | **3ms** (connected calls) | — | host saturated |

**30K finding:** the box cannot host 30K WS client connections + 6 client processes + the server; bots stalled in connect retries. But calls that DID get through saw **p50 2-6ms** — the database is not the limiter; client hardware is. 30K validation needs client machines separate from the server (`voltra-sim serve` on the server box, `--external --id-offset` clients elsewhere).

**New pitfalls:**
102. **Never set the sim LRU cap below the working set** — eviction inside `apply_delta_batch` on every insert is a TPS cliff, not a graceful degradation. Cap = OOM safety net only.
103. **Benchmarks above ~5K connections MUST use serve/--external split** — in-process numbers above that measure tokio scheduler thrash, not the database. Building voltra-sim while `serve` is running fails at link time (exe lock) — kill it first.
104. **`--id-offset` must differ per client process** — otherwise processes fight over the same `gp_N` player rows and spawn/respawn semantics skew.

### Session 53b — Lobby-partitioned game sim (75 players/instance)

Owner direction: real games shard players into instances (~75/lobby), nobody runs 15K players in one shared world. Sim restructured to match:
- `game --lobby-size 75` (default 75). `lobby = id / lobby_size`; all keys lobby-prefixed (`l{lobby}_p{id}`, `l{lobby}_npc_…`); peers (sim_transfer targets) picked within the same lobby only — instances are fully isolated key spaces.
- `Metrics.lobby_hists: DashMap<usize, Mutex<Histogram>>` + `record_lobby()`; report prints per-instance latency: median/best/worst lobby p50+p99 (`print_lobby_summary`).
- **Measured (15K CCU = 202 lobbies × 75, server+3 client procs sharing the box): 53.0K TPS combined, p99 333ms, per-lobby p99 spread best 328ms → worst 336ms (8ms!) — zero noisy-neighbor effect. PASS, 0.1% errors, memory flat 670MB.**
- Pitfall 105: per-lobby latency floor in these runs is host scheduling (15K bots + server on one box) — per-lobby *fairness* is the meaningful metric in-box; absolute latency needs remote clients.

### Session 54b — Template performance parity (native everywhere)

- **game_reducers.rs.txt: 18 → 28 `#[reducer]` fns** — ported the 10 missing JS reducers to native (apply_damage incl. player-or-NPC fallback, open_loot_box seeded roll, guild_create/invite/accept/kick incl. `ctx.caller_id` owner check, accept_quest/complete_quest, create_match, reset_weekly via `ctx.tables.list_rows_with_keys`). chat_reducers.rs.txt already had full parity (15/15).
- **`ReducerContext::timestamp()` method added** — template code called `ctx.timestamp()` but only the field existed; embedded templates had never been compile-checked. (Rust allows same-name field+method.)
- **Embedded scaffold VERIFIED end-to-end**: `voltra init --template rust/game-ready` → `cd embedded && cargo build --release` compiles clean and boots with all 28 native reducers auto-registered. On THIS box use `cargo +stable-x86_64-pc-windows-gnu build` (no MSVC installed; repo pins gnu via rust-toolchain.toml, scaffolds don't — correct for normal users). PITFALL 110: building scaffolded projects from MSYS bash hits coreutils `link` shadowing → use PowerShell.
- **Unity + Godot templates now ship `server/`** — the full native game server (EMBEDDED_* + GAME_REDUCERS_RS via `write_native_game_server`), so `voltra init --template unity|godot` = client + native-speed backend in one scaffold. "Same or better across templates" now holds: native/WASM templates at full benchmark speed; JS templates remain the dev on-ramp with `voltra build` → WASM as their production path.

### Session 54 — OCC lost-update fix + third-party driver validation + write ceiling + soak

**1. Lost-update race FIXED — optimistic concurrency control on TableStore (the production-blocker):**
- `StoredRow.version: u64` — bumped on every write (`write_row_unlocked`); rebuilt naturally on WAL replay.
- `TableStore::get_row_with_version` / `row_version`; `apply_delta_batch_versioned(deltas, read_versions)` validates INSIDE the row locks: every row the txn read AND writes must still be at its read version, else `VoltraError::TxnConflict` (new variant). Plain `apply_delta_batch` = versioned with empty read set.
- `ReducerContext.read_versions: Mutex<HashMap<(table,key),u64>>` — recorded on first `get_row` of each key (missing row = v0, so insert races conflict too); `commit()` passes the read set; `reset_for_retry()` clears writes+reads for re-execution. Read-only rows do NOT conflict (lost-update guard only, no write-skew enforcement — same as Redis WATCH-on-written-keys).
- **Both worker loops retry on TxnConflict (max 5)**: server.rs inline loop; main.rs moved execute+commit INSIDE spawn_blocking with an `Outcome` enum (Done/ReducerErr/Panicked/CommitErr) since retry needs the ctx.
- 3 regression tests in context.rs: concurrent RMW conflicts + retry converges (100-30-20=50, zero lost), read-only rows don't abort, insert race detected. **541 lib tests green.**
- PITFALL 109: any new write path that bypasses `ctx.commit()` (admin endpoints use set_row directly) skips OCC — fine for last-write-wins admin ops, never route reducer RMW through them.

**2. Third-party driver mileage (caveat 3): ioredis + node-postgres, 21/21 PASS** against the live binary — ioredis: PING/SET/INCR/EXPIRE/HSET/LPUSH/SADD/ZADD WITHSCORES/MULTI-EXEC/SCAN/DBSIZE/100-deep pipeline/pub-sub; node-postgres: version()/CREATE/INSERT..RETURNING/**parameterized queries (extended protocol $1)**/aggregates/BEGIN-COMMIT/information_schema. (No Python pip on this box; Node drivers are the mainstream choice anyway.)

**3. Write-path ceiling measured (caveat 2, data half): 351K TPS** — `stress --clients 50 --pipeline 512 --reducer stress_write` = 8.78M full-path writes in 25s, 0 errors, p99 95ms, memory flat, WITH OCC checks active. 30K real players @5-10 actions/s = 150-300K TPS → inside the ceiling. Connection-count half of 30K validation still requires remote client machines.

**4. 24h soak RUNNING** (caveat 2, soak half): server on :3500/:3501 (dirs in %TEMP%/voltra_soak), `voltra-soak --duration-secs 86400 --clients 100 --rate-per-client 10 --csv /tmp/voltra_soak/soak.csv`, logs /tmp/voltra_soak/soak.log. First sample: 1023 TPS, 0 err, 24.5MB. Leak detector fails the run if memory grows >200%. For the full week: same command with `--duration-secs 604800`.

### Session 53d — 20Hz tick coalescing + Unity & Godot client templates

**Tick coalescing (subscriptions.rs, config.rs):** `SubscriptionManager.pending: DashMap<(table,row_key), RowDelta>` + `start_tick_flusher(tick_ms)` (spawned in both server.rs and main.rs startup). When enabled, `publish_deltas` stages latest-per-row; the flush task drains every tick and calls the old path (`publish_now`). Coalescing happens BEFORE matching/encoding so it cuts predicate-matching + encode + delivery by the same factor. Config `sub_tick_ms` default **50ms (20Hz)**, env `VOLTRA_SUB_TICK_MS`, 0 = immediate (Manager::new defaults off — lib tests unaffected). Last-write-wins per row incl. deletes; cross-row ordering within a tick is not preserved (state-sync semantics).
**Measured:** 1K subscribed players (the run that previously wedged the server): **p50 1.78ms, worst-lobby p99 8.13ms, PASS**; fan-out demand ~1M frames/s coalesced to 41.5K/s delivered (24×). 5K subscribed: p50 6.4ms, 19.1K TPS, PASS (tail spikes during connect ramp noted).

**Unity + Godot templates (`voltra init --template unity|godot`):** `src/engine_templates/` — `unity_VoltraClient.cs` (zero-dep C# client: minimal MessagePack writer/reader, ClientWebSocket, async Call + Subscribe, MainThreadQueue), `unity_VoltraBehaviour.cs` (MonoBehaviour pumping callbacks on Update), `godot_voltra_client.gd` (Godot 4 autoload: WebSocketPeer, awaitable call_reducer, row_update signal, inner MsgPack class), + READMEs. Wire format follows voltra-client-ts/src/protocol.ts conventions (structs=arrays, enums=1-entry maps, Vec<u8>=bin, bare ReducerResponse arrays). PITFALL 108: any new client SDK must mirror protocol.ts — rmp_serde encodes structs positionally.

### Session 53c — Subscription fan-out stress mode + 3 server fixes → 567K frames/s sustained

Game bots now subscribe to their lobby (`sim_players WHERE lobby = 'lN'`) like real clients; `sim_spawn` writes a `lobby` field parsed from the pid prefix. `WsConn::call` decodes `ServerMessage` properly (fan-out frames counted in `SUB_FRAMES`, only `ReducerResponse` completes a call — also makes `ok` reflect `r.success`). Report prints fan-out frames/s.

**Three server bugs found and fixed under fan-out load (websocket.rs):**
1. **Per-frame flush** — write task did `sink.send()` per message = one flush syscall per frame; at 344K frames/s the server drowned (TPS 31K→4.1K). Fixed: drain-and-feed batching — `feed()` up to 256 queued frames, `flush()` once.
2. **Cooperative-scheduling starvation from fix #1** — `feed()`+`flush()` on a writable socket can complete without ever returning Pending; 1000 hot write tasks starved the entire runtime (accepts, reads, health endpoint — server appeared dead while alive). Fixed: `tokio::task::yield_now().await` per batch. PITFALL 106: any hot loop of always-ready awaits needs an explicit yield.
3. **Reducer responses dropped under flood** — responses and fan-out frames share the per-client write channel (cap 256, both `try_send`); when fan-out filled it, responses were dropped → 5s client timeouts (1.1% errors). Fixed: response task uses blocking `send().await` (backpressure on that client only); fan-out frames remain droppable (stale game state is shed first). PITFALL 107.

**Measured (single box, server+client sharing 24 cores):**
- 500 players subscribed (7 lobbies×75): **567K fan-out frames/s sustained**, p50 11ms, worst-lobby p99 44ms, PASS — silky WITH live subscriptions.
- 1000 players subscribed: 13.2K TPS / p50 19ms while healthy, but ~1M frames/s demand exceeds the shared box → cascade collapse at ~20s (client can't decode fast enough → TCP backpressure → server write queues fill → response backpressure → timeouts).

**Known next step (designed, not built): tick-based delta coalescing** — real engines sync state at 10–20Hz, not per-write. Per-subscription tick aggregation (latest version of each row per tick window) would cut fan-out volume ~5–10× and is the proper fix for >1M frames/s demand. Slow-consumer eviction (Redis-style client-output-buffer-limit) is the companion safety valve.

### Session 52 — MVCC engine + full Redis (RESP) + PostgreSQL (pgwire) protocol layers

**Direction from owner**: build all three at once, full compatibility: 1) MVCC store, 2) Redis protocol, 3) PostgreSQL protocol.

**New deps**: `im 15` (persistent HAMT/Vector/OrdSet — O(1) clones for version creation), `ordered-float 4` (zset score index), `sqlparser 0.45` (PostgreSQL-dialect SQL parser), `bytes` serde feature.

**1. MVCC engine — `src/mvcc/` (mod.rs + aof.rs, 16 tests)**
- `MvccStore`: `DashMap<NsKey, Chain>` of version chains (`SmallVec<[Version; 2]>`, newest first). Readers pick newest version with `commit_ts <= read_ts` — readers never block writers, writers never block readers.
- **Single sequencer OS thread** owns ALL mutation (Redis-style): `Batch::Apply` closures get a `Writer` with linearizable read-modify-write (INCR semantics for free); `Batch::Commit` does first-committer-wins conflict detection (PostgreSQL snapshot isolation). Group commit: drains up to 512 batches per wakeup, one AOF write + policy fsync.
- `pin_snapshot() -> SnapshotGuard` (RAII, refcounted BTreeMap) — GC floor. GC thread prunes dead versions + tombstoned chains every 5s; active expiry reaps TTL'd keys through the sequencer every 100ms (versioned + AOF-durable deletes).
- Namespaces: 0–15 Redis DBs, 64 PG catalog, 65+ PG tables.
- Datum enum: Str(Bytes)/Hash/List/Set/ZSet (im collections)/Row(im::HashMap<String, Scalar>). ZSet = dual index (by_member + by_score OrdSet).
- AOF: `[len][crc32][rmp(AofRecord{ts, ops})]`, torn-tail tolerant; SAVE writes snapshot (tmp+rename) and truncates AOF; replay skips records ≤ snapshot ts (crash between rename and truncate is safe). FsyncPolicy: Always/EverySec (default, Redis-like)/No.
- Effects (`WriteOp::Put/Del`) are resolved BEFORE logging — nondeterministic commands (SPOP etc.) replay deterministically.

**2. Redis layer — `src/redis/` (resp, engine, cmd_string, cmd_hash_list, cmd_set_zset, pubsub, mod, 40 tests)**
- ~150 commands: full strings (SET with EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET, GETEX/GETDEL, INCR*, bitmaps SETBIT/GETBIT/BITCOUNT), keys (EXPIRE w/ NX/XX/GT/LT, RENAME, COPY, SCAN w/ MATCH/COUNT/TYPE, KEYS, RANDOMKEY, FLUSHDB/ALL, SWAPDB), hashes (incl. HRANDFIELD, HSCAN NOVALUES), lists (LMOVE/RPOPLPUSH, LPOS, LINSERT; blocking BLPOP/BRPOP/BLMOVE/BRPOPLPUSH via 20ms sequencer polls), sets (SINTERCARD, S*STORE), zsets (ZADD NX/XX/GT/LT/CH/INCR, unified ZRANGE BYSCORE/BYLEX/REV/LIMIT, ZRANGEBYLEX, Z*STORE w/ WEIGHTS/AGGREGATE), MULTI/EXEC/DISCARD/WATCH (MVCC ts-based conflict abort), full pub/sub (channels + glob patterns), HELLO 2/3 (RESP2+RESP3), AUTH, SELECT 0-15, INFO/CONFIG/CLIENT/COMMAND/DBSIZE/SAVE/TIME/DEBUG SLEEP/MEMORY USAGE/ACL stubs.
- **Key design — `Db` trait** (`engine.rs`): every data command implemented ONCE against the trait. `SnapDb` (lock-free snapshot) runs reads on connection tasks in parallel across cores; `mvcc::Writer` runs writes inside the sequencer. `is_write()` routes. EXEC = one Apply closure dispatching all queued commands atomically.
- RESP2/RESP3 parser handles inline commands, rejects allocation bombs; encoder degrades RESP3 types (Map/Set/Push/Double/Bool/Verbatim) for proto-2 clients.
- Not supported (returns clear errors): Lua scripting (→ reducers), streams, cluster commands, REPLICAOF (→ Voltra replication).

**3. PostgreSQL layer — `src/pg/` (types, catalog, executor, mod, tests; 18 tests)**
- pgwire v3: startup (SSLRequest declined politely), trust + cleartext auth, ParameterStatus/BackendKeyData, simple query (multi-statement), extended protocol (Parse/Bind/Describe/Execute/Close/Sync, $n params, text+binary param decode). Describe(portal) executes eagerly + caches (single execution guaranteed); Describe(statement) resolves column metadata from catalog without executing.
- SQL executor (sqlparser 0.45 AST): CREATE/DROP TABLE (SERIAL/PK/NOT NULL), INSERT multi-row VALUES + INSERT..SELECT + RETURNING, SELECT (WHERE, expressions, INNER/LEFT JOIN..ON, GROUP BY + COUNT/SUM/AVG/MIN/MAX + HAVING, ORDER BY incl. output aliases + positions, LIMIT/OFFSET, DISTINCT, non-correlated subqueries: IN (SELECT), scalar, EXISTS), UPDATE/DELETE + RETURNING, TRUNCATE, BEGIN/COMMIT/ROLLBACK, SET/SHOW, scalar fns (LOWER/UPPER/LENGTH/CONCAT/COALESCE/NULLIF/GREATEST/LEAST/ABS/ROUND/FLOOR/CEIL/NOW/VERSION/RANDOM), CASE, CAST/::, LIKE/ILIKE, BETWEEN, IS NULL.
- **Transactions = real snapshot isolation**: BEGIN pins MVCC snapshot; reads see frozen state + own writes (overlay); COMMIT submits effects with conflict keys → concurrent update aborts with `could not serialize access` (40001). Aborted txn rejects statements until ROLLBACK (25P02).
- Catalog persisted as JSON blobs in ns 64; rowid counters rebuilt from key scan on boot (survives AOF replay). Rows = `Datum::Row` keyed by 8-byte BE rowid.
- `information_schema.tables/columns`, `pg_catalog.pg_tables` shims; `version()` → "PostgreSQL 16.4 (Voltra)".

**4. Wiring (config.rs, server.rs, main.rs, sim.rs)**
- Config: `redis_port` (default 6379), `pg_port` (default 5432), `redis_password`, `pg_password`; 0 = disabled. Env: VOLTRA_REDIS_PORT/VOLTRA_PG_PORT/VOLTRA_REDIS_PASSWORD/VOLTRA_PG_PASSWORD.
- `server::spawn_protocol_listeners(&config)` — called in both main.rs start path and run_server; **bind failures are non-fatal** (warn + continue) so parallel test servers don't die racing for 6379/5432. MVCC data dir = `<wal dir>/mvcc_data`.
- sim.rs embedded/stress servers set both ports to 0.

**Live-verified** (production binary, raw TCP): Redis PING/SET/GET/HSET/HGETALL/INCR/ZADD/ZRANGE/EXPIRE/TTL/DBSIZE; PG startup + CREATE/INSERT/SELECT/aggregates/BEGIN-UPDATE-COMMIT; **kill -9 + restart recovered everything** (strings, hashes, counters, zsets, ticking TTLs, SQL table + transactional update, catalog + rowid counters).

**Build status after Session 52:**
- `cargo build` → zero errors. `cargo test --lib` → **538 tests passing** (was 466; +72: 16 mvcc + 40 redis + 18 pg, with 2 prior duplicates renumbered).

**New pitfalls:**
95. **All MVCC mutation goes through the sequencer** — never mutate `chains` outside `sequencer_loop`/`apply_ops` (GC pruning of strictly-dead versions is the one exception). Redis write commands MUST be dispatched via `store.apply()`, never against `SnapDb`.
96. **`Writer::get` lazily expires** — reading an expired key inside the sequencer auto-stages a `Del`. Helpers in `redis::engine` read the value BEFORE the TTL (`read_hash` etc. return `(coll, exp)`); preserve that order or a dead TTL gets re-attached.
97. **`is_write()` and `is_data_command()` must stay in sync** — a write command missing from `is_write` would dispatch to a read-only snapshot (debug_assert + silently lost write in release). Test `write_classification_is_complete` guards this; extend it when adding commands.
98. **sqlparser 0.45 uses struct variants** — `Statement::Insert { .. }`, `Statement::CreateTable { .. }`, `Statement::Delete { from: FromTable, .. }`, `Function.args: Vec<FunctionArg>`. Do NOT upgrade sqlparser without sweeping `src/pg/executor.rs`; 0.46+ moved these to tuple variants + `FunctionArguments`.
99. **PG DDL auto-commits** even inside BEGIN (v1 behavior, documented in exec_create_table).
100. **MvccStore lifecycle** — `close()` sets the shutdown flag; background threads poll it at 100ms. Tests must call `store.close()`; the AOF test sleeps ~250ms before reopening the same data dir so the old sequencer releases the file handle.
101. **Protocol listener bind failures must stay non-fatal** — integration tests spawn 9 parallel servers that all race for 6379/5432; only one wins, the rest log warnings. Never convert `spawn_protocol_listeners` errors into a process exit.

### Session 51 — Memory/RAM optimization: hybrid row encoding + slot-based locks

**Motivation**: sim benchmark showed 136.5MB → 2,149MB (+1573%) over 600s at 3.1M rows (~649 bytes/row), driven by JSON row bytes + per-row DashMap lock entries.

**What was built (`src/table/mod.rs`):**

**1. Hybrid row storage encoding** (replaces `serde_json::to_vec` / `serde_json::from_slice`):
- New `encode_row(value: &Value) -> Result<Bytes>` free function:
  - Small rows (MsgPack bytes < `ZSTD_THRESHOLD = 256`): tag `0x00` + raw MsgPack
  - Large rows (≥ 256 bytes): tag `0x01` + zstd-compressed MsgPack (level 1)
  - Typical game rows (30-80 bytes) hit the small path — zero compression overhead
  - Blob rows, inventory arrays hit the large path — ~8-10x smaller than JSON
- New `decode_row_bytes(data: &[u8]) -> Result<Value>` free function:
  - Reads tag byte, dispatches to raw `rmp_serde::from_slice` or `zstd::decode_all` + `rmp_serde::from_slice`
  - Unknown tag → `SerializationError`
- Updated all 6 call sites that directly read `row.data` (previously `serde_json::from_slice`):
  - `decode_row` (canonical decoder)
  - `write_row_unlocked` (old value capture for index maintenance)
  - `delete_row_unlocked` (old value for index removal)
  - `create_index` (backfill existing rows)
  - `scan_column`, `count_by_field`, `distinct_field_values` (columnar reads)
- `write_row_unlocked` returns `payload_arc: None` in its delta (previously `Some(arc_bytes)`). `row_data: Some(final_value)` carries the value; `row_data_value()` falls through correctly.

**2. Fixed-slot mutex pool replaces per-row `DashMap<String, Arc<Mutex<()>>>`**:
- `const LOCK_SLOTS: usize = 512` — 512 fixed `Mutex<()>` slots per table in a `Box<[Mutex<()>]>`
- `fn slot_for_key(key: &str) -> usize` — FNV-1a hash → `[0, 512)`
- Two distinct keys may share a slot (false contention) but remain serializable-isolated
- `apply_delta_batch` locking changed from `Vec<Arc<Mutex<()>>>` to `Vec<(Arc<Table>, usize)>` pairs; sorted by `(table_ptr, slot)` and deduped before locking
- **Memory savings**: eliminates `~128 bytes × N_rows` (DashMap entry + key string + Arc<Mutex> heap alloc)

**Benchmark results (500 players + 500 chat, 60s):**
| Metric | Before | After |
|---|---|---|
| TPS | ~13K | ~42K |
| p50 latency | ~33ms | ~11ms |
| p99 latency | ~41ms | ~22ms |
| Memory growth | heavy | essentially flat |

**Build status after Session 51:**
- `cargo build --lib` → zero errors, zero warnings
- `cargo test --lib` → **466 tests passing** (unchanged count)

**New pitfalls:**
91. **Hybrid row tag bytes — never interpret raw `row.data` without `decode_row_bytes`** — stored bytes now start with a tag byte (`0x00` = raw MsgPack, `0x01` = zstd+MsgPack). Any code that reads `row.data` directly (bypassing `decode_row()` / `decode_row_bytes()`) will get garbage. All read paths go through the helpers; do not add new direct reads.
92. **`payload_arc` is no longer set by `write_row_unlocked`** — `write_row_unlocked` sets `payload_arc: None` in returned deltas; `row_data: Some(value)` carries the value. `row_data_value()` checks `payload_arc` first (for context.rs deltas which still set it), then falls back to `row_data`. Do not add code that depends on `payload_arc` being set in write-path deltas.
93. **`ZSTD_THRESHOLD = 256` is per MsgPack bytes, not JSON bytes** — since MsgPack is ~40% smaller than JSON, a row with 350-byte JSON (~210 bytes MsgPack) would NOT be compressed. Only rows with MsgPack > 256 bytes get compressed. This is intentional.
94. **Slot-based locks may over-serialize** — two unrelated rows that hash to the same slot will serialize against each other. This is correct (no data races) but slightly sub-optimal. At 512 slots and random key distribution, collision probability is 1/512 per pair. Do NOT use slot locking for anything other than apply_delta_batch write isolation.

### Session 49 — Multi-tenancy: complete namespace isolation (loop task 3 of 4)

**What was built:**

**`src/tenant.rs`** (NEW, ~450 lines, 9 unit tests):
- `physical_table(tenant_id, logical)` → `"tn:<id>:<logical>"` / `logical_table()` strips prefix
- `belongs_to_tenant(physical, tenant_id)` — exact prefix check (no false positives on prefix collision)
- `TenantInfo { id, name, api_key, max_rows, max_calls_per_sec, created_at }` — persisted to `__tenants` system table
- `TenantRegistry::load(tables)` — hydrates from `__tenants` on startup (WAL/snapshot replay populates it first)
- `create(name, max_rows, max_calls_per_sec)` → generates `id = slug-XXXXXX` + `api_key = ndbt_<32hex>`, returns delta for caller to WAL-journal
- `delete(tenant_id)` — drops all `tn:<id>:*` rows + the tenant row, returns all deltas
- `resolve_key(raw_token)` — fast DashMap lookup; supports `key:role` suffix convention
- Token-bucket rate limiter with continuous refill per tenant (0 = unlimited)
- `tenant_row_count` / `row_quota` — for quota enforcement at commit time
- `summary_json(include_keys)` — admin API response; keys masked unless `include_keys = true`

**`src/reducer/context.rs`** (updated):
- `pub tenant_id: Option<String>` + private `tenant_registry: Option<Arc<TenantRegistry>>`
- `with_tenant(tenant_id, registry)` builder — enables namespace isolation for the context
- `phys(table_name)` — resolves logical → physical name; passes through `__*` and `tn:*` unchanged
- All read/write methods (`get_row`, `set_row`, `delete_row`) use `phys()` — completely transparent to reducer code
- `get_counter` / `set_counter` in tenant path: counters stored as regular rows in `tn:<id>:counters` (no `counter_add` atomic, see pitfall 84)
- `commit()` — quota check before `apply_delta_batch`: counts pending inserts + current rows against quota
- RLS enforcement uses `logical_table()` to strip prefix before schema lookup

**`src/network/websocket.rs`** (updated):
- `PendingCall.tenant_id: Option<String>` — carried from handshake to worker
- `start_listener` / `handle_client` take `Arc<TenantRegistry>` parameter (last arg)
- Handshake: `ndbt_*` tokens resolved via `TenantRegistry::resolve_key`; on success sets `tenant_id` cell; invalid tenant key → 401
- Tenant rate limit: `tenant_registry.check_rate(tid)` gated in addition to per-caller rate limiter
- Subscribe: `rewrite_query_for_tenant(query, tenant_id)` rewrites first token to physical table name
- `sub_task`: `strip_tenant_frames(frames, tenant_id)` decodes each outbound frame and strips `tn:<id>:` prefix so clients see logical table names
- Helpers: `rewrite_query_for_tenant`, `strip_tenant_prefix_from_frame`, `strip_tenant_frames`

**`src/main.rs`** (updated):
- `TenantRegistry::load(tables.clone())` initialized after WAL/snapshot recovery, before `start_listener`
- `AdminState.tenant_registry: Arc<TenantRegistry>` — threaded through metrics server
- Worker loop: `ctx = ctx.with_tenant(tid, tenant_w)` when `call.tenant_id.is_some()`
- Tenant admin endpoints:
  - `GET  /admin/api/tenants` — list (keys masked)
  - `POST /admin/api/tenants` — create; returns `{ id, api_key, name }`; WAL-journaled
  - `DELETE /admin/api/tenants?id=<id>` — delete tenant + all data; WAL-journaled
- All `PendingCall` constructions updated with `tenant_id: None` (scheduler + admin console are global)

**`src/lib.rs`** (updated): `pub mod tenant;` + re-exports `TenantRegistry, TenantInfo, physical_table, logical_table`

**`src/server.rs`** (updated): `TenantRegistry::load` + pass to `start_listener`

**New pitfalls:**
83. **Tenant `counter_add` reads from global "counters" table** — `apply_delta_batch`'s `counter_add` handler always calls `self.get_counter(name)` which reads from `self.tables.get_row("counters", name)`, NOT from the physical tenant counter table. For tenant contexts, `set_counter` is therefore converted to a regular row write to `tn:<id>:counters` (no atomic RMW). Single-tenant counter increments are still row-locked; multi-tenant counter isolation is guaranteed. What is lost is the N-worker concurrent-increment atomicity — acceptable for isolated tenant workloads.
84. **Tenant subscription frame rewrite is O(decode+encode) per frame** — `strip_tenant_prefix_from_frame` decodes MsgPack, replaces `table_name`, re-encodes for each outbound subscription frame on tenant connections. Non-tenant connections skip this entirely. This is the correct trade-off: tenant clients see logical names on the wire.
85. **`TenantRegistry::load` must be called AFTER WAL/snapshot replay** — the `__tenants` table is populated by replay. Calling it before replay means zero tenants are loaded.
86. **Tenant admin endpoints require `VOLTRA_API_KEY`** — `admin_auth_check` guards all `/admin/api/*` routes. `POST /admin/api/tenants` returns the raw `api_key` exactly once; it is never shown again through `GET /admin/api/tenants` (keys are masked).

**Build status after Session 49:**
- `cargo build` / `cargo build --release` → **zero errors, zero warnings** (sim warnings are pre-existing).
- `cargo test --lib` → **443 tests passing** (was 433 before; +10: 9 tenant tests + 1 new context test).

---

### Session 48 — Production wave: CPU timeouts + admin console (loop tasks 1-2 of 4)

**Loop plan (user-directed, via /loop):** 1) Reducer CPU timeouts ✅ 2) Operational UX admin dashboard ✅ 3) Multi-tenancy (next) 4) Horizontal scaling/cluster resurrection. No partial systems allowed.

**1. Reducer CPU timeouts (`src/reducer/v8.rs`, `src/reducer/registry.rs`):**
- QuickJS interrupt handler (`rt.set_interrupt_handler`) checks a thread-local `QJS_DEADLINE: Cell<Option<Instant>>`; returns `true` past deadline → script aborted.
- `DeadlineGuard` RAII arms/clears the deadline around `reducer_fn.call`; timeout error = `"Reducer timeout: exceeded N ms CPU budget"`.
- After a timeout the warm Context is EVICTED from `QJS_CTXS` (partially-mutated JS globals) — next call rebuilds from source. DB state safe (error path skips commit).
- Default timeout: per-module `timeout_ms` > `VOLTRA_REDUCER_TIMEOUT_MS` env > 5000ms.
- WASM already capped at 1M fuel; native reducers are trusted (documented, not killable).
- 4 new tests: infinite loop killed <1s, worker survives + same script retryable, staged writes discarded, fast reducers unaffected. 433 lib tests green.

**2. Admin console (`src/admin_dashboard.html` NEW ~700 lines, `src/main.rs`):**
- `GET /admin` on the metrics port serves an embedded single-file dark-theme dashboard (include_str!, no build step, vanilla JS + canvas charts).
- Tabs: Overview (TPS/p99/memory/WAL/queue/uptime cards + 4 live charts, 2s poll of /healthz + /metrics with client-side Prometheus parsing incl. histogram quantiles), Tables (browser, filter, row add/edit/delete via modal), SQL console (Ctrl+Enter, history in localStorage), Reducers (list + invoke with JSON args), Schema viewer, Operations (backup now, replication status/promote, paste-a-migration, API key, server info).
- New endpoints in `handle_metrics_request`:
  - `POST /admin/api/call` — dispatches a real `PendingCall` through `queue_probe` (caller_id="admin-console", caller_role="admin"), 30s timeout.
  - `POST /admin/api/sql` — parse+execute in `spawn_blocking`.
  - `POST /admin/api/row` / `DELETE /admin/api/row` — DURABLE writes: `set_row`/`delete_row` → `publish_deltas` → WAL append with `__admin_set_row`/`__admin_delete_row` reducer name (unlike /seed which bypasses both).
  - Helpers: `admin_auth_check` (Bearer == VOLTRA_API_KEY when configured; open in dev), `bad_request`, `server_error`, `url_decode`.
- All 5 endpoints live-verified with curl; dashboard JS syntax-checked with `node --check`.

**Also in session 48 (pre-loop):**
- `run_server_with_handle(config) -> (ServerHandle, impl Future)` in `src/server.rs` — `ServerHandle { tables, subs, wal_file_size }` for embedded stats without HTTP. Exported from lib.rs. `BatchedWalWriter::file_size_arc()` added.
- `src/bin/sim.rs` + `[[bin]] voltra-sim` — high-end simulation benchmark (game/chat/mixed/scale scenarios, 24 embedded JS reducers, virtual-user behavioral state machines, HDR latency, live mem/WAL/rows/conn stats via ServerHandle). Verified: 500 players → 2.86M calls, 43K TPS, p99 19ms, 0 errors.

**New pitfalls:**
79. **rquickjs interrupt handler is per-Runtime, registered once** — it reads `QJS_DEADLINE` thread-local; never register a second handler. No deadline armed (None) = never interrupt (context build, preamble eval).
80. **Evict warm QJS context after timeout** — a killed script leaves partially-mutated JS globals; `QJS_CTXS.remove(&script_key)` forces a clean rebuild. Do not skip this.
81. **Admin row writes must publish + WAL-append** — `/admin/api/row` is a durable write path unlike `/seed`. If you add admin mutations, follow the same delta → publish_deltas → wal append pattern.
82. **`queue_probe` is a full kanal sender, not just a depth probe** — `/admin/api/call` sends real PendingCalls through it. Renaming/removing it breaks both /healthz depth and admin invocation.

### Sessions 1–26
(See previous AGENTS.md for full detail. Summary: TableStore, kanal channel, N-worker dispatch, BatchedWalWriter, snapshots, auth, query engine, indexes, scheduled reducers, TypeScript/Rust SDKs, schema migrations, WASM-first JS, columnar storage, end-to-end bench, templates, typed schema, React hooks.)

### Session 37 — Integration test port collision fix

**Root cause**: All 9 integration tests spawn real server child processes in parallel. Every child process inherits the `cargo test` CWD (the project root), which contains `voltra.toml`. `Config::from_env()` calls `find_config_in_cwd()` which walks up from CWD and loads that TOML, giving `metrics_port = 3001` to every server instance. When 9 servers all race to bind port 3001, only one wins — the rest exit immediately before the WebSocket listener has a chance to start. Every test then hits the 5-second poll timeout and panics with "Server did not become ready within 5s".

**Fix** (`tests/integration.rs` — `spawn_server_with_env` only):
- Added `VOLTRA_METRICS_PORT` env var set to `ws_port + 1000` for every spawned server.
- Port mapping: WS 18080 → metrics 19080, WS 18081 → 19081, …, WS 18093 → 19093.
- `VOLTRA_METRICS_PORT` is already handled by `apply_env_overrides()` in `config.rs` — it takes priority over the TOML value. No server code changed.

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
- Tests are pure in-process — no actual HTTP connections, no running server. The cluster HTTP layer is tested at the integration level via the existing `voltra start` path.
- `wire_to_row_deltas_drops_invalid_base64` confirms graceful degradation: a corrupt delta from a peer is silently skipped rather than crashing the receiving node.

**Build status after Session 36:**
- `cargo test` → 121 tests passing.
- `cargo build --release` → zero errors, zero warnings.

### Session 35 — TODO-026: `voltra seed` command

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
voltra start
voltra seed seed.json                   # seed from file
voltra seed seed.json --dry-run          # preview only
voltra seed seed.json --metrics-url http://127.0.0.1:3001
voltra get players                       # verify rows landed
```

**Build status after Session 35:**
- `cargo build --release` → zero errors, zero warnings (expected).

### Session 32 — v8.rs complete rewrite + scheduler name fixes

**Root cause fixed**: `src/reducer/v8.rs` was fundamentally broken in three ways:
1. `__voltra_set` only called `.as_number()` on the third argument — every call with a JSON object (all game reducers) silently wrote `0` and discarded the object. All game reducers (spawn, attack, buy_item, etc.) were broken.
2. Scheduler calls with no `args_json` passed empty bytes `[]` to `rmp_serde::from_slice` which crashed with `MessagePack decode error: IO error while reading marker: failed to fill whole buffer`.
3. `__voltra_get` only pre-fetched counters — calling `__voltra_get("players", "alice")` always returned `null`.

**Fixes applied** (`src/reducer/v8.rs` — complete rewrite):
- `__voltra_set` now accepts any JS value (objects, arrays, strings, numbers). Objects → `ctx.set_row()`. Plain numbers in `"counters"` table → `ctx.set_counter()` for backward compat.
- `__voltra_get` now calls `ctx.get_row()` for any table — full read-your-writes support.
- Empty args bytes → default to `Value::Array(vec![])` instead of crashing.
- Added `__voltra_delete(table, key)` — JS reducers can now delete rows.
- Added `__voltra_get_all(table)` — returns all rows as a JS array.
- Added `__voltra_caller_id` and `__voltra_caller_role` as JS globals — reducers can gate logic on who called them.

**Scheduler name fixes** (`src/main.rs` — targeted edit):
- Template was generating `cleanup_expired_sessions` → fixed to `cleanup_sessions` (matches registered reducer).
- Template was generating `refresh_matchmaking` → fixed to `refresh` (matches registered reducer).

**Verified working**:
- `voltra start` → no more MessagePack errors, all 3 schedulers fire cleanly.
- `voltra call spawn '["player1", 0, 0, "warrior"]'` → returns correct player object with stats.
- `voltra watch "players WHERE zone = 'zone_0_0'"` → initial_snapshot delivers full player row.
- 6 new unit tests added to v8.rs.

**Known remaining issue**: `voltra call attack '["player1", "enemy1", "sword", 25]'` returns `{"error": "Target not found"}` — correct behavior since `enemy1` was never spawned. Attack logic itself is fine.

### Session 27 — PowerShell Args Fix + TODO-022 partial
- `parse_args_json()` auto-quotes bare words in brackets for PowerShell compatibility.
- `PermissionsConfig`, `caller_role`, and websocket permissions check all wired.

### Session 28 — TODO-022 complete wiring (main.rs)
- `Arc<PermissionsConfig>` passed to `start_listener`.
- `ctx.caller_role` set in worker loop.
- Scheduler `PendingCall` gets `caller_role: "scheduler"`.

### Session 29 — Template system redesign
- `main.rs` completely rebuilt with 4 templates: `rust/basic`, `rust/game-ready`, `rust/chat`, `typescript`.
- `voltra templates` subcommand lists all templates.

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

**TODO-021 Optimistic Updates — TypeScript SDK** (`voltra-client-ts/src/`):
- `types.ts`: added `OptimisticCache`, `OptimisticOptions { optimistic, onRollback? }`.
- `client.ts`: `call()` now accepts optional third `OptimisticOptions` arg.
  - Pre-call: `snapshotCache()` deep-clones rowCache → `rollbackSnapshot`.
  - Applies `optimistic(rollbackSnapshot)` to live cache immediately.
  - On server error: `applyOptimisticCache(rollbackSnapshot)` + `onRollback?()`.
  - On timeout: same rollback.
  - On disconnect: `rejectAllPending()` rolls back all in-flight optimistic calls.
  - `applyOptimisticCache(cache)` and `snapshotCache()` are private helpers.

**TODO-021 Optimistic Updates — Rust SDK** (`voltra-client-rust/src/client.rs`):
- `CacheSnapshot = HashMap<String, HashMap<String, serde_json::Value>>`.
- `snapshot_dashmap_cache()` / `apply_snapshot_to_cache()` helpers.
- `Command::ApplyOptimistic` variant registers the rollback snapshot with the background task.
- `call_optimistic(reducer, args, |cache| new_cache)` — public async method.
  - Applies speculative state before sending the reducer call.
  - Background `dispatch_message()` removes snapshot on success, rolls back on failure.

---

## Current Build Status

After Session 51 (memory/RAM optimization):
- `cargo build` / `cargo build --release` → **zero errors, zero warnings**.
- `cargo test --lib` → **466 lib tests passing**. Zero failures.
- Memory: row storage switched from JSON to hybrid MsgPack/zstd encoding; per-row lock DashMap replaced with fixed 512-slot array.
- Multi-tenancy: full namespace isolation, quota enforcement, WAL-durable admin CRUD, live subscription prefix rewriting.
- Horizontal scaling: `src/cluster/` fully wired — shard routing, delta fan-out, gossip, proxy calls, dynamic join.

### Session 50 — Horizontal scaling: cluster system resurrected (loop task 4 of 4)

**Loop plan completion:** Task 4 of 4 — resurrect and complete the cluster system removed in Session 44.

**What was built:**

New module `src/cluster/` (4 files, restored from `pre-cluster-removal` tag + adapted to current architecture):

- **`src/cluster/mod.rs`** — `ClusterConfig`, `ClusterBus`, `PeerEntry`, `shard_for_key()`:
  - `ClusterConfig::from_env(my_shard_id, shard_count)` reads `VOLTRA_PEERS` (named: `shard1=http://...,shard2=http://...` or positional URL list), `VOLTRA_CLUSTER_SECRET`, `VOLTRA_GOSSIP_INTERVAL_MS`, `VOLTRA_CLUSTER_HTTP_TIMEOUT_MS`.
  - `ClusterBus::new(config) -> Arc<Self>` — DashMap of peers, lazy global `reqwest::blocking::Client`.
  - `fanout_deltas(&deltas)` — fire-and-forget fan-out, no-op when single-node.
  - `apply_peer_deltas(deltas, tables, subs)` — apply incoming peer deltas + local subscription fan-out.
  - `proxy_call(shard_id, reducer, args, caller_id, role) -> Result<Vec<u8>>` — forward to owning shard.
  - `add_peer(NodeInfo)` — dynamic registration via `/cluster/join`.
  - `peers_snapshot()` — JSON-serializable view for `/cluster/peers` endpoint.
  - `shard_for_key(key, shard_count) -> u32` — FNV-1a 64-bit, deterministic across all nodes.
  - 15 unit tests.

- **`src/cluster/fanout.rs`** — `fanout_to_peers()`, `start_fanout_retry()`, wire format:
  - Per-peer `spawn_blocking` tasks, 3-attempt exponential back-off (50/200/800ms).
  - `FanoutRetryState` — per-peer bounded `VecDeque<Arc<Vec<u8>>>` (max 1024 entries), background retry task every 5s draining up to 64 entries per healthy peer.
  - `row_deltas_to_wire()` / `wire_to_row_deltas()` — rmp → base64 → JSON for set/delete.
  - 8 unit tests.

- **`src/cluster/gossip.rs`** — `start_gossip()`:
  - Pings `GET /cluster/health` on every peer every `gossip_interval_ms` (default 5s).
  - 3 consecutive failures → peer marked unhealthy, skipped in fan-out.
  - Graceful shutdown via `watch::Receiver`.

- **`src/cluster/proxy.rs`** — `proxy_call()`:
  - POST `/cluster/call` JSON: `{ reducer_name, args_b64, caller_id, caller_role, target_shard_id? }`.
  - Response: `{ ok, result_b64 }` or `{ ok: false, error }`.

**Changes to existing files:**

- **`src/lib.rs`**: `pub mod cluster;` added.

- **`src/main.rs`**:
  - Reads `VOLTRA_SHARD_ID` + `VOLTRA_SHARD_COUNT` env vars (defaults 0/1).
  - `ClusterBus::new(ClusterConfig::from_env(...))` initialized after tenant_registry.
  - `cluster_bus` added to `AdminState`.
  - `cluster_w.fanout_deltas(&deltas)` called in worker loop immediately after `subs_w.publish_deltas()` — only fires for non-empty delta sets, no-op in single-node mode.
  - `start_gossip` + `start_fanout_retry` spawned before worker pool.
  - 5 new HTTP endpoints in `handle_metrics_request`:
    - `GET  /cluster/health` — liveness probe (returns `{ ok, shard_id }`)
    - `GET  /cluster/peers`  — peer list + health (`{ cluster_enabled, my_shard_id, shard_count, peers }`)
    - `POST /cluster/deltas` — receives replicated deltas, validates secret, applies + fan-outs
    - `POST /cluster/call`   — receives proxied reducer call, dispatches through real queue, returns result
    - `POST /cluster/join`   — dynamic peer registration (no restart needed)

**How to run a 2-node cluster:**
```powershell
# Node 0 (shard 0):
$env:VOLTRA_SHARD_ID="0"; $env:VOLTRA_SHARD_COUNT="2"
$env:VOLTRA_PEERS="shard1=http://127.0.0.1:4001"
$env:VOLTRA_CLUSTER_SECRET="mysecret"
$env:VOLTRA_METRICS_PORT="3001"; $env:VOLTRA_PORT="3000"
cargo run --release -- start

# Node 1 (shard 1):
$env:VOLTRA_SHARD_ID="1"; $env:VOLTRA_SHARD_COUNT="2"
$env:VOLTRA_PEERS="shard0=http://127.0.0.1:3001"
$env:VOLTRA_CLUSTER_SECRET="mysecret"
$env:VOLTRA_METRICS_PORT="4001"; $env:VOLTRA_PORT="4000"
cargo run --release -- start
```

**Shard routing (caller's responsibility):** Use `voltra::cluster::shard_for_key(row_key, shard_count)` to determine which node owns a given row. Reducer calls for rows owned by other shards should use `ClusterBus::proxy_call()`. The `voltra cluster-status` CLI command shows all peer health.

**New pitfalls:**
87. **`GLOBAL_HTTP_CLIENT` is a process-wide `OnceLock`** — the timeout is set on first call; subsequent calls ignore their `timeout_ms` arg. Set `VOLTRA_CLUSTER_HTTP_TIMEOUT_MS` before any cluster activity if non-default timeout is needed.
88. **Fan-out is fire-and-forget** — `fanout_deltas()` returns immediately after spawning blocking tasks. The local commit already succeeded; delivery failures are retried by the background task. Never block the worker loop waiting for fan-out.
89. **`/cluster/call` dispatches through the real queue** — proxied calls consume a queue slot and are subject to the same `reducer_queue_cap` limit. If the queue is full, the proxy returns 500 "Reducer queue closed".
90. **FNV-1a shard assignment is deterministic** — every node MUST use the same `shard_count` value. `VOLTRA_SHARD_COUNT` must be identical on all nodes. Changing `shard_count` requires a coordinated rolling restart (rows do not migrate automatically).

### Session 49 — Multi-tenancy (loop task 3 of 4)
See summary above. Multi-tenancy is now complete and production-ready.

### Session 47 — Production-readiness wave (solo, no agents)

**1. JS backend: Boa → rquickjs (QuickJS)** (`src/reducer/v8.rs` rewritten, `Cargo.toml`):
- `boa_engine`/`boa_gc` REMOVED; `rquickjs = "0.12"` (features=["full"]) added.
- One `Runtime` per OS thread (thread-local, 64 MiB memory cap via `set_memory_limit`), one `Context` per (thread, script path) — warm after first call.
- `Value<'js>` is invariant over `'js` → host fns CANNOT return `Value` from `Func::from` closures. Solution: raw host fns return `Option<String>` (JSON), a JS preamble (`JS_PREAMBLE`) wraps them with JSON.parse/stringify (`__voltra_get` etc. defined in JS over `__voltra_get_raw` etc.).
- `CURRENT_CTX: Cell<*mut ReducerContext>` thread-local pinned before each call.
- `EvalOptions` is NOT Clone — build a fresh one per eval_with_options call.
- `From<rquickjs::Error> for VoltraError` added to `src/error.rs`.

**2. Disk persistence (sled)** (`src/persistence/mod.rs`, prior session, kept):
- `VOLTRA_PERSISTENCE_PATH` enables write-through row persistence; rows load before snapshot+WAL on boot.

**3. WAL streaming replication + failover** (`src/replication/mod.rs` NEW):
- Async log-shipping: replica polls primary `GET /replication/wal?from_seq=N&max=M` (entries = base64(rmp(WalEntry))), applies deltas, fans out to its own subscribers, appends to its own WAL.
- Config: `VOLTRA_ROLE=replica` + `VOLTRA_PRIMARY_URL=http://primary:3001` + `VOLTRA_REPLICA_POLL_MS` (default 500).
- Global `IS_REPLICA: AtomicBool` — worker loops (both main.rs AND server.rs) reject reducer calls on replicas with "read-only replica" error. Reads + subscriptions still work.
- Failover: `POST /replication/promote` (or `voltra promote`) flips to primary, pull loop stops, writes accepted. `global_seq.fetch_max(last_applied+1)` prevents seq reuse after promotion.
- `GET /replication/status` + role/lag in `/healthz`.

**4. Automated backups + restore + PITR** (`src/backup.rs` NEW):
- `backup_now()` = snapshot + WAL copy + backup.json into `<dir>/backup_<unixsecs>_<seq>/`; `rotate_backups(keep)`; `restore_to_dirs(backup, wal_path, snap_dir, until_ts_nanos)` — PITR rewrites WAL keeping only entries with timestamp <= cutoff.
- Background task: `VOLTRA_BACKUP_DIR` + `VOLTRA_BACKUP_INTERVAL_SECS` (+ `VOLTRA_BACKUP_KEEP`, default 5).
- Endpoints/CLI: `POST /backup`, `voltra backup`, `voltra backups <dir>`, `voltra restore <backup> --wal-path W --snapshot-dir S [--until-ts NANOS]`, `voltra promote`.
- `AdminState` struct (wal_path, backup_dir, backup_keep) threaded through `start_metrics_server` → `handle_metrics_request`.

**5. WAL group commit — durability fix** (`src/wal/batch_writer.rs`):
- OLD: flusher held entries until 100ms timer or 512KB — server ACKED writes up to 100ms before they hit the OS; kill -9 lost them (crash_recovery tests were red).
- NEW: group commit — drain everything queued, write in one syscall, repeat. Same throughput under load (next batch = arrivals during previous write), durability window now microseconds. Explicit `Flush` acks sent only AFTER data is on disk.

**6. Anonymous-auth bug fix** (`src/network/websocket.rs`):
- With NO auth configured, connections without an Authorization header were REJECTED: the code called `auth_v.validate("Bearer ")` which returns `Denied("Empty token")` before the `AuthMode::None` arm. Now checks `matches!(auth_v.mode(), AuthMode::None)` directly. (Integration tests never caught it — they always send a Bearer header.)

**7. Protocol fuzz tests** (`tests/protocol_fuzz_test.rs` NEW — 9 tests, ~45k hostile inputs):
- Random bytes / bit-flipped valid frames / every-byte truncations into `decode_client_message` + `decode_reducer_call`; msgpack allocation-bomb headers (str32/array32/map32/bin32 claiming 4 GiB); 1000-deep nested JSON; garbage + corrupted WAL files into WalReader; garbage into `replication::decode_entries`. Deterministic xorshift PRNG (seed printed on failure).

**8. Soak harness** (`src/bin/soak.rs` NEW, `[[bin]] voltra-soak`):
- N clients × rate × duration with auto-reconnect; samples `/healthz` (memory/queue/WAL/rows) every interval; CSV export; HDR latency percentiles; PASS/FAIL verdict (error rate > `--max-error-pct` or memory growth > `--max-memory-growth-pct` ⇒ exit 1).
- Week-long run: `voltra-soak --duration-secs 604800 --clients 100 --csv soak.csv`.

**New pitfalls:**
73. **rquickjs `Value<'js>` invariance** — never try to return `Value` from a `Func::from` closure; use the raw-string + JS-preamble bridge pattern in v8.rs.
74. **rquickjs `EvalOptions` is not Clone** — construct a fresh one per eval call.
75. **`Start-Process -Environment` does not exist in PowerShell 5.1** — use the Bash tool with env-var prefixes to spawn servers with custom env.
76. **WAL flusher must group-commit** — do not reintroduce timer-only flushing; ACKed writes must reach the OS promptly or crash tests fail (correctly).
77. **Anonymous auth check uses `auth_v.mode()`** — never `validate("Bearer ")` with an empty token to probe whether auth is configured.
78. **Replica seq handoff** — after applying replicated entries, `global_seq.fetch_max(last+1)`; without it a promoted replica reuses sequence numbers.

After Session 45 (TODO-028 complete — `run_server` library API + scaffold `#[reducer]` templates):
- `cargo build` → **zero errors, zero warnings** (full workspace: voltra + voltra-macros).
- `cargo test --lib` → **417 lib tests passing**, zero failures.
- `src/server.rs` — `pub async fn run_server(config: Config) -> Result<()>` is complete and correct.
- All three scaffold functions now write an `embedded/` subdirectory with `Cargo.toml`, `src/main.rs`, and `src/reducers.rs` using the `#[reducer]` macro syntax.

After Session 44 Wave 2 (TODO-027 — `voltra-macros` proc macro crate):
- `cargo build --offline` → **zero errors, zero warnings** (full workspace: voltra + voltra-macros).
- `cargo test --lib --offline` → **417 lib tests passing**, zero failures.
- `cargo test --package voltra-macros --offline` → **2 unit tests passing**.
- `voltra-client-rust/`: **builds clean**.
- All Wave 1 features: **COMPLETE** — bounded queue, queue metric, WAL crash test, SDK race fix, `voltra migrate`, benchmark fix, C# WASM reducers, Go WASM reducers.
- TODO-027 `voltra-macros`: **COMPLETE** — `#[reducer]`, `#[table]`, `ret!`, inventory auto-registration, `ctx.get/set/delete` shortcuts.

---

### Session 45 — TODO-028: `run_server` library API + `#[reducer]` scaffold templates

**What was built:**

**`src/server.rs`** — `pub async fn run_server(config: Config) -> Result<()>` (complete, zero-error):
- Full WAL + snapshot bootstrap (same crash-recovery path as main server):
  - `find_latest_snapshot(dir) -> Option<(PathBuf, u64)>` — no `?`, returns raw `Option`
  - `load_snapshot(&path, &tables) -> Result<SnapshotMeta>` — takes `&TableStore`, not `Arc`
  - `WalReader::open().read_all_entries()` — batch-reads all WAL entries; applies via `tables.apply_delta(delta)`
- `BatchedWalWriter::open(path, fsync_interval_ms, wal_batch_size, unsafe_no_fsync)` — 4-arg constructor (not `::new`)
- `IdentityIssuer`: load-or-generate pattern (no `load_or_generate` helper exists — manually checks path, calls `load_from_file` or `generate` + `save_to_file`)
- `AuthValidator::from_env()` — reads env vars; not `AuthValidator::new(api_key)`
- `PresenceManager::new(heartbeat_timeout_ms, offline_timeout_ms)` — 2 args, not 0
- `TtlManager::new()` — 0 args, not `TtlManager::new(tables.clone())`
- `RateLimiterConfig { capacity, refill_rate, enabled }` — constructed from `config.*` fields
- `ReducerContext::new(tables, ts).with_schema(schema).with_ttl(ttl)` — builder pattern
- `PendingCall` has no `timestamp` field — `ts = now_nanos()` computed per-call inline
- `ctx.commit()` returns `Result<Vec<RowDelta>>` directly — no `take_committed_deltas()`
- `call.response_tx.send(response)` — not Optional, no `reply_tx`
- `ReducerResponse::success(call_id, bytes)` / `ReducerResponse::error(call_id, msg)` constructors
- `wal_w.append(&entry, seq_num)` — sync, takes sequence number as second arg
- Multi-worker pool: `num_cpus::get().max(2)` workers, each running blocking Tokio task
- `kanal::bounded_async(queue_cap)` bounded queue with graceful shutdown via `watch::Receiver`

**`src/main.rs`** — scaffold updates (3 functions):
- Added 5 new `include_str!` constants: `EMBEDDED_CARGO_TOML`, `EMBEDDED_MAIN_RS`, `BASIC_REDUCERS_RS`, `GAME_REDUCERS_RS`, `CHAT_REDUCERS_RS`
- `scaffold_rust_basic()`: now also writes `embedded/Cargo.toml`, `embedded/src/main.rs`, `embedded/src/reducers.rs` (basic #[reducer] reducers)
- `scaffold_rust_game_ready()`: same, with game reducers
- `scaffold_rust_chat()`: same, with chat reducers
- Each `embedded/` directory is a self-contained Cargo project that compiles to a single Voltra + reducers binary

**WASM pooling engine** (`src/reducer/wasm.rs`) — completed in prior session:
- `PoolingAllocationConfig` pre-allocates memory slots at startup
- `build_pooling_engine()` → `build_standard_engine()` fallback chain
- Cuts WASM instantiation from ~1-5ms to ~10-50µs

**JS template fix** (52 files in `templates/`) — completed in prior session:
- All JS reducers now use `function reducer(args) { ... return ...; }` format matching v8.rs runtime

**New pitfalls:**
64. **`find_latest_snapshot` returns `Option<(PathBuf, u64)>`, not `Result<Option<...>>`** — no `?` operator, use `if let Some((path, seq)) = find_latest_snapshot(...)`.
65. **`load_snapshot` takes `&TableStore`, not `Arc<TableStore>`** — pass `&tables` where `tables: Arc<TableStore>` (Deref coercion handles it).
66. **`BatchedWalWriter::open` takes 4 args** — `(path, fsync_interval_ms, batch_size, unsafe_no_fsync: bool)`. The fourth arg is `false` for safe (fsync-on-flush) mode.
67. **`PendingCall` has no `timestamp` field** — compute it inline: `let ts = SystemTime::now().duration_since(UNIX_EPOCH)...as_nanos() as u64`.
68. **`AuthValidator::from_env()`** — reads auth config from env vars. `AuthValidator::new(mode)` is the manual constructor taking an `AuthMode` enum.
69. **`PresenceManager::new(heartbeat_ms, offline_ms)`** — 2 required args. No default constructor.
70. **`TtlManager::new()`** — 0 args. Does NOT take a `TableStore` reference.
71. **`ReducerContext` builder**: `.with_schema(Arc<SchemaRegistry>)` and `.with_ttl(Arc<TtlManager>)` return `Self` — chain after `ReducerContext::new(tables, ts)`.
72. **`ctx.schema` field, not `ctx.schema_registry`** — the field name is `schema: Option<Arc<SchemaRegistry>>`.

---

### Session 44 Wave 2 — TODO-027: `voltra-macros` proc macro crate

**What was built:**

New crate `voltra-macros/` (workspace member, `proc-macro = true`):

- **`voltra-macros/src/reducer.rs`** — `#[reducer]` proc macro:
  - Parses the input `fn`: first param is ctx (any type annotation, ignored), rest are args.
  - Generates:
    1. Inner `fn __voltra_reducer_<name>(ctx: &mut ::voltra::reducer::context::ReducerContext, args: &[u8]) -> Result<Vec<u8>>` — deserialises positional MessagePack args, runs user body.
    2. Struct `<PascalName>Reducer` implementing `::voltra::reducer::backend::ReducerBackend`.
    3. `::voltra::inventory::submit! { NativeReducerItem { name, make } }` — auto-registers the reducer at startup.
  - `#[allow(unreachable_code)]` suppresses warnings when user always calls `ret!(...)`.
  - No-arg reducers skip the `rmp_serde::from_slice` step entirely.

- **`voltra-macros/src/table.rs`** — `#[table(name = "players")]` proc macro:
  - Derives `Serialize + Deserialize` on the target struct.
  - Generates `table_name() -> &'static str`, `from_json(Value) -> Option<Self>`, `to_json(&self) -> Value`.
  - Falls back to snake_case of the struct name when no `name =` attribute is given.

- **`voltra-macros/src/lib.rs`** — exports `#[reducer]` and `#[table]` attribute macros.

**Changes to main `voltra` crate:**

- `Cargo.toml`:
  - Added `[workspace]` section with `members = ["voltra-macros"]`, `resolver = "2"`.
  - Added `inventory = "0.3"` and `voltra-macros = { path = "voltra-macros" }` deps.

- `src/lib.rs`:
  - `#[doc(hidden)] pub use inventory;` — makes `::voltra::inventory::submit!` available to generated code without requiring users to add `inventory` as a direct dep.
  - `pub use voltra_macros::{reducer, table};` — `#[voltra::reducer]` and `#[voltra::table]` work.
  - `ret!(...)` macro exported via `#[macro_export]` — expands to `return Ok(rmp_serde::to_vec(&json!(...))?)`.

- `src/reducer/registry.rs`:
  - `pub struct NativeReducerItem { name: &'static str, make: fn() -> Box<dyn ReducerBackend> }` — the item type submitted by `#[reducer]`.
  - `inventory::collect!(NativeReducerItem)` — registers the collection point.
  - `ReducerRegistry::new()` iterates `inventory::iter::<NativeReducerItem>()` and registers all submitted items before loading file-based modules.
  - 4 new unit tests: `test_native_reducer_item_can_be_registered_manually`, `test_inventory_collect_macro_present`, `test_ctx_set_get_delete_shortcuts`, `test_ctx_set_persists_after_commit`.

- `src/reducer/context.rs`:
  - `ctx.get(table, key) -> Result<Option<Value>>` — shorthand for `get_row`.
  - `ctx.set(table, key, value) -> Result<()>` — shorthand for `set_row` (accepts any `Into<String>` + `Into<Value>`).
  - `ctx.delete(table, key) -> Result<()>` — shorthand for `delete_row`.

**Usage (for external users of voltra):**

```rust
use voltra::{reducer, ret};

#[reducer]
fn heal(ctx: Ctx, target_id: String, amount: i32) {
    let row = ctx.get("players", &target_id)?
        .unwrap_or_else(|| serde_json::json!({ "hp": 0 }));
    let hp = row["hp"].as_i64().unwrap_or(0) as i32 + amount;
    ctx.set("players", &target_id, serde_json::json!({ "hp": hp }))?;
    ret!({ "ok": true, "new_hp": hp })
}

#[table(name = "players")]
pub struct Player {
    pub hp: i32,
    pub alive: bool,
    pub zone: String,
}
```

**Design decisions:**
- Generated code uses `::voltra::...` absolute paths — macros are for USERS of the crate, not for internal use within voltra itself.
- `inventory` re-exported as `::voltra::inventory` so users don't need `inventory` as a direct dep.
- `NativeReducerItem.make` is a plain `fn() -> Box<dyn ReducerBackend>` (not a closure) so it can live in a `static`.
- The `ctx` param type annotation is intentionally ignored by the macro — users write any placeholder type (e.g. `Ctx`, `_`) and the generated inner function always uses `&mut ReducerContext`.
- `ret!` uses `$crate::error::VoltraError::reducer_error` — resolves correctly from user crates that have voltra as a dep.

**New pitfalls:**
59. **`#[reducer]` macro generates `::voltra::...` paths** — cannot be used inside the `voltra` crate itself (circular path issue). Always use it from user/downstream crates.
60. **`inventory::collect!` must be called exactly once** — it's in `registry.rs`. Do not add another `collect!` for `NativeReducerItem` anywhere else.
61. **`inventory::submit!` must be at module scope, not inside a function** — the `#[reducer]` macro emits it at the module level of the expanded output. This is correct and required for the linker-section magic to work.
62. **`ret!` uses `return`** — it exits the generated inner `__voltra_reducer_*` function, not a closure. The `#[allow(unreachable_code)]` wrapper suppresses dead-code warnings when `ret!` is the last statement.
63. **`ctx.set()` accepts `impl Into<Value>`** — pass `serde_json::json!({...})` literals, `serde_json::to_value(&my_struct).unwrap()`, or anything else that converts to `serde_json::Value`. The underlying `set_row` handles schema validation.

---

## 🎯 SESSION 44 — DIRECTION SET BY PROJECT OWNER (read this)

**Goal:** Make Voltra the easiest, highest-performance self-hosted game backend to build on. Full detail in `TODO.md` → "🎯 THE GOAL".

Three pillars:
1. **Multi-language reducers** — add **C# (→ WASM via .NET 8 WASI)** and **Go (→ WASM via TinyGo)**
   running in the existing Wasmtime backend. Parallelism is already provided by Voltra's N-worker
   dispatch (`num_cpus`); the languages just need to compile to `.wasm`. (TODO-032, TODO-033.)
   **Do NOT embed the native Go runtime or .NET CLR** — Go's scheduler assumes process ownership and
   the CLR is a heavyweight GC'd dependency; both fight the DB for memory. WASM is the chosen path.
2. **Production hardening** — TODO-034…TODO-040 (bounded queue, queue metric, WAL crash test, SDK
   race fix, `voltra migrate`, benchmark fix).
3. **Ease of use** — TODO-027…TODO-031 (macros, codegen, engine templates).

**MAJOR DECISION — cluster + Raft REMOVED (deferred).** The owner is deferring all distribution.
TODO-034 removes `src/cluster/` and `src/raft/`, reverts the worker write path to single-node
`commit()` → `publish_deltas()` → WAL append (faster than per-write consensus). **Recovery:** a
`pre-cluster-removal` git tag preserves the Raft/cluster code (Sessions 36, 40–43) for later
resurrection. Do not try to keep Raft "dormant but compiled" — fully remove it from the build.

**Wave model (NOT hundreds of agents).** More agents on shared files = merge chaos (proven the hard
way in the Session 43 9-agent merge). Sequence: Wave 0 solo (TODO-034+035, foundation), then
parallel waves of ~5 agents on disjoint file sets. See `TODO.md` execution-order block.

### Session 44 — Wave 1 solo: all Wave 1 TODOs complete

**All Wave 1 work done solo (no agents) in this session.**

**Completed items:**

- **TODO-035 — Bounded reducer queue** (`src/config.rs`, `src/main.rs`, `src/network/websocket.rs`):
  - `kanal::bounded_async(config.reducer_queue_cap)` replaces unbounded channel.
  - Default cap: 16 384 (env: `VOLTRA_REDUCER_QUEUE_CAP`).
  - `try_send` in websocket.rs: fail-fast on full queue, returns `"server overloaded"` to client.

- **TODO-040 — Real queue-depth metric** (`src/main.rs`):
  - `queue_probe: kanal::AsyncSender<PendingCall>` clone passed to `start_metrics_server`.
  - `/healthz` endpoint now includes `"reducer_queue_depth": N`.

- **TODO-039 — `voltra migrate` CLI** (`src/cli.rs`, `src/main.rs`, `src/migrations.rs`):
  - `cmd_migrate(metrics_url, dir, dry_run)` reads sorted `*.toml` files from migrations dir.
  - `POST /migrate` endpoint in metrics server: calls `apply_migration_str()` per file.
  - `apply_migration_str(filename, content, tables)` added to `migrations.rs` — idempotent, TOML-string version of per-file apply logic.

- **TODO-031 — `GET /schema`** (`src/main.rs`):
  - Returns `{"tables": {...schema...}, "reducers": [...], "version": "..."}` from metrics server.

- **TODO-037 — WAL crash-recovery test** (`tests/crash_recovery_test.rs`):
  - 2 tests: basic counter survives crash+restart, paired writes don't tear.
  - Uses real server process, real WAL, kill + restart, HTTP verification.

- **TODO-038 — Benchmark scaling fix** (`benches/end_to_end.rs`):
  - `VOLTRA_METRICS_PORT = port + 1000` in `spawn_server`.
  - `BENCH_SCALE_MODE` accepts "1", "true", "yes".
  - Broadcast subscription changed to `"counters"` (actual write target of `increment` reducer).

- **TODO-036 — SDK optimistic-update race fix**:
  - **TypeScript** (`voltra-client-ts/src/client.ts`): stack-replay approach — `serverBaseCache` + `optimisticLayers: OptimisticLayer[]` + `recomputeRowCache()`. Rolling back any layer re-applies remaining layers correctly. Removed old `applyTargetedOptimistic` / `rollbackTouchedRows` / `TouchedRollback` / `rowsEqual` / `stableStringify`.
  - **Rust** (`voltra-client-rust/src/client.rs`): same approach — `server_base_cache: CacheSnapshot` + `optimistic_layers: Vec<(u64, OptimisticMutation)>` + `recompute_and_apply()`. `call_optimistic` changed from `FnOnce` to `Fn` bound. `Command::ApplyOptimistic` → `Command::RegisterLayer`. `apply_to_cache` → `apply_to_base` (writes to HashMap, not DashMap).

- **TODO-032 — C# reducer path** (`src/main.rs`, `src/reducer/wasm.rs`, `docs/reducers-csharp.md`):
  - `build_multi_lang_reducers()` detects `reducers/*.csproj` → `dotnet publish -r wasi-wasm`.
  - `scaffold_csharp_reducers()` generates `.csproj`, `Voltra.cs` (host bindings), `Combat.cs`.
  - `CSHARP_CSPROJ`, `CSHARP_VOLTRA_BINDINGS`, `CSHARP_COMBAT_CS` template strings.
  - `call_reducer_typed` extended with i64 fat-pointer ABI (C# `UnmanagedCallersOnly` can't multi-value return): `(i32,i32) → i64` where high 32 = ptr, low 32 = len.
  - Template added to `TEMPLATES` as `"csharp-reducers"`.

- **TODO-033 — Go reducer path** (`src/main.rs`, `docs/reducers-go.md`):
  - `build_multi_lang_reducers()` detects `reducers/go.mod` + `*.go` → `tinygo build -target wasi`.
  - `scaffold_go_reducers()` generates `go.mod`, `voltra/voltra.go` (host bindings via `//go:wasmimport`), `combat.go`.
  - `GO_VOLTRA_BINDINGS`, `GO_COMBAT_GO` template strings.
  - TinyGo's multi-value WASM returns work natively with the existing `call_reducer_typed`.
  - Template added to `TEMPLATES` as `"go-reducers"`.

**New pitfalls:**
59. **C# `[UnmanagedCallersOnly]` cannot return WASM multi-value** — use i64 fat-pointer: `high 32 = ptr, low 32 = len`. Voltra's `call_reducer_typed` in `wasm.rs` now tries this signature as a third fallback.
60. **TinyGo multi-value returns work natively** — no special encoding needed. `//export funcname` + `func f(a, b int32) (int32, int32)` compiles to the multi-value WASM ABI that Wasmtime's `get_typed_func::<(i32,i32),(i32,i32)>` picks up.
61. **`voltra build` detects C# before Go** — if both `reducers/*.csproj` AND `reducers/go.mod` exist, C# takes priority. This is a known limitation; future work could support mixed language projects.
62. **`call_optimistic` in Rust SDK changed to `Fn`** — previously `FnOnce`, now requires `Fn + Send + Sync + 'static`. This allows the background task to replay the mutation when a sibling layer is rolled back (TODO-036 fix). Update any callers that used move-only closures.
63. **`Command::ApplyOptimistic` is gone from Rust SDK** — replaced with `Command::RegisterLayer`. Any code that sent `ApplyOptimistic` manually must be updated to `RegisterLayer`.

### Session 43 — 9-feature production wave: all branches merged to master

**9 parallel agents worked in separate worktrees; all merged to master in this session.**

**Merged features:**

1. **Docker/CI** (`worktree-agent-a3e166553d5a72911` → `8ef3c55`):
   - `Dockerfile`, `docker-compose.yml` (3-node Raft), `docker-compose.single.yml`, `.dockerignore`
   - `.github/workflows/ci.yml`, `.github/workflows/release.yml`
   - `deploy/voltra.service`, `deploy/install.sh`, `deploy/README.md`

2. **Documentation** (`worktree-agent-a5ea819ef7d1a9efd` → `361fd16`):
   - `docs/` directory: getting-started, architecture, protocol, reducers, SDK-ts, SDK-rust, deployment, cluster, CLI reference, FAQ
   - `README.md` full rewrite (108 lines)

3. **SDK auto-reconnect** (`worktree-agent-a2ae98adfdfc5c4ae` → `93ced02`):
   - TS: `scheduleReconnect()`, `ReconnectOptions`, `pendingCalls`, `activeSubscriptions` re-issue on reconnect, `disconnect()`
   - Rust: `ReconnectConfig`, `ClientEvent`, `events()`, `disconnect()`

4. **Row-level security** (`worktree-agent-a800c2103673ccb8d` → `aa4f10a`):
   - `src/schema.rs`: `RlsPolicy { Public, OwnerField, RoleGated, OwnerFieldWithAdmin }`, `rls_check()`, `rls` field on `TableSchema`
   - `src/error.rs`: `VoltraError::PermissionDenied(String)`
   - `src/table/mod.rs`: `get_row_rls()` silently filters denied rows
   - `src/reducer/context.rs`: RLS enforced in `get_row()` and `commit()`, bypassed for scheduler/system

5. **LRU eviction** (`worktree-agent-a9513896b1b9dfc8a` → `91060fd`):
   - `src/table/eviction.rs`: `EvictionPolicy { None, LruRowCap, LruByteCap }`, `LruTracker`
   - `src/table/mod.rs`: `with_eviction()` constructor, eviction in `apply_delta_batch`
   - `src/config.rs`: `[eviction]` TOML section + `VOLTRA_EVICTION_POLICY` env

6. **Graceful shutdown** (`worktree-agent-a4efb4affb025bbea` → `6eef8bd`):
   - `tokio-util` CancellationToken / `watch::Receiver<()>` shutdown signal
   - Worker loop uses `select!` to drain reducer queue then exit
   - 30s drain timeout; `eprintln!("[voltra] Shutdown complete.")`
   - `handle_client` sends WebSocket Close frame on shutdown

7. **Prometheus metrics** (`worktree-agent-acd03a0abf18790a3` → `19f7ad0`):
   - `src/metrics.rs`: `Metrics` struct, 11 counters/gauges/histograms, `render()`
   - Hooked into worker loop (reducer latency histogram), connection lifecycle (connect/disconnect gauges), gauge refresh task
   - `GET /metrics` endpoint in metrics server returns Prometheus exposition format

8. **TLS (WSS)** (`worktree-agent-a2fccae24b95cf6e4` → `9da491a`):
   - `src/network/tls.rs`: `load_tls_config()`, `generate_self_signed()`
   - `handle_client<S>` generic over `AsyncRead + AsyncWrite` — same code path for plain + TLS
   - `[tls]` TOML config section; auto-generates self-signed cert if paths not configured
   - Dependencies: `tokio-rustls 0.25`, `rustls 0.22`, `rustls-pemfile 2.0`, `rcgen 0.12`

9. **JWT + Ed25519 identity** (`worktree-agent-aff5cdd7b954d973d` → `297e1b4`):
   - `src/auth.rs`: `NeonClaims`, `IdentityIssuer` (generate/issue/verify/save/load key pair)
   - `POST /auth/token` issues JWT; `GET /auth/public-key` returns PEM public key
   - WebSocket Bearer token branch: detects `eyJ` prefix → JWT verify path; else API key path
   - Ed25519 key pair persisted to `<wal_dir>/identity_key.pem` across restarts
   - Dependencies: `jsonwebtoken 9`, `ed25519-dalek 2` (pkcs8+pem features), `pkcs8 0.10`, `rand 0.8`

**Post-merge compile fix:**
- `crate::table::EvictionPolicy` in `main.rs` binary → `voltra::table::EvictionPolicy` (binary crate's `crate` ≠ lib crate root)

**Final start_listener signature (websocket.rs):**
```rust
pub async fn start_listener(
    host: String, port: u16,
    reducer_tx: kanal::AsyncSender<PendingCall>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    max_connections: usize,
    api_key: Option<String>,
    active_connections: Arc<AtomicUsize>,
    permissions: Arc<PermissionsConfig>,
    sql_timeout_ms: u64,
    auth_validator: Arc<AuthValidator>,
    rate_limiter: Arc<RateLimiterRegistry>,
    presence: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    identity_issuer: Arc<IdentityIssuer>,   // JWT — appended last among older params
    mut shutdown: watch::Receiver<()>,       // graceful shutdown
    metrics: Arc<Metrics>,                   // Prometheus
    tls: Option<Arc<rustls::ServerConfig>>,  // TLS/WSS
) -> Result<()>
```

**New pitfalls:**
54. **`crate::` in `main.rs` binary refers to the binary crate root, not `lib.rs`** — use `voltra::` (the lib crate name) to reference types defined in `src/lib.rs` from within `src/main.rs`. E.g. `voltra::table::EvictionPolicy`, not `crate::table::EvictionPolicy`.
55. **`start_metrics_server` takes both `prom: Arc<Metrics>` and `identity_issuer: Arc<IdentityIssuer>`** — they come before `mut shutdown: watch::Receiver<()>`. Never drop either param when modifying the signature.
56. **JWT `is_jwt()` check uses `eyJ` prefix** — JWTs are base64url-encoded JSON starting with `{"`. The first two bytes are always `ey`. Clients send `Authorization: Bearer eyJ...` for JWT auth; the server detects this and verifies via `IdentityIssuer::verify()` rather than raw API key comparison.
57. **`EvictionPolicy::None` is the default (no eviction)** — do not configure eviction unless explicitly requested. Setting `LruRowCap { max_rows_per_table: 0 }` would evict every row; always `.max(1)` the cap.
58. **RLS `OwnerField` reads `caller_id` from `ReducerContext`** — the field named by `owner_field` in the stored row must match `ctx.caller_id`. Scheduler-issued writes have `caller_id = "scheduler"` and bypass RLS. Ensure scheduler reducers don't write to RLS-protected tables unless the bypass is intentional.

### Session 41 — Wave 3 completion: Raft write path + consensus integration tests

**What was completed in this session:**

- **Raft write path wired into worker loop** (`src/main.rs`):
  - Replaced `ctx.commit()` + direct `subs.publish_deltas()` + `wal_w.append()` with `ctx.drain_pending_deltas()` + `raft_w.client_write(RaftRequest { ... })`.
  - `drain_pending_deltas()` extracts staged deltas without applying them locally — the Raft state machine's `apply()` becomes the sole apply path on every node, including the leader.
  - WAL append is still written after `client_write` succeeds (for fast crash-recovery; Raft log is the authoritative distributed log, WAL is the local fast-recovery path).
  - Legacy cluster `bus_w.fanout_deltas()` is still called for non-Raft peers.
  - `subs_w` renamed to `_subs_w` (subscription fan-out now done inside the state machine, not the worker).

- **Zero-warning build achieved**: removed the unused `NeonRaft` import from `main.rs` and the unused `EntryPayload` import from `storage.rs` test module.

- **6 Raft consensus integration tests** (`tests/raft_consensus_test.rs` — NEW):
  - In-process network: `InProcFactory + InProcNetwork` implementing `RaftNetworkFactory + RaftNetwork` using a `DashMap<NodeId, Arc<NeonRaft>>`. No HTTP, no subprocesses, no ports.
  - `test_single_node_leader_election_and_write` — bootstrap, elect leader, write, verify state machine applied.
  - `test_single_node_multiple_writes_ordered` — 5 sequential writes all land correctly.
  - `test_three_node_membership_change` — `add_learner` × 2 then `change_membership` to `{1,2,3}` voter set.
  - `test_three_node_log_replication` — write on leader, verify all 3 state machines contain the row within 2 s.
  - `test_failover_new_leader_elected_after_leader_dies` — shut down node 1, verify nodes 2+3 elect a new leader and accept post-failover writes.
  - `test_minority_cannot_commit_without_quorum` — partition 2 of 3 nodes away from leader, verify `client_write` does not commit within 1.5 s (timeout) and the row is absent from the state machine.

**P0 Raft consensus is now COMPLETE.**

---

### Session 40 — Wave 3: Raft Consensus P0 Implementation

**What was built:**

New module: `src/raft/` with four files:

- **`src/raft/mod.rs`** — Type config, re-exports, configuration helper:
  - `RaftRequest` — the payload flowing through the Raft log (reducer_name, args, deltas, timestamp_ms). `Serialize + Deserialize` so openraft can replicate it.
  - `RaftResponse` — returned after `apply()` (applied delta count).
  - `openraft::declare_raft_types!(TypeConfig)` — ties Voltra types to openraft: NodeId=u64, Node=BasicNode, Entry=openraft::Entry<TypeConfig>, SnapshotData=Cursor<Vec<u8>>, AsyncRuntime=TokioRuntime.
  - `NeonRaft = openraft::Raft<TypeConfig>` convenience alias.
  - `build_raft_config()` — heartbeat 250ms, election 750-1500ms, max 300 entries/RPC, snapshot every 10k entries.
  - 5 unit tests.

- **`src/raft/storage.rs`** — `MemLogStore` implementing `RaftLogStorage + RaftLogReader`:
  - In-memory `BTreeMap<u64, Entry>` is the authoritative log store (O(log n) by index, ordered ranges).
  - Vote persisted to JSON file at `<wal_dir>/raft_vote.json` (survive crashes for correctness).
  - `append()` inserts entries and calls `callback.log_io_completed(Ok(()))` immediately (all-memory, no async I/O needed).
  - `truncate(log_id)` removes entries from `log_id.index..` (conflict resolution on follower).
  - `purge(log_id)` removes entries `..=log_id.index` (after snapshot install).
  - `get_log_reader()` returns a clone (shared `Arc<RwLock<>>>`).
  - 7 unit tests (using direct inner-map insertion since `LogFlushed::new` is `pub(crate)` in openraft).

- **`src/raft/state_machine.rs`** — `NeonStateMachine` implementing `RaftStateMachine`:
  - `apply()`: for `EntryPayload::Normal(req)` → `TableStore::apply_delta_batch(&req.deltas)` → `SubscriptionManager::publish_deltas()` fan-out. Membership entries stored in `last_membership`. Blank entries are no-ops.
  - `build_snapshot()`: serializes all TableStore rows + counters to `SerializedState` JSON, returns as `Cursor<Vec<u8>>`.
  - `install_snapshot()`: calls `TableStore::clear_all()` then replays all rows and counters from the snapshot.
  - `NeonSnapshotBuilder` is a separate type holding `Arc<RwLock<StateMachineInner>>`.
  - 4 unit tests including full snapshot roundtrip.

- **`src/raft/http.rs`** — HTTP handlers for incoming Raft RPCs:
  - `handle_raft_append` → `POST /raft/append` (AppendEntries — heartbeat + log replication).
  - `handle_raft_vote` → `POST /raft/vote` (RequestVote — leader election).
  - `handle_raft_snapshot` → `POST /raft/snapshot` (InstallSnapshot — catch-up for stale followers).
  - `handle_raft_metrics` → `GET /raft/metrics` (current leader, term, commit index, membership).
  - `handle_raft_add_learner` → `POST /raft/add-learner` (add a new node as learner).
  - `handle_raft_change_membership` → `POST /raft/change-membership` (promote to voter / quorum changes).
  - `handle_raft_init` → `POST /raft/init` (bootstrap single-node cluster).
  - 3 unit tests.

- **`src/raft/network.rs`** — `NeonNetworkFactory + NeonNetwork` implementing Raft HTTP transport:
  - `append_entries` → `POST <peer>/raft/append`
  - `install_snapshot` → `POST <peer>/raft/snapshot`
  - `vote` → `POST <peer>/raft/vote`
  - `full_snapshot` → uses openraft's default `Chunked::send_snapshot` (chunk-based fragmentation).
  - Cluster secret injected as `x-voltra-cluster-secret` header.
  - 4 unit tests.

**Files modified:**

- `Cargo.toml` — added `openraft = { version = "0.9", features = ["serde", "storage-v2"] }` and `anyerror = "0.1"`.
- `src/lib.rs` — added `pub mod raft;`.
- `src/table/mod.rs` — added 3 methods: `get_all_rows(table_name) → HashMap`, `get_all_counters_map() → HashMap`, `clear_all()` (for snapshot install).
- `src/main.rs` — Raft node initialised at server startup:
  - `MemLogStore + NeonStateMachine + NeonNetworkFactory` constructed.
  - `openraft::Raft::new()` creates the Raft node.
  - Single-node mode auto-bootstraps; multi-node mode waits for `/raft/change-membership`.
  - `start_metrics_server` and `handle_metrics_request` now accept `Arc<NeonRaft>`.
  - 7 new Raft routes registered in `handle_metrics_request`.

**Correctness guarantees provided by openraft (not custom code):**
- Leader election via randomized election timeouts (750–1500ms).
- Quorum write: entries only committed when replicated to `⌊N/2⌋ + 1` nodes.
- Term-based split-brain prevention: stale-term AppendEntries rejected automatically.
- Log conflict resolution: `truncate()` called when follower log diverges from leader.
- Snapshot transfer: `install_full_snapshot()` for nodes too far behind.
- Membership changes: joint-consensus safe membership transitions via `change_membership()`.

**P0 status: COMPLETE (consensus layer scaffolded + wired + tested)**

**Remaining Raft work (P0 tail):**
1. **Vote persistence to disk** — currently `MemLogStore::new(None)` is passed; should be `MemLogStore::new(Some(wal_dir.join("raft_vote.json")))` so vote survives server restarts.
2. **Reducer write path through Raft** — currently reducer workers `apply_delta_batch` directly. For multi-node consistency, the leader should call `raft.client_write(RaftRequest { deltas, ... })` instead; followers should detect non-leader status and proxy to leader via `/cluster/call`.
3. **Leader forwarding in WebSocket handler** — followers that receive a reducer call should check `raft.current_leader()` and forward if not leader.
4. **Multi-node integration test** — spin up 3 Voltra nodes, call `change_membership`, write via node 1, verify node 2 and 3 see the data.

**New pitfalls (add to Common Pitfalls):**
37. **`#[openraft::add_async_trait]` is for trait definitions only** — impl blocks must use plain `async fn`. Rust 1.75+ handles `async fn` in impl blocks natively via RPITIT.
38. **`LogFlushed::new` is `pub(crate)` in openraft 0.9** — cannot construct in external crates. Tests that need to call `append()` must insert directly into `inner.entries` via `Arc<RwLock<>>`.
39. **`Entry.log_id` and `LogId.index` are public fields, not methods** — use `entry.log_id.index` not `entry.log_id().index()`.
40. **`StorageError::write_snapshot` is on `StorageIOError`, not `StorageError`** — use `StorageError::IO { source: StorageIOError::write_state_machine(anyerror::AnyError::new(&e)) }`.
41. **Raft `initialize()` must be called exactly once** — on a fresh node it bootstraps the cluster; on an already-initialised node it returns `Err(AlreadyInitialized)`, which should be logged as `warn` and ignored.
42. **`storage-v2` feature must be enabled** — `openraft = { features = ["serde", "storage-v2"] }`. Without it, `RaftLogStorage` and `RaftStateMachine` traits are not implementable by external crates (the `Sealed` impl is gated on `#[cfg(feature = "storage-v2")]`).
43. **`anyerror` must be a direct dependency** — openraft's `StorageIOError` constructors take `impl Into<AnyError>`. The `anyerror` crate is a transitive dep of openraft but not available without adding it to Cargo.toml.
44. **`Wait::leader()` does not exist in openraft 0.9** — use `wait.current_leader(expected_id, msg)` to block until the given node is leader.
45. **`change_membership` takes `BTreeSet<NodeId>`, not `BTreeMap<NodeId, Node>`** — `ChangeMembers` implements `From<BTreeSet<NID>>` but NOT `From<BTreeMap<NID, N>>`. Pass `BTreeSet::<u64>::from_iter([1,2,3])`. Node addresses were registered via `add_learner` earlier; they don't need to be re-supplied.
46. **In-process Raft network: `Raft::append_entries/vote/install_snapshot` are public** — these methods accept the raw RPC structs and return the response structs, allowing a test-only `RaftNetworkFactory` implementation that routes RPCs directly to in-memory `Arc<NeonRaft>` instances. No HTTP needed for unit/integration testing.
47. **Raft write path is two-phase: drain then `client_write`** — after the reducer executes, call `ctx.drain_pending_deltas()` (NOT `ctx.commit()`). The deltas flow into `RaftRequest`, are replicated by Raft, and applied via the state machine's `apply()`. Double-applying (commit + raft) will cause every row to be written twice on the leader.

After Session 39 (5-agent production-readiness wave):
- `cargo build --lib` → **zero errors, zero warnings**.
- `cargo test --lib` → **264 lib unit tests passing** (was 232 before the wave; +32 from the wave, of which Agent 5 contributed 7 new schema tests).
- `cargo check --test schema_validation_test` and `cargo check --test wal_recovery_test` → both pass, confirming the new integration-style test files type-check against the public API.
- `cargo build` (full bin) → ❗ STILL BROKEN: pre-existing argument-count mismatch at `src/main.rs:783` calling `start_listener` (10 args supplied, signature now requires 11 — likely a peer agent's incomplete `start_listener` signature change). This blocks `cargo test` (full), `cargo test --tests`, and any `cargo test --test <name>` for tests/ files because they need the `bin "voltra"` to compile. The lib alone is healthy.
- `voltra-client-rust/`: untouched in this session.
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
23. **`__voltra_set` accepts full JSON objects** — v8.rs rewritten in Session 32. For `counters` table with a plain number it calls `set_counter()`; for everything else `set_row()`. Never revert to `.as_number()` only.
24. **Scheduler empty args** — schedulers with no `args_json` send empty bytes. `execute()` in v8.rs defaults to `Value::Array(vec![])`. Never call `rmp_serde::from_slice` on potentially empty bytes without this guard.
25. **`__voltra_get` reads any table** — uses `ctx.get_row()` with read-your-writes support. Do not revert to counter-only pre-fetch.
26. **Scheduler reducer names must match registered names exactly** — use `refresh` not `refresh_matchmaking`, `cleanup_sessions` not `cleanup_expired_sessions`.
27. **`edit_file` for modifications, full write only for new files** — never rewrite a large file to change two lines.
28. **`POST /seed` bypasses WAL and reducers** — rows written by `/seed` are not journaled and do not fan-out to live subscribers. This is intentional for dev/test. Never use seed for production data ingestion. If you need WAL-backed writes, call a reducer instead.
29. **`voltra seed` uses HTTP, not WebSocket** — it talks to the metrics port (default 3001), not the WebSocket port (3000). Ensure `voltra start` is running before seeding.
30. **Array-format seed rows must have a `"key"` string field** — it is extracted as the row key and stripped from the stored data. Object-format seed tables use map keys as row keys directly.
31. **`shard_for_key(key, shard_count)` is the canonical shard assignment** — uses FNV-1a 64-bit hash. Every node must call the same function with the same `shard_count` to agree on ownership. Never use a different hash function.
32. **`ClusterConfig::parse_peers` is `pub(crate)`** — needed for unit tests. Do not make it `pub`; peer list is an internal detail.
33. **`VOLTRA_BLOB_PATH` env var controls the blob store directory** — `TableStore::new()` reads this; falls back to `$TEMP/voltra_blobs`. Integration tests must set a unique path per server port (e.g. `voltra_blobs_18080`) to prevent parallel servers from colliding on the same `blobs.bin` file.
34. **Integration tests MUST set `VOLTRA_METRICS_PORT` uniquely** — `Config::from_env()` loads `voltra.toml` from the project root (via `find_config_in_cwd()`), giving every child server `metrics_port = 3001`. All parallel servers race to bind that port; losers exit silently before the WebSocket listener starts, causing the "Server did not become ready within 5s" panic. The fix is `VOLTRA_METRICS_PORT = ws_port + 1000` in `spawn_server_with_env`. Already applied in Session 37.
35. **`ensure_server_built()` must NOT call `cargo build`** — `cargo test` holds the build-directory lock the entire time it runs. Any nested `cargo build` call from within an integration test will try to acquire the same lock and **deadlock** (or fail with "could not acquire lock"), causing all 9 server processes to never start and every test to time out with "Server did not become ready within 5s". The correct implementation simply asserts the binary exists — `cargo test` already compiled it. Applied in Session 38 (revised).
36. **Required columns must check both missing AND null** — explicit JSON null was previously accepted for required fields. The schema validator now rejects both cases (`obj.contains_key(name) == false` OR `obj.get(name).map(|v| v.is_null()).unwrap_or(true)`), returning `"Required column '<name>' must not be null"` for the explicit-null case. Optional columns with explicit null are still accepted; required columns with a default fall back to the default even when the row supplied an explicit null. Applied in Session 39.
