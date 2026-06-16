# NeonDB – Phase 0: Initiative & Planning

**Document Version**: 1.0  
**Date**: 2026-06-05  
**Status**: ✅ APPROVED  

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [High-Level System Architecture](#high-level-system-architecture)
3. [Technology Stack & Rationale](#technology-stack--rationale)
4. [Repository Structure & File Layout](#repository-structure--file-layout)
5. [Risk Assessment & Mitigation](#risk-assessment--mitigation)
6. [Development Timeline Estimate](#development-timeline-estimate)
7. [Success Metrics & Acceptance Criteria](#success-metrics--acceptance-criteria)
8. [Open Questions for Approval](#open-questions-for-approval)

---

## Executive Summary

**NeonDB** is a unified in-memory database + application server designed for maximum throughput and minimum latency, 100% self-hostable on consumer hardware via Dokploy with zero cloud fees.

**Key Design Principles:**
- Single-threaded execution model (maximize CPU cache locality, eliminate lock contention)
- Append-only Write-Ahead Log (WAL) for ACID durability
- User-defined "reducers" (deterministic functions) process all writes
- Real-time subscriptions via WebSocket with incremental updates
- Multi-language support (Rust native/WASM, TypeScript via embedded V8)
- Single Docker image deployable to any Docker host

**Core Vision**: Make real-time multiplayer backends as simple and performant as possible for indie developers and small teams.

---

## High-Level System Architecture

### System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         NeonDB Server                           │
│                      (Single Rust Process)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │          WebSocket Listener Thread (tokio)               │   │
│  │  • Accepts & maintains 10k+ concurrent connections       │   │
│  │  • Parses binary protocol (MessagePack)                  │   │
│  │  • Queues reducer calls to main thread                   │   │
│  │  • Sends subscription updates from broadcast queue       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            ↓ (FIFO Queue)                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │        Single-Threaded Reducer Engine                    │   │
│  │  • Execute ONE reducer at a time (determinism)           │   │
│  │  • Access in-memory tables & compute deltas              │   │
│  │  • Log transaction to WAL (O_DIRECT + fsync)             │   │
│  │  • Compute affected subscriptions & queue updates        │   │
│  └──────────────────────────────────────────────────────────┘   │
│           ↓ (Committed Rows)     ↓ (Updates Queue)               │
│  ┌─────────────────────┐    ┌────────────────────────────────┐   │
│  │   In-Memory Tables  │    │ Broadcast Queue (mpsc channel) │   │
│  │  • Row-oriented or  │    │  • Serialized deltas for each  │   │
│  │    column-oriented  │    │    subscription               │   │
│  │  • Optimized for    │    │  • Sent back to listener       │   │
│  │    CPU cache        │    │    thread for WebSocket push   │   │
│  │  • No GC in hot     │    │                                │   │
│  │    path             │    │                                │   │
│  └─────────────────────┘    └────────────────────────────────┘   │
│           ↑ (on restart)                                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │    Append-Only Write-Ahead Log (WAL)                     │   │
│  │  • Binary format (timestamp, reducer_id, args, delta)    │   │
│  │  • O_DIRECT for direct I/O, fsync @ configurable ints   │   │
│  │  • Persisted to disk (volume mount in Docker)            │   │
│  │  • Replay on crash to rebuild in-memory state            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

                    ↕ (WebSocket over TCP/IP)

┌─────────────────────────────────────────────────────────────────┐
│                       Client SDK                                 │
│              (TypeScript/Rust, any platform)                     │
├─────────────────────────────────────────────────────────────────┤
│  • Connect via WebSocket (binary protocol)                       │
│  • Maintain local reactive cache (replica of server state)       │
│  • Send reducer calls (reducer_id, args)                         │
│  • Subscribe to queries (parameterized SQL-like)                 │
│  • Receive incremental updates & auto-update local cache         │
│  • React hooks (TS/JS) or reactive signals (Rust)                │
│  • Support optimistic updates with server reconciliation         │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Write Path (Reducer Execution)**:
   - Client sends `{ reducer_id, args }` via WebSocket to listener thread
   - Listener queues to reducer engine (FIFO)
   - Single thread dequeues, runs reducer (reads tables, writes to context)
   - Reducer computes row deltas
   - Deltas logged to WAL (fsync)
   - In-memory tables updated
   - Subscription diff algorithm computes affected subscriptions
   - Deltas pushed back to subscribed clients
   - Response sent to calling client (with new row IDs if needed)

2. **Read Path (Subscriptions)**:
   - Client sends subscription query (e.g., `SELECT * FROM users WHERE score > 100 LIMIT 50`)
   - Server parses & stores parameterized subscription
   - Server snapshot-queries current matching rows, sends to client
   - On future reducers, server computes which subscriptions are affected
   - Incremental updates pushed (RowInsert, RowUpdate, RowDelete)
   - Client SDK applies deltas to local cache
   - UI framework (React, Svelte, etc.) reactively re-renders

3. **Crash Recovery**:
   - On restart, server reads WAL from disk sequentially
   - Re-executes each reducer call in order (deterministic)
   - Rebuilds in-memory tables to exact state before crash
   - Resumes listening for new connections

---

## Technology Stack & Rationale

### Core Server (NeonDB Kernel)

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Language** | **Rust** | Type safety, no GC, near-C performance, excellent concurrency libraries (tokio, crossbeam). Compiled binary is deployment-friendly. |
| **Async Runtime** | **tokio** | Industry standard for Rust network services. Non-blocking I/O for 10k+ connections. |
| **WebSocket Library** | **tungstenite** (or **tokio-tungstenite**) | Lightweight, easy to integrate with tokio. Handles binary frames natively. |
| **Serialization (Protocol)** | **MessagePack** (or **bincode** for max speed) | Compact binary format. Fast encode/decode. Good for real-time updates. |
| **Reducer Sandboxing (Rust)** | **Wasmtime** (WASM) or **libloading** (native .so) | WASM: high isolation, portable, slower (2-5x overhead). Native: 10-50% faster, requires user to compile trustworthy code. Provide both options; user chooses at deploy time. |
| **Reducer Sandboxing (TS)** | **Deno Core** or **rusty_v8** + custom isolate | Deno Core: full V8 with TypeScript support, but heavyweight (~50MB). rusty_v8: lighter, can create isolated contexts per call or reuse. Choose rusty_v8 for simplicity & control. |
| **WAL Format** | **Custom binary (compact + version-aware)** | Full control over encoding, efficient delta representation, easy versioning. |
| **Concurrency (Multi-threaded I/O)** | **tokio spawn + crossbeam mpsc** | Listener thread spawned in tokio, reducer engine on main thread. Channel for queueing and broadcast for updates. |
| **Subscription Diff Algorithm** | **Bitmask or Bloom filter for affected subscriptions** | After a reducer commits, iterate subscriptions and test if row changes match their WHERE clause. For 10k subscriptions and 1000 writes/sec, this is feasible with careful indexing. |

### Client SDKs

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **TypeScript/JS SDK** | **Native TypeScript**, compiled to JS + `.d.ts` | Use `tsc` or `swc` for compilation. Ship as npm package. Include React hooks (`useNeonDBQuery`, `useNeonDBReducer`) for easy UI integration. |
| **Rust SDK** | **Native Rust crate** | Ship as `neondb-client` on crates.io. Include reactive signal support (e.g., `tokio::sync::watch` or a simple `Signal<T>` struct). |
| **Local Cache** | **In-memory HashMap<TableName, Vec<Row>>** (TS) or **IndexMap<String, Vec<Row>>** (Rust) | Each SDK maintains a replica of subscribed tables. Apply deltas incrementally. Support `get()`, `list()`, `subscribe()` APIs. |
| **Optimistic Updates** | **Immediate client-side cache update, reconcile on server response** | User calls `reducer()`, SDK optimistically updates local cache, sends to server, waits for confirmation. If mismatch, re-sync. |

### Deployment & DevOps

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Container** | **Docker** (single stage build) | Multi-stage: build server in Rust, copy binary to slim base (scratch or Alpine). Expose WebSocket port (default 8000), volume for `/data/wal`. |
| **Orchestration** | **Dokploy** (Docker Compose under the hood) | User deploys via Dokploy dashboard, sets env vars, done in <5 min. Auto TLS via Traefik. |
| **Database Schema** | **Schema-as-code** (TOML or JSON) + migrations via WAL replay | No SQL DDL. Instead, define tables in a `schema.neondb` file. On server startup, if tables don't exist, create them. Migrations = replay WAL with new reducer logic. |
| **CLI Tool** | **Rust binary** (`neondb-cli`) | `neondb init`, `neondb build`, `neondb run`, `neondb migrate`. Published on crates.io and as GitHub releases. |

### Optional / Future Versions

- **QuickJS** as alternative to V8 (smaller, lighter, slower)
- **S3 snapshots** for faster recovery than WAL replay
- **Distributed mode** with leader-follower replication

---

## Repository Structure & File Layout

```
NeonDB/
├── Cargo.workspace.toml                    # Monorepo root
│
├── neondb-server/                          # Core server (Rust)
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs                         # Entry point, arg parsing, server init
│   │   ├── lib.rs                          # Main crate exports
│   │   ├── config.rs                       # Config loading (env vars, TOML)
│   │   ├── wal/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs                   # O_DIRECT WAL append, fsync
│   │   │   ├── reader.rs                   # Replay WAL on startup
│   │   │   └── entry.rs                    # Binary format (timestamp, reducer_id, args, delta)
│   │   ├── table/
│   │   │   ├── mod.rs
│   │   │   ├── in_memory.rs                # In-memory storage (HashMap<K, Row> or columnar)
│   │   │   ├── row.rs                      # Row struct, serialization
│   │   │   └── schema.rs                   # Table schema, column types
│   │   ├── reducer/
│   │   │   ├── mod.rs
│   │   │   ├── context.rs                  # ReducerContext (read/write API for reducers)
│   │   │   ├── wasm.rs                     # Wasmtime integration for Rust/WASM reducers
│   │   │   ├── typescript.rs               # V8 isolate for TypeScript reducers
│   │   │   ├── registry.rs                 # Reducer lookup by name/id
│   │   │   └── engine.rs                   # Single-threaded FIFO queue executor
│   │   ├── subscription/
│   │   │   ├── mod.rs
│   │   │   ├── parser.rs                   # Parse simple SQL-like query: SELECT * FROM t WHERE x=y
│   │   │   ├── matcher.rs                  # Check if a row matches a subscription query
│   │   │   ├── store.rs                    # Maintain active subscriptions per client
│   │   │   └── diff.rs                     # Compute affected subscriptions after reducer
│   │   ├── network/
│   │   │   ├── mod.rs
│   │   │   ├── websocket.rs                # tokio + tungstenite listener
│   │   │   ├── protocol.rs                 # Binary message format (MessagePack)
│   │   │   ├── client.rs                   # Per-client connection state (subscriptions, client_id)
│   │   │   └── broadcast.rs                # mpsc broadcast channel for updates
│   │   ├── error.rs                        # Custom error types
│   │   └── lib.rs                          # Main library interface
│   ├── benches/
│   │   ├── throughput.rs                   # Measure TPS for different reducer types
│   │   └── latency.rs                      # P50, P99 latency
│   ├── tests/
│   │   ├── crash_recovery.rs
│   │   ├── subscription_correctness.rs
│   │   └── integration.rs
│   ├── Dockerfile
│   └── README.md
│
├── neondb-client-ts/                       # TypeScript/JavaScript SDK
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── index.ts                        # Main export
│   │   ├── client.ts                       # NeonDBClient class
│   │   ├── websocket.ts                    # WebSocket connection management
│   │   ├── cache.ts                        # Local in-memory cache
│   │   ├── protocol.ts                     # Binary message encoding/decoding
│   │   ├── hooks.ts                        # React hooks (useNeonDBQuery, useNeonDBReducer)
│   │   └── types.ts                        # TypeScript interfaces
│   ├── tests/
│   │   └── client.test.ts
│   └── README.md
│
├── neondb-client-rust/                     # Rust SDK
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── client.rs
│   │   ├── websocket.rs
│   │   ├── cache.rs
│   │   ├── protocol.rs
│   │   └── signal.rs                       # Reactive signal support
│   ├── tests/
│   │   └── client.rs
│   └── README.md
│
├── neondb-cli/                             # Command-line tool
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs
│   │   ├── commands/
│   │   │   ├── init.rs                     # `neondb init`
│   │   │   ├── build.rs                    # `neondb build`
│   │   │   ├── run.rs                      # `neondb run`
│   │   │   └── migrate.rs                  # `neondb migrate`
│   │   └── config.rs
│   └── README.md
│
├── examples/
│   ├── chat-room/                          # Chat app with TS reducers
│   │   ├── schema.neondb
│   │   ├── reducers.ts
│   │   ├── server.ts                       # or `neondb run`
│   │   ├── client.tsx                      # React client
│   │   └── README.md
│   ├── tic-tac-toe/                        # Game with Rust WASM reducers
│   │   ├── schema.neondb
│   │   ├── src/lib.rs                      # Game logic (compiled to WASM)
│   │   ├── Cargo.toml
│   │   ├── server.ts                       # Client logic
│   │   └── README.md
│   └── mmo-movement/                       # Simple MMO with 1000 players
│       ├── schema.neondb
│       ├── reducers.ts
│       ├── load_test.ts
│       └── README.md
│
├── docker-compose.yml                      # Local dev setup with volume
├── Dockerfile                              # Multi-stage build (if using neondb-server as root)
├── Cargo.lock
├── README.md                               # Project overview
├── PHASE_0_PLANNING.md                     # This file
├── DEPLOYMENT.md                           # Docker, Dokploy, env vars
└── PERFORMANCE.md                          # Benchmarks, optimization notes

```

### Schema File Format (Example: `schema.neondb`)

```toml
[table.users]
columns = [
  { name = "id", type = "u64", primary_key = true },
  { name = "username", type = "string", unique = true },
  { name = "score", type = "i32" },
  { name = "created_at", type = "i64" },
]

[table.messages]
columns = [
  { name = "id", type = "u64", primary_key = true },
  { name = "user_id", type = "u64" },
  { name = "content", type = "string" },
  { name = "created_at", type = "i64" },
]

[reducer.create_user]
language = "typescript"
handler = "reducers.ts:createUser"

[reducer.send_message]
language = "typescript"
handler = "reducers.ts:sendMessage"
```

---

## Risk Assessment & Mitigation

| Risk | Severity | Impact | Mitigation Strategy |
|------|----------|--------|---------------------|
| **V8 Integration Complexity** | HIGH | V8 API is large, embedding is non-trivial. Isolation per call could be slow. | Start with simple integration (single isolate, context reuse). Use `rusty_v8` bindings. Consider QuickJS if V8 fails. Allocate 5–10 developer-days for Phase 2. |
| **Subscription Diffing Scalability** | HIGH | With 10k subscriptions and 1000 writes/sec, diff computation could be O(subscriptions × updates). Bottleneck. | Use efficient matching: bitmasks or Bloom filters to pre-filter irrelevant subscriptions. Profile in Phase 3. If needed, switch to row-versioning or subscription indexing. |
| **Single-Thread Scalability Ceiling** | MEDIUM | Even optimized single thread will hit ~500k TPS limit (vs. 304k target, so achievable). Network I/O might be bottleneck instead. | Measure early in Phase 1 with profiler (perf, flamegraph). If network is bottleneck, optimize protocol (compression, batching). If CPU is bottleneck, acceptable—still beats cloud. |
| **WAL Replay Time on Restart** | MEDIUM | 10 GB WAL at 300k TPS = ~33 seconds replay (acceptable). But if 1M TPS, could be 10 seconds. User tolerance TBD. | Implement incremental snapshots in Phase 5+ (optional). For now, document as acceptable. Provide progress bar in CLI. |
| **Rust WASM Compilation Speed** | LOW | Every user reducer build requires `wasm-pack` or manual compilation. Slow feedback loop. | Provide build cache & incremental compilation. CLI uses `cargo build --release` for .so files. TS is interpreted, no build step. |
| **WebSocket Connection Limits** | MEDIUM | 10k connections means 10k tokio tasks + select() on all. Not all OS's support this easily. | Use Linux epoll (default on tokio). Test with `ab` or custom harness. Consider connection pooling/multiplexing if needed (Phase 6). |
| **Determinism Across Platforms** | MEDIUM | Rust reducers must be deterministic across x86_64 and ARM. Float operations differ. | Disallow floats in reducers; use fixed-point or i32/i64 only. Document constraint. Easy to check at compile time. |
| **Data Consistency After Partial Writes** | MEDIUM | If fsync fails mid-WAL entry, data could be corrupted on next restart. | Use journaling: write entry to temp file, fsync, then rename atomic swap. Or use larger fsync intervals & accept some data loss. Configurable via `FSYNC_INTERVAL_MS`. |
| **V8 Memory Leaks in Long-Running Server** | MEDIUM | Buggy TS reducer could leak memory. V8 isn't designed for high isolation. | Isolate per reducer call (slower but safer). Add memory limits per isolate. Monitor in Phase 6. |
| **Client Optimistic Update Conflicts** | LOW | If client sends update before reducer completes, and reducer result differs, cache is out of sync. | Full diff-based reconciliation on next reducer call. Or require server ack before accepting further ops. Pattern is well-known. |

### Mitigation Priorities

1. **Phase 1**: Profile single-threaded throughput early. If <200k TPS with naive implementation, investigate (likely compiler flags or algorithm).
2. **Phase 2**: Prototype V8 integration on a small example. If >20% overhead, switch to QuickJS.
3. **Phase 3**: Load test subscription diffing with 10k mock subscriptions. If >1ms per diff, optimize.
4. **Phase 6**: Full chaos testing (kill process, corrupt WAL, etc.). Ensure no panics.

---

## Development Timeline Estimate

All estimates assume **1 senior full-stack engineer** with Rust + systems knowledge.

### Breakdown by Phase

| Phase | Name | Effort (Dev-Days) | Notes |
|-------|------|-------------------|-------|
| **0** | Initiative & Planning | **1** | ✓ You are here. Production of this doc. |
| **1** | Core Engine Skeleton | **8–10** | WAL reader/writer, simple in-memory table, basic reducer, WebSocket listener. Hardest part: async tokio integration. |
| **2** | User-Defined Reducers (WASM & TS) | **12–15** | V8 integration is the long pole. Wasmtime is straightforward. Testing both paths. ~5 days just for V8 prototyping. |
| **3** | Subscription Engine & Incremental Updates | **10–12** | Query parsing (simple), matcher (straightforward), diff algorithm (complex if naive). Heavy testing. |
| **4** | Client SDKs (TS + Rust) | **8–10** | TS SDK: 4 days, Rust SDK: 3 days, React hooks: 2 days, test examples: 2 days. Well-trodden path. |
| **5** | Production Readiness & Dokploy Deploy | **6–8** | Docker, docker-compose, env var handling, CLI enhancements, graceful shutdown, config file. Straightforward. |
| **6** | Performance Tuning & Benchmarking | **7–10** | Profiling, optimization loops, report writing, final soak tests. Unpredictable—depends on findings. |
| **7** (Optional) | Optional: Snapshots & Distributed Replication | **20+** | Not in V1 scope. Future work. |

### **Total Estimated Effort: 52–75 developer-days** (6.5–9.5 weeks for 1 FTE, or 13–19 weeks for part-time)

### Critical Path

```
Phase 0 (1 day) → Approval
  ↓
Phase 1 (8 days) → Test WAL & single-threaded baseline
  ↓
Phase 2 (12 days) → Prototype V8; if works, proceed; else pivot to QuickJS (add 2 days)
  ↓
Phase 3 (10 days) → Subscription diff algorithm; profile; optimize if needed (add 5–10 days)
  ↓
Phase 4 (8 days) → Client SDKs; test with examples
  ↓
Phase 5 (6 days) → Docker, Dokploy setup
  ↓
Phase 6 (7 days) → Soak tests, benchmarks, optimization report
```

**Parallel Opportunities**:
- Phases 2 & 4 can overlap (client SDK doesn't depend on reducer engine details, just protocol).
- Examples (Phase 4) can start once Phase 1 has basic reducer support.

### Approval Gate

**Before each phase**, I will message:
> **Phase X – [Name] – Ready to begin. Please confirm.**

**After each phase**, I will report completion and ask:
> **Phase X complete. Shall I proceed to Phase Y?**

You provide explicit go/no-go. If issues arise, we pivot or extend the phase.

---

## Success Metrics & Acceptance Criteria

### Phase 1 Acceptance
- [ ] `neondb-server` binary compiles without warnings
- [ ] Server listens on `0.0.0.0:8000` for WebSocket connections
- [ ] Single reducer (e.g., `increment(x: i32) -> u64`) can be called 1000 times/second
- [ ] Crash recovery: kill process, restart, WAL is replayed, state is correct
- [ ] README documents how to build, run, test locally

### Phase 2 Acceptance
- [ ] Example Rust reducer (WASM): compiled to `.wasm`, loaded by server, executes correctly
- [ ] Example TS reducer: parsed from `reducers.ts`, executed in V8 isolate, modifies tables
- [ ] Both reducers can write to same table without interference
- [ ] Benchmark: Rust reducer TPS vs. TS reducer TPS (target: TS within 20% of Rust native)

### Phase 3 Acceptance
- [ ] Client can subscribe to `SELECT * FROM table WHERE column = value`
- [ ] After reducer, matching subscriptions receive incremental updates
- [ ] Multiple clients subscribed to different queries, each gets correct deltas
- [ ] Benchmark: 10k subscriptions, 1000 reducers/sec, diff computation <1ms per reducer

### Phase 4 Acceptance
- [ ] TypeScript SDK published to npm (as preview)
- [ ] Rust SDK published to crates.io (as preview)
- [ ] Chat room example works: 10 clients, send messages, see real-time updates
- [ ] React hooks (`useNeonDBQuery`, `useNeonDBReducer`) render correctly

### Phase 5 Acceptance
- [ ] Docker image builds with `docker build -t neondb-server .`
- [ ] `docker-compose up` starts server + volume persistence
- [ ] Env vars: `NEONDB_PORT`, `NEONDB_WAL_PATH`, `NEONDB_FSYNC_INTERVAL_MS` work
- [ ] Deployed to Dokploy, accessible from external client within 5 minutes

### Phase 6 Acceptance (Production Ready)
- [ ] Soak test: 10 simulated players, 1000 reducers/second each, 24 hours, zero crashes
- [ ] Crash recovery: 10 GB WAL replayed and ready in <5 seconds
- [ ] Newbie can deploy to Dokploy in <5 minutes with setup instructions
- [ ] Example game (tic-tac-toe or MMO movement) runs without observable lag
- [ ] **Benchmarking tool**: Standalone binary that simulates multiple clients, measures p50/p95/p99 latency and TPS (included as part of deliverable, not just a script)
- [ ] Benchmark report: TPS & latency (p99) across all reducer runtimes

### Final Acceptance: Production-Ready Declaration
NeonDB is **production-ready** when all Phase 6 criteria pass AND:
- Zero open high-severity bugs
- Example game is deployed, tested, and documented
- Setup instructions are <1 page
- All code has unit tests (>70% coverage)
- README + DEPLOYMENT.md are comprehensive

---

## Open Questions for Approval

Below are critical decisions that require your input before proceeding:

### 1. **V8 vs. QuickJS for TypeScript Reducers?**

**Option A: V8** (via `rusty_v8`)
- ✅ Full TypeScript support, excellent performance, industry standard
- ❌ Large binary (~50 MB), complex API, memory overhead
- Est. effort: 12 days (Phase 2)

**Option B: QuickJS** (via `qjscc`)
- ✅ Tiny binary (~5 MB), simple C API, lightweight
- ❌ Limited TypeScript support (would need transpilation), slower (20-40% vs V8)
- Est. effort: 8 days (Phase 2)

**Option C: Deno Core**
- ✅ Full TypeScript + Deno stdlib, modern
- ❌ Heavyweight (~100 MB), less control
- Est. effort: 10 days

**Recommendation**: **Option A (V8)**. TypeScript performance is critical for the use case, and V8 is battle-tested. If it proves problematic in Phase 2, pivot to QuickJS. 

**Your choice?** ________

### 2. **Rust Reducer Sandboxing: WASM or Native .so?**

**Option A: WASM only** (Wasmtime)
- ✅ Portable, safe, no recompilation per platform
- ❌ 2-5x slower, harder to integrate with Rust ecosystem
- Good for: users who want portability

**Option B: Native .so only** (libloading)
- ✅ Near-native performance
- ❌ No isolation, requires user to compile trustworthy code, platform-specific
- Good for: performance-critical games

**Option C: Both** (user chooses at deploy time)
- ✅ Flexibility
- ❌ More code to maintain
- Effort: +3 days

**Recommendation**: **Option C (Both)**. Add `reducer_runtime: "wasm" | "native"` to schema. Phase 1 starts with WASM only (simpler), Phase 2 adds native option.

**Your choice?** ________

### 3. **Subscription Query Language: Custom or SQL Subset?**

**Option A: Simple custom DSL**
```
SELECT users WHERE score > 100 LIMIT 50
```
- ✅ Easy to implement and parse
- ❌ Non-standard, users must learn new syntax

**Option B: SQL subset (tinySQL parser)**
```
SELECT id, username FROM users WHERE score > 100 ORDER BY score DESC LIMIT 50
```
- ✅ Familiar to SQL developers
- ❌ More complex parser, error handling

**Recommendation**: **Option A for Phase 1** (custom DSL). If users request SQL in Phase 4+, add `sqlparser-rs` and extend. 

**Your choice?** ________

### 4. **In-Memory Table Layout: Row-Oriented or Column-Oriented?**

**Option A: Row-Oriented** (HashMap<RowID, Vec<Field>>)
- ✅ Natural for OLTP, easy access patterns, simple serialization
- ❌ Less CPU-cache efficient for wide tables

**Option B: Column-Oriented** (HashMap<ColumnName, Vec<Value>>)
- ✅ Better for subscriptions that select few columns, CPU cache efficient
- ❌ More complex implementation, slower for full-row updates

**Recommendation**: **Option A (Row-Oriented)** for V1. Simpler, adequate for target workload. Phase 6 optimization: profile and consider columnar if needed.

**Your choice?** ________

### 5. **Docker Deployment: Single Stage or Multi-Stage Build?**

**Option A: Single-stage (Fast iteration)**
- Build everything in container, output final binary
- ✅ Simple Dockerfile
- ❌ Large image (~500 MB if V8 included)

**Option B: Multi-stage (Production optimized)**
- Build in one stage (Rust compiler + V8 headers), copy binary to `scratch` or Alpine
- ✅ Small image (~50 MB)
- ❌ Longer build time, more complex Dockerfile

**Recommendation**: **Option B (Multi-stage)** from the start. Keeps deployment lean and professional.

**Your choice?** ________

### 6. **WAL Format: Custom Binary or MessagePack?**

**Option A: MessagePack** (fast serialization lib)
- ✅ Compact, versioned, well-tested
- ❌ Slight overhead vs. hand-crafted binary

**Option B: Hand-crafted Binary**
- ✅ Absolute control, smallest size, fastest
- ❌ More code, error-prone, hard to version

**Recommendation**: **Option A (MessagePack)**. Maintainability + performance is good enough. Phase 6 optimization: profile and hand-craft if needed.

**Your choice?** ________

---

## Next Steps

1. **Review this document** against your vision. Are there gaps, disagreements, or unclear design decisions?
2. **Answer the 6 open questions** above. I will use your choices to guide Phase 1.
3. **Provide explicit approval**: "Phase 0 approved. Proceed to Phase 1." or "Please revise [section X] before proceeding."
4. Once approved, I will begin **Phase 1 – Core Engine Skeleton** with a message: `"Phase 1 – Core Engine Skeleton – Ready to begin. Please confirm."`

---

## Summary of Decisions to Finalize

| Question | Recommendation | Your Decision |
|----------|-----------------|-------------|
| V8 vs QuickJS for TypeScript | V8 (Option A) | ✅ **V8** |
| Rust sandboxing: WASM vs Native | Both, user selects (Option C) | ✅ **Both** (default: WASM) |
| Subscription query language | Custom DSL (Option A) | ✅ **Custom DSL** |
| Table layout: Row or Column | Row-oriented (Option A) | ✅ **Row-oriented** |
| Docker build: Single or Multi-stage | Multi-stage (Option B) | ✅ **Multi-stage** |
| WAL format: MessagePack or Custom | MessagePack (Option A) | ✅ **MessagePack** |

---

---

## Approval & Next Steps

**Date Approved**: 2026-06-05  
**Approved By**: User  
**All 6 design questions**: ✅ Resolved

**Additional Directives**:
- Include a standalone **benchmarking tool** (Phase 6) to simulate multiple clients and report p50/p95/p99 latency + TPS (not just a shell script, a proper binary)

**Current Status**: Awaiting Phase 1 Detailed Specification Review & Execution Approval  

**Next Document**: `PHASE_1_SPECIFICATION.md` (Ready for review)

---

**Phase 0 Complete. Proceeding to produce Phase 1 detailed technical specification.**

