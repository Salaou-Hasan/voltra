# NeonDB Production Readiness Audit & Roadmap

**Date:** 2026-06-07  
**Codebase:** commit `2ac06ba` (307 tests passing)  
**Auditor:** Claude Opus 4.6 — full source review of all `src/`, `tests/`, SDKs, and templates.

---

## 1. Capability Matrix

### Core Database

| Feature | Status | Evidence |
|---------|--------|----------|
| WAL persistence | **COMPLETE** | `BatchedWalWriter` — background flusher thread, configurable fsync interval, MessagePack-encoded entries |
| Crash recovery | **COMPLETE** | `recover_from_wal()` replays all entries > snapshot sequence; partial trailing entries tolerated (log::warn, skip) |
| Tables | **COMPLETE** | `DashMap`-backed `TableStore` — lock-free reads, per-row write locks in sorted key order |
| Query engine (SQL) | **COMPLETE** | Full SQL: SELECT (JOIN/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET/DISTINCT/UNION/subqueries/CASE/aggregates), INSERT, UPDATE, DELETE |
| Reducers | **COMPLETE** | 3 runtimes: Native Rust, Boa JS 0.19, Wasmtime 21 WASM. Panic isolation, timeout, memory caps (WASM) |
| Worker pools | **COMPLETE** | N workers (one per CPU core), kanal MPMC async channel dispatch |
| Scheduler | **COMPLETE** | Configurable interval-based scheduled reducer calls (TOML config). Missed-tick-skip behavior |
| Module system | **COMPLETE** | Auto-loads `modules/` directory — `.wasm`, `.wat`, `.js` with optional `.json` metadata sidecar |
| Indexing | **COMPLETE** | Secondary field indexes (`create_index`/`drop_index`/`index_lookup`), maintained on write |
| Transactions | **PARTIAL** | Single-reducer atomicity via `apply_delta_batch()` (all-or-nothing commit). No multi-reducer ACID transactions, no isolation levels, no rollback-on-read-conflict |
| Schema enforcement | **COMPLETE** | `schema.toml` — typed columns (String/i64/f64/bool/bytes/any), required/optional, defaults. Validates on every `set_row` |
| Migrations | **COMPLETE** | `migrations/*.toml` — add_field, remove_field, rename_field. Idempotent with `__migrations` tracking table |
| Snapshots | **COMPLETE** | Atomic write (tmp+rename+fsync), auto-trigger every N WAL entries, recovery loads latest + replays remainder |

### Realtime Infrastructure

| Feature | Status | Evidence |
|---------|--------|----------|
| WebSocket networking | **COMPLETE** | tokio + tokio-tungstenite, MessagePack framing, per-client write task with mpsc channel |
| Live subscriptions | **COMPLETE** | `SubscriptionManager` with reverse-index (O(matching)), predicate tree (AND/OR/IN/Comparison), ORDER BY, LIMIT |
| Initial state sync | **COMPLETE** | `subscribe_with_snapshot()` delivers existing matching rows immediately on subscribe |
| Pub/Sub (table-level) | **COMPLETE** | Every committed delta fans out to all matching subscribers as `Arc<Bytes>` (zero re-encode) |
| Presence | **PROTOTYPE** | Connection count tracked (`active_connections` AtomicUsize). No per-user presence state, no heartbeat-based presence, no "who is online" query |
| Event propagation | **COMPLETE** | Subscription diffs propagate across cluster via fan-out (POST /cluster/deltas) |

### Game Platform Features

| Feature | Status | Evidence |
|---------|--------|----------|
| Players | **PROTOTYPE** | `rust/game-ready` template has spawn/attack/buy_item JS reducers. No progression system, no profile management |
| Combat | **PROTOTYPE** | Template `attack` reducer with HP deduction. No damage formulas, cooldowns, AOE, status effects |
| NPCs | **PROTOTYPE** | Template mentions NPCs. No AI behavior, no pathfinding, no spawn rules |
| Quests | **PROTOTYPE** | Template has quest accept/complete reducers. No quest chains, prerequisites, time gates |
| Matchmaking | **PROTOTYPE** | Template `refresh` reducer. No Elo/MMR, no queue system, no lobby management |
| Guilds | **PROTOTYPE** | Template has guild create/join/leave. No ranks, no permissions, no bank |
| Economy | **PROTOTYPE** | Template has buy_item with gold check. No marketplace, no auctions, no trades |
| Leaderboards | **PARTIAL** | Counters table + ORDER BY DESC LIMIT N on subscriptions gives live leaderboards. No time-windowed boards, no percentile ranks |
| World simulation | **PROTOTYPE** | Template mentions zones. No tick-based simulation, no spatial queries, no physics |

### Infrastructure

| Feature | Status | Evidence |
|---------|--------|----------|
| Replication (delta fan-out) | **PARTIAL** | POST /cluster/deltas replicates committed deltas to peers + WAL-journals them. No consensus, no conflict resolution |
| Clustering | **PARTIAL** | FNV-1a shard routing, gossip heartbeat, dynamic peer join, proxy calls, fan-out retry queue (backoff). No leader election, no auto-rebalance |
| Node communication | **COMPLETE** | HTTP-based cluster bus with shared secret auth, health endpoint, peers endpoint |
| Metrics endpoint | **PARTIAL** | `/metrics` (Prometheus-ish text), `/healthz` (JSON), `/stats` (tables). No histogram, no per-reducer latency, no queue depth |
| Benchmark tooling | **COMPLETE** | criterion benches (3 scenarios) + inline `neondb bench` CLI command with HDR histogram latency reporting |

---

## 2. Reliability Audit

### Failure Modes

| Scenario | Survives? | How |
|----------|-----------|-----|
| Process crash | **YES** | WAL replayed on restart from last snapshot. Data up to `fsync_interval_ms` before crash is durable |
| Server crash (OS kill) | **YES** | Same as process crash — WAL is fsync'd periodically |
| Power loss | **PARTIAL** | Data written since the last fsync is lost (up to `fsync_interval_ms` worth). With `fsync_interval_ms=100`, worst case = 100ms of writes lost |
| Disk failure | **NO** | Single-disk, no replication to disk. Total data loss unless backup exists |
| Network interruption | **PARTIAL** | Clients disconnect and must reconnect. No automatic reconnection server-side. Fan-out retry queue buffers up to 1024 payloads per dead peer |
| Cluster node failure | **PARTIAL** | Gossip marks node unhealthy after 3 consecutive failures. Fan-out retries. No automatic failover, no replica promotion |

### Persistence Features

| Feature | Status | Notes |
|---------|--------|-------|
| Snapshot support | **YES** | Auto every `snapshot_interval` WAL entries. Atomic write (tmp+rename) |
| Snapshot recovery | **YES** | `load_snapshot()` + `recover_from_wal()` replays only post-snapshot entries |
| WAL recovery correctness | **YES** | Partial trailing entries detected and skipped (not treated as corruption). Tested |
| Backup capability | **MANUAL** | Copy `snapshots/` directory + WAL file while server is running. No hot backup API |
| Restore capability | **MANUAL** | Stop server, place snapshot + WAL, restart. Server auto-replays |
| Disaster recovery workflow | **DOCUMENTED** | In OPERATIONS.md. No automated DR |
| Failover capability | **MISSING** | No leader election, no automatic promotion of replica to primary |
| Split-brain protection | **MISSING** | No quorum, no fencing tokens, no consensus protocol |

---

## 3. Scalability Audit

### Measured/Estimated Limits

| Metric | Estimate | Basis |
|--------|----------|-------|
| Concurrent connections | **10,000+** | tokio async, per-client mpsc channel, Arc<Bytes> fan-out. Bounded by `max_connections` config |
| Reducer throughput | **50,000-150,000 TPS** | N workers (one per core), kanal MPMC dispatch, in-memory DashMap. Bench infrastructure exists |
| Read throughput | **1M+ ops/sec** | Lock-free DashMap reads. No disk I/O for reads (all in-memory) |
| Write throughput | **50,000-100,000 TPS** | Bounded by WAL fsync batching (100ms window) and per-row lock contention |
| Memory scaling | **~200-500 bytes/row** | DashMap overhead + Arc<Bytes> payload + row lock entry. 1M rows ≈ 200-500 MB |
| CPU scaling | **Linear to core count** | One worker per core. DashMap shards = max(16, next_pow2(cores * 4)) |

### Under Load

| Condition | Behavior |
|-----------|----------|
| 70% CPU | Normal operation. Workers fully utilized. Latency increases slightly as kanal channel backs up |
| 90% CPU | Reducer queue depth grows. Tail latency spikes (p99 climbs). Subscription fan-out may lag behind commits |
| Memory constrained | No backpressure mechanism. DashMap will keep growing. Eventually OOM-killed by OS. BlobStore writes to disk but index stays in memory |
| Sustained load | WAL file grows unbounded until snapshot fires. After snapshot, no auto-truncation — manual rotation required |

### Missing Scalability Features

- **No backpressure**: When reducer queue fills, producers (WebSocket handler) are never slowed
- **No memory limit on TableStore**: Can grow until process is OOM-killed
- **No WAL auto-rotation**: File grows without bound between snapshots
- **No connection-level rate limiting**: A single client can flood the reducer queue
- **No read replicas**: All reads go to the same in-memory store

---

## 4. Distributed Systems Audit

### Cluster Coordination

| Feature | Status | Notes |
|---------|--------|-------|
| Node discovery | **PARTIAL** | Static `NEONDB_PEERS` env var + dynamic `POST /cluster/join`. No mDNS, no service registry |
| Membership tracking | **COMPLETE** | `DashMap<shard_id, PeerEntry>` with health state |
| Heartbeats | **COMPLETE** | Gossip task pings `/cluster/health` every `NEONDB_GOSSIP_INTERVAL_MS` (default 5s) |
| Leader election | **MISSING** | No Raft, no Paxos, no epoch-based leader. Every node is peer-equal |
| Quorum writes | **MISSING** | Writes commit locally then fan-out async. No write acknowledgment from peers |

### Replication

| Feature | Status | Notes |
|---------|--------|-------|
| WAL replication | **MISSING** | WAL is local only. Peers receive delta fan-out (application-level), not raw WAL stream |
| State replication | **PARTIAL** | POST /cluster/deltas delivers committed deltas. WAL-journaled on receiver. No full-state sync for new nodes |
| Replica promotion | **MISSING** | No mechanism to promote a replica to primary |
| Recovery after failure | **PARTIAL** | Fan-out retry queue (1024 entries, exponential backoff). Misses during extended outage are lost |

### Ownership Model

| Entity | Ownership Strategy | Notes |
|--------|-------------------|-------|
| Rows | **Shard-based** | `shard_for_key(key, shard_count)` via FNV-1a hash. Static assignment |
| Players | Row ownership | Player row key → shard. No migration |
| Guilds | Row ownership | Guild row key → shard. Cross-shard guild operations require proxy calls |
| Matches | Row ownership | No dedicated match lifecycle. Would be row-key routed |
| Rooms | Not implemented | — |
| Worlds | Not implemented | — |
| NPCs | Row ownership | Same as any other row |

**How ownership is tracked:** `RowDelta.shard_id` field on every delta. `TableStore.shard_id` / `shard_count` configured at startup.

**How ownership changes:** It doesn't. Shard assignment is static hash-based. Re-sharding requires full cluster restart with new `shard_count` and data redistribution (not automated).

---

## 5. Autoscaling Audit

### Automatic Scaling

| Feature | Status |
|---------|--------|
| Node creation | **MISSING** |
| Node removal | **MISSING** |
| Capacity detection | **MISSING** |
| Load balancing | **MISSING** |

### Workload Rebalancing

| Feature | Status |
|---------|--------|
| Hot partition detection | **MISSING** |
| Load redistribution | **MISSING** |
| Session migration | **MISSING** |
| World migration | **MISSING** |
| Match migration | **MISSING** |

**Can NeonDB automatically respond to overloaded nodes?** No. There is no feedback mechanism between load metrics and cluster topology.

**Can workload move between nodes without downtime?** No. Shard assignment is static (hash of key mod shard_count). Moving data between nodes would require: (1) pausing writes to affected keys, (2) streaming rows to new owner, (3) updating routing table, (4) resuming writes. None of this exists.

### Required Architecture for Autoscaling

```
┌──────────────────────────────────────────────────────────────┐
│ Control Plane (new component)                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │ Load Monitor│──│ Shard Mapper │──│ Migration Engine  │   │
│  │ (per-node   │  │ (consistent  │  │ (stream rows,     │   │
│  │  CPU/mem/   │  │  hashing     │  │  pause/resume,    │   │
│  │  queue)     │  │  ring)       │  │  fence old owner) │   │
│  └─────────────┘  └──────────────┘  └───────────────────┘   │
└──────────────────────────────────────────────────────────────┘
         │                    │                    │
    ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
    │ Node A  │         │ Node B  │         │ Node C  │
    │ shard 0 │◄───────►│ shard 1 │◄───────►│ shard 2 │
    └─────────┘         └─────────┘         └─────────┘
```

Key components needed:
1. **Consistent hash ring** replacing static mod-N sharding
2. **Virtual shards** (256+) mapped to physical nodes (allows granular rebalance)
3. **Migration engine** — stream rows between nodes during rebalance
4. **Fencing tokens** — prevent old owner from accepting writes during migration
5. **Control plane** — centralized or consensus-based decision maker for splits/merges

---

## 6. Security Audit

### Authentication

| Feature | Status | Notes |
|---------|--------|-------|
| API key auth | **COMPLETE** | Bearer token validated at WebSocket upgrade. Configurable via `NEONDB_API_KEY` or TOML |
| Role parsing | **COMPLETE** | `Bearer <key>:<role>` — role extracted, validated (`^[a-zA-Z0-9_-]{1,32}$`) |
| User accounts | **MISSING** | No user registration, login, password hashing. Auth is key-based only |
| Sessions | **MISSING** | No session tokens, no session expiry, no session revocation |
| JWT | **MISSING** | No JWT verification. Bearer token is a static shared secret |
| OAuth / SSO | **MISSING** | No OAuth2 flow, no OIDC, no social login |

### Authorization

| Feature | Status | Notes |
|---------|--------|-------|
| Roles | **COMPLETE** | Extracted from Bearer suffix. Scheduler role bypasses all checks |
| Permissions | **COMPLETE** | `PermissionsConfig` — per-reducer role allowlist. Open/Closed default policy |
| Row-level access control | **MISSING** | Any authenticated user can read any row. No per-table or per-row ACLs |
| Column-level access control | **MISSING** | — |

### Protection

| Feature | Status | Notes |
|---------|--------|-------|
| Rate limiting | **MISSING** | No per-client, per-IP, or per-reducer rate limits |
| Abuse prevention | **PARTIAL** | `max_connections` config caps total connections. Reducer timeout prevents infinite loops |
| DDoS mitigation | **MISSING** | No SYN flood protection, no connection throttling, no IP blocklist |
| Secret management | **PARTIAL** | API key from env var. Cluster secret from env var. No rotation, no vault integration |
| TLS | **MISSING** | WebSocket and HTTP are plaintext. TLS termination must be handled by reverse proxy |
| Input validation | **PARTIAL** | Schema validates row data types. SQL is parsed (no injection). Args byte cap enforced. But no per-field sanitization |

---

## 7. Operations Audit

| Feature | Status | Notes |
|---------|--------|-------|
| Monitoring | **PARTIAL** | `/metrics` endpoint with 4 gauges. No time-series, no per-reducer breakdown |
| Metrics | **PARTIAL** | active_subscriptions, active_connections, total_rows, uptime_nanos |
| Alerting | **MISSING** | No built-in alerting. Must scrape `/metrics` externally |
| Dashboard | **MISSING** | No web UI. CLI only |
| Logging | **COMPLETE** | `env_logger` with configurable level. Structured-ish log lines |
| Tracing | **MISSING** | No OpenTelemetry, no request IDs, no distributed tracing |
| Health checks | **COMPLETE** | `/healthz` returns JSON with status, row count, connection count |
| Rolling upgrades | **MISSING** | No blue/green, no canary. Single-binary restart causes downtime |
| Schema migrations | **COMPLETE** | `migrations/*.toml` with idempotent tracking |
| Cluster administration | **PARTIAL** | `/cluster/peers` (GET), `/cluster/join` (POST), `/cluster/health` (GET). No node drain, no graceful decommission |

---

## 8. Redis Replacement Analysis

| Redis Feature | NeonDB Equivalent | Gap |
|---------------|-------------------|-----|
| Cache (GET/SET) | `TableStore.get_row` / `set_row` | No TTL, no eviction policy |
| Pub/Sub | Live subscriptions with predicate filtering | More powerful than Redis pub/sub (server-side filtering). But no channel-pattern wildcards |
| Presence | Connection count only | No per-user presence state, no heartbeat-based timeout |
| Session storage | Can store sessions as rows | No built-in expiry/cleanup |
| Counters | `counter_add` atomic increment | Complete — atomic even under concurrency |
| Leaderboards | ORDER BY + LIMIT subscriptions | Live-updating. Missing: time-windowed boards, ZADD equivalent |
| TTL data | **MISSING** | No expiry mechanism. Would need a scheduled reducer to clean up |
| Lists/Queues | **MISSING** | No LPUSH/RPOP equivalent. Rows are key-value, not ordered lists |
| Streams | **MISSING** | No append-only log with consumer groups |
| Geospatial | **MISSING** | No GEOADD/GEORADIUS |
| Lua scripting | Reducers (JS/WASM) | More powerful — full execution context with state access |

**Verdict:** NeonDB replaces ~60% of common Redis use cases (cache, pub/sub, counters, leaderboards, scripting) but lacks TTL, queues, streams, and geospatial.

---

## 9. PostgreSQL Replacement Analysis

| PostgreSQL Feature | NeonDB Equivalent | Gap |
|-------------------|-------------------|-----|
| Durable storage | WAL + snapshots | Complete for the data model (key-value + JSON). Not relational |
| Transactions (ACID) | Single-reducer atomicity | No multi-statement transactions, no isolation levels, no savepoints |
| Indexes | Secondary field indexes (DashMap-based) | Single-field only. No composite, no partial, no expression indexes |
| Queries (SQL) | Full SQL engine | SELECT/INSERT/UPDATE/DELETE with JOINs, aggregates, subqueries. Missing: views, CTEs, window functions |
| Constraints | Schema enforcement (type + required/optional) | No foreign keys, no UNIQUE constraint, no CHECK constraints |
| Replication | Delta fan-out to peers | No streaming replication, no logical replication, no pg_basebackup equivalent |
| Backups | Manual snapshot copy | No pg_dump equivalent, no PITR (point-in-time recovery) |
| Recovery | Snapshot + WAL replay | Works for single-node. No timeline branching |
| Stored procedures | Reducers | More powerful (full JS/WASM runtime). No PL/pgSQL but not needed |
| JSON support | Native (all data is JSON) | Stronger than PostgreSQL JSONB — entire data model is JSON |
| Full-text search | **MISSING** | No tsvector, no trigram, no search ranking |
| Partitioning | Shard-based | Horizontal only. No declarative partitioning |

**Verdict:** NeonDB replaces ~40% of PostgreSQL functionality. It's a JSON document store with SQL query capability, not a relational database. Missing: foreign keys, UNIQUE, multi-row transactions, PITR, full-text search, views, CTEs.

---

## 10. Platform Service Analysis

| Service | Readiness | Notes |
|---------|-----------|-------|
| Matchmaking | **Prototype** | Template reducer only. No queue, no skill rating, no region matching, no lobby lifecycle |
| Presence | **Missing** | Only connection count. No per-user online/offline/idle state, no last-seen timestamps |
| Chat | **Prototype** | `rust/chat` template has rooms/threads/reactions. No persistence guarantees, no message history pagination, no moderation queue |
| Guilds | **Prototype** | Template has create/join/leave. No ranks, permissions, bank, activity log |
| Economy | **Prototype** | Template has gold-based buy. No marketplace, auctions, trade escrow, currency exchange |
| Analytics | **Missing** | No event tracking, no funnels, no retention metrics, no A/B testing |
| AI NPC generation | **Missing** | No LLM integration, no behavior trees, no dialogue system |

---

## 11. Architecture Review

### Strengths

1. **Zero-copy fan-out** — `Arc<Bytes>` subscription delivery means one serialization per delta, regardless of subscriber count. This is architecturally superior to most competitors.

2. **Lock-free reads** — DashMap provides concurrent reads without blocking. Only writes take per-row locks, acquired in sorted order (deadlock-free).

3. **Single binary** — No external dependencies (no Redis, no PostgreSQL, no ZooKeeper). Deploy one file. This is a massive operational advantage.

4. **Multi-runtime reducers** — Native Rust (zero overhead), JS (rapid development), WASM (sandboxed + portable). Users pick the right tool for each reducer.

5. **SQL engine** — Full SQL with JOINs and aggregates over the same in-memory store. Unusual and powerful for a game backend.

6. **Integrated WAL + snapshots** — Recovery is automatic and tested. The batched async writer achieves high throughput without sacrificing durability.

7. **Predicate-filtered subscriptions** — Clients subscribe with WHERE clauses. Server-side filtering reduces bandwidth dramatically versus "subscribe to everything and filter client-side."

### Weaknesses

1. **No consensus protocol** — The cluster has no agreed-upon source of truth. Fan-out is fire-and-forget. If the primary crashes mid-fan-out, some peers have the delta, others don't. There is no way to detect or recover from this inconsistency.

2. **No backpressure** — A slow consumer (WebSocket client with full send buffer) never signals the reducer pipeline to slow down. Under sustained overload, memory grows without bound.

3. **In-memory only** — All data must fit in RAM. There is no disk-backed storage for cold data, no eviction policy, no tiered storage.

4. **Static sharding** — Adding or removing nodes requires full restart and manual data redistribution. Cannot scale dynamically.

5. **No TLS** — All traffic is plaintext. Production requires an external TLS terminator (nginx, Caddy, cloud load balancer).

6. **Single-writer per key** — `apply_delta_batch` acquires per-row locks in sorted order. Under high contention on hot keys, this serializes all writers to those keys.

7. **Boa JS memory unbounded** — The JS runtime (Boa 0.19) has no heap cap. A malicious/buggy JS reducer can exhaust process memory before the timeout fires.

### Risks

1. **Data loss window** — `fsync_interval_ms=100` means up to 100ms of writes are lost on power failure. For financial data this is unacceptable; for game state it's usually fine.

2. **Split-brain in cluster** — Two partitioned nodes can both accept writes to the same key simultaneously. No conflict resolution exists. When connectivity resumes, last-write-wins semantics with no detection.

3. **WAL unbounded growth** — Between snapshots, the WAL grows without limit. A 24-hour run at 50k TPS produces a ~100GB WAL file.

4. **OOM crash** — No memory limit on TableStore or subscription buffers. Under sustained write pressure with many subscriptions, process memory can grow until the OS kills it.

---

## 12. Prioritized Roadmap

### Tier 0 — Critical (Must-have before production)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| T0-1 | **TLS support** (native or document reverse-proxy requirement) | 2 days | All production traffic is currently plaintext |
| T0-2 | **Backpressure / flow control** — slow clients must not cause unbounded memory growth | 3 days | Prevents OOM under load |
| T0-3 | **WAL auto-rotation** — truncate WAL after snapshot confirmed | 1 day | Prevents disk exhaustion |
| T0-4 | **Connection-level rate limiting** — per-client reducer call rate cap | 2 days | Prevents single client from monopolizing server |
| T0-5 | **Graceful shutdown** — drain in-flight reducers, flush WAL, notify clients | 1 day | Partially exists (Ctrl+C handler); needs "draining" state |
| T0-6 | **Health check improvements** — report WAL lag, queue depth, memory usage | 1 day | Required for operational monitoring |

### Tier 1 — High Priority (Required for reliability and scale)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| T1-1 | **Consensus protocol (Raft)** — leader election, log replication, split-brain protection | 3 weeks | Foundation for all reliable clustering |
| T1-2 | **Automatic failover** — promote replica on leader failure | 1 week | Zero-downtime for node failures |
| T1-3 | **Full-state sync for new nodes** — stream all rows from existing cluster member | 1 week | Required for elastic scaling |
| T1-4 | **Consistent hash ring** — virtual shards mapped to physical nodes | 1 week | Enables adding/removing nodes without full restart |
| T1-5 | **Per-user presence** — heartbeat-based online/offline/idle with last-seen | 3 days | Critical for multiplayer games |
| T1-6 | **TTL / auto-expiry** — rows with configurable time-to-live | 3 days | Replaces Redis TTL. Needed for sessions, temporary state |
| T1-7 | **Row-level access control** — per-table or per-row read/write policies | 1 week | Required for multi-tenant deployments |
| T1-8 | **OpenTelemetry integration** — distributed tracing, request IDs | 3 days | Required for debugging production issues |
| T1-9 | **Memory limits with eviction** — LRU eviction for cold rows to disk | 2 weeks | Prevents OOM; enables datasets larger than RAM |

### Tier 2 — Competitive Features (Major platform improvements)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| T2-1 | **JWT authentication** — verify signed tokens, extract claims as roles/identity | 3 days | Standard auth for web/mobile clients |
| T2-2 | **Matchmaking service** — queue system, Elo/MMR, region-aware, lobby lifecycle | 2 weeks | Replaces dedicated matchmaking service |
| T2-3 | **Chat service** — message history, pagination, unread counts, moderation queue | 1 week | Replaces dedicated chat backend |
| T2-4 | **Economy engine** — marketplace, auctions, trade escrow, currency exchange | 2 weeks | Replaces custom game economy backends |
| T2-5 | **Admin dashboard (web UI)** — tables, connections, metrics, logs, cluster status | 2 weeks | Operational necessity for non-CLI users |
| T2-6 | **Point-in-time recovery (PITR)** — restore to any timestamp from WAL archive | 1 week | Critical for production data safety |
| T2-7 | **Hot backup API** — consistent snapshot while server is running, streamed over HTTP | 3 days | Automated backup without downtime |
| T2-8 | **Horizontal read replicas** — read-only nodes that receive delta stream | 1 week | Scale reads without scaling writes |

### Tier 3 — Differentiators (Competitive moat)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| T3-1 | **Spatial queries** — 2D/3D spatial index, radius queries, zone-based subscriptions | 2 weeks | Unique for game backends. Replaces custom spatial servers |
| T3-2 | **AI NPC service** — LLM integration for dialogue, behavior generation, procedural quests | 3 weeks | No competitor offers this natively |
| T3-3 | **Analytics engine** — event ingestion, funnels, retention, real-time dashboards | 3 weeks | Replaces Mixpanel/Amplitude for games |
| T3-4 | **Server-authoritative physics** — tick-based simulation with client prediction reconciliation | 4 weeks | Enables competitive multiplayer without cheating |
| T3-5 | **Edge deployment** — compile to WASM for Cloudflare Workers / Deno Deploy | 4 weeks | Sub-10ms latency for global player base |
| T3-6 | **Visual reducer editor** — drag-and-drop game logic builder, compiles to WASM | 6 weeks | Enables non-programmers to build game backends |
| T3-7 | **Replay system** — record and replay game sessions from WAL for spectating/debugging | 2 weeks | Unique competitive advantage |

---

## 13. Execution Plan

### Phase 1 — Production Foundation (Weeks 1-2)

**Goal:** Make single-node deployment production-safe.

| Week | Tasks | Dependencies |
|------|-------|-------------|
| 1 | T0-1 (TLS docs), T0-2 (backpressure), T0-3 (WAL rotation), T0-6 (health) | None — all independent |
| 2 | T0-4 (rate limiting), T0-5 (graceful shutdown), T1-6 (TTL), T1-8 (OpenTelemetry) | T0-6 provides metrics for T0-4 |

**Deliverable:** A single NeonDB node that can run 24/7 without OOM, without disk exhaustion, with basic observability.

### Phase 2 — Security & Multi-tenancy (Weeks 3-4)

**Goal:** Make it safe for multiple users/games on one server.

| Week | Tasks | Dependencies |
|------|-------|-------------|
| 3 | T2-1 (JWT auth), T1-7 (row-level ACL), T1-5 (presence) | T2-1 provides identity for T1-7 |
| 4 | Rate limiting polish, admin API for key rotation, audit logging | T2-1 complete |

**Deliverable:** Multi-tenant-safe deployment with proper authentication and authorization.

### Phase 3 — Reliable Clustering (Weeks 5-8)

**Goal:** Multi-node deployment that survives node failures.

| Week | Tasks | Dependencies |
|------|-------|-------------|
| 5-6 | T1-1 (Raft consensus) | None — greenfield module |
| 7 | T1-2 (automatic failover), T1-3 (full-state sync) | T1-1 (leader election) |
| 8 | T1-4 (consistent hash ring), T2-8 (read replicas) | T1-1 + T1-3 |

**Deliverable:** A 3+ node cluster that survives single-node failure with zero data loss and automatic recovery.

### Phase 4 — Platform Services (Weeks 9-12)

**Goal:** Replace dedicated game infrastructure services.

| Week | Tasks | Dependencies |
|------|-------|-------------|
| 9-10 | T2-2 (matchmaking), T2-3 (chat) | Phase 2 (auth) |
| 11-12 | T2-4 (economy), T2-5 (admin dashboard) | Phase 1-3 all complete |

**Deliverable:** A platform that genuinely replaces PostgreSQL + Redis + Socket.IO + dedicated game services.

### Phase 5 — Differentiators (Weeks 13-20)

**Goal:** Features no competitor offers.

| Week | Tasks | Dependencies |
|------|-------|-------------|
| 13-14 | T3-1 (spatial queries), T2-6 (PITR) | Phase 3 (clustering) for PITR |
| 15-17 | T3-2 (AI NPCs), T3-3 (analytics) | Phase 4 (platform services) |
| 18-20 | T3-4 (physics), T3-7 (replay) | Phase 3 (WAL streaming for replay) |

---

## 14. Critical Path Summary

The shortest path from current state to "production-grade distributed realtime platform":

```
Current State (Single-node, in-memory, no TLS, no rate limiting)
    │
    ▼  [2 weeks]
Production-Safe Single Node (backpressure, WAL rotation, health, TLS)
    │
    ▼  [2 weeks]
Multi-Tenant Safe (JWT, row ACL, presence, audit)
    │
    ▼  [4 weeks]
Reliable Cluster (Raft, failover, state sync, hash ring)
    │
    ▼  [4 weeks]
Platform Services (matchmaking, chat, economy, dashboard)
    │
    ▼  [8 weeks]
Differentiated Product (spatial, AI, analytics, physics, replay)
```

**Total estimated engineering effort: 20 weeks (1 senior engineer) or 10 weeks (2 engineers, parallelized after Phase 2).**

**The single highest-impact item is T0-2 (backpressure) — without it, sustained production load will eventually OOM the process. This should be the very first implementation.**

---

## 15. Final Assessment

NeonDB is an **impressive prototype** with genuinely innovative architecture (zero-copy fan-out, multi-runtime reducers, integrated SQL, predicate subscriptions). The core engine is solid — 307 tests pass, the code is well-structured, performance characteristics are strong.

**What it IS today:** A high-performance single-node real-time data engine suitable for development, demos, and low-stakes production use (hobby games, internal tools, prototypes).

**What it ISN'T yet:** A production-grade distributed platform. The gaps in consensus, failover, backpressure, TLS, and rate limiting make it unsuitable for any deployment where data loss, downtime, or security breaches would be unacceptable.

**What makes it special:** The architecture is fundamentally sound. The hardest part (high-performance concurrent engine with zero-copy fan-out) is already solved. The remaining work is mostly "well-known distributed systems problems" (Raft, consistent hashing, backpressure) — hard to implement correctly but well-documented in literature.

**Positioning:** NeonDB is the only open-source, single-binary platform offering: real-time subscriptions + SQL + reducers + clustering + game services in one deployment. Its architecture is designed to replace multiple purpose-built tools (Supabase Realtime, Nakama, Redis, Postgres) with a single self-hosted binary.
