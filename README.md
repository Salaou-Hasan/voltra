# NeonDB

Self-hosted, real-time, in-memory game backend in Rust.

NeonDB is a single-binary WebSocket server for games and real-time applications. Clients call **reducers** (named, atomic functions) over WebSocket, data lands in a lock-free in-memory table store, every write is durably logged to a WAL, and subscribers receive live row diffs instantly. Three reducer runtimes — native Rust, JavaScript (QuickJS), and WASM (Wasmtime/Cranelift JIT) — let you write game logic in whichever language fits the problem.

[![Version](https://img.shields.io/badge/version-1.0.21-blue)](#installation)
[![Tests](https://img.shields.io/badge/tests-541%20passing-brightgreen)](#testing)
[![TPS](https://img.shields.io/badge/throughput-53K%20TPS%20%4015K%20CCU-blue)](#benchmarks)
[![Platforms](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)](#installation)

---

## Installation

**Download a pre-built binary** (recommended):

```bash
# Windows
curl -LO https://github.com/Salaou-Hasan/neondb-releases/releases/latest/download/neondb-x86_64-windows.exe
mv neondb-x86_64-windows.exe neondb.exe

# Linux (x86_64)
curl -LO https://github.com/Salaou-Hasan/neondb-releases/releases/latest/download/neondb-x86_64-linux
chmod +x neondb-x86_64-linux && mv neondb-x86_64-linux neondb
```

**Or build from source** (requires Rust 1.78+):

```bash
git clone https://github.com/Salaou-Hasan/NeonDB
cd NeonDB && cargo build --release
# binary is at target/release/neondb
```

**Keep it up to date:**

```bash
neondb update          # install latest release
neondb update --check  # just check, don't install
```

---

## Quick Start

```bash
# 1. Scaffold a game project
neondb init my-game --template game/basic

# 2. Start the server (from inside the project)
cd my-game
neondb start           # builds + runs your game binary automatically

# 3. Call a reducer
neondb call spawn '["alice", "lobby_1", "warrior"]'

# 4. Watch live updates
neondb watch "players WHERE alive = true"

# 5. Add more systems
neondb add combat      # attack, respawn, abilities
neondb add inventory   # items, equip slots
neondb add leaderboard # score submit, top-N, weekly reset
```

---

## Project Templates

```bash
neondb templates    # list all templates
neondb modules      # list all add-on modules
```

| Template | Description |
|---|---|
| `game/basic` | Spawn, move, despawn, health — the minimal multiplayer foundation |
| `game/full` | All 9 modules pre-configured: combat, inventory, economy, matchmaking, guilds, quests, leaderboard, chat, world |
| `game/unity` | Unity C# SDK + full game server. Drop `unity/` into `Assets/Scripts/NeonDB/` |
| `game/godot` | Godot 4 GDScript SDK + full game server. Add `godot/` as an Autoload |

### Add-on Modules (`neondb add <module>`)

Each module adds ready-made reducers + schema to an existing project:

| Module | What it adds |
|---|---|
| `combat` | `attack`, `respawn`, ability system, NPC damage |
| `inventory` | Items, qty stacking, equip slots |
| `leaderboard` | Score submit, global top-N, scheduled weekly reset |
| `matchmaking` | Queue, ELO pairing, match creation (scheduled) |
| `guilds` | Create, invite, accept, kick |
| `quests` | Accept, progress tracking, claim reward |
| `economy` | Gold/gem wallets, shop buy/sell, transfers, loot boxes |
| `world` | World tick, NPC spawn, session cleanup (scheduled) |
| `chat` | Rooms, messages, per-room presence |

---

## Writing Reducers

### Native Rust — zero overhead

```rust
use neondb::{reducer, ret};

#[reducer]
fn heal(ctx: Ctx, player_id: String, amount: i32) {
    let mut row = ctx.get("players", &player_id)?.unwrap_or_default();
    let hp = row["hp"].as_i64().unwrap_or(0) + amount as i64;
    ctx.set("players", &player_id, serde_json::json!({ "hp": hp }))?;
    ret!({ "ok": true, "new_hp": hp })
}
```

`#[reducer]` auto-registers the function at startup — no boilerplate, no registration calls.

### JavaScript — QuickJS

```js
// modules/heal.js
function reducer(args) {
  const [id, amount] = args;
  const p = __neondb_get("players", id) || { hp: 0 };
  p.hp += amount;
  __neondb_set("players", id, p);
  return { ok: true, new_hp: p.hp };
}
```

- 64 MB heap cap per thread; warm context reused across calls.
- CPU timeout (default 5s, configurable per module or via `NEONDB_REDUCER_TIMEOUT_MS`).
- Killed script evicted from warm cache — next call rebuilds cleanly.

### WASM — C#, Go, or any `.wasm`

```bash
neondb init my-game --template csharp-reducers   # .NET 8 WASI
neondb init my-game --template go-reducers        # TinyGo
neondb build                                       # compiles to .wasm
neondb start
```

---

## Architecture

```
Client ──WebSocket──► Listener
                         │  MessagePack decode
                         ▼
                    PendingCall queue (kanal, bounded 16K)
                         │
                    N parallel workers (Tokio blocking threads)
                         │
                    ReducerContext (staged writes, OCC read-set)
                         │  commit() — versioned, retries on conflict
                         ▼
                    TableStore (DashMap, hybrid MsgPack/zstd rows)
                         │  apply_delta_batch()
                         ├──► WAL (group-commit, fsync)
                         ├──► Subscription fan-out (Arc<Bytes>, zero-copy)
                         └──► Cluster fan-out (peer delta replication)
```

### Key design choices

- **Zero global locks on reads** — `DashMap` shards give lock-free concurrent reads.
- **Serializable isolation** — `apply_delta_batch` acquires per-key slot locks in sorted order; no lost updates. OCC read-set validation catches concurrent RMW conflicts (retries up to 5×).
- **Hybrid row encoding** — small rows (< 256 bytes MsgPack) stored raw; large rows compressed with zstd level 1. Memory stays flat under sustained load.
- **Fixed-slot mutex pool** — 512-slot array replaces per-row `DashMap<String, Mutex>`, eliminating ~128 bytes/row of lock overhead.
- **Subscription delivery** — `Arc<Bytes>` fan-out: one encode per commit, zero re-encodes per subscriber. Optional 20Hz tick coalescing cuts fan-out volume ~24× for high-frequency games.
- **Group-commit WAL** — batches drain on every write syscall; acknowledged only after data reaches the OS. Durability window: microseconds.
- **Bounded reducer queue** — `kanal::bounded_async(16384)` with fail-fast backpressure; queue depth exposed on `/healthz`.

---

## Features

| Feature | Status |
|---|---|
| **Core** | |
| WebSocket API, MessagePack framing | ✅ |
| Lock-free in-memory TableStore (DashMap) | ✅ |
| Serializable isolation + OCC lost-update protection | ✅ |
| Atomicity on panic — full rollback | ✅ |
| Write-ahead log, async group-commit, configurable fsync | ✅ |
| Atomic snapshots (fsync + rename) | ✅ |
| WAL crash recovery | ✅ |
| Hybrid row encoding (MsgPack + zstd for large rows) | ✅ |
| Redis protocol (RESP2/RESP3, ~150 commands) | ✅ |
| PostgreSQL protocol (pgwire v3, full SQL + transactions) | ✅ |
| **Reducers** | |
| Native Rust reducers (`#[reducer]` + `#[table]` macros) | ✅ |
| JavaScript reducers (QuickJS via rquickjs, 64MB heap cap) | ✅ |
| WASM reducers (Wasmtime 21, Cranelift JIT, pooled instances) | ✅ |
| C# reducers (→ WASM via .NET 8 WASI) | ✅ |
| Go reducers (→ WASM via TinyGo) | ✅ |
| Reducer CPU timeouts (JS/WASM) | ✅ |
| Scheduled reducers (`[[scheduler]]` in config) | ✅ |
| **Subscriptions** | |
| Live subscriptions with initial state sync | ✅ |
| WHERE predicates: comparison, IN, AND, OR | ✅ |
| ORDER BY (numeric + lexicographic) | ✅ |
| LIMIT N | ✅ |
| 20Hz tick coalescing (configurable, `sub_tick_ms`) | ✅ |
| Secondary indexes (O(1) lookup, auto-maintained) | ✅ |
| Columnar read API (scan, count, distinct) | ✅ |
| **Auth & Security** | |
| API key auth (`Authorization: Bearer`) | ✅ |
| JWT + Ed25519 identity (`POST /auth/token`) | ✅ |
| Role-based access control (`[permissions]` in config) | ✅ |
| Row-level security (public / owner-field / role-gated) | ✅ |
| Per-reducer caller identity (`ctx.caller_id`, `ctx.caller_role`) | ✅ |
| TLS / WSS (`[tls]` config, auto-generates self-signed cert) | ✅ |
| **Operations** | |
| Admin dashboard (dark-theme UI at `/admin`) | ✅ |
| Prometheus metrics (`GET /metrics`, 11 counters/gauges/histograms) | ✅ |
| Automated backups + rotation + PITR restore | ✅ |
| WAL streaming replication (`NEONDB_ROLE=replica`) | ✅ |
| One-command failover (`neondb promote`) | ✅ |
| Graceful shutdown (worker drain, WAL flush) | ✅ |
| LRU row eviction (`[eviction]` config) | ✅ |
| Schema migrations (`migrations/*.toml` + `neondb migrate`) | ✅ |
| `neondb seed` — bulk-seed rows from JSON | ✅ |
| `neondb update` — self-update from GitHub releases | ✅ |
| Schema API (`GET /schema`) | ✅ |
| **Scaling** | |
| Multi-tenancy — full namespace isolation per tenant | ✅ |
| Per-tenant rate limiting + row quotas | ✅ |
| Horizontal cluster — shard routing, delta fan-out, gossip | ✅ |
| Cluster proxy calls (`/cluster/call`) | ✅ |
| Dynamic peer join (`/cluster/join`) | ✅ |
| **SDKs** | |
| TypeScript client SDK + optimistic updates | ✅ |
| Rust client SDK + optimistic updates | ✅ |
| Unity C# client (zero-dep, MessagePack) | ✅ |
| Godot 4 GDScript client (WebSocketPeer, signal-based) | ✅ |
| Docker + docker-compose (single + 3-node cluster) | ✅ |

---

## Benchmarks

**15K CCU lobby-partitioned game sim** (`neondb-sim game --lobby-size 75`, server + 3 client processes):

| Metric | Value |
|---|---|
| Concurrent users | 15,000 (202 lobbies × 75 players) |
| Combined TPS | **53,000** |
| p99 latency | 333ms |
| Per-lobby p99 spread | best 328ms → worst 336ms (8ms — zero noisy-neighbor) |
| Errors | 0.1% |
| Memory | flat at 670MB |

**Write-path ceiling** (`stress --clients 50 --pipeline 512`):

| Metric | Value |
|---|---|
| Throughput | **351,000 TPS** (8.78M writes in 25s) |
| Errors | 0 |
| p99 | 95ms |

**Fan-out** (500 subscribed players):

| Metric | Value |
|---|---|
| Fan-out frames/s | **567,000 sustained** |
| p50 latency | 11ms |
| worst-lobby p99 | 44ms |

Memory efficiency (hybrid MsgPack/zstd row storage + fixed-slot lock pool):
- Typical game row (position, HP, level): ~15–25 bytes stored (vs ~80 bytes JSON)
- Per-row lock overhead: ~0 bytes (512-slot fixed pool, no per-row allocation)

---

## Multi-Tenancy

```bash
# Create a tenant
curl -X POST http://localhost:3001/admin/api/tenants \
  -H "Authorization: Bearer $NEONDB_API_KEY" \
  -d '{"name":"acme","max_rows":100000,"max_calls_per_sec":500}'
# → { "id": "acme-a1b2c3", "api_key": "ndbt_..." }

# Connect as the tenant — all table access is namespace-isolated
wscat -H "Authorization: Bearer ndbt_..." -c ws://localhost:3000
```

- Full namespace isolation: every table is physically prefixed `tn:<id>:<table>`.
- Clients see logical names on the wire — prefix stripped in subscription frames.
- Per-tenant row quotas enforced at commit time.
- Per-tenant token-bucket rate limiter with continuous refill.

---

## Horizontal Scaling (Cluster)

```bash
# Node 0
NEONDB_SHARD_ID=0 NEONDB_SHARD_COUNT=2 \
NEONDB_PEERS="shard1=http://node1:4001" \
NEONDB_CLUSTER_SECRET=mysecret \
neondb start

# Node 1
NEONDB_SHARD_ID=1 NEONDB_SHARD_COUNT=2 \
NEONDB_PEERS="shard0=http://node0:3001" \
NEONDB_CLUSTER_SECRET=mysecret \
NEONDB_PORT=4000 NEONDB_METRICS_PORT=4001 \
neondb start
```

- **Shard routing**: `shard_for_key(key, shard_count)` — FNV-1a 64-bit hash, deterministic across all nodes.
- **Delta fan-out**: after each commit, deltas are replicated to all healthy peers with 3-attempt exponential back-off.
- **Gossip health**: background task pings peers every 5s; 3 failures → unhealthy (skipped in fan-out).
- **Dynamic join**: `POST /cluster/join` — no restart needed.
- **Check status**: `neondb cluster-status`

---

## Replication & Failover

```bash
# Primary
NEONDB_ROLE=primary neondb start

# Replica (streams WAL from primary, read-only)
NEONDB_ROLE=replica NEONDB_PRIMARY_URL=http://primary:3001 neondb start

# Promote replica to primary (instant, single command)
neondb promote --metrics-url http://replica:4001
```

---

## Admin Dashboard

```
http://localhost:3001/admin
```

Single-file dark-theme dashboard (no build step, embedded in the binary):

- **Overview** — TPS, p99 latency, memory, WAL size, queue depth, uptime — live charts polled every 2s.
- **Tables** — browse/filter all tables; add, edit, delete rows via modal.
- **SQL console** — run ad-hoc queries; Ctrl+Enter to execute; history in localStorage.
- **Reducers** — list all registered reducers; invoke with JSON args.
- **Schema viewer** — column definitions, types, RLS policies.
- **Operations** — trigger backup, view replication status, paste-and-run migrations, server info.

---

## Configuration

```toml
# neondb.toml
port = 3000
metrics_port = 3001
workers = 0            # 0 = num_cpus
wal_dir = ".neondb"
reducer_queue_cap = 16384
reducer_timeout_ms = 5000
sub_tick_ms = 50       # subscription coalescing interval (0 = immediate)

[auth]
api_key = ""           # empty = open in dev
jwt_secret = ""        # auto-generated Ed25519 key

[tls]
cert_path = ""         # empty = auto-generate self-signed
key_path  = ""

[eviction]
policy = "none"        # "lru_row_cap" | "lru_byte_cap"
max_rows_per_table = 0
max_bytes_total = 0

[permissions]
spawn = ["admin", "player"]
delete_player = ["admin"]
```

Key env vars: `NEONDB_PORT`, `NEONDB_API_KEY`, `NEONDB_WAL_DIR`, `NEONDB_REDUCER_TIMEOUT_MS`, `NEONDB_SUB_TICK_MS`, `NEONDB_SHARD_ID`, `NEONDB_SHARD_COUNT`, `NEONDB_PEERS`, `NEONDB_CLUSTER_SECRET`, `NEONDB_BACKUP_DIR`, `NEONDB_ROLE`, `NEONDB_PRIMARY_URL`, `NEONDB_REDIS_PORT`, `NEONDB_PG_PORT`.

---

## Testing

```bash
cargo test --lib       # 541 unit tests
cargo test             # + integration tests (requires debug binary)
cargo bench            # criterion throughput + end-to-end benchmarks
neondb-sim game --players 500 --duration 60
neondb-sim game serve  # server-only mode for external clients
```

---

## CLI Reference

```
neondb init <name> [--template <t>]    Scaffold a new project
neondb templates                        List available templates
neondb modules                          List available add-on modules
neondb add <module>                     Add a module to the current project
neondb start                            Start server (auto-detects game projects)
neondb build                            Compile JS/C#/Go reducers to WASM
neondb call <reducer> <args-json>       Call a reducer
neondb get <table> [key]                Read rows
neondb watch <query>                    Subscribe to live updates
neondb seed <file.json>                 Bulk-seed rows
neondb migrate [--dir migrations/]      Apply pending migrations
neondb status                           Show server metrics
neondb backup                           Trigger a manual backup
neondb backups <dir>                    List backups in a directory
neondb restore <backup> --wal-path W    Restore (supports --until-ts for PITR)
neondb promote                          Promote replica to primary
neondb cluster-status                   Show cluster peer health
neondb update                           Self-update to latest release
neondb update --check                   Check for updates without installing
```

---

---

## Documentation

- [docs/getting-started.md](docs/getting-started.md) — 5-minute tutorial
- [docs/architecture.md](docs/architecture.md) — system design overview
- [docs/protocol.md](docs/protocol.md) — wire protocol and message reference
- [docs/reducers.md](docs/reducers.md) — writing reducers in Rust, JS, WASM, C#, Go
- [docs/sdk-typescript.md](docs/sdk-typescript.md) — TypeScript SDK reference
- [docs/sdk-rust.md](docs/sdk-rust.md) — Rust SDK reference
- [docs/cluster.md](docs/cluster.md) — horizontal scaling and cluster setup
- [docs/deployment.md](docs/deployment.md) — Docker, systemd, production checklist
- [docs/cli-reference.md](docs/cli-reference.md) — every CLI subcommand
- [docs/faq.md](docs/faq.md) — frequently asked questions

---

## License

MIT
