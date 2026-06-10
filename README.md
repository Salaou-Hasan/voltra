# NeonDB

Self-hosted, real-time, in-memory game backend in Rust.

NeonDB is a single-binary WebSocket server for games and real-time applications. Clients call **reducers** (named, atomic functions) over WebSocket, data lands in a lock-free in-memory table store, every write is durably logged to a WAL, and subscribers receive live row diffs instantly. Three reducer runtimes — native Rust, JavaScript (QuickJS), and WASM (Wasmtime/Cranelift JIT) — let you write game logic in whichever language fits the problem.

[![Tests](https://img.shields.io/badge/tests-466%20passing-brightgreen)](#testing)
[![TPS](https://img.shields.io/badge/throughput-~42K%20TPS%20(real--world)-blue)](#benchmarks)
[![Platforms](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)](#installation)

---

## Quick Start

```bash
# 1. Install (requires Rust 1.78+)
cargo install --path .

# 2. Scaffold a project
neondb init my-game --template rust/game-ready

# 3. Start the server
neondb start

# 4. Call a reducer and watch results
neondb call spawn '["player1", 0, 0, "warrior"]'
neondb watch "players WHERE alive = true"
```

---

## Features

| Feature | Status |
|---|---|
| **Core** | |
| WebSocket API, MessagePack framing | ✅ |
| Lock-free in-memory TableStore (DashMap) | ✅ |
| Serializable isolation — per-row write locks | ✅ |
| Atomicity on panic — full rollback | ✅ |
| Write-ahead log, async group-commit, configurable fsync | ✅ |
| Atomic snapshots (fsync + rename) | ✅ |
| WAL crash recovery | ✅ |
| Hybrid row encoding (MsgPack + zstd for large rows) | ✅ |
| **Reducers** | |
| Native Rust reducers | ✅ |
| JavaScript reducers (QuickJS via rquickjs, 64MB heap cap) | ✅ |
| WASM reducers (Wasmtime 21, Cranelift JIT, WASM pooling) | ✅ |
| C# reducers (→ WASM via .NET 8 WASI) | ✅ |
| Go reducers (→ WASM via TinyGo) | ✅ |
| `#[reducer]` + `#[table]` proc macros | ✅ |
| Reducer CPU timeouts (JS/WASM) | ✅ |
| Scheduled reducers (`[[scheduler]]` in config) | ✅ |
| **Subscriptions** | |
| Live subscriptions with initial state sync | ✅ |
| WHERE predicates: comparison, IN, AND, OR | ✅ |
| ORDER BY (numeric + lexicographic) | ✅ |
| LIMIT N | ✅ |
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
| Schema migrations (`migrations/*.toml`) | ✅ |
| `neondb migrate` CLI | ✅ |
| `neondb seed` — bulk-seed rows from JSON | ✅ |
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
| Docker + docker-compose (single + 3-node) | ✅ |

---

## Architecture

```
Client ──WebSocket──► Listener
                         │  MessagePack decode
                         ▼
                    PendingCall queue (kanal, bounded)
                         │
                    N parallel workers (Tokio blocking threads)
                         │
                    ReducerContext (staged writes)
                         │  commit()
                         ▼
                    TableStore (DashMap, hybrid MsgPack/zstd rows)
                         │  apply_delta_batch()
                         ├──► WAL (group-commit, fsync)
                         ├──► Subscription fan-out (Arc<Bytes>, zero-copy)
                         └──► Cluster fan-out (peer delta replication)
```

### Key design choices

- **Zero global locks on reads** — `DashMap` gives lock-free concurrent reads for subscriptions.
- **Serializable isolation** — `apply_delta_batch` acquires per-key slot locks in sorted order before writing; no lost updates.
- **Hybrid row encoding** — small rows (< 256 bytes MsgPack) stored raw for zero overhead; large rows compressed with zstd level 1. Memory stays essentially flat under sustained load.
- **Fixed-slot mutex pool** — 512-slot array replaces per-row `DashMap<String, Mutex>`, eliminating ~128 bytes/row of lock overhead.
- **Subscription delivery** — `Arc<Bytes>` fan-out: one encode per commit, zero re-encodes per subscriber.
- **Group-commit WAL** — batches drain on every write syscall; acknowledged only after data reaches the OS. Crash-safe with microsecond durability windows.
- **Bounded reducer queue** — `kanal::bounded_async(16384)` with fail-fast backpressure on overflow.

---

## Reducer Runtimes

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

### JavaScript — QuickJS (rquickjs)

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

```powershell
neondb init my-game --template csharp-reducers   # .NET 8 WASI
neondb init my-game --template go-reducers        # TinyGo
neondb build                                       # compiles to .wasm
neondb start
```

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

```powershell
# Node 0
$env:NEONDB_SHARD_ID="0"; $env:NEONDB_SHARD_COUNT="2"
$env:NEONDB_PEERS="shard1=http://127.0.0.1:4001"
$env:NEONDB_CLUSTER_SECRET="mysecret"
cargo run --release -- start

# Node 1
$env:NEONDB_SHARD_ID="1"; $env:NEONDB_SHARD_COUNT="2"
$env:NEONDB_PEERS="shard0=http://127.0.0.1:3001"
$env:NEONDB_CLUSTER_SECRET="mysecret"
$env:NEONDB_METRICS_PORT="4001"; $env:NEONDB_PORT="4000"
cargo run --release -- start
```

- **Shard routing**: `shard_for_key(key, shard_count)` — FNV-1a 64-bit hash, deterministic across all nodes.
- **Delta fan-out**: after each commit, deltas are replicated to all healthy peers.
- **Gossip health**: background task pings peers every 5s; 3 failures → unhealthy (skipped in fan-out).
- **Bounded retry queue**: up to 1024 pending payloads per peer, drained every 5s.
- **Dynamic join**: `POST /cluster/join` — no restart needed.

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

All settings are available as environment variables (take precedence over `neondb.toml`):

```toml
# neondb.toml
port = 3000
metrics_port = 3001
workers = 0            # 0 = num_cpus
wal_dir = ".neondb"
reducer_queue_cap = 16384
reducer_timeout_ms = 5000

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

Key env vars: `NEONDB_PORT`, `NEONDB_API_KEY`, `NEONDB_WAL_DIR`, `NEONDB_REDUCER_TIMEOUT_MS`, `NEONDB_SHARD_ID`, `NEONDB_SHARD_COUNT`, `NEONDB_PEERS`, `NEONDB_CLUSTER_SECRET`, `NEONDB_BACKUP_DIR`, `NEONDB_ROLE`, `NEONDB_PRIMARY_URL`.

---

## Benchmarks

Real-world simulation (`neondb-sim mixed`, 500 game players + 500 chat users, JS reducers, 60s):

| Metric | Value |
|---|---|
| Avg TPS | ~42,000 |
| p50 latency | ~11ms |
| p99 latency | ~22ms |
| Memory growth | essentially flat |
| Errors | 0 |

Memory efficiency (hybrid MsgPack/zstd row storage + fixed-slot lock pool):
- Typical game row (position, HP, level): ~15-25 bytes stored (vs ~80 bytes JSON before)
- Per-row lock overhead: ~0 bytes (512-slot fixed pool, no per-row allocation)
- Result: memory stays near baseline even at millions of rows

---

## Testing

```bash
cargo test --lib       # 466 unit tests
cargo test             # + integration tests (requires debug binary)
cargo bench            # criterion throughput + end-to-end benchmarks
target/release/neondb-sim mixed --players 500 --users 500 --duration 60
```

---

## CLI Reference

```
neondb init <name> [--template <template>]   Scaffold a new project
neondb templates                              List available templates
neondb start                                  Start the server
neondb build                                  Compile JS/C#/Go reducers to WASM
neondb call <reducer> <args-json>            Call a reducer
neondb get <table> [key]                     Read rows
neondb watch <query>                         Subscribe to live updates
neondb seed <file.json>                      Bulk-seed rows
neondb migrate [--dir migrations/]          Apply pending migrations
neondb backup                                Trigger a manual backup
neondb backups <dir>                         List backups in a directory
neondb restore <backup> --wal-path W ...    Restore (supports --until-ts PITR)
neondb promote                               Promote replica to primary
neondb cluster-status                        Show cluster peer health
```

---

## Templates

| Template | Description |
|---|---|
| `rust/basic` | Minimal native Rust reducers |
| `rust/game-ready` | MMORPG scaffold — spawn, attack, move, inventory |
| `rust/chat` | Discord-scale chat rooms |
| `typescript` | TypeScript client + JS reducers |
| `csharp-reducers` | .NET 8 WASI reducers compiled to WASM |
| `go-reducers` | TinyGo reducers compiled to WASM |

---

## vs SpacetimeDB

| | NeonDB | SpacetimeDB |
|---|---|---|
| License | MIT | BSL (source-available) |
| Hosting | Fully self-hosted | Cloud + self-hosted |
| Reducer runtimes | Rust native, QuickJS, WASM, C# (WASI), Go (TinyGo) | Rust, C#, TypeScript |
| JS engine | QuickJS (rquickjs, 64MB cap, CPU timeouts) | V8 (C++) |
| Consensus / replication | WAL streaming replication + promote; optional cluster fan-out | Proprietary |
| Multi-tenancy | Built-in (namespace isolation, quotas, rate limits) | Cloud plans only |
| Admin UI | Built-in dark-theme dashboard at `/admin` | Cloud UI only |
| Observability | Prometheus `/metrics`, 11 metrics | Custom |
| Windows support | Yes (no native deps) | Partial |
| Memory efficiency | Hybrid MsgPack/zstd; ~15-25 bytes/row | Higher |

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
