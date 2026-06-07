# NeonDB

A self-hosted, high-throughput real-time game backend written in Rust.
Think SpacetimeDB but open-source, self-hosted, and runs on your own hardware including a $5 VPS.

[![Tests](https://img.shields.io/badge/tests-91%20passing-brightgreen)](#testing)
[![TPS](https://img.shields.io/badge/throughput-~2.9M%20TPS-blue)](#benchmarks)
[![Platforms](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)](#installation)

---

## What NeonDB Does

NeonDB is a WebSocket-first database server for games and real-time apps.

- Clients connect over **WebSocket** and call **reducers** (named functions that read/write data atomically).
- Every write is committed to an in-memory **TableStore** (DashMap-backed, lock-free reads, serializable isolation).
- Writes are durably logged to a **WAL** and periodically snapshotted for fast restart.
- Clients subscribe to table queries and receive **live diffs** the instant matching rows change.
- Three reducer runtimes: **native Rust**, **JavaScript** (Boa), and **WASM** (Wasmtime/Cranelift JIT).

---

## Feature List

| Feature | Status |
|---|---|
| WebSocket reducer API (MessagePack framing) | done |
| In-memory TableStore (DashMap, lock-free reads) | done |
| Serializable isolation (per-row write locks) | done |
| Atomicity on panic (full rollback) | done |
| Write-ahead log (async batched, configurable fsync) | done |
| Atomic snapshots (every N commits, fsync+rename) | done |
| Live subscriptions + initial state sync | done |
| Subscription predicates: WHERE, IN, AND | done |
| Two-frame subscription protocol (O(1) encode) | done |
| Secondary indexes (O(1) hash lookup, auto-maintained) | done |
| Columnar read API (scan, count, distinct) | done |
| Native Rust reducers | done |
| JavaScript reducers (Boa 0.19, pure-Rust) | done |
| WASM reducers (Wasmtime + Cranelift JIT) | done |
| neondb build — compile JS to WASM via javy | done |
| Schema migrations (migrations/*.toml) | done |
| Scheduled reducers ([[scheduler]] in config) | done |
| API key auth (Authorization: Bearer) | done |
| Per-reducer caller identity (ctx.caller_id) | done |
| Admin HTTP server (/metrics, /healthz, /tables) | done |
| Graceful shutdown (drain workers, flush WAL) | done |
| Docker + Dokploy deployment | done |
| TypeScript client SDK (@neondb/client) | done |
| Rust client SDK (neondb-client) | done |
| Standalone benchmark binary (neondb-bench) | done |

---

## Installation

### Requirements

- Rust 1.78 or newer — install from https://rustup.rs
- Windows, Linux, or macOS

### Build from source

```bash
git clone <your-repo-url>
cd neondb
cargo build --release
```

The binary lands at `target/release/neondb` (or `neondb.exe` on Windows).

Install it globally so you can run `neondb` from anywhere:

```bash
cargo install --path .
```

### Docker

```bash
docker compose up -d
```

See DOKPLOY_DEPLOYMENT.md for VPS deployment.

---

## Quick Start

### 1. Start the server

```bash
neondb start
```

Or without installing:

```bash
cargo run --release -- start
```

You will see:

```
INFO  Starting NeonDB Server
INFO  WebSocket listener started on 127.0.0.1:3000
INFO  Admin/metrics endpoint available on http://127.0.0.1:3001
```

### 2. Check it is alive

```bash
neondb status
```

```
Server is UP
{
  "status": "ok",
  "total_rows": 0,
  "active_connections": 0
}
```

### 3. Call a reducer

```bash
neondb call increment '["score", 1]'
```

```
Reducer 'increment' succeeded.
Result: {
  "new_value": 1,
  "timestamp": 1717000000000
}
```

### 4. Read the data back

```bash
neondb get counters score
```

### 5. Watch live updates in a second terminal

```bash
# Terminal A
neondb watch counters

# Terminal B — run a few increments
neondb call increment '["score", 1]'
neondb call increment '["score", 5]'
```

Terminal A prints a live diff line for each write.

---

## CLI Reference

### Server Commands

#### neondb start

Start the server.

```
neondb start [OPTIONS]

  -a, --host <HOST>              Listen address         (default: 127.0.0.1)
  -p, --port <PORT>              WebSocket port         (default: 3000)
  -d, --data-dir <DIR>           Data directory for WAL
      --wal-path <PATH>          Explicit WAL file path
  -f, --fsync-interval-ms <MS>   WAL fsync interval     (0 = per-write)
```

Examples:

```bash
# Development — fast, localhost only
neondb start

# Production — listen everywhere, batch fsync every 100 ms
neondb start --host 0.0.0.0 --fsync-interval-ms 100

# Specific data directory
neondb start --data-dir /var/lib/neondb
```

#### neondb init [PATH]

Scaffold a new project with a starter neondb.toml.

```bash
neondb init my-game
cd my-game
neondb start
```

#### neondb build

Compile JavaScript reducers in modules/ to WASM using javy (10-50x faster via Cranelift JIT).
Compiled `.wasm` files are loaded automatically on the next start in preference to `.js`.

**javy is a standalone binary — do NOT run `cargo install javy` (that installs a library crate, not the CLI).**

Download the release binary for your OS from:
https://github.com/bytecodealliance/javy/releases

```
Windows : extract javy-x86_64-windows.zip  → add the folder to PATH
Linux   : gunzip javy-x86_64-linux.gz      → chmod +x javy → mv /usr/local/bin/javy
macOS   : gunzip javy-x86_64-macos.gz      → chmod +x javy → mv /usr/local/bin/javy
```

Then compile:

```bash
neondb build                              # compiles modules/*.js → modules/*.wasm
neondb build --modules-dir src/reducers   # custom directory
```

### Inspect Commands

These hit the admin HTTP port (default http://127.0.0.1:3001).

#### neondb status

Show server health and metrics.

```bash
neondb status
neondb status --metrics-url http://my-server:3001
```

#### neondb tables

List all tables with row counts.

```bash
neondb tables
```

Output:

```
TABLE                         ROWS
-----                         ----
counters                        12
players                        200
-----                         ----
TOTAL                          212
```

#### neondb get TABLE [KEY]

Read all rows from a table, or a single row by key.

```bash
# All rows
neondb get players

# Single row
neondb get players hero_1
```

### Interactive Commands

These connect to the WebSocket port.

#### neondb call REDUCER [ARGS]

Call any reducer once and print the result.

```bash
# Built-in increment — args are a JSON array [name, delta]
neondb call increment '["score", 5]'

# Custom JS/WASM reducer with object args
neondb call spawn_player '{"name": "Alice", "level": 1}'

# With API key
neondb call increment '["score", 1]' --api-key mysecret

# Custom server URL
neondb call increment '["score", 1]' --url ws://my-server:3000
```

#### neondb watch QUERY

Subscribe and stream live diffs until Ctrl-C.

```bash
# All rows in a table
neondb watch counters

# Filtered — only rows where level > 5
neondb watch "players WHERE level > 5"

# IN operator
neondb watch "players WHERE status IN ('active', 'vip')"

# Compound AND
neondb watch "players WHERE score > 100 AND level > 5"

# With API key and custom server
neondb watch counters --api-key mysecret --url ws://my-server:3000
```

#### neondb bench

Quick throughput test against a running server.

```bash
neondb bench --clients 20 --calls 1000
neondb bench --url ws://my-server:3000 --api-key mysecret
```

---

## Configuration

NeonDB searches for neondb.toml upward from the current directory.
Environment variables override TOML values.

### neondb.toml

```toml
[project]
name = "my-game"
version = "0.1.0"

[server]
host = "127.0.0.1"
port = 3000
metrics_port = 3001

# Security (optional — remove to allow unauthenticated connections)
api_key = "change-me"

# WAL durability
wal_path = "/var/lib/neondb/neondb.wal"
fsync_interval_ms = 100
wal_batch_size = 100000
wal_batch_interval_ms = 100

# Snapshots — bound restart time at scale
snapshot_interval = 1000000
snapshot_dir = "/var/lib/neondb/snapshots"

# Limits
max_connections = 500
reducer_timeout_ms = 5000

# Subscription protocol (true = O(1) encoding for many subs per client)
two_frame_protocol = false

# Multi-node sharding
shard_id = 0
shard_count = 1

# Scheduled reducers
[[scheduler]]
reducer = "cleanup_expired"
interval_ms = 60000

[[scheduler]]
reducer = "leaderboard_refresh"
interval_ms = 300000
args_json = '{"top_n": 100}'
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| NEONDB_HOST | 127.0.0.1 | WebSocket listen address |
| NEONDB_PORT | 3000 | WebSocket listen port |
| NEONDB_METRICS_PORT | 3001 | Admin HTTP port |
| NEONDB_API_KEY | (none) | Bearer token — require on all connections |
| NEONDB_WAL_PATH | OS temp | Write-ahead log path |
| NEONDB_FSYNC_INTERVAL_MS | 0 | WAL fsync interval ms (0 = per write) |
| NEONDB_WAL_BATCH_SIZE | 100000 | Max entries per WAL batch |
| NEONDB_WAL_BATCH_INTERVAL_MS | 100 | WAL batch flush interval ms |
| NEONDB_SNAPSHOT_INTERVAL | 1000000 | Commits between snapshots |
| NEONDB_SNAPSHOT_DIR | OS temp | Directory for snapshot files |
| NEONDB_MAX_CONNECTIONS | 500 | Max simultaneous WebSocket clients |
| NEONDB_REDUCER_TIMEOUT_MS | 5000 | Reducer execution timeout |
| NEONDB_TWO_FRAME_PROTOCOL | 0 | Set 1 for two-frame subscription mode |
| NEONDB_SHARD_ID | 0 | This node's shard index |
| NEONDB_SHARD_COUNT | 1 | Total shard count (multi-node) |
| RUST_LOG | info | Verbosity: trace / debug / info / warn / error |

---

## Writing Reducers

A reducer is a named function that runs inside a single atomic transaction.
It can read, write, and delete rows across any tables. The write is either fully committed or fully rolled back — never partial.

### Native Rust (built in)

The built-in `increment` reducer is always available:

```
call: "increment"
args: ["counter_name", delta_integer]
```

To add your own native reducer, edit `src/reducer/context.rs` and register it in `src/reducer/registry.rs`.

### JavaScript — modules/my_reducer.js

Create a `.js` file in `modules/`. It is loaded automatically on start.

```js
function my_reducer(ctx, args) {
  const player = ctx.get_row("players", args.player_id);
  ctx.set_row("players", args.player_id, {
    ...player,
    hp: Math.max(0, player.hp - args.damage),
  });
  ctx.increment_counter("total_damage", args.damage);
  return { ok: true };
}
```

For better performance, compile to WASM first (10-50x faster via Wasmtime JIT):

```bash
# Download javy from https://github.com/bytecodealliance/javy/releases
# (do NOT use cargo install javy — that is a library crate, not the CLI)
neondb build        # produces modules/my_reducer.wasm
neondb start        # automatically prefers the .wasm version
```

### WASM — modules/my_reducer.wasm

Drop any `.wasm` file into `modules/`. It is executed by Wasmtime's Cranelift JIT — 10-50x faster than the Boa JS interpreter for compute-heavy logic.

---

## Subscriptions

### Query syntax

```
TABLE_NAME
TABLE_NAME WHERE field op value
TABLE_NAME WHERE field IN (v1, v2, ...)
TABLE_NAME WHERE predicate AND predicate
```

Operators: `==  !=  >  <  >=  <=`

Examples:

```
counters
players WHERE level >= 10
players WHERE status IN ('active', 'vip', 'moderator')
players WHERE score > 1000 AND level > 5
```

### Initial state sync

When a client subscribes, NeonDB immediately delivers all currently matching rows
as `"initial_snapshot"` diffs before any future updates arrive.
The client always starts with a complete, consistent view.

---

## Schema Migrations

Place `.toml` files in `migrations/`. They run automatically at startup, in lexicographic order, and are idempotent.

```toml
# migrations/001_add_xp.toml
[[steps]]
operation = "add_field"
table = "players"
field = "xp"
default_value = 0

[[steps]]
operation = "rename_field"
table = "players"
old_field = "old_name"
new_field = "display_name"

[[steps]]
operation = "remove_field"
table = "players"
field = "deprecated_flag"
```

Supported operations: `add_field`, `remove_field`, `rename_field`.

---

## Client SDKs

### TypeScript

```bash
cd neondb-client-ts
npm install
npm run build
```

```typescript
import { NeonDBClient } from "@neondb/client";

const client = new NeonDBClient({
  url: "ws://localhost:3000",
  apiKey: "optional-key",
});

await client.connect();

// Call a reducer
const bytes = await client.call("increment", ["score", 1]);
console.log("new_value:", client.decodeResult(bytes!).new_value);

// Live subscription
const sub = client.subscribe("counters WHERE value > 10", (diff) => {
  console.log(diff.operation, diff.rowKey, diff.rowData);
});

sub.unsubscribe();
client.disconnect();
```

### Rust

```bash
cd neondb-client-rust
cargo build
```

```rust
use neondb_client::{NeonDBClient, ClientOptions};

let client = NeonDBClient::connect(ClientOptions {
    url: "ws://localhost:3000".to_string(),
    ..Default::default()
}).await?;

let sub = client.subscribe("counters").await?;
while let Some(diff) = sub.recv().await {
    println!("{}: {}", diff.operation, diff.row_key);
}
```

---

## Admin HTTP API

Runs on port 3001 by default.

| Endpoint | Description |
|---|---|
| GET /healthz | Health check JSON |
| GET /metrics | Prometheus-style plaintext metrics |
| GET /tables | List all tables with row counts |
| GET /tables/NAME | Dump all rows of a table |

```bash
curl http://localhost:3001/healthz
curl http://localhost:3001/tables
curl http://localhost:3001/tables/players
curl http://localhost:3001/metrics
```

---

## Benchmarks

Measured on a Ryzen 7 / 32GB RAM / NVMe machine.

| Scenario | Throughput |
|---|---|
| Raw engine, single thread | ~297K TPS |
| Parallel engine, 24 threads | ~1.65M TPS |
| In-process (no network) | ~2.9M ops/s |
| WebSocket round-trip, 10 clients | ~40K TPS |

```bash
# Engine benchmarks
cargo bench --bench scenario1_pure_engine
cargo bench --bench scenario2_full_pipeline
cargo bench --bench scenario3_game_genres

# Live WebSocket benchmark
neondb-bench --clients 20 --calls 1000
```

---

## Testing

```bash
# Unit tests (85 tests)
cargo test

# Integration tests (6 tests, spawns real server)
cargo test --test integration

# Include the perf e2e test (slow)
cargo test -- --include-ignored
```

Status: 85 unit + 6 integration = 91 tests, all green, zero warnings.

---

## Deployment

### Docker Compose

```bash
docker compose up -d
```

### Systemd (bare metal)

```bash
sudo cp target/release/neondb /usr/local/bin/

sudo tee /etc/systemd/system/neondb.service << 'EOF'
[Unit]
Description=NeonDB Game Backend
After=network.target

[Service]
ExecStart=/usr/local/bin/neondb start --host 0.0.0.0
Environment=NEONDB_API_KEY=change-me
Environment=NEONDB_WAL_PATH=/var/lib/neondb/neondb.wal
Environment=NEONDB_SNAPSHOT_DIR=/var/lib/neondb/snapshots
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now neondb
```

### Dokploy (recommended for VPS + auto-TLS)

See DOKPLOY_DEPLOYMENT.md.

---

## Project Structure

```
NeonDB/
├── src/
│   ├── main.rs              CLI, server bootstrap, worker loop
│   ├── cli.rs               status / tables / get / call / watch / bench
│   ├── config.rs            Config — TOML + env loading
│   ├── migrations.rs        Schema migration engine
│   ├── subscriptions.rs     SubscriptionManager — reverse index, fan-out
│   ├── table/mod.rs         TableStore — DashMap, indexes, columnar API
│   ├── reducer/
│   │   ├── context.rs       ReducerContext — staged writes, atomic RMW
│   │   ├── registry.rs      Auto-load modules/ directory
│   │   ├── native.rs        Native Rust backend
│   │   ├── v8.rs            Boa JS backend
│   │   └── wasm.rs          Wasmtime backend
│   ├── network/
│   │   ├── message.rs       Wire types
│   │   ├── protocol.rs      MessagePack helpers
│   │   └── websocket.rs     WebSocket listener
│   ├── wal/
│   │   ├── batch_writer.rs  Async batched WAL
│   │   ├── snapshot.rs      Atomic snapshots
│   │   └── reader.rs        WAL replay
│   └── bin/neondb_bench.rs  Standalone benchmark binary
├── benches/                 Criterion benchmarks
├── tests/integration.rs     End-to-end tests
├── modules/                 Drop .js / .wasm reducers here
├── migrations/              Drop migration .toml files here
├── neondb-client-ts/        TypeScript SDK
├── neondb-client-rust/      Rust SDK
├── Dockerfile
├── docker-compose.yml
└── neondb.toml
```

---

## Production Deployment

- [docs/SELF_HOSTING.md](docs/SELF_HOSTING.md) — 100% free hosting options (Oracle Cloud, Fly.io, Cloudflare Tunnel, Coolify/Dokploy)
- [OPERATIONS.md](OPERATIONS.md) — operator runbook: backups, health checks, tuning, disaster recovery
- [DEPLOYMENT.md](DEPLOYMENT.md) — Docker-based deployment (existing)
- [DOKPLOY_DEPLOYMENT.md](DOKPLOY_DEPLOYMENT.md) — Dokploy-specific guide (existing)

---

## License

MIT
