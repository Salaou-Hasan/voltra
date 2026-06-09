# NeonDB

Self-hosted, real-time, in-memory game backend in Rust.

NeonDB is a single-binary WebSocket server for games and real-time applications. Clients call **reducers** (named, atomic functions) over WebSocket, data lands in a lock-free in-memory table store, every write is durably logged to a WAL, and subscribers receive live row diffs with sub-millisecond latency. Three reducer runtimes: native Rust, JavaScript (Boa), and WASM (Wasmtime/Cranelift JIT) let you write game logic in whichever language fits the problem.

[![Tests](https://img.shields.io/badge/tests-426%20passing-brightgreen)](#testing)
[![TPS](https://img.shields.io/badge/throughput-~2.9M%20TPS-blue)](#benchmarks)
[![Platforms](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)](#installation)

---

## Quick Start

```bash
# 1. Install (requires Rust 1.78+)
cargo install --path .

# 2. Scaffold a project
neondb init my-game --template rust/game-ready

# 3. Compile JS reducers to WASM (optional, 10-50x faster)
cd my-game
neondb build

# 4. Start the server
neondb start

# 5. Call a reducer and watch results
neondb call increment '["score", 1]'
neondb watch counters
```

---

## Features

| Feature | Status |
|---|---|
| WebSocket API, MessagePack framing | done |
| In-memory TableStore (DashMap, lock-free reads) | done |
| Serializable isolation (per-row write locks) | done |
| Atomicity on panic -- full rollback | done |
| Write-ahead log, async batched, configurable fsync | done |
| Atomic snapshots (fsync + rename) | done |
| Live subscriptions with initial state sync | done |
| Subscription predicates: WHERE, IN, AND, OR | done |
| ORDER BY and LIMIT on subscriptions | done |
| Secondary indexes (O(1) lookup, auto-maintained) | done |
| Columnar read API (scan, count, distinct) | done |
| Native Rust reducers | done |
| JavaScript reducers (Boa 0.19, pure-Rust, no V8) | done |
| WASM reducers (Wasmtime 21, Cranelift JIT) | done |
| `neondb build` -- compile JS to WASM via javy | done |
| Schema migrations (`migrations/*.toml`) | done |
| Scheduled reducers (`[[scheduler]]` in config) | done |
| API key auth (`Authorization: Bearer`) | done |
| Role-based access control (`[permissions]` in config) | done |
| Per-reducer caller identity (`ctx.caller_id`) | done |
| Admin HTTP server (`/health`, `/metrics`, `/tables`) | done |
| `neondb seed` -- bulk-seed rows from JSON | done |
| TypeScript client SDK | done |
| Rust client SDK | done |
| Optimistic updates (TS + Rust SDKs) | done |
| Raft consensus (openraft 0.9) | done |
| HLC / last-write-wins conflict resolution | done |
| Cluster leader forwarding | done |
| Docker + docker-compose | done |
| TLS termination | (partial) proxy-only (nginx/Caddy in front) |
| Transparent shard routing | (partial) client-routed, not server-transparent |
| Graceful shutdown drain | (partial) workers drain, WAL flushes |
| JS heap memory limit | (partial) uncapped (WASM backend has hard limit) |

---

## vs SpacetimeDB

| | NeonDB | SpacetimeDB |
|---|---|---|
| License | MIT | BSL (source-available) |
| Hosting | Fully self-hosted | Cloud + self-hosted |
| Reducer runtimes | Rust native, JS (Boa), WASM | Rust, C#, TypeScript |
| Consensus | Raft (openraft) | Proprietary |
| JS engine | Pure-Rust Boa (no C++ V8) | V8 (C++) |
| Windows support | Yes (no native deps) | Partial |
| Production-ready | Core solid; TLS/memlimits in progress | Yes (cloud) |

---

## Documentation

- [docs/getting-started.md](docs/getting-started.md) -- 5-minute tutorial
- [docs/architecture.md](docs/architecture.md) -- system design overview
- [docs/protocol.md](docs/protocol.md) -- wire protocol and message reference
- [docs/reducers.md](docs/reducers.md) -- writing reducers in Rust, JS, and WASM
- [docs/sdk-typescript.md](docs/sdk-typescript.md) -- TypeScript SDK reference
- [docs/sdk-rust.md](docs/sdk-rust.md) -- Rust SDK reference
- [docs/cluster.md](docs/cluster.md) -- clustering and Raft consensus
- [docs/deployment.md](docs/deployment.md) -- Docker, systemd, production checklist
- [docs/cli-reference.md](docs/cli-reference.md) -- every CLI subcommand
- [docs/faq.md](docs/faq.md) -- frequently asked questions

---

## License

MIT

