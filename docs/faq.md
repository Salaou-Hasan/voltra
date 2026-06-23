# Frequently Asked Questions

---

## Is Voltra production-ready?

The core engine is solid. The following components are stable and well-tested:

- In-memory TableStore with serializable isolation
- Write-ahead log with configurable durability (fsync per-write or batched)
- Atomic snapshots and WAL replay on startup
- WebSocket reducer dispatch with per-row write locking
- Subscriptions with initial state sync
- All three reducer runtimes (native, JS, WASM)
- Schema migrations
- TypeScript and Rust client SDKs with optimistic updates
- Raft consensus (426 tests passing including 6 Raft integration tests)

The following items are partial or in progress:

- **TLS**: supported natively via `[tls]` config section (auto-generates self-signed cert; bring your own for production). For advanced termination, a reverse proxy (Caddy or nginx) still works. See [docs/deployment.md](deployment.md).
- **JS heap memory limit**: QuickJS enforces a 64 MB memory cap per runtime. The CPU timeout is also enforced. For untrusted code, the WASM backend provides the strongest isolation.
- **Graceful shutdown**: workers drain and the WAL flushes, but there is no formal quiesce-and-drain for in-flight WebSocket connections.
- **Transparent shard routing**: multiple Voltra clusters can be used as shards, but routing is client-side. There is no built-in proxy layer.

For a single-node game server or backend with moderate traffic, Voltra is ready to use. For a large-scale production deployment, review the items above and evaluate whether they affect your use case.

---

## Can I use Voltra without knowing Rust?

Yes. The JS runtime (QuickJS) lets you write reducers in plain JavaScript with no Rust knowledge required:

1. Run `voltra init my-game --template rust/game-ready`.
2. Edit the generated `.js` files in `modules/`.
3. Run `voltra start`.

You can also compile JS reducers to WASM via javy for better performance:

```bash
voltra build   # compiles modules/*.js → modules/*.wasm
```

The TypeScript SDK lets you connect from a Node.js or browser client without any Rust.

If you want to add native Rust reducers for maximum performance, see [docs/reducers.md](reducers.md). That does require Rust knowledge, but it is optional.

---

## What throughput can I expect?

The numbers below were measured on a Ryzen 7 / 32 GB RAM / NVMe machine. Your results will vary based on hardware, network, and reducer complexity.

| Scenario | Throughput |
|---|---|
| In-process engine, no network | ~2.9 M ops/s |
| Parallel engine, 24 threads | ~1.65 M TPS |
| Single-thread engine | ~297 K TPS |
| WebSocket round-trip, 10 clients | ~40 K TPS |

The WebSocket number is bounded by network RTT and per-connection serialization, not by the TableStore. Increasing client count improves aggregate throughput up to the point where server cores are saturated.

To run your own benchmarks:

```bash
# Criterion micro-benchmarks
cargo bench

# Live WebSocket benchmark
voltra bench --clients 50 --calls 1000
```

---

## How do I back up Voltra?

Voltra state is stored in two locations:

1. **WAL file** (`VOLTRA_WAL_PATH`): append-only log of every write since the last snapshot.
2. **Snapshot directory** (`VOLTRA_SNAPSHOT_DIR`): periodic full dumps of the in-memory state.

To back up, copy both directories. The server does not need to be stopped, but it is safer to copy during low-traffic periods.

```bash
rsync -a /var/lib/voltra/ backup:/backups/voltra/$(date +%Y%m%d)/
```

To restore, copy the files back and start the server. It will load the latest snapshot and replay the WAL entries that followed it.

You can also trigger a manual snapshot via the HTTP API (endpoint under development; currently snapshots are triggered automatically every N commits via `VOLTRA_SNAPSHOT_INTERVAL`).

---

## Can I do schema migrations?

Yes. Place `.toml` files in the `migrations/` directory. They run automatically at startup in lexicographic order and are idempotent.

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

Supported operations: `add_field`, `remove_field`, `rename_field`. See `migrations/README.md` for the full format.

---

## Is clustering supported?

Yes. Voltra uses openraft 0.9 for Raft consensus. A 3-node cluster tolerates one node failure; a 5-node cluster tolerates two. See [docs/cluster.md](cluster.md) for bootstrapping instructions.

Multi-cluster sharding is supported but client-routed: the application must implement routing logic using the canonical `fnv1a_64(key) % shard_count` function. Transparent server-side shard routing is not yet implemented.

---

## Why does voltra build fail with a javy error?

The `voltra build` command requires the `javy` CLI, which is a standalone binary — it is not available via `cargo install javy`. Download the correct release binary for your OS from:

https://github.com/bytecodealliance/javy/releases

The current expected subcommand is `javy build` (not `javy compile`). Make sure you have javy 8.x or later on your PATH.

---

## What happens if I run voltra call from PowerShell?

The CLI includes a PowerShell compatibility fix: bare words inside `[...]` are automatically quoted. For example:

```powershell
voltra call my_reducer '[general, alice]'
```

is treated as if you typed `["general", "alice"]`. If your args contain objects or nested arrays, use standard JSON quoting.
