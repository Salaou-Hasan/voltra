# CLI Reference

All commands are accessed via the `neondb` binary. Use `neondb --help` or `neondb <command> --help` for built-in help.

---

## neondb init

Scaffold a new NeonDB project. Without `--template`, opens an interactive selector.

```
neondb init [NAME] [--template TEMPLATE]
```

| Argument | Description |
|---|---|
| `NAME` | Directory name for the new project. Prompted interactively if omitted. |
| `--template` | Template to use. See `neondb templates` for available names. |

Templates:

| Name | Description |
|---|---|
| `rust/basic` | Foundation project: users, sessions, inventory, role-based auth |
| `rust/game-ready` | Full game engine: players, combat, economy, quests, guilds, world |
| `rust/chat` | Chat server: rooms, threads, reactions, presence, moderation |
| `typescript` | TypeScript-first: React hooks, full client SDK, package.json |
| `native/game-ready` | Rust reducers compiled to WASM for near-native throughput |

```bash
neondb init my-game
neondb init my-game --template rust/game-ready
neondb init my-chat --template rust/chat
```

---

## neondb templates

List all available project templates.

```
neondb templates
```

---

## neondb build

Compile JavaScript reducers in the `modules/` directory to WASM using javy. The compiled `.wasm` files are automatically preferred over `.js` on the next server start.

```
neondb build [--modules-dir DIR]
```

| Flag | Default | Description |
|---|---|---|
| `-m`, `--modules-dir` | `modules` | Directory containing `.js` reducer files |

Requires `javy` on PATH. Download from https://github.com/bytecodealliance/javy/releases. Do NOT install via `cargo install javy` — that installs a library crate, not the CLI.

```bash
neondb build
neondb build --modules-dir src/reducers
```

---

## neondb start

Start the NeonDB server. Config is loaded from `neondb.toml` in the current directory (or searched upward). Environment variables override TOML values.

```
neondb start [OPTIONS]
```

| Flag | Env var | Default | Description |
|---|---|---|---|
| `-a`, `--host` | `NEONDB_HOST` | `127.0.0.1` | WebSocket listen address |
| `-p`, `--port` | `NEONDB_PORT` | `3000` | WebSocket listen port |
| `-d`, `--data-dir` | | | Data directory (sets WAL path to `DIR/neondb.wal`) |
| `--wal-path` | `NEONDB_WAL_PATH` | OS temp | Explicit WAL file path |
| `-f`, `--fsync-interval-ms` | `NEONDB_FSYNC_INTERVAL_MS` | `0` | WAL fsync interval; 0 = per-write |

Additional environment variables:

| Variable | Default | Description |
|---|---|---|
| `NEONDB_METRICS_PORT` | `3001` | Admin HTTP port |
| `NEONDB_API_KEY` | (none) | Required Bearer token; all connections must present this |
| `NEONDB_WAL_BATCH_SIZE` | `100000` | Max entries buffered before WAL flush |
| `NEONDB_WAL_BATCH_INTERVAL_MS` | `100` | Max ms between WAL flushes |
| `NEONDB_SNAPSHOT_INTERVAL` | `1000000` | Commits between snapshots (0 = disabled) |
| `NEONDB_SNAPSHOT_DIR` | OS temp | Snapshot directory |
| `NEONDB_MAX_CONNECTIONS` | `500` | Max simultaneous WebSocket clients |
| `NEONDB_REDUCER_TIMEOUT_MS` | `5000` | Max reducer execution time |
| `NEONDB_TWO_FRAME_PROTOCOL` | `0` | Set `1` for two-frame subscription encoding |
| `NEONDB_CLUSTER_SECRET` | (none) | Shared secret for inter-node Raft requests |
| `RUST_LOG` | `info` | Log verbosity: `trace`, `debug`, `info`, `warn`, `error` |

```bash
neondb start
neondb start --host 0.0.0.0
neondb start --port 3000 --data-dir /var/lib/neondb
NEONDB_API_KEY=changeme neondb start
```

---

## neondb status

Show server health and metrics. Hits the admin HTTP port.

```
neondb status [--metrics-url URL]
```

| Flag | Default | Description |
|---|---|---|
| `--metrics-url` | `http://127.0.0.1:3001` | Admin server URL |

```bash
neondb status
neondb status --metrics-url http://my-server:3001
```

---

## neondb tables

List all tables and their row counts.

```
neondb tables [--metrics-url URL]
```

```bash
neondb tables
```

Output:

```
TABLE       ROWS
-----       ----
counters      12
players      200
-----       ----
TOTAL        212
```

---

## neondb get

Read rows from a table via the admin HTTP endpoint.

```
neondb get TABLE [KEY] [--metrics-url URL]
```

| Argument | Description |
|---|---|
| `TABLE` | Table name |
| `KEY` | Optional row key. If omitted, returns all rows. |

```bash
# All rows
neondb get players

# Single row
neondb get players alice

# Custom server
neondb get players --metrics-url http://my-server:3001
```

---

## neondb call

Connect to the WebSocket port and call a reducer once, printing the result.

```
neondb call REDUCER [ARGS] [--url URL] [--api-key KEY]
```

| Argument/Flag | Description |
|---|---|
| `REDUCER` | Reducer name |
| `ARGS` | JSON-encoded args (array or object). Omit for no-arg reducers. |
| `--url` | WebSocket URL (default: `ws://127.0.0.1:3000`) |
| `--api-key` | Bearer token |

On PowerShell, bare words inside `[...]` are automatically quoted (e.g. `[general, alice]` is parsed as `["general", "alice"]`).

```bash
neondb call increment '["score", 1]'
neondb call spawn '["player1", 0, 0, "warrior"]'
neondb call deal_damage '{"attacker_id": "player1", "defender_id": "enemy1"}'
neondb call increment '["score", 5]' --api-key changeme
neondb call increment '["score", 1]' --url ws://my-server:3000
```

---

## neondb watch

Connect to the WebSocket port, subscribe to a query, and stream live diffs to stdout until Ctrl-C.

```
neondb watch QUERY [--url URL] [--api-key KEY]
```

The initial snapshot (all matching rows at subscribe time) is printed first, then live diffs as rows change.

```bash
neondb watch counters
neondb watch "players WHERE level >= 5"
neondb watch "players WHERE status IN ('active', 'vip')"
neondb watch "players WHERE score > 100 AND level > 5"
neondb watch "players ORDER BY score DESC LIMIT 10"
neondb watch counters --api-key changeme --url ws://my-server:3000
```

---

## neondb seed

Bulk-insert rows from a JSON file into a running server. Uses the admin HTTP port.

```
neondb seed FILE [--metrics-url URL] [--dry-run]
```

| Argument/Flag | Description |
|---|---|
| `FILE` | Path to seed JSON file |
| `--metrics-url` | Admin server URL (default: `http://127.0.0.1:3001`) |
| `--dry-run` | Parse and preview only; do not write |

Seed file format (either style is accepted):

```json
{
  "players": {
    "alice": { "hp": 100, "level": 5 },
    "bob":   { "hp": 80,  "level": 3 }
  },
  "counters": [
    { "key": "score", "value": 0 }
  ]
}
```

Seeded rows bypass the WAL and reducer pipeline. They do not fan-out to live subscribers. For dev/test use only.

```bash
neondb seed seed.json
neondb seed seed.json --dry-run
neondb seed seed.json --metrics-url http://my-server:3001
```

---

## neondb bench

Run a WebSocket throughput benchmark against a running server.

```
neondb bench [--url URL] [--clients N] [--calls N] [--warmup N] [--api-key KEY]
```

| Flag | Default | Description |
|---|---|---|
| `--url` | `ws://127.0.0.1:3000` | WebSocket URL |
| `-c`, `--clients` | `10` | Concurrent client connections |
| `-n`, `--calls` | `500` | Calls per client |
| `--warmup` | `50` | Warmup calls per client (not counted) |
| `--api-key` | (none) | Bearer token |

```bash
neondb bench
neondb bench --clients 50 --calls 1000
neondb bench --url ws://my-server:3000 --api-key changeme
```

---

## neondb cluster-status

Show the status of cluster peers. Hits the admin HTTP port.

```
neondb cluster-status [--metrics-url URL]
```

```bash
neondb cluster-status
```

---

## neondb generate-npc

AI-generate an NPC template and cache it in the running server (requires a configured AI endpoint).

```
neondb generate-npc NPC_TYPE [--context TEXT] [--url URL] [--api-key KEY]
```

```bash
neondb generate-npc goblin
neondb generate-npc dragon --context "volcanic dungeon final boss"
```
