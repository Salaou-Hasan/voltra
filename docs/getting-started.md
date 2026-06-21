# Getting Started

This guide walks you from zero to a running Voltra server with live subscriptions in about 5 minutes.

---

## Prerequisites

- Rust 1.78 or newer — install from https://rustup.rs
- Windows, Linux, or macOS

---

## Step 1: Install the CLI

```bash
git clone <your-repo-url>
cd Voltra
cargo install --path .
```

Expected output (last few lines):

```
   Compiling voltra v0.1.0
    Finished release [optimized] target(s)
     Installing ~/.cargo/bin/voltra
      Installed package `voltra`
```

If you want to skip installing and just run from source, replace every `voltra` command below with `cargo run --release --`.

---

## Step 2: Scaffold a project

```bash
voltra init my-game --template rust/game-ready
cd my-game
```

This creates:

```
my-game/
  voltra.toml       server config
  modules/          JS reducer scripts
  migrations/       schema migration files
  seed.json         sample data
```

To see all available templates:

```bash
voltra templates
```

---

## Step 3: Start the server

```bash
voltra start
```

Expected output:

```
INFO  Starting Voltra Server
INFO  Loading WAL from ./wal ...
INFO  WebSocket listener started on 127.0.0.1:3000
INFO  Admin/metrics endpoint available on http://127.0.0.1:3001
INFO  Voltra ready
```

The server is now accepting WebSocket connections on port 3000 and HTTP on port 3001.

### Common startup errors

**Error: Address already in use (port 3000)**

Another process owns port 3000. Either stop that process, or start Voltra on a different port:

```bash
voltra start --port 3001
```

**Error: Could not find voltra.toml**

Run `voltra start` from inside the project directory, or pass `--wal-path` explicitly.

---

## Step 4: Call the built-in increment reducer

Open a second terminal:

```bash
voltra call increment '["score", 1]'
```

Expected output:

```
Reducer 'increment' succeeded.
```

Call it a few more times:

```bash
voltra call increment '["score", 5]'
voltra call increment '["score", 10]'
```

Read the counter back:

```bash
voltra get counters score
```

Expected output:

```json
{
  "value": 16
}
```

---

## Step 5: Subscribe to live updates

In a third terminal, subscribe to the counters table:

```bash
voltra watch counters
```

Expected output (initial snapshot):

```
[initial_snapshot] counters / score
  {"value": 16}
Watching counters ... (Ctrl-C to stop)
```

Now go back to the second terminal and call increment again:

```bash
voltra call increment '["score", 1]'
```

The watch terminal immediately prints:

```
[update] counters / score
  {"value": 17}
```

Press Ctrl-C to stop watching.

---

## Step 6: Try a filtered subscription

```bash
voltra watch "counters WHERE value > 10"
```

Only rows where `value > 10` are delivered. Rows that do not match the predicate are silently skipped.

Full subscription query syntax:

```
TABLE [WHERE predicate] [ORDER BY field [ASC|DESC]] [LIMIT N]
```

Examples:

```
counters
counters WHERE value > 100
players WHERE level >= 5 AND zone = "town"
players WHERE status IN ("active", "vip")
players ORDER BY score DESC LIMIT 10
```

---

## What's next

- [docs/reducers.md](reducers.md) — write your own reducers in JS or WASM
- [docs/protocol.md](protocol.md) — connect from your own client
- [docs/sdk-typescript.md](sdk-typescript.md) — TypeScript SDK
- [docs/sdk-rust.md](sdk-rust.md) — Rust SDK
- [docs/cli-reference.md](cli-reference.md) — all CLI commands
