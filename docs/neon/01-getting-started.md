# Getting Started with Neon

This guide takes you from zero to a running multiplayer game server in under five minutes.

---

## Step 1 — Install NeonDB

**Option A: Download the binary (recommended)**

Go to [github.com/Salaou-Hasan/neondb-releases](https://github.com/Salaou-Hasan/neondb-releases) and download the latest `neondb.exe` for Windows (or the Linux/macOS binary for your platform). Place it somewhere on your `PATH`.

Verify it works:

```
neondb --version
```

Expected output:
```
neondb 1.0.14
```

**Option B: Build from source**

```
git clone https://github.com/Salaou-Hasan/NeonDB
cd NeonDB
cargo build --release
```

The binary will be at `target/release/neondb.exe`.

---

## Step 2 — Create a New Project

```
neondb init my-game --template neon/basic
```

Expected output:
```
Created project: my-game/
  reducers.neon       <- your game logic (edit this)
  neondb.toml         <- server configuration
  schema.toml         <- optional field validation
  Cargo.toml          <- Rust package (set your game name here)
  src/
    main.rs           <- server bootstrap (rarely edit)
    reducers.rs       <- AUTO-GENERATED (never edit)
  clients/
    NeonDBClient.cs   <- Unity client
    neondb_client.gd  <- Godot client
```

---

## Step 3 — Look at reducers.neon

```
cd my-game
```

Open `reducers.neon`. The basic template gives you a starting point:

```neon
table players {
    hp:    int   = 100,
    alive: bool  = true,
    x:     float = 0.0,
    y:     float = 0.0,
    name:  str   = "",
}

reducer spawn(player_id: str, name: str, x: float, y: float) {
    players[player_id] = { hp: 100, alive: true, x: x, y: y, name: name }
    return { ok: true }
}

reducer move_player(player_id: str, x: float, y: float) {
    let p = players[player_id] else { error("player not found") }
    players[player_id].x = x
    players[player_id].y = y
    return { ok: true }
}
```

This is all your game logic. Tables declare the shape of your data. Reducers are functions clients call.

---

## Step 4 — Build

```
neondb build
```

Expected output:
```
[neondb] Compiling reducers.neon...
[neondb] Generated src/reducers.rs (312 lines)
[neondb] Running cargo build --release...
   Compiling my-game v0.1.0
    Finished release [optimized] target(s) in 4.2s
[neondb] Build complete. Binary: target/release/my-game.exe
```

`neondb build` does two things:
1. Translates `reducers.neon` into `src/reducers.rs` (native Rust code)
2. Runs `cargo build --release` to compile everything to a native binary

Every time you change `reducers.neon`, run `neondb build` again.

---

## Step 5 — Start the Server

```
neondb start
```

Expected output:
```
[neondb] Loading config from neondb.toml
[neondb] WAL directory: ./data/wal
[neondb] Registered 2 native reducers: spawn, move_player
[neondb] WebSocket listening on ws://127.0.0.1:3000
[neondb] Metrics server on http://127.0.0.1:3001
[neondb] Ready.
```

Your game server is running. It accepts WebSocket connections on port 3000. Leave this terminal open.

---

## Step 6 — Call a Reducer

Open a second terminal (keep the server running in the first).

```
neondb call spawn '["alice", "Alice", 0.0, 0.0]'
```

Expected output:
```
{"ok":true}
```

The player `alice` now exists in the database. Call `move_player`:

```
neondb call move_player '["alice", 3.5, 7.2]'
```

Expected output:
```
{"ok":true}
```

---

## Step 7 — Watch Live Updates

In a third terminal, subscribe to player changes:

```
neondb watch "players WHERE name = 'Alice'"
```

Expected output (initial snapshot):
```
[initial_snapshot] players/alice: {"hp":100,"alive":true,"x":3.5,"y":7.2,"name":"Alice"}
```

Now go back to the second terminal and move Alice again:

```
neondb call move_player '["alice", 10.0, 20.0]'
```

The watch terminal will immediately print:
```
[update] players/alice: {"hp":100,"alive":true,"x":10.0,"y":20.0,"name":"Alice"}
```

This is the real-time subscription system. Game clients use this to receive live state.

---

## What's Next?

- Add more reducers to `reducers.neon` — combat, chat, inventory, guilds
- Run `neondb build` after every change
- Connect your Unity or Godot client using the SDK in `clients/`
- See [13 — Complete Examples](13-complete-examples.md) for full game templates

---

## Common Errors

**"neondb: command not found"**
The binary is not on your PATH. Add the folder containing `neondb.exe` to your system PATH environment variable.

**"cargo not found" during build**
Install Rust from [rustup.rs](https://rustup.rs). NeonDB needs Rust to compile your reducers.

**"Address already in use"**
Port 3000 or 3001 is taken by another process. Stop the other process, or change ports in `neondb.toml`:
```toml
[server]
port = 3100
metrics_port = 3101
```

**"player not found"**
You called a reducer with a player ID that doesn't exist yet. Call `spawn` first.
