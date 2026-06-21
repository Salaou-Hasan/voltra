# Voltra — Native Rust Reducers (WASM)

Write your game logic in **Rust**, compile to **WebAssembly**, and drop the
`.wasm` files into `modules/`. Voltra loads them at startup and executes them
via [Wasmtime](https://wasmtime.dev/) with Cranelift JIT — performance within
5–10% of native Rust for CPU-bound logic, with zero V8/Node.js dependency.

---

## Why Native Rust WASM?

| | JS reducers | Native Rust WASM |
|---|---|---|
| **Language** | JavaScript (Boa engine) | Rust |
| **Performance** | Interpreted | Cranelift JIT (~native) |
| **Type safety** | Runtime | Compile time |
| **Startup cost** | None | One-time JIT compilation |
| **Ecosystem** | npm (not available) | Cargo (full `no_std` subset) |
| **Error messages** | Stringly typed | Rust `Result<T, E>` |

For game backends with tight latency budgets (damage calculation, physics
ticks, matchmaking) the JIT cost pays off immediately. The Boa JS engine is
better for rapid prototyping and server-side scripting.

---

## Project Structure

```
my-game/
├── Cargo.toml              ← Workspace manifest (lists all reducer crates)
├── voltra-reducer/         ← Helper library (Context, reducer! macro)
│   ├── Cargo.toml
│   └── src/lib.rs
├── reducers/
│   ├── spawn/              ← One crate per reducer
│   │   ├── Cargo.toml      ← [lib] crate-type = ["cdylib"]
│   │   └── src/lib.rs
│   ├── attack/
│   ├── buy_item/
│   └── ...
├── modules/                ← Built .wasm files land here (gitignored)
├── voltra.toml             ← Server config
├── build.ps1               ← Windows build script
└── build.sh                ← Linux/macOS build script
```

Each reducer is its own tiny `cdylib` crate. Keeping them separate means:

- The WASM module is minimal (only the code it needs).
- You can update one reducer without rebuilding the others.
- The server can hot-reload individual files.

---

## Prerequisites

1. **Rust toolchain** — https://rustup.rs/

2. **WASM target:**

   ```bash
   rustup target add wasm32-unknown-unknown
   ```

3. **Voltra server binary** — built from this repo or downloaded from releases.

---

## Building

### Windows (PowerShell)

```powershell
# Debug build (faster, larger .wasm files)
.\build.ps1

# Release build (Cranelift optimisations — use for production)
.\build.ps1 -Release
```

### Linux / macOS

```bash
chmod +x build.sh
./build.sh            # debug
./build.sh --release  # release
```

The scripts:
1. Run `cargo build -p <crate> --target wasm32-unknown-unknown` for each crate.
2. Copy the resulting `.wasm` file to `modules/<name>.wasm`.
3. Print a summary with file sizes.

---

## Running

```bash
# Start the Voltra server (reads voltra.toml, loads modules/*.wasm)
voltra start

# In another terminal — call a reducer
voltra call spawn '["alice", 0, 0, "warrior"]'

# Watch live updates
voltra watch "players WHERE alive = true"
```

---

## Adding a New Reducer

1. **Create the crate directory:**

   ```bash
   mkdir -p reducers/my_reducer/src
   ```

2. **Write `reducers/my_reducer/Cargo.toml`:**

   ```toml
   [package]
   name = "my_reducer"
   version = "0.1.0"
   edition = "2021"

   [lib]
   crate-type = ["cdylib"]

   [dependencies]
   voltra-reducer = { path = "../../voltra-reducer" }
   serde_json = "1"
   rmp-serde = "1"
   ```

3. **Write `reducers/my_reducer/src/lib.rs`:**

   ```rust
   use voltra_reducer::{Context, Result};
   use serde_json::{json, Value};

   pub fn my_reducer(ctx: &mut Context, args: Value) -> Result<Value> {
       let player_id = args[0].as_str().ok_or("player_id required")?;
       // ... your logic ...
       Ok(json!({ "ok": true }))
   }

   voltra_reducer::reducer!(my_reducer);
   ```

4. **Register the crate in `Cargo.toml`** (workspace members list):

   ```toml
   members = [
       ...
       "reducers/my_reducer",
   ]
   ```

5. **Add to build scripts** — append `"my_reducer"` to the `$crates` array in
   `build.ps1` and `build.sh`.

6. **Build and run:**

   ```bash
   ./build.sh --release
   voltra start
   voltra call my_reducer '["alice"]'
   ```

---

## Context API Reference

Every reducer receives a `&mut Context` as its first argument.

### `ctx.get_row(table, key) -> Option<serde_json::Value>`

Read a row from any table. Returns `None` if the row does not exist.

```rust
if let Some(player) = ctx.get_row("players", "alice") {
    let hp = player["hp"].as_i64().unwrap_or(0);
}
```

### `ctx.set_row(table, key, value) -> Result<()>`

Insert or replace a row. The value must be a JSON object.

```rust
ctx.set_row("players", "alice", &json!({
    "hp": 100, "level": 1, "class": "warrior"
}))?;
```

### `ctx.delete_row(table, key) -> Result<()>`

Delete a row. Does not error if the row does not exist.

```rust
ctx.delete_row("sessions", session_id)?;
```

### `ctx.caller_id() -> String`

The identity string of whoever called this reducer (e.g. a user ID from the
JWT, or `"scheduler"` for scheduled calls).

```rust
let uid = ctx.caller_id();
if uid.is_empty() {
    return Err("unauthenticated".into());
}
```

### `ctx.caller_role() -> String`

The role of the caller. Set by the `Bearer <key>:<role>` token format.

```rust
if ctx.caller_role() != "admin" {
    return Err("forbidden: admin only".into());
}
```

---

## Included Reducers

| Reducer | Purpose |
|---|---|
| `spawn` | Create a player with class-based stats |
| `despawn` | Remove a player from the world |
| `move_player` | Update position and zone |
| `update_stats` | Change a single stat field |
| `attack` | Resolve combat, award XP on kill |
| `spawn_npc` | Create an NPC/enemy |
| `buy_item` | Purchase from shop, deduct currency |
| `sell_item` | Sell item, refund 50% of price |
| `world_tick` | Periodic HP/MP regeneration (scheduler) |
| `cleanup_sessions` | Remove expired sessions (scheduler) |
| `submit_score` | High-score leaderboard submission |

---

## Scheduler Integration

Reducers run automatically by adding entries to `voltra.toml`:

```toml
[[scheduler]]
reducer  = "world_tick"
interval = "3s"

[[scheduler]]
reducer   = "cleanup_sessions"
interval  = "60s"
args_json = "[0]"
```

The `caller_id()` for scheduled calls is `"scheduler"` and `caller_role()` is
`"scheduler"`. Use these to gate logic that should only run from the scheduler.

---

## Performance Notes

- Each reducer call creates a fresh 64 KiB scratch buffer. For reducers that
  process many rows in one call (e.g. `world_tick`), this is amortised across
  the entire invocation.
- The WASM instance is created once at server startup and reused for all calls.
  There is no per-call JIT compilation cost.
- `get_row` auto-retries with a doubled buffer on `-2` (buffer too small). For
  rows larger than 64 KiB, the buffer grows to 128 KiB, 256 KiB, etc. This is
  rare for typical game rows.
- Release builds (`./build.sh --release`) enable Cranelift's optimisation
  passes. The resulting `.wasm` files are 40–70% smaller and run significantly
  faster than debug builds.

---

## Troubleshooting

**"wasm32-unknown-unknown target not found"**
Run `rustup target add wasm32-unknown-unknown`.

**"module has no exported memory"**
The `voltra-reducer` crate does not declare a WASM memory — the `cdylib` linker
creates one automatically. If you see this error, ensure you are using
`crate-type = ["cdylib"]` (not `["lib"]`) in the reducer's `Cargo.toml`.

**"Import env::voltra_get_row not found"**
This means the `.wasm` module was loaded outside of Voltra (e.g. in a generic
WASM runner). It is expected outside the server. Inside Voltra, the host ABI is
always present.

**"reducer export not found"**
The `reducer!` macro generates the `reducer` export. Make sure you have
`voltra_reducer::reducer!(your_function_name);` at the bottom of `lib.rs`.

**Build succeeds but server says "no module named X"**
Check that the `.wasm` file was copied to `modules/` and that the module name
in `voltra.toml` matches the filename without the `.wasm` extension.
