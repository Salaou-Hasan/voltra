# Reducers

A reducer is a named function that runs inside the server as a single atomic transaction. It can read and write rows in any table. All writes are staged and then committed atomically — if the reducer panics or returns an error, every staged write is discarded and the TableStore is unchanged.

---

## Why reducers?

Game logic often needs to read, compute, and write in a single operation — for example, applying damage requires reading the defender's HP, subtracting, and writing the new value. Without atomicity, two concurrent damage events can race and produce an incorrect result. Reducers solve this by running under per-row write locks with serializable isolation.

---

## Native Rust Reducers

Native reducers are compiled into the server binary. They have zero overhead and full type safety.

The built-in `increment` reducer is always available:

```
reducer_name: "increment"
args:         ["counter_name", delta]
result:       {"value": new_value}
```

To add your own native reducer, implement it in `src/reducer/context.rs` and register it in `src/reducer/registry.rs`. Native reducers receive a `&mut ReducerContext` which provides read/write access to the TableStore.

---

## JavaScript Reducers (Boa 0.19)

Create a `.js` file in the `modules/` directory. It is loaded automatically when the server starts. The file must define a top-level function with the same name as the file (without the `.js` extension).

### Example: deal_damage.js

```js
function deal_damage(args) {
  const attacker = __voltra_get("players", args.attacker_id);
  const defender = __voltra_get("players", args.defender_id);

  if (!defender) {
    throw new Error("Target not found");
  }

  const weapon = __voltra_get("items", args.weapon_id);
  const power = attacker.attack + (weapon ? weapon.bonus : 0);
  const newHp = Math.max(0, defender.hp - power);

  __voltra_set("players", args.defender_id, { ...defender, hp: newHp });
  __voltra_set("combat_log", Date.now().toString(), {
    attacker: args.attacker_id,
    defender: args.defender_id,
    damage: power,
    ts: Date.now(),
  });
}
```

### Host API

JS reducers access the database through these global functions:

| Function | Description |
|---|---|
| `__voltra_get(table, key)` | Read a row. Returns the row object or `null` if not found. Includes read-your-own-writes: a `__voltra_set` in the same call is visible to subsequent `__voltra_get` calls. |
| `__voltra_set(table, key, value)` | Write a row. For the `"counters"` table with a plain number, calls the counter increment path. For any other value, writes the full object. |
| `__voltra_delete(table, key)` | Delete a row. |
| `__voltra_get_all(table)` | Returns all rows in a table as an array of `[key, value]` pairs. |
| `__voltra_caller_id` | String: the identity of the client that called this reducer (from `X-Voltra-Identity` header or TCP peer address). |
| `__voltra_caller_role` | String: the role extracted from `Bearer <key>:<role>`, or `""` if no role was provided. |

### Performance

The Boa JS engine is an interpreter. For compute-heavy reducers, compile to WASM with javy for 10–50x better throughput:

```bash
# Download javy from https://github.com/bytecodealliance/javy/releases
# Do NOT use cargo install javy — that installs a library crate, not the CLI.
voltra build   # compiles modules/*.js → modules/*.wasm
voltra start   # automatically prefers the .wasm version
```

### Security note

JS reducers run with the same memory access as the server process. The wall-clock timeout (`reducer_timeout_ms`) is enforced, but there is no hard JS heap cap. Treat JS reducers as operator-authored semi-trusted code, not a sandbox for untrusted uploads. Use the WASM backend with a `ResourceLimiter` for untrusted code.

---

## WASM Reducers (Wasmtime 21)

Drop a `.wasm` or `.wat` file into `modules/`. Wasmtime uses Cranelift JIT compilation, making WASM reducers 10–50x faster than the Boa interpreter.

### Host imports

A WASM reducer module must import the following functions from the `"env"` namespace:

```wat
(import "env" "voltra_get"     (func $get     (param i32 i32 i32 i32) (result i32)))
(import "env" "voltra_set"     (func $set     (param i32 i32 i32 i32 i32 i32)))
(import "env" "voltra_delete"  (func $delete  (param i32 i32 i32 i32)))
(import "env" "voltra_get_all" (func $get_all (param i32 i32) (result i32)))
```

All strings are passed as `(pointer, length)` pairs into linear memory. Return values are written into memory provided by the caller.

### Building from JavaScript

The recommended WASM build path is via javy:

```bash
# Write a standard JS reducer in modules/my_reducer.js
voltra build   # runs: javy build modules/my_reducer.js -o modules/my_reducer.wasm
```

The resulting `.wasm` file is automatically preferred over the `.js` file on the next server start.

### Building from Rust

Use the `voltra-reducer` crate as a dependency:

```toml
[dependencies]
voltra-reducer = { path = "../voltra-reducer" }
```

See the `native/game-ready` template for a full example of a workspace with multiple Rust reducer crates compiled to WASM.

---

## Scheduler Reducers

Reducers can be called automatically on a recurring schedule by adding a `[[scheduler]]` block to `voltra.toml`:

```toml
[[scheduler]]
reducer = "cleanup_sessions"
interval_ms = 60000

[[scheduler]]
reducer = "refresh"
interval_ms = 300000
args_json = '{"top_n": 100}'
```

Scheduled calls are made with `caller_role = "scheduler"` and always bypass the permissions check (the scheduler is trusted).

If `args_json` is omitted, the reducer receives an empty args array.

The reducer name must exactly match the name registered in the `ReducerRegistry` — use the bare function name (e.g. `cleanup_sessions`, not `cleanup_expired_sessions`).

---

## Permissions

Restrict which roles can call a reducer by adding a `[permissions]` section to `voltra.toml`:

```toml
[server]
permissions_default_policy = "closed"   # deny unlisted reducers by default

[permissions]
increment      = ["user", "admin"]
delete_player  = ["admin"]
reset_scores   = ["admin", "moderator"]
```

Clients pass their role as `Bearer <key>:<role>`. A call without a role is treated as the empty string `""`. The scheduler always bypasses permission checks.

Default policy options:

- `open` (default): unlisted reducers are callable by any authenticated client.
- `closed`: unlisted reducers are denied unless the caller is the scheduler.
