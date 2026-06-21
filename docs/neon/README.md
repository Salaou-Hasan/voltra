# Neon Language

**Neon** is the purpose-built language for Voltra game backends. You write simple, readable game logic in `.vol` files. `voltra build` compiles them to native Rust — zero interpreter, zero overhead, full performance.

---

## The Compilation Pipeline

```
reducers.vol
      │
      │  voltra build
      ▼
src/reducers.rs   ← generated Rust source (never edit this)
      │
      │  cargo build  (runs automatically)
      ▼
voltra binary     ← your game server, running at native speed
```

There is no JavaScript. There is no interpreter. Every `.vol` file becomes native machine code.

---

## 30-Second Example

Create `reducers.vol`:

```neon
table players {
    hp:    int   = 100,
    alive: bool  = true,
    x:     float = 0.0,
    name:  str   = "",
}

reducer spawn(player_id: str, name: str, x: float, y: float) {
    players[player_id] = { hp: 100, alive: true, x: x, name: name }
    return { ok: true }
}

reducer damage(player_id: str, amount: int) {
    let p = players[player_id] else { error("player not found") }
    players[player_id].hp = max(0, p.hp - amount)
    if players[player_id].hp == 0 {
        players[player_id].alive = false
    }
    return { ok: true, hp: players[player_id].hp }
}
```

Build and run:

```
voltra build
voltra start
voltra call spawn '["alice", "Alice", 0.0, 0.0]'
voltra call damage '["alice", 30]'
```

That's it. Your multiplayer game backend is live.

---

## Documentation

| File | Topic |
|---|---|
| [01 — Getting Started](01-getting-started.md) | Install, init, build, run, first call |
| [02 — Project Structure](02-project-structure.md) | Every file in a Neon project explained |
| [03 — Tables](03-tables.md) | Declaring tables, field types, defaults |
| [04 — Reducers](04-reducers.md) | Writing game logic, parameters, returns, errors |
| [05 — Data Access](05-data-access.md) | Read, write, update, delete rows |
| [06 — Control Flow](06-control-flow.md) | if/else, loops, break, continue |
| [07 — Operators and Types](07-operators-and-types.md) | All types, all operators, variables |
| [08 — Math and Random](08-builtins-math-random.md) | min, max, clamp, sqrt, rand_int, and more |
| [09 — String Builtins](09-builtins-strings.md) | len, concat, split, parse_int, and more |
| [10 — Array Builtins](10-builtins-arrays.md) | push, pop, slice, array_contains, and more |
| [11 — Table Query Builtins](11-builtins-table-queries.md) | count_rows, find_all, top_n, sort_by, and more |
| [12 — Cluster Builtins](12-builtins-cluster.md) | Route across multiple servers |
| [13 — Complete Examples](13-complete-examples.md) | Battle royale, chat server, trading card game |

---

## Why Neon?

| Pain with other backends | Neon solution |
|---|---|
| Configure a database, write ORM models, write API handlers, deploy — for every feature | One `.vol` file. `voltra build`. Done. |
| Runtime scripting is slow | Neon compiles to native Rust — same speed as hand-written Rust |
| Atomic transactions are hard | Every reducer is atomic by default. Error halfway? Nothing was written. |
| Real-time subscriptions need extra infrastructure | Built in. `voltra watch "players WHERE zone = 'lobby'"` |
| Scaling requires rewrites | Add cluster builtins. Same `.vol` file, distributed. |
