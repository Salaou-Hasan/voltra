# Reducers

Reducers are the functions that make up your game logic. Every action a player can take — spawn, move, attack, buy, chat — is a reducer.

---

## What a Reducer Is

A reducer is an **atomic function** that runs on the server. "Atomic" means:

- All writes inside a reducer either **all succeed** or **none happen**
- If you call `error()` halfway through, nothing that ran before it gets saved
- Two players calling the same reducer at the same time cannot corrupt each other's data

This is the core safety guarantee of Voltra. You never need to think about locks, transactions, or race conditions. Just write the logic.

---

## Declaring a Reducer

```neon
reducer spawn(player_id: str, name: str, x: float, y: float) {
    players[player_id] = { hp: 100, alive: true, x: x, y: y, name: name }
    return { ok: true }
}
```

The syntax is:

```
reducer <name>(<param>: <type>, ...) {
    <body>
}
```

Every reducer must end with a `return` statement that returns a JSON object `{ ... }`, or an `error()` call that stops execution.

---

## Parameters

Reducers accept zero or more parameters. Each parameter has a name and a type.

Supported types:
- `str` — a text string
- `int` — a whole number
- `float` — a decimal number
- `bool` — true or false

```neon
reducer no_args() {
    return { ok: true }
}

reducer one_arg(player_id: str) {
    return { found: exists("players", player_id) }
}

reducer many_args(player_id: str, x: float, y: float, speed: int, sprint: bool) {
    // ...
    return { ok: true }
}
```

Parameters are passed by clients as a JSON array, in order:

```
voltra call many_args '["alice", 3.5, 7.2, 5, true]'
```

---

## Return Values

Every reducer must return a JSON object. The object can have any fields you want — the client receives it as the response.

```neon
reducer get_player(player_id: str) {
    let p = players[player_id] else { error("not found") }
    return { hp: p.hp, alive: p.alive, name: p.name }
}
```

The client receives:
```json
{"hp": 87, "alive": true, "name": "Alice"}
```

You can return any fields:

```neon
return { ok: true }
return { ok: false, reason: "out of gold" }
return { count: 42, updated: true }
return { items: ["sword", "shield"], gold: 500 }
```

---

## Errors

Call `error("message")` to stop execution and return an error to the client. **No writes made before the error call are saved.**

```neon
reducer buy_item(player_id: str, item_name: str, price: int) {
    let p = players[player_id] else { error("player not found") }
    if p.gold < price {
        error("not enough gold")
    }
    players[player_id].gold -= price
    // item is granted here
    return { ok: true }
}
```

If `p.gold < price`, the `error()` is called, execution stops, and the `players[player_id].gold -= price` line **never runs**. The client receives:

```json
{"error": "not enough gold", "success": false}
```

Use `error()` for:
- Player not found
- Precondition not met (not enough gold, wrong state, wrong zone)
- Invalid input (name too long, value out of range)
- Permission checks

---

## caller_id and caller_role

Two special variables are available inside every reducer:

### caller_id

The ID of the player who called this reducer. Set automatically from the player's connection.

```neon
reducer send_message(room_id: str, text: str) {
    // caller_id is automatically set — no need to pass it as a parameter
    let key = concat(room_id, concat(":", caller_id))
    messages[key] = { author: caller_id, text: text, at: timestamp() }
    return { ok: true }
}
```

Use `caller_id` instead of trusting a `player_id` parameter. A client could lie about their `player_id`; they cannot lie about their `caller_id`.

### caller_role

The role of the player who called this reducer. Set from the `Authorization` header: `Bearer <key>:<role>`.

```neon
reducer ban_player(target_id: str) {
    if caller_role != "admin" {
        error("permission denied")
    }
    players[target_id].banned = true
    return { ok: true }
}
```

Built-in roles: `"player"` (default), `"admin"`, `"scheduler"` (for timed reducers), `"moderator"` (your choice). You can define any role names you want.

---

## Atomic Writes

All writes inside a reducer are buffered until the `return` statement. They are applied atomically as a single batch.

```neon
reducer transfer_gold(from_id: str, to_id: str, amount: int) {
    let from = players[from_id] else { error("sender not found") }
    let to   = players[to_id]   else { error("receiver not found") }

    if from.gold < amount {
        error("not enough gold")
    }

    // Both writes happen together — you will never see one without the other
    players[from_id].gold -= amount
    players[to_id].gold   += amount

    return { ok: true }
}
```

If anything goes wrong after the two write lines — a crash, a server error — neither write is saved. You never end up in a state where gold was subtracted from one player but not added to another.

---

## No-Argument Reducers

Reducers can take zero parameters. These are useful for scheduled tasks or status queries.

```neon
reducer ping() {
    return { ok: true, time: timestamp() }
}

reducer player_count() {
    return { count: count_rows("players") }
}
```

---

## Multiple Reducers

A `reducers.vol` file can have any number of reducers. There is no limit.

```neon
table players { ... }
table items   { ... }

reducer spawn(player_id: str, name: str) { ... }
reducer despawn(player_id: str) { ... }
reducer move_player(player_id: str, x: float, y: float) { ... }
reducer attack(attacker_id: str, target_id: str, damage: int) { ... }
reducer pickup_item(player_id: str, item_id: str) { ... }
reducer drop_item(player_id: str, item_id: str) { ... }
reducer buy_item(player_id: str, item_name: str) { ... }
```

Each reducer is registered by name. Clients call them by name:

```
voltra call attack '["alice", "bob", 25]'
```

---

## Scheduled Reducers

Reducers can be run on a timer by the server without any client calling them. Configure them in `voltra.toml`:

```toml
[schedulers]
cleanup = { reducer = "cleanup_sessions", interval_secs = 60 }
leaderboard = { reducer = "update_ranks", interval_secs = 300 }
```

Inside a scheduled reducer, `caller_id` will be `"scheduler"` and `caller_role` will be `"scheduler"`.

```neon
reducer cleanup_sessions() {
    for id, p in players {
        if p.last_seen < timestamp() - 300000000000 {
            delete players[id]
        }
    }
    return { ok: true }
}
```

---

## Calling Reducers from Clients

From the CLI:
```
voltra call <reducer_name> '[arg1, arg2, ...]'
```

From TypeScript:
```typescript
const result = await db.call("attack", ["alice", "bob", 25]);
```

From Unity (C#):
```csharp
var result = await db.Call("attack", new object[] { "alice", "bob", 25 });
```

From Godot:
```gdscript
var result = await Voltra.call_reducer("attack", ["alice", "bob", 25])
```
