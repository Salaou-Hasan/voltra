# Data Access

Everything your reducer does with game data goes through table reads and writes. This document covers every pattern.

---

## Reading a Row

```voltra
let p = players[player_id]
```

This reads the row with key `player_id` from the `players` table. The result is stored in the variable `p`.

**If the row does not exist**, execution stops and returns an error to the client:
```json
{"error": "Row not found: players/alice", "success": false}
```

This default behavior is usually what you want. If a player calls `attack` with a player ID that doesn't exist, the reducer should fail.

---

## Reading with a Custom Error

```voltra
let p = players[player_id] else { error("player not found") }
```

The `else { ... }` block runs only if the row is missing. Use this to give the client a more descriptive error message.

```voltra
reducer attack(attacker_id: str, target_id: str, damage: int) {
    let attacker = players[attacker_id] else { error("attacker does not exist") }
    let target   = players[target_id]   else { error("target does not exist") }
    // ...
    return { ok: true }
}
```

---

## Reading with a Fallback Return

```voltra
let p = players[player_id] else { return { found: false } }
```

Instead of erroring, return a specific response. Useful when "not found" is a valid outcome, not an error.

```voltra
reducer check_player(player_id: str) {
    let p = players[player_id] else { return { found: false } }
    return { found: true, hp: p.hp, name: p.name }
}
```

---

## Writing a Full Row

```voltra
players[player_id] = { hp: 100, alive: true, x: 0.0, name: "Alice" }
```

This creates or completely replaces the row at `player_id`. Any fields not specified will be filled with their default values from the table declaration.

Use this for:
- Creating a new player (spawn)
- Resetting a player to initial state
- Setting all fields at once

```voltra
reducer spawn(player_id: str, name: str) {
    players[player_id] = { hp: 100, alive: true, x: 0.0, y: 0.0, name: name }
    return { ok: true }
}
```

---

## Writing a Single Field

```voltra
players[player_id].hp = 50
```

This reads the existing row and changes only the `hp` field. All other fields are untouched.

Use this when you want to update one thing without affecting everything else:

```voltra
reducer set_position(player_id: str, x: float, y: float) {
    players[player_id].x = x
    players[player_id].y = y
    return { ok: true }
}
```

---

## Compound Write Operators

```voltra
players[player_id].hp   -= 30    // hp = hp - 30
players[player_id].gold += 100   // gold = gold + 100
players[player_id].xp   *= 2     // xp = xp * 2
players[player_id].gold /= 2     // gold = gold / 2
players[player_id].kills %= 10   // kills = kills % 10
```

These are **read-modify-write** operations. Voltra reads the current value, applies the operation, and writes it back. Because reducers are atomic, you never get a lost update even if multiple clients call the same reducer simultaneously.

```voltra
reducer gain_xp(player_id: str, amount: int) {
    players[player_id].xp += amount
    return { ok: true }
}
```

---

## Deleting a Row

```voltra
delete players[player_id]
```

Removes the row entirely. If the row does not exist, this is a no-op (no error).

```voltra
reducer despawn(player_id: str) {
    delete players[player_id]
    return { ok: true }
}
```

---

## Checking if a Row Exists

```voltra
let found = exists("players", player_id)
if found {
    // player exists
}
```

`exists` returns `true` if the row is present, `false` if not. Use this when you want to branch on existence without reading the row.

```voltra
reducer safe_damage(target_id: str, amount: int) {
    if not exists("players", target_id) {
        return { ok: false, reason: "target not in game" }
    }
    players[target_id].hp -= amount
    return { ok: true }
}
```

---

## Reading Fields from a Row

After reading a row into a variable, access its fields with dot notation:

```voltra
let p = players[player_id]
let current_hp   = p.hp
let player_name  = p.name
let is_alive     = p.alive
let pos_x        = p.x
```

Use these values in expressions:

```voltra
reducer apply_damage(player_id: str, amount: int) {
    let p = players[player_id] else { error("not found") }
    let new_hp = max(0, p.hp - amount)
    players[player_id].hp = new_hp
    if new_hp == 0 {
        players[player_id].alive = false
    }
    return { ok: true, new_hp: new_hp, died: new_hp == 0 }
}
```

---

## Complete Example: Player Buys an Item

This example shows several data access patterns working together:

```voltra
table players {
    hp:        int  = 100,
    gold:      int  = 0,
    alive:     bool = true,
    name:      str  = "",
    inventory: str  = "[]",
}

table shop {
    item_name: str = "",
    price:     int = 0,
    in_stock:  int = 0,
}

reducer buy_item(item_id: str) {
    // Use caller_id — never trust a passed player_id
    let player_id = caller_id

    // Read the player — error if not found
    let p = players[player_id] else { error("you are not spawned") }

    // Read the shop item — custom error message
    let item = shop[item_id] else { error("item does not exist") }

    // Check preconditions
    if item.in_stock <= 0 {
        error("out of stock")
    }
    if p.gold < item.price {
        error("not enough gold")
    }

    // Deduct gold and reduce stock (atomic — both happen or neither does)
    players[player_id].gold -= item.price
    shop[item_id].in_stock  -= 1

    return {
        ok:        true,
        item:      item.item_name,
        gold_left: p.gold - item.price,
    }
}
```

---

## Summary of Data Access Patterns

| Pattern | Syntax |
|---|---|
| Read row (error if missing) | `let p = table[key]` |
| Read with custom error | `let p = table[key] else { error("msg") }` |
| Read with fallback return | `let p = table[key] else { return { ... } }` |
| Write full row | `table[key] = { field: value, ... }` |
| Write one field | `table[key].field = value` |
| Increment field | `table[key].field += amount` |
| Decrement field | `table[key].field -= amount` |
| Multiply field | `table[key].field *= factor` |
| Divide field | `table[key].field /= divisor` |
| Modulo field | `table[key].field %= divisor` |
| Delete row | `delete table[key]` |
| Check existence | `exists("table", key)` |
| Read field | `p.field_name` |
