# Control Flow

Voltra supports if/else, loops, break, and continue. This document covers every control flow construct with examples.

---

## if / else if / else

```voltra
if hp <= 0 {
    players[id].alive = false
}
```

```voltra
if hp <= 0 {
    players[id].alive = false
} else if hp <= 25 {
    players[id].status = "critical"
} else if hp <= 50 {
    players[id].status = "injured"
} else {
    players[id].status = "healthy"
}
```

The condition can be any expression that evaluates to a boolean. You can use `and`, `or`, and `not` to combine conditions:

```voltra
if p.alive and p.hp > 0 {
    // player is actually alive
}

if not p.alive or p.hp <= 0 {
    error("target is already dead")
}

if p.level >= 10 and p.xp >= 1000 {
    // player can level up
}
```

---

## The else Block on Row Reads

When you read a row, the `else` block runs if the row is missing:

```voltra
let p = players[id] else { error("player not found") }
```

Without the `else`, the server automatically errors with a generic "Row not found" message:

```voltra
let p = players[id]
// if id doesn't exist, execution stops here with a generic error
```

The `else` block can contain:
- `error("custom message")` — stop with a specific error
- `return { ... }` — return a response (not found is a valid non-error outcome)
- Any other statements — run fallback logic

```voltra
// Error with a descriptive message
let guild = guilds[guild_id] else { error("guild does not exist") }

// Return a structured "not found" response instead of an error
let profile = profiles[user_id] else { return { found: false, profile: null } }

// Create the row if it doesn't exist (upsert pattern)
let counter = counters[counter_id] else {
    counters[counter_id] = { value: 0, name: counter_id }
    return { created: true, value: 0 }
}
```

---

## for Loops: Iterating Table Rows

Iterate over every row in a table with a `for id, row in table` loop:

```voltra
for id, p in players {
    // id is the row key (string)
    // p is the row value (object)
}
```

Example — find all alive players:

```voltra
reducer count_alive() {
    let count = 0
    for id, p in players {
        if p.alive {
            count += 1
        }
    }
    return { alive: count }
}
```

Example — deal area-of-effect damage to all players:

```voltra
reducer aoe_blast(damage: int) {
    for id, p in players {
        if p.alive {
            let new_hp = max(0, p.hp - damage)
            players[id].hp = new_hp
            if new_hp == 0 {
                players[id].alive = false
            }
        }
    }
    return { ok: true }
}
```

### Collect-then-Delete Pattern

You can delete rows while iterating a table. The iteration is over a snapshot taken at the start of the loop, so deletes inside the loop are safe:

```voltra
reducer cleanup_dead() {
    for id, p in players {
        if not p.alive {
            delete players[id]
        }
    }
    return { ok: true }
}
```

---

## for Loops: Iterating Arrays

Iterate over every element in an array with a `for item in arr` loop:

```voltra
let skills = ["fireball", "shield", "dash"]
for skill in skills {
    // skill is each element in turn
}
```

Example — apply multiple damage types:

```voltra
reducer apply_effects(player_id: str, fire: int, poison: int, frost: int) {
    let damages = [fire, poison, frost]
    for dmg in damages {
        players[player_id].hp -= dmg
    }
    players[player_id].hp = max(0, players[player_id].hp)
    return { ok: true, hp: players[player_id].hp }
}
```

---

## while Loops

Run a block while a condition is true:

```voltra
while condition {
    // ...
}
```

Example — grant XP and level up as many times as needed:

```voltra
reducer grant_xp(player_id: str, xp_amount: int) {
    players[player_id].xp += xp_amount
    let p = players[player_id]

    // Level up repeatedly while enough XP has accumulated
    while players[player_id].xp >= players[player_id].level * 100 {
        players[player_id].xp    -= players[player_id].level * 100
        players[player_id].level += 1
        players[player_id].max_hp += 10
        players[player_id].hp     = players[player_id].max_hp
    }

    return {
        ok:    true,
        level: players[player_id].level,
        xp:    players[player_id].xp,
    }
}
```

Example — countdown timer with a while loop:

```voltra
let n = 5
while n > 0 {
    n = n - 1
}
```

---

## break

Exit a loop early:

```voltra
reducer find_weak_player() {
    let found_id = ""
    for id, p in players {
        if p.hp < 20 {
            found_id = id
            break
        }
    }
    if len(found_id) == 0 {
        return { found: false }
    }
    return { found: true, id: found_id }
}
```

`break` works inside `for` loops (both row and array) and `while` loops.

---

## continue

Skip to the next iteration of a loop:

```voltra
reducer heal_injured() {
    for id, p in players {
        // Skip players who are full health or dead
        if not p.alive {
            continue
        }
        if p.hp >= p.max_hp {
            continue
        }
        players[id].hp = min(p.max_hp, p.hp + 10)
    }
    return { ok: true }
}
```

---

## Nested Loops

Loops can be nested. `break` and `continue` affect the innermost loop:

```voltra
reducer check_all_matchups() {
    let conflicts = 0
    for id_a, pa in players {
        for id_b, pb in players {
            if id_a == id_b {
                continue   // skip self
            }
            if pa.zone == pb.zone {
                conflicts += 1
            }
        }
    }
    // conflicts counts each pair twice, so divide by 2
    return { conflicts: conflicts / 2 }
}
```

---

## Combining Control Flow

Real reducers combine everything together:

```voltra
reducer zone_cleanup(zone: str, poison_damage: int) {
    // Iterate all players
    for id, p in players {
        // Skip players not in this zone
        if p.zone != zone {
            continue
        }

        // Skip dead players
        if not p.alive {
            continue
        }

        // Apply poison
        let new_hp = max(0, p.hp - poison_damage)
        players[id].hp = new_hp

        // Check if poison killed them
        if new_hp == 0 {
            players[id].alive = false
        }
    }

    return { ok: true }
}
```
