# Math and Random Builtins

Voltra provides a complete set of math functions for game calculations: damage formulas, movement, XP curves, cooldowns, and randomness.

---

## min(a, b)

**Returns** the smaller of two values.

```voltra
min(3, 7)       // 3
min(10, 10)     // 10
min(-5, 0)      // -5
```

**Game use — clamp HP at zero:**
```voltra
reducer apply_damage(player_id: str, damage: int) {
    let p = players[player_id] else { error("not found") }
    players[player_id].hp = max(0, p.hp - damage)
    return { ok: true, hp: players[player_id].hp }
}
```

---

## max(a, b)

**Returns** the larger of two values.

```voltra
max(3, 7)       // 7
max(10, 10)     // 10
max(-5, 0)      // 0
```

**Game use — ensure minimum damage:**
```voltra
let actual_damage = max(1, raw_damage - target.armor)
```

---

## abs(x)

**Returns** the absolute value (removes the sign).

```voltra
abs(-15)    // 15
abs(15)     // 15
abs(0)      // 0
```

**Game use — distance without direction:**
```voltra
let dx = abs(p.x - target.x)
let dy = abs(p.y - target.y)
```

---

## floor(x)

**Returns** the largest integer less than or equal to `x` (round down).

```voltra
floor(3.7)    // 3.0
floor(3.0)    // 3.0
floor(-3.2)   // -4.0
```

**Game use — grid snapping:**
```voltra
let grid_x = int(floor(p.x / 32.0)) * 32
```

---

## ceil(x)

**Returns** the smallest integer greater than or equal to `x` (round up).

```voltra
ceil(3.2)    // 4.0
ceil(3.0)    // 3.0
ceil(-3.7)   // -3.0
```

**Game use — time division (round up to nearest second):**
```voltra
let seconds = int(ceil(float(ms) / 1000.0))
```

---

## round(x)

**Returns** `x` rounded to the nearest integer (0.5 rounds up).

```voltra
round(3.4)    // 3.0
round(3.5)    // 4.0
round(-3.5)   // -3.0
```

**Game use — display a clean stat:**
```voltra
let shown_dps = round(damage * attacks_per_second)
```

---

## sqrt(x)

**Returns** the square root of `x`.

```voltra
sqrt(9.0)    // 3.0
sqrt(2.0)    // 1.4142...
sqrt(0.0)    // 0.0
```

**Game use — Euclidean distance between two players:**
```voltra
reducer in_range(player_id: str, target_id: str, range: float) {
    let p = players[player_id] else { error("not found") }
    let t = players[target_id] else { error("not found") }
    let dx = p.x - t.x
    let dy = p.y - t.y
    let dist = sqrt(dx * dx + dy * dy)
    return { in_range: dist <= range, distance: dist }
}
```

---

## pow(x, y)

**Returns** `x` raised to the power `y`.

```voltra
pow(2.0, 10.0)    // 1024.0
pow(3.0, 2.0)     // 9.0
pow(10.0, 0.0)    // 1.0
```

**Game use — XP curve (exponential leveling):**
```voltra
reducer xp_needed_for_level(level: int) {
    let xp = int(pow(float(level), 1.5) * 100.0)
    return { xp_needed: xp }
}
// Level 1: 100 XP, Level 5: 559 XP, Level 10: 1581 XP, Level 20: 8944 XP
```

**Game use — crit damage multiplier:**
```voltra
let crit_multiplier = pow(2.0, float(crit_stacks) * 0.5)
let crit_damage = int(float(base_damage) * crit_multiplier)
```

---

## clamp(x, lo, hi)

**Returns** `x` clamped to the range `[lo, hi]`. If `x < lo`, returns `lo`. If `x > hi`, returns `hi`. Otherwise returns `x`.

```voltra
clamp(150, 0, 100)   // 100
clamp(-10, 0, 100)   // 0
clamp(50,  0, 100)   // 50
```

**Game use — enforce HP bounds:**
```voltra
players[id].hp = int(clamp(float(new_hp), 0.0, float(p.max_hp)))
```

**Game use — keep a player in bounds:**
```voltra
players[id].x = clamp(new_x, -500.0, 500.0)
players[id].y = clamp(new_y, -500.0, 500.0)
```

---

## sign(x)

**Returns** `-1`, `0`, or `1` depending on the sign of `x`.

```voltra
sign(-42.0)    // -1.0
sign(0.0)      // 0.0
sign(17.0)     // 1.0
```

**Game use — knockback direction:**
```voltra
let dir = sign(target.x - attacker.x)
players[target_id].x += dir * 5.0
```

---

## log2(x)

**Returns** the base-2 logarithm of `x`.

```voltra
log2(1.0)     // 0.0
log2(2.0)     // 1.0
log2(1024.0)  // 10.0
```

**Game use — bit position of a flag:**
```voltra
let bit = int(log2(float(flag)))
```

---

## log10(x)

**Returns** the base-10 logarithm of `x`.

```voltra
log10(1.0)      // 0.0
log10(10.0)     // 1.0
log10(1000.0)   // 3.0
```

**Game use — score magnitude for display tier:**
```voltra
let tier = int(log10(float(score) + 1.0))
// 0-9: tier 0, 10-99: tier 1, 100-999: tier 2, ...
```

---

## rand_int(lo, hi)

**Returns** a random integer in the range `[lo, hi]` (both inclusive).

```voltra
rand_int(1, 6)      // roll a 6-sided die: 1, 2, 3, 4, 5, or 6
rand_int(0, 99)     // 0 to 99 inclusive
rand_int(10, 20)    // 10 to 20 inclusive
```

**Game use — random loot drop:**
```voltra
let loot_pool = ["sword", "shield", "potion", "gold_bag", "gem"]
let roll = rand_int(0, array_len(loot_pool) - 1)
let loot = get_index(loot_pool, roll)
return { loot: loot }
```

**Game use — crit chance:**
```voltra
let roll = rand_int(1, 100)
let is_crit = roll <= crit_chance_pct
let damage = base_damage
if is_crit {
    damage = damage * 2
}
```

**Game use — random spawn zone:**
```voltra
let zones = ["north", "south", "east", "west"]
let zone = get_index(zones, rand_int(0, 3))
players[caller_id].zone = zone
```

---

## rand_float()

**Returns** a random float in the range `[0.0, 1.0)`.

```voltra
rand_float()    // e.g. 0.732, 0.041, 0.999
```

**Game use — drop rate check:**
```voltra
let drop_chance = 0.05    // 5% chance
if rand_float() < drop_chance {
    // rare item dropped
}
```

**Game use — random position in a circle:**
```voltra
let angle  = rand_float() * 6.2832    // 0 to 2*pi
let radius = rand_float() * 100.0
let spawn_x = center_x + radius * cos_approx
let spawn_y = center_y + radius * sin_approx
```

**Game use — weighted random choice:**
```voltra
// 60% common, 30% rare, 10% legendary
let r = rand_float()
let rarity = "common"
if r >= 0.60 and r < 0.90 {
    rarity = "rare"
} else if r >= 0.90 {
    rarity = "legendary"
}
```

---

## Practical: Complete Damage Formula

```voltra
reducer attack(attacker_id: str, target_id: str) {
    let attacker = players[attacker_id] else { error("attacker not found") }
    let target   = players[target_id]   else { error("target not found") }

    if not target.alive {
        error("target is already dead")
    }

    // Base damage from attacker's weapon stat
    let base = attacker.weapon_damage

    // Armor reduction (minimum 1 damage)
    let mitigated = max(1, base - target.armor)

    // Critical hit: 15% chance, 2x damage
    let damage = mitigated
    let is_crit = rand_int(1, 100) <= 15
    if is_crit {
        damage = damage * 2
    }

    // Apply damage, clamp to [0, max_hp]
    let new_hp = clamp(float(target.hp - damage), 0.0, float(target.max_hp))
    players[target_id].hp = int(new_hp)

    if players[target_id].hp == 0 {
        players[target_id].alive = false
        players[attacker_id].kills += 1
    }

    return {
        ok:      true,
        damage:  damage,
        is_crit: is_crit,
        target_hp: players[target_id].hp,
        killed:  players[target_id].hp == 0,
    }
}
```
