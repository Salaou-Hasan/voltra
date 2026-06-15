# Operators and Types

---

## The Four Types

Neon has four primitive types. Every variable and every table field is one of these.

### int

A 64-bit signed whole number.

```neon
let hp     = 100
let damage = -25
let level  = 1
let score  = 99999
```

Range: approximately -9.2 × 10^18 to +9.2 × 10^18. For game purposes, treat it as unlimited.

### float

A 64-bit decimal number.

```neon
let x        = 0.0
let speed    = 3.14
let health   = 99.5
let modifier = -0.25
```

Floats are written with a decimal point. `3` is an `int`; `3.0` is a `float`.

### str

A text string.

```neon
let name   = "Alice"
let zone   = "zone_3"
let empty  = ""
let msg    = "hello, world!"
```

Strings use double quotes. There are no single-quoted strings in Neon.

### bool

True or false.

```neon
let alive   = true
let stunned = false
let done    = true
```

---

## Variables

Declare a variable with `let`:

```neon
let x = 10
let name = "Alice"
let alive = true
let speed = 2.5
```

Variables are **mutable** — you can change them after declaring:

```neon
let count = 0
count = count + 1
count = count + 1
// count is now 2
```

There is no `const` or `var` — only `let`. All `let` variables are mutable.

Variables are **local** to the reducer. They do not persist between reducer calls. Use table rows to store persistent state.

---

## Literals

| Type | Examples |
|---|---|
| `int` | `0`, `42`, `-7`, `1000000` |
| `float` | `0.0`, `3.14`, `-1.5`, `100.0` |
| `str` | `"hello"`, `""`, `"zone_1"` |
| `bool` | `true`, `false` |
| array | `["a", "b", "c"]`, `[1, 2, 3]`, `[]` |
| object | `{ hp: 100, name: "Alice" }` |

Arrays and objects can be stored in table rows and returned from reducers. They are not typed by the Neon type system — they are JSON values.

---

## Arithmetic Operators

| Operator | Meaning | Example |
|---|---|---|
| `+` | Addition | `hp + 10` |
| `-` | Subtraction | `hp - damage` |
| `*` | Multiplication | `level * 100` |
| `/` | Division | `gold / 2` |
| `%` | Modulo (remainder) | `score % 10` |
| `-` (unary) | Negation | `-damage` |

**int / int = int** (integer division, truncates toward zero):
```neon
let result = 7 / 2    // result is 3, not 3.5
```

**int + float = float** (int is promoted to float):
```neon
let x = 1 + 2.5    // x is 3.5 (float)
```

---

## Comparison Operators

All comparison operators return a `bool`.

| Operator | Meaning | Example |
|---|---|---|
| `==` | Equal | `hp == 0` |
| `!=` | Not equal | `zone != "lobby"` |
| `<` | Less than | `hp < 25` |
| `>` | Greater than | `gold > 100` |
| `<=` | Less or equal | `hp <= 0` |
| `>=` | Greater or equal | `level >= 10` |

```neon
if hp == 0 { players[id].alive = false }
if name != "" { /* name is set */ }
if score >= 1000 { /* high score */ }
```

---

## Logic Operators

| Operator | Meaning | Example |
|---|---|---|
| `and` | Both true | `alive and hp > 0` |
| `or` | Either true | `stunned or frozen` |
| `not` | Invert | `not alive` |

These are **keywords**, not symbols. Do not use `&&`, `||`, or `!`.

```neon
if p.alive and p.hp > 0 {
    // actually alive
}

if p.hp <= 0 or not p.alive {
    error("target is dead")
}

if not exists("players", id) {
    return { found: false }
}
```

---

## Bitwise Operators

For cases where you need to work with flags or bitmasks:

| Operator | Meaning | Example |
|---|---|---|
| `&` | Bitwise AND | `flags & 0x01` |
| `\|` | Bitwise OR | `flags \| 0x04` |
| `^` | Bitwise XOR | `flags ^ mask` |
| `<<` | Left shift | `1 << bit_pos` |
| `>>` | Right shift | `value >> 4` |

```neon
// Check if bit 3 is set
let has_shield = flags & 8
if has_shield != 0 {
    damage = damage / 2
}

// Set bit 2
players[id].flags = players[id].flags | 4
```

---

## Compound Assignment Operators

These are shorthand for read-modify-write on table fields and variables:

| Operator | Equivalent |
|---|---|
| `x += n` | `x = x + n` |
| `x -= n` | `x = x - n` |
| `x *= n` | `x = x * n` |
| `x /= n` | `x = x / n` |
| `x %= n` | `x = x % n` |

They work on both local variables and table fields:

```neon
let count = 0
count += 1              // variable

players[id].gold += 100  // table field
players[id].hp   -= 30   // table field
players[id].xp   *= 2    // table field
```

---

## Operator Precedence

From highest to lowest:

| Priority | Operators |
|---|---|
| 1 (highest) | Unary `-`, `not` |
| 2 | `*`, `/`, `%` |
| 3 | `+`, `-` |
| 4 | `<<`, `>>` |
| 5 | `&` |
| 6 | `^` |
| 7 | `\|` |
| 8 | `==`, `!=`, `<`, `>`, `<=`, `>=` |
| 9 | `and` |
| 10 (lowest) | `or` |

When in doubt, use parentheses:

```neon
// Ambiguous — use parens to be explicit
let ok = hp > 0 and alive or respawning

// Clear
let ok = (hp > 0 and alive) or respawning
```

---

## Type Coercion

Neon promotes types in expressions:

| Operation | Result |
|---|---|
| `int + int` | `int` |
| `float + float` | `float` |
| `int + float` | `float` (int promoted) |
| `int / int` | `int` (truncating) |
| `float / int` | `float` |

There is no implicit string coercion. Use `str(x)` to convert a number to a string:

```neon
let msg = concat("level: ", str(level))
```

---

## Explicit Type Casts

| Function | Converts to |
|---|---|
| `int(x)` | `int` (truncates float, parses str) |
| `float(x)` | `float` (promotes int, parses str) |
| `str(x)` | `str` (any value to string) |
| `bool(x)` | `bool` (0 / "" / false = false; else true) |

```neon
let level_str = str(level)         // 5  →  "5"
let x_int     = int(3.9)           // 3.9 → 3  (truncates)
let parsed    = int("42")          // "42" → 42
let as_float  = float(10)          // 10 → 10.0
let truthy    = bool(1)            // 1 → true
let falsy     = bool(0)            // 0 → false
```

---

## Variables vs Table Fields

A local variable (`let x = ...`) lives only for the duration of one reducer call. When the reducer returns, it is gone.

A table field (`players[id].hp = ...`) persists to disk via the WAL. It is there on the next call, next server restart, forever — until you delete it.

```neon
reducer example(player_id: str) {
    // local variable — gone after this reducer returns
    let multiplier = 2

    // table field — persisted to disk
    players[player_id].score *= multiplier

    return { ok: true }
}
```
