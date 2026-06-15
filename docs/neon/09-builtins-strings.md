# String Builtins

Neon has a complete set of string functions for building messages, parsing input, validating names, and constructing row keys.

---

## len(s)

**Returns** the number of characters in `s`.

```neon
len("Alice")    // 5
len("")         // 0
len("hello!")   // 6
```

**Game use — validate player name length:**
```neon
reducer set_name(player_id: str, name: str) {
    if len(trim(name)) < 2 {
        error("name too short (minimum 2 characters)")
    }
    if len(name) > 20 {
        error("name too long (maximum 20 characters)")
    }
    players[player_id].name = trim(name)
    return { ok: true }
}
```

---

## concat(a, b)

**Returns** a new string with `b` appended to `a`.

```neon
concat("hello", " world")    // "hello world"
concat("player_", "alice")   // "player_alice"
concat("zone", str(3))       // "zone3"
```

**Game use — build composite row keys:**
```neon
let msg_key = concat(room_id, concat("_", str(timestamp())))
messages[msg_key] = { author: caller_id, text: text }
```

**Game use — build a status message:**
```neon
let msg = concat(p.name, concat(" entered zone ", zone_name))
```

To concat more than two strings, nest the calls:
```neon
let full = concat(first, concat(" ", concat(middle, concat(" ", last))))
```

---

## contains(s, sub)

**Returns** `true` if `sub` appears anywhere inside `s`.

```neon
contains("hello world", "world")    // true
contains("hello world", "xyz")      // false
contains("zone_3_5", "3")           // true
contains("", "x")                   // false
```

**Game use — chat content moderation:**
```neon
if contains(to_lower(text), "badword") {
    error("message contains prohibited content")
}
```

**Game use — check zone family:**
```neon
if contains(p.zone, "dungeon") {
    // player is in some dungeon zone
}
```

---

## starts_with(s, prefix)

**Returns** `true` if `s` begins with `prefix`.

```neon
starts_with("zone_3_5", "zone_")    // true
starts_with("lobby",    "zone_")    // false
starts_with("", "")                 // true
```

**Game use — zone category check:**
```neon
if starts_with(p.zone, "pvp_") {
    // player is in a PvP zone
}
```

**Game use — filter NPC entries:**
```neon
for id, row in players {
    if starts_with(id, "npc_") {
        // this is an NPC, not a real player
    }
}
```

---

## ends_with(s, suffix)

**Returns** `true` if `s` ends with `suffix`.

```neon
ends_with("player_alice", "_alice")    // true
ends_with("player_alice", "_bob")      // false
```

**Game use — identify file type or key pattern:**
```neon
if ends_with(item_id, "_legendary") {
    // legendary item
}
```

---

## to_upper(s)

**Returns** `s` with all letters converted to uppercase.

```neon
to_upper("alice")      // "ALICE"
to_upper("Hello!")     // "HELLO!"
to_upper("zone_3")     // "ZONE_3"
```

**Game use — normalize for case-insensitive comparison:**
```neon
if to_upper(input) == to_upper(expected) {
    // match regardless of capitalization
}
```

---

## to_lower(s)

**Returns** `s` with all letters converted to lowercase.

```neon
to_lower("ALICE")      // "alice"
to_lower("Hello!")     // "hello!"
to_lower("Zone_3")     // "zone_3"
```

**Game use — normalize player name for lookup:**
```neon
let canonical = to_lower(trim(name))
let existing = name_index[canonical] else { return { taken: false } }
return { taken: true, owner: existing.owner_id }
```

---

## trim(s)

**Returns** `s` with leading and trailing whitespace removed.

```neon
trim("  Alice  ")    // "Alice"
trim("\t hello \n") // "hello"
trim("no spaces")   // "no spaces"
```

**Game use — sanitize user input:**
```neon
let clean_name = trim(name)
if len(clean_name) == 0 {
    error("name cannot be blank")
}
```

---

## replace(s, from, to)

**Returns** `s` with every occurrence of `from` replaced by `to`.

```neon
replace("hello world", "world", "neon")    // "hello neon"
replace("a_b_c", "_", "-")                 // "a-b-c"
replace("bad word here", "bad word", "***") // "*** here"
```

**Game use — sanitize chat message:**
```neon
let clean = replace(text, "badword", "***")
messages[msg_id] = { text: clean, author: caller_id }
```

**Game use — build a key by replacing spaces:**
```neon
let slug = replace(to_lower(guild_name), " ", "_")
guilds[slug] = { name: guild_name, owner_id: caller_id }
```

---

## substring(s, start, end)

**Returns** the portion of `s` from index `start` (inclusive) to index `end` (exclusive). Indices are zero-based.

```neon
substring("hello world", 0, 5)    // "hello"
substring("hello world", 6, 11)   // "world"
substring("zone_3_5",   5, 6)     // "3"
```

**Game use — extract zone number:**
```neon
// zone format: "zone_N"
let zone_num_str = substring(zone_id, 5, len(zone_id))
let zone_num = parse_int(zone_num_str)
```

---

## index_of(s, sub)

**Returns** the index of the first occurrence of `sub` in `s`, or `-1` if not found.

```neon
index_of("hello world", "world")    // 6
index_of("hello world", "xyz")      // -1
index_of("aababc", "ab")            // 1
```

**Game use — find separator position:**
```neon
let sep = index_of(composite_key, ":")
if sep == -1 {
    error("invalid key format")
}
let table_part = substring(composite_key, 0, sep)
let row_part   = substring(composite_key, sep + 1, len(composite_key))
```

---

## split(s, sep)

**Returns** an array of substrings by splitting `s` on `sep`.

```neon
split("a,b,c", ",")          // ["a", "b", "c"]
split("zone_3_5", "_")       // ["zone", "3", "5"]
split("hello", "")           // ["h", "e", "l", "l", "o"]
split("no-sep-here", ",")    // ["no-sep-here"]
```

**Game use — parse a zone coordinate string:**
```neon
// zone format: "zone_X_Y"  e.g. "zone_3_5"
reducer warp_to_zone(player_id: str, zone_str: str) {
    let parts = split(zone_str, "_")
    if array_len(parts) != 3 {
        error("invalid zone format, expected zone_X_Y")
    }
    let x = parse_int(get_index(parts, 1))
    let y = parse_int(get_index(parts, 2))
    players[player_id].zone = zone_str
    players[player_id].x = float(x) * 100.0
    players[player_id].y = float(y) * 100.0
    return { ok: true, x: x, y: y }
}
```

---

## join(arr, sep)

**Returns** a string by joining all elements of `arr` with `sep` between them.

```neon
join(["a", "b", "c"], ",")      // "a,b,c"
join(["sword", "shield"], " + ")  // "sword + shield"
join(["one"], "-")               // "one"
join([], ",")                    // ""
```

**Game use — build a display string from skill list:**
```neon
let skill_list = ["fireball", "ice_shard", "lightning"]
let display = join(skill_list, ", ")
// "fireball, ice_shard, lightning"
return { skills: display }
```

---

## parse_int(s)

**Returns** the integer value of a numeric string. Returns `0` if the string is not a valid integer.

```neon
parse_int("42")     // 42
parse_int("-7")     // -7
parse_int("0")      // 0
parse_int("abc")    // 0
parse_int("3.14")   // 0  (not an integer string)
```

**Game use — parse a numeric ID from a composite key:**
```neon
let parts = split(row_key, "_")
let numeric_id = parse_int(get_index(parts, 1))
```

---

## parse_float(s)

**Returns** the float value of a numeric string. Returns `0.0` if not valid.

```neon
parse_float("3.14")    // 3.14
parse_float("100")     // 100.0
parse_float("-0.5")    // -0.5
parse_float("abc")     // 0.0
```

---

## char_at(s, i)

**Returns** the single character at index `i` (zero-based) as a one-character string.

```neon
char_at("hello", 0)    // "h"
char_at("hello", 4)    // "o"
char_at("zone_3", 5)   // "3"
```

**Game use — check first character of a key:**
```neon
if char_at(player_id, 0) == "g" {
    // guest player
}
```

---

## repeat(s, n)

**Returns** `s` repeated `n` times.

```neon
repeat("ab", 3)     // "ababab"
repeat("-", 10)     // "----------"
repeat("*", 0)      // ""
```

**Game use — build a progress bar:**
```neon
let filled = int(float(hp) / float(max_hp) * 10.0)
let bar = concat(repeat("#", filled), repeat(".", 10 - filled))
// e.g. "######...." at 60% HP
return { bar: bar }
```

---

## Practical Example: Player Name Validation and Normalization

```neon
reducer register_player(display_name: str) {
    let name = trim(display_name)

    // Length check
    if len(name) < 2 {
        error("name must be at least 2 characters")
    }
    if len(name) > 16 {
        error("name must be 16 characters or fewer")
    }

    // Build a canonical key (lowercase, no spaces)
    let key = replace(to_lower(name), " ", "_")

    // Check if taken
    if exists("players", key) {
        error("name already taken")
    }

    players[key] = {
        name:    name,
        hp:      100,
        alive:   false,
        gold:    0,
    }

    return { ok: true, player_id: key }
}
```
