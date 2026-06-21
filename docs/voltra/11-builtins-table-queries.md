# Table Query Builtins

These builtins let you search, count, aggregate, and sort table data without iterating every row manually.

---

## count_rows("table")

**Returns** the number of rows currently in the named table.

```voltra
count_rows("players")       // e.g. 1247
count_rows("items")         // e.g. 8903
count_rows("leaderboard")   // e.g. 500
```

**Game use — count active players:**
```voltra
reducer server_status() {
    return {
        players_online: count_rows("players"),
        items_in_world: count_rows("items"),
    }
}
```

**Game use — enforce a server population cap:**
```voltra
reducer spawn(player_id: str, name: str) {
    if count_rows("players") >= 1000 {
        error("server full")
    }
    players[player_id] = { name: name, hp: 100, alive: true }
    return { ok: true }
}
```

---

## sum_field("table", "field")

**Returns** the sum of a numeric field across all rows in the table.

```voltra
sum_field("players", "gold")     // total gold held by all players
sum_field("items", "quantity")   // total item quantity in world
```

**Game use — economy audit:**
```voltra
reducer economy_report() {
    return {
        total_gold_in_circulation: sum_field("players", "gold"),
        total_items:               sum_field("items", "quantity"),
    }
}
```

---

## avg_field("table", "field")

**Returns** the average (mean) of a numeric field across all rows. Returns `0.0` if the table is empty.

```voltra
avg_field("players", "hp")      // average HP of all players
avg_field("players", "level")   // average player level
```

**Game use — check server health balance:**
```voltra
reducer balance_report() {
    return {
        avg_player_hp:    avg_field("players", "hp"),
        avg_player_level: avg_field("players", "level"),
        avg_player_gold:  avg_field("players", "gold"),
    }
}
```

---

## min_field("table", "field")

**Returns** the minimum value of a numeric field across all rows.

```voltra
min_field("players", "hp")      // lowest HP any player has
min_field("players", "level")   // lowest level
```

**Game use — find the most vulnerable player:**
```voltra
let lowest_hp = min_field("players", "hp")
return { most_vulnerable_hp: lowest_hp }
```

---

## max_field("table", "field")

**Returns** the maximum value of a numeric field across all rows.

```voltra
max_field("leaderboard", "score")   // highest score on the board
max_field("players", "level")       // highest level player
```

**Game use — dynamic difficulty:**
```voltra
reducer spawn_boss() {
    let top_level = int(max_field("players", "level"))
    let boss_hp   = top_level * 500
    bosses["world_boss"] = { hp: boss_hp, alive: true, level: top_level }
    return { ok: true, boss_hp: boss_hp }
}
```

---

## find_first("table", "field", value)

**Returns** the first row where `field` equals `value`, or null if no row matches.

```voltra
let p = find_first("players", "name", "Alice")
// p is a full row object, or null
```

**Note:** Row order in a table is not guaranteed. `find_first` returns *a* matching row, not necessarily the one created first. Use it when only one row should match (e.g. unique username lookup).

**Game use — find player by display name:**
```voltra
reducer find_player(name: str) {
    let p = find_first("players", "name", name)
    if p == null {
        return { found: false }
    }
    return { found: true, hp: p.hp, zone: p.zone }
}
```

**Game use — find an open guild:**
```voltra
reducer find_open_guild() {
    let g = find_first("guilds", "open", true)
    if g == null {
        return { found: false }
    }
    return { found: true, name: g.name }
}
```

---

## find_all("table", "field", value)

**Returns** an array of all rows where `field` equals `value`.

```voltra
let guild_members = find_all("players", "guild_id", "shadow-wolves")
// array of player row objects
```

**Game use — get all members of a guild:**
```voltra
reducer guild_roster(guild_id: str) {
    let members = find_all("players", "guild_id", guild_id)
    return { count: array_len(members), members: members }
}
```

**Game use — count alive players in a zone:**
```voltra
reducer zone_alive_count(zone: str) {
    let in_zone = find_all("players", "zone", zone)
    let alive   = 0
    for p in in_zone {
        if p.alive {
            alive += 1
        }
    }
    return { total: array_len(in_zone), alive: alive }
}
```

---

## sort_by("table", "field", "asc" | "desc")

**Returns** an array of all rows sorted by `field` in ascending or descending order. Numbers are sorted numerically; strings are sorted lexicographically.

```voltra
let by_score = sort_by("leaderboard", "score", "desc")   // highest first
let by_level = sort_by("players", "level", "asc")        // lowest first
let by_name  = sort_by("players", "name", "asc")         // A to Z
```

**Game use — full sorted leaderboard:**
```voltra
reducer get_leaderboard() {
    let sorted = sort_by("leaderboard", "score", "desc")
    return { entries: sorted }
}
```

**Game use — find the weakest and strongest players:**
```voltra
reducer hp_extremes() {
    let by_hp = sort_by("players", "hp", "asc")
    let count  = array_len(by_hp)
    return {
        weakest:   array_first(by_hp),
        strongest: array_last(by_hp),
        total:     count,
    }
}
```

---

## top_n("table", "field", n)

**Returns** an array of the top `n` rows ranked by `field` (highest value first). Equivalent to `sort_by(..., "desc")` followed by `slice(..., 0, n)`, but more efficient.

```voltra
let top10 = top_n("leaderboard", "score", 10)
let top3  = top_n("players", "kills", 3)
```

**Game use — leaderboard endpoint:**
```voltra
reducer top_players(count: int) {
    let top = top_n("players", "score", count)
    return { leaderboard: top, count: array_len(top) }
}
```

**Game use — reward top 3 players at season end:**
```voltra
reducer season_rewards() {
    if caller_role != "admin" {
        error("permission denied")
    }
    let winners = top_n("players", "score", 3)
    let rewards = [1000, 500, 250]
    let i = 0
    for winner in winners {
        players[winner.id].gold += get_index(rewards, i)
        i += 1
    }
    return { ok: true, rewarded: array_len(winners) }
}
```

---

## keys_of("table")

**Returns** an array of all row keys (strings) in the named table.

```voltra
let all_player_ids = keys_of("players")
let all_guild_ids  = keys_of("guilds")
```

**Game use — broadcast a message to all players:**
```voltra
reducer admin_broadcast(message: str) {
    if caller_role != "admin" {
        error("permission denied")
    }
    let ids = keys_of("players")
    for id in ids {
        notifications[concat(id, concat(":", str(timestamp())))] = {
            to:      id,
            message: message,
            read:    false,
        }
    }
    return { ok: true, sent_to: array_len(ids) }
}
```

**Game use — delete all rows in a table (reset):**
```voltra
reducer reset_leaderboard() {
    if caller_role != "admin" {
        error("permission denied")
    }
    let ids = keys_of("leaderboard")
    for id in ids {
        delete leaderboard[id]
    }
    return { ok: true, cleared: array_len(ids) }
}
```

---

## exists("table", key)

**Returns** `true` if a row with the given key exists in the table.

```voltra
exists("players", "alice")    // true if alice is spawned
exists("guilds", "my-guild")  // true if guild exists
```

This is preferred over reading the row just to check existence — it does not allocate or decode the row data.

```voltra
reducer spawn(name: str) {
    let key = to_lower(trim(name))
    if exists("players", key) {
        error("name already taken")
    }
    players[key] = { name: name, hp: 100, alive: true }
    return { ok: true, player_id: key }
}
```

---

## timestamp()

**Returns** the current server time as an integer (nanoseconds since Unix epoch).

```voltra
let now = timestamp()
// e.g. 1718400000000000000
```

Use it for:
- Recording when something happened
- Checking if a cooldown has expired
- Generating unique keys

```voltra
reducer log_event(event_type: str) {
    let now = timestamp()
    let key = concat(caller_id, concat(":", str(now)))
    events[key] = { type: event_type, player: caller_id, at: now }
    return { ok: true, at: now }
}
```

**Game use — cooldown check:**
```voltra
reducer use_ability(player_id: str, ability: str) {
    let p   = players[player_id] else { error("not found") }
    let now = timestamp()
    // cooldown is 5 seconds = 5,000,000,000 nanoseconds
    if now - p.last_ability_use < 5000000000 {
        error("ability on cooldown")
    }
    players[player_id].last_ability_use = now
    return { ok: true }
}
```

---

## Practical: Global Leaderboard

```voltra
table players {
    name:    str  = "",
    score:   int  = 0,
    kills:   int  = 0,
    deaths:  int  = 0,
    alive:   bool = false,
    hp:      int  = 100,
}

reducer record_kill(killer_id: str, victim_id: str) {
    players[killer_id].kills += 1
    players[killer_id].score += 100
    players[victim_id].deaths += 1
    players[victim_id].alive  = false
    return { ok: true }
}

reducer get_leaderboard(count: int) {
    let top     = top_n("players", "score", count)
    let total   = count_rows("players")
    let avg_kdr = avg_field("players", "kills")
    return {
        leaderboard:   top,
        total_players: total,
        avg_kills:     avg_kdr,
        top_score:     max_field("players", "score"),
    }
}

reducer alive_count() {
    let all_alive = find_all("players", "alive", true)
    return {
        alive: array_len(all_alive),
        total: count_rows("players"),
    }
}
```
