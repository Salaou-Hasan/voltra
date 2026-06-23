# Tables

Tables are how Voltra stores game data. Every player, item, guild, room, and leaderboard entry lives in a table.

---

## Declaring a Table

```voltra
table players {
    hp:    int   = 100,
    alive: bool  = true,
    x:     float = 0.0,
    name:  str   = "",
}
```

The syntax is:

```
table <table_name> {
    <field_name>: <type> = <default>,
    ...
}
```

Every field needs a **default value**. This is required — if a reducer writes a partial row (missing some fields), the missing fields are filled in with their defaults automatically.

---

## Field Types

Voltra has four types:

| Type | Description | Example values |
|---|---|---|
| `int` | Whole number (64-bit signed) | `0`, `100`, `-50`, `999999` |
| `float` | Decimal number (64-bit) | `0.0`, `3.14`, `-1.5`, `100.0` |
| `str` | Text string | `""`, `"Alice"`, `"zone_3"` |
| `bool` | True or false | `true`, `false` |

Arrays and nested objects can also be stored in rows — they are written as JSON values and Voltra stores them as-is. Field type declarations only cover the top-level fields.

---

## What a Table Actually Is

Under the hood, a Voltra table is a **key-value store** where:
- Each **row key** is a string you choose (like a player ID or item ID)
- Each **row value** is a JSON object

When you write:
```voltra
players["alice"] = { hp: 100, alive: true, x: 0.0, name: "Alice" }
```

Voltra stores the string `"alice"` as the key and the object as the value, in a DashMap-backed in-memory store backed by a write-ahead log (WAL) on disk.

Row keys are always strings, even if the keys look like numbers. `players["1"]` and `players[1]` are the same — the integer `1` is automatically converted to the string `"1"` when used as a row key.

---

## Table Data is Persisted

Every write to a table is logged to the WAL before the response is sent to the client. If the server restarts, it replays the WAL and restores all table data automatically. You do not need to do anything special for persistence — it is always on.

---

## Table Declarations are Documentation

The table declaration in `reducers.vol` tells Voltra the intended structure of your rows. The server uses it for:
- Editor tooling and autocomplete
- Default value filling when a row is written with missing fields
- Documentation for other developers

The server does **not** enforce types by default. If you write `players["alice"].hp = "oops"`, the string gets stored as-is. To enforce strict types, use `schema.toml` (see [02 — Project Structure](02-project-structure.md)).

---

## Example Tables

### Players

```voltra
table players {
    hp:       int   = 100,
    max_hp:   int   = 100,
    alive:    bool  = true,
    x:        float = 0.0,
    y:        float = 0.0,
    zone:     str   = "spawn",
    name:     str   = "",
    level:    int   = 1,
    xp:       int   = 0,
    gold:     int   = 0,
}
```

Row keys for players are usually the player's connection ID, user ID, or a slug like `"alice"`.

### Items

```voltra
table items {
    name:     str   = "",
    owner_id: str   = "",
    slot:     str   = "inventory",
    damage:   int   = 0,
    rarity:   str   = "common",
    quantity: int   = 1,
}
```

Row keys for items might be a UUID or `"<owner_id>_<slot>"` depending on your game design.

### Guilds

```voltra
table guilds {
    name:        str  = "",
    owner_id:    str  = "",
    member_count: int = 1,
    level:       int  = 1,
    open:        bool = true,
}
```

Row keys for guilds could be a generated slug like `"shadow-wolves-4821"`.

### Rooms (Chat or Game)

```voltra
table rooms {
    name:         str  = "",
    owner_id:     str  = "",
    member_count: int  = 0,
    max_members:  int  = 100,
    private:      bool = false,
    topic:        str  = "",
}
```

### Leaderboard

```voltra
table leaderboard {
    player_name: str   = "",
    score:       int   = 0,
    rank:        int   = 0,
    updated_at:  int   = 0,
}
```

Row keys for leaderboard entries are usually the player ID so each player has exactly one entry.

---

## Multiple Tables

A single `reducers.vol` file can declare any number of tables:

```voltra
table players {
    hp:   int  = 100,
    name: str  = "",
}

table items {
    name:     str = "",
    owner_id: str = "",
}

table guilds {
    name:     str  = "",
    owner_id: str  = "",
}

reducer create_guild(guild_id: str, name: str) {
    guilds[guild_id] = { name: name, owner_id: caller_id }
    return { ok: true }
}
```

Reducers can read and write any table. There is no restriction on which tables a reducer can access.

---

## No Joins, No Foreign Keys

Voltra tables are independent key-value stores. There are no SQL-style joins or foreign key constraints. If you need to look up related data, you do it manually in your reducer:

```voltra
reducer get_guild_owner_hp(guild_id: str) {
    let guild = guilds[guild_id] else { error("guild not found") }
    let owner = players[guild.owner_id] else { error("owner not in game") }
    return { hp: owner.hp }
}
```

This design keeps the server fast — there are no query planners, no index rebuilds, and no lock escalation chains. Every read is an O(1) key lookup.
