// Embedded template content, the template/module registries, and the
// `VOLTRA_SOURCE_DIR` path constant. All loaded from `templates/` and
// `engine_templates/` at compile time. Pure data — no logic lives here.

// ─────────────────────────────────────────────────────────────────────────────
// Template registry
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct Template {
    pub(crate) name: &'static str,
    pub(crate) category: &'static str,
    pub(crate) description: &'static str,
}

pub(crate) const TEMPLATES: &[Template] = &[
    // ── Voltra Language (DSL → native Rust, zero interpreted layer) ─────────────
    Template { name: "voltra/basic",      category: "Voltra Language", description: "Spawn, move, despawn, combat — write game logic in .vol DSL, compiles to native Rust speed." },
    Template { name: "voltra/game-ready", category: "Voltra Language", description: "Full game in .vol: combat, economy, guilds, quests, leaderboard, chat — compile once, run forever." },
    Template { name: "voltra/chat",       category: "Voltra Language", description: "Chat rooms, presence, moderation — minimal .vol server you can understand in 5 minutes." },
    // ── Rust (handwritten native reducers) ────────────────────────────────────
    Template { name: "game/basic", category: "Rust", description: "Spawn, move, despawn, health — the minimal multiplayer foundation. Add modules with `voltra add`." },
    Template { name: "game/full",  category: "Rust", description: "All modules pre-configured: combat, inventory, economy, matchmaking, guilds, quests, leaderboard, chat, world." },
    Template { name: "game/unity", category: "Unity",       description: "Unity C# SDK + full game server. Copy unity/ into Assets/Scripts/Voltra/, configure URL, play." },
    Template { name: "game/godot", category: "Godot 4",     description: "Godot GDScript SDK + full game server. Add godot/ as an autoload, configure URL, play." },
];

/// Available add-on modules (`voltra add <name>`).
pub(crate) const MODULES: &[(&str, &str)] = &[
    ("chat", "Rooms, messages, per-room presence"),
    ("inventory", "Items, qty stacking, equip slots"),
    ("leaderboard", "Score submit, global top-N, weekly reset"),
    ("matchmaking", "Queue, ELO-pair, match creation (scheduled)"),
    ("guilds", "Create, invite, accept, kick"),
    ("quests", "Accept, progress tracking, claim reward"),
    (
        "economy",
        "Gold/gem wallets, shop buy/sell, transfers, loot boxes",
    ),
    ("combat", "Attack, ability system, NPC damage, respawn"),
    (
        "world",
        "World tick, NPC spawn, session cleanup (scheduled)",
    ),
    // ── Voltra V1 runtime modules (also usable via `voltra init --genre` /
    //    `--modules`, see `voltra::runtime::builtin_genres()`) ──────────────
    ("sessions", "Session-token issue/validate/revoke"),
    ("lobby", "Lobby create/join/leave with capacity tracking"),
    ("tick", "Per-lobby tick counter bookkeeping"),
    (
        "ecs",
        "Hot-state entity records: transform, velocity, alive",
    ),
    ("aoi", "Area-of-interest view distance + grid cell tracking"),
    ("delta", "Per-client delta cursor ack/reset bookkeeping"),
    ("runtime-persistence", "Lobby snapshot recording + pruning"),
    ("movement", "Input + movement integration over ecs entities"),
    ("weapons", "Weapon equip, fire (ammo/cooldown), reload"),
    ("hit-detection", "Hit claim submission and recording"),
    ("equipment", "Equipped-item stat slots (loadout)"),
    ("parties", "Party create/invite/leave with membership"),
    ("replay", "Tick/delta frame capture and pruning"),
];

/// Path to the Voltra source on the machine that compiled this binary.
/// Used to add a [patch] section so scaffolded projects build offline.
pub(crate) const VOLTRA_SOURCE_DIR: &str = env!("CARGO_MANIFEST_DIR");

// ═══════════════════════════════════════════════════════════════════════════════
// Embedded template content — loaded from templates/ at compile time
// ═══════════════════════════════════════════════════════════════════════════════

pub(crate) const MIGRATIONS_README: &str = "# Migrations\nPlace `.toml` files here.\n";
pub(crate) const SCALING_MD: &str = include_str!("../../templates/scaling.md.txt");

// ── Voltra language template content (inline — no extra template files needed) ──
/// VS Code language association — makes .vol files use Rust syntax highlighting.
pub(crate) const VSCODE_VOLTRA_SETTINGS: &str = r#"{
  "files.associations": {
    "*.vol": "voltra"
  }
}
"#;

/// Concatenate a slice of string slices with a newline between each.
pub(crate) fn concat_strs(parts: &[&str]) -> String {
    parts.join("\n")
}

// ── Voltra language reference (written to docs/voltra/README.md) ────────────────

pub(crate) const VOLTRA_LANG_REFERENCE: &str = r#"# Voltra Language Reference

Voltra is Voltra's built-in language for writing game-server logic. Files live in
`reducers/`, compile to native Rust with `voltra build`, and run at full speed —
no interpreter, no overhead.

---

## Tables

Declare persistent tables with typed columns and default values:

```voltra
table players {
    hp:    int   = 100,
    alive: bool  = true,
    x:     float = 0.0,
    name:  str   = "",
}
```

Types: `int` (i64), `float` (f64), `bool`, `str`.

---

## Reducers

Entry points called by clients over WebSocket:

```voltra
reducer spawn(player_id: str, name: str) {
    players[player_id] = { hp: 100, alive: true, name: name }
    return { ok: true }
}
```

Parameters are typed (`str`, `int`, `float`, `bool`).

---

## Row operations

| Syntax | What it does |
|--------|-------------|
| `table[key] = { field: val, ... }` | Insert / replace a row |
| `let p = table[key] else { error("msg") }` | Read row or handle missing |
| `delete table[key]` | Delete a row |
| `p.field` | Read a field |
| `table[key].field = expr` | Update a single field |
| `table[key].field += expr` | Increment in place |

---

## Control flow

```voltra
if hp <= 0 {
    players[id].alive = false
} else if hp <= 25 {
    // critical
} else {
    // healthy
}

while cur_xp >= cur_lvl * 100 {
    cur_xp  = cur_xp - cur_lvl * 100
    cur_lvl = cur_lvl + 1
}

for id, p in players {
    if p.alive == false { delete players[id] }
}
```

---

## Return values

```voltra
return { ok: true, hp: new_hp }   // send data back to the client
error("Player not found")          // return an error to the client
```

---

## Built-in functions

### Counters (persistent global integers)
```voltra
let n = counter("online")          // read counter (returns int, 0 if missing)
set_counter("online", n + 1)       // write counter
```

### Time
```voltra
let ts = timestamp()               // server time as int (nanoseconds)
```

### Math
```voltra
min(a, b)  max(a, b)  abs(x)
floor(x)   ceil(x)    round(x)   sqrt(x)   pow(x, e)
clamp(x, lo, hi)      sign(x)    log2(x)   log10(x)
```

### Random
```voltra
let roll = rand_int(1, 100)        // seeded from timestamp
```

### Strings
```voltra
concat("Hello, ", name)
to_upper(s)   to_lower(s)   trim(s)
len(s)        contains(s, sub)
split(s, sep) index_of(s, sub)  substring(s, start, end)
str(42)       // int/float → str
int(s)        // str → int
```

### Arrays
```voltra
let arr = [1, 2, 3]
push(arr, 4)
pop(arr)
let n = array_len(arr)
let v = get_index(arr, 0)
remove_at(arr, 0)
```

### Table queries
```voltra
let n   = count_rows("players")
let s   = sum_field("players", "score")
let avg = avg_field("players", "score")
let top = top_n("players", "score", 10)           // top-10 rows by field
let all = sort_by("players", "hp", "desc")
let one = find_first("players", "alive", true)
```

### Caller identity (set by the client's auth token)
```voltra
let id   = caller_id    // string — who made the call
let role = caller_role  // string — their role ("admin", "user", etc.)
```

---

## Workflow

```
1. Edit files in reducers/
2. voltra build      ← compiles .vol → src/reducers.rs → native binary
3. voltra start      ← starts the server
```

Changes to `.vol` files require `voltra build` before they take effect.
"#;

// ── voltra/basic per-file constants ────────────────────────────────────────────

pub(crate) const VOLTRA_BASIC_SCHEMA: &str = r#"// schema.vol — table definitions
// Add fields here, then run: voltra build

table players {
    hp:    int   = 100,
    alive: bool  = true,
    x:     float = 0.0,
    y:     float = 0.0,
    kills: int   = 0,
    name:  str   = "",
}
"#;

pub(crate) const VOLTRA_BASIC_SPAWN: &str = r#"// spawn.vol — player lifecycle

reducer spawn(player_id: str, name: str, x: float, y: float) {
    players[player_id] = { hp: 100, alive: true, x: x, y: y, kills: 0, name: name }
    set_counter("online", counter("online") + 1)
    return { ok: true, player_id: player_id }
}

reducer despawn(player_id: str) {
    let p = players[player_id] else { error("Player not found") }
    delete players[player_id]
    set_counter("online", counter("online") - 1)
    return { ok: true }
}
"#;

pub(crate) const VOLTRA_BASIC_MOVEMENT: &str = r#"// movement.vol — position updates

reducer move_player(player_id: str, x: float, y: float) {
    let p = players[player_id] else { error("Player not found") }
    players[player_id].x = x
    players[player_id].y = y
    return { ok: true, x: x, y: y }
}
"#;

pub(crate) const VOLTRA_BASIC_COMBAT: &str = r#"// combat.vol — damage & healing

reducer damage(target_id: str, amount: int) {
    let p = players[target_id] else { error("Player not found") }
    let new_hp = max(0, p.hp - amount)
    players[target_id].hp = new_hp
    if new_hp <= 0 {
        players[target_id].alive = false
    }
    return { hp: new_hp, alive: new_hp > 0 }
}

reducer heal(target_id: str, amount: int) {
    let p = players[target_id] else { error("Player not found") }
    let new_hp = min(100, p.hp + amount)
    players[target_id].hp = new_hp
    return { hp: new_hp }
}
"#;

pub(crate) const VOLTRA_BASIC_SYSTEM: &str = r#"// system.vol — stats & scheduled maintenance

reducer get_stats() {
    let online = counter("online")
    let total  = count_rows("players")
    let ts     = timestamp()
    return { online: online, total_players: total, server_time: ts }
}

// Add to voltra.toml [[scheduler]] to run automatically
reducer cleanup_dead() {
    let removed = 0
    for id, p in players {
        if p.alive == false {
            delete players[id]
            removed = removed + 1
        }
    }
    return { removed: removed }
}
"#;

// ── voltra/game-ready per-file constants ───────────────────────────────────────

pub(crate) const VOLTRA_GAME_SCHEMA: &str = r#"// schema.vol — table definitions

table players {
    hp:     int   = 100,
    max_hp: int   = 100,
    level:  int   = 1,
    xp:     int   = 0,
    alive:  bool  = true,
    x:      float = 0.0,
    y:      float = 0.0,
    kills:  int   = 0,
    gold:   int   = 0,
    name:   str   = "",
    guild:  str   = "",
}

table guilds {
    owner:        str   = "",
    member_count: int   = 0,
    score:        float = 0.0,
    name:         str   = "",
}
"#;

pub(crate) const VOLTRA_GAME_SPAWN: &str = r#"// spawn.vol — player lifecycle

reducer spawn(player_id: str, name: str, x: float, y: float) {
    players[player_id] = { hp: 100, max_hp: 100, level: 1, xp: 0,
                           alive: true, x: x, y: y, kills: 0,
                           gold: 50, name: name, guild: "" }
    set_counter("total_players", counter("total_players") + 1)
    return { ok: true, player_id: player_id }
}

reducer despawn(player_id: str) {
    let p = players[player_id] else { error("Player not found") }
    delete players[player_id]
    set_counter("total_players", counter("total_players") - 1)
    return { ok: true }
}
"#;

pub(crate) const VOLTRA_GAME_MOVEMENT: &str = r#"// movement.vol — position updates

reducer move_player(player_id: str, x: float, y: float) {
    let p = players[player_id] else { error("Player not found") }
    players[player_id].x = x
    players[player_id].y = y
    return { ok: true, x: x, y: y }
}
"#;

pub(crate) const VOLTRA_GAME_COMBAT: &str = r#"// combat.vol — damage & healing

reducer take_damage(player_id: str, amount: int, attacker_id: str) {
    let p = players[player_id] else { error("Player not found") }
    let new_hp = max(0, p.hp - amount)
    players[player_id].hp = new_hp
    if new_hp <= 0 {
        players[player_id].alive = false
        let killer = players[attacker_id] else { return { died: true, killer: "unknown" } }
        players[attacker_id].kills += 1
        set_counter("total_kills", counter("total_kills") + 1)
        return { died: true, killer: attacker_id }
    } else if new_hp <= 25 {
        return { died: false, hp: new_hp, status: "critical" }
    } else if new_hp <= 50 {
        return { died: false, hp: new_hp, status: "wounded" }
    } else {
        return { died: false, hp: new_hp, status: "healthy" }
    }
}

reducer heal(player_id: str, amount: int) {
    let p = players[player_id] else { error("Player not found") }
    let new_hp = min(p.max_hp, p.hp + amount)
    players[player_id].hp = new_hp
    return { hp: new_hp }
}
"#;

pub(crate) const VOLTRA_GAME_PROGRESSION: &str = r#"// progression.vol — XP, leveling, loot

reducer grant_xp(player_id: str, amount: int) {
    let p = players[player_id] else { error("Player not found") }
    let cur_xp  = p.xp + amount
    let cur_lvl = p.level
    while cur_xp >= cur_lvl * 100 {
        cur_xp  = cur_xp - cur_lvl * 100
        cur_lvl = cur_lvl + 1
    }
    players[player_id].level = cur_lvl
    players[player_id].xp    = cur_xp
    return { level: cur_lvl, xp: cur_xp }
}

reducer roll_loot(player_id: str) {
    let roll = rand_int(1, 100)
    if roll >= 90 {
        return { rarity: "legendary", roll: roll }
    } else if roll >= 60 {
        return { rarity: "rare",      roll: roll }
    } else if roll >= 30 {
        return { rarity: "uncommon",  roll: roll }
    } else {
        return { rarity: "common",    roll: roll }
    }
}
"#;

pub(crate) const VOLTRA_GAME_ECONOMY: &str = r#"// economy.vol — gold transfers

reducer transfer_gold(from_id: str, to_id: str, amount: int) {
    let from = players[from_id] else { error("Sender not found") }
    let to   = players[to_id]   else { error("Recipient not found") }
    if from.gold < amount {
        error("Insufficient gold")
    }
    players[from_id].gold -= amount
    players[to_id].gold   += amount
    return { ok: true, transferred: amount }
}
"#;

pub(crate) const VOLTRA_GAME_GUILDS: &str = r#"// guilds.vol — guild management

reducer create_guild(guild_id: str, name: str) {
    let owner = caller_id
    guilds[guild_id] = { owner: owner, member_count: 1, score: 0.0, name: name }
    players[owner].guild = guild_id
    return { ok: true, guild_id: guild_id }
}

reducer join_guild(guild_id: str) {
    let player_id = caller_id
    let g = guilds[guild_id] else { error("Guild not found") }
    guilds[guild_id].member_count += 1
    players[player_id].guild = guild_id
    return { ok: true }
}

reducer leave_guild() {
    let player_id = caller_id
    let p = players[player_id] else { error("Player not found") }
    let gid = p.guild
    guilds[gid].member_count -= 1
    players[player_id].guild = ""
    return { ok: true }
}
"#;

pub(crate) const VOLTRA_GAME_LEADERBOARD: &str = r#"// leaderboard.vol — rankings

reducer leaderboard(field: str) {
    let rows = sort_by("players", field, "desc")
    return { rows: rows }
}

reducer top_killers() {
    let top = top_n("players", "kills", 10)
    return { top: top }
}
"#;

pub(crate) const VOLTRA_GAME_SYSTEM: &str = r#"// system.vol — stats & scheduled maintenance

reducer get_stats() {
    let total  = count_rows("players")
    let kills  = counter("total_kills")
    let avg_k  = avg_field("players", "kills")
    let ts     = timestamp()
    return { total_players: total, total_kills: kills, avg_kills: avg_k, server_time: ts }
}

// Add to voltra.toml [[scheduler]] to run automatically
reducer cleanup_dead() {
    let removed = 0
    for id, p in players {
        if p.alive == false {
            delete players[id]
            removed = removed + 1
        }
    }
    return { removed: removed }
}
"#;

// ── voltra/chat per-file constants ─────────────────────────────────────────────

pub(crate) const VOLTRA_CHAT_SCHEMA_VOLTRA: &str = r#"// schema.vol — table definitions

table rooms {
    name:         str = "",
    member_count: int = 0,
    created_by:   str = "",
}

table room_members {
    room:   str = "",
    player: str = "",
}

table messages {
    room:   str = "",
    sender: str = "",
    text:   str = "",
    ts:     int = 0,
}
"#;

pub(crate) const VOLTRA_CHAT_ROOMS: &str = r#"// rooms.vol — room lifecycle

reducer create_room(room_id: str, name: str) {
    let creator = caller_id
    rooms[room_id] = { name: name, member_count: 0, created_by: creator }
    return { ok: true, room_id: room_id }
}

reducer join_room(room_id: str) {
    let player_id = caller_id
    let r = rooms[room_id] else { error("Room not found") }
    let member_key = concat(room_id, concat(":", player_id))
    room_members[member_key] = { room: room_id, player: player_id }
    rooms[room_id].member_count += 1
    return { ok: true, room: room_id, members: r.member_count + 1 }
}

reducer leave_room(room_id: str) {
    let player_id = caller_id
    let member_key = concat(room_id, concat(":", player_id))
    let r = rooms[room_id] else { return { ok: true } }
    delete room_members[member_key]
    rooms[room_id].member_count -= 1
    return { ok: true }
}
"#;

pub(crate) const VOLTRA_CHAT_MESSAGES: &str = r#"// messages.vol — messaging & room listing

reducer send_message(room_id: str, text: str) {
    let sender = caller_id
    let r = rooms[room_id] else { error("Room not found") }
    let trimmed = trim(text)
    if len(trimmed) == 0 {
        error("Message cannot be empty")
    }
    let msg_key = concat(room_id, concat(":", str(timestamp())))
    messages[msg_key] = { room: room_id, sender: sender, text: trimmed, ts: timestamp() }
    set_counter("total_messages", counter("total_messages") + 1)
    return { ok: true, room: room_id }
}

reducer list_rooms() {
    let rows = sort_by("rooms", "member_count", "desc")
    return { rooms: rows }
}
"#;

pub(crate) const VOLTRA_CHAT_SYSTEM: &str = r#"// system.vol — presence, moderation & cleanup

reducer online_count() {
    let count = count_rows("room_members")
    let msgs  = counter("total_messages")
    return { online: count, total_messages: msgs }
}

reducer room_members(room_id: str) {
    let members = find_all("room_members", "room", room_id)
    return { room: room_id, members: members, count: array_len(members) }
}

reducer kick_from_room(room_id: str, target_id: str) {
    let requester = caller_id
    let r = rooms[room_id] else { error("Room not found") }
    if r.created_by != requester {
        error("Only the room creator can kick members")
    }
    let member_key = concat(room_id, concat(":", target_id))
    delete room_members[member_key]
    rooms[room_id].member_count -= 1
    return { ok: true, kicked: target_id }
}

// Add to voltra.toml [[scheduler]] to run automatically
reducer cleanup_old_messages() {
    let cutoff = timestamp() - 86400000000000
    let removed = 0
    for id, m in messages {
        if m.ts < cutoff {
            delete messages[id]
            removed = removed + 1
        }
    }
    return { removed: removed }
}
"#;

// ── Legacy single-file constants (kept for backward compatibility) ────────────

pub(crate) const VOLTRA_CHAT_SCHEMA: &str = r#"# Chat server schema
[[table]]
name = "rooms"

[[table.column]]
name = "name"
type = "string"

[[table.column]]
name = "member_count"
type = "integer"

[[table.column]]
name = "created_by"
type = "string"

[[table]]
name = "room_members"

[[table.column]]
name = "room"
type = "string"

[[table.column]]
name = "player"
type = "string"

[[table]]
name = "messages"

[[table.column]]
name = "room"
type = "string"

[[table.column]]
name = "sender"
type = "string"

[[table.column]]
name = "text"
type = "string"

[[table.column]]
name = "ts"
type = "integer"
"#;

// ── Voltra module snippets (appended to reducers.vol by `voltra add <module>`) ─
pub(crate) const VOLTRA_MOD_CHAT: &str = r#"
// ── chat module ───────────────────────────────────────────────────────────────
table chat_messages {
    room:   str = "",
    sender: str = "",
    text:   str = "",
    ts:     int = 0,
}

table chat_members {
    room:   str = "",
    player: str = "",
}

reducer join_room(room: str) {
    let player_id = caller_id
    let key = concat(room, concat(":", player_id))
    chat_members[key] = { room: room, player: player_id }
    set_counter(concat("room_count:", room), counter(concat("room_count:", room)) + 1)
    return { ok: true, room: room }
}

reducer leave_room(room: str) {
    let player_id = caller_id
    let key = concat(room, concat(":", player_id))
    delete chat_members[key]
    set_counter(concat("room_count:", room), counter(concat("room_count:", room)) - 1)
    return { ok: true }
}

reducer send_message(room: str, text: str) {
    let sender = caller_id
    let trimmed = trim(text)
    if len(trimmed) == 0 { error("Message cannot be empty") }
    let key = concat(room, concat(":", str(timestamp())))
    chat_messages[key] = { room: room, sender: sender, text: trimmed, ts: timestamp() }
    return { ok: true }
}

reducer cleanup_chat() {
    let cutoff = timestamp() - 86400000000000
    for id, m in chat_messages {
        if m.ts < cutoff { delete chat_messages[id] }
    }
}
"#;

pub(crate) const VOLTRA_MOD_INVENTORY: &str = r#"
// ── inventory module ──────────────────────────────────────────────────────────
table inventories {
    owner: str = "",
    item:  str = "",
    qty:   int = 0,
    slot:  int = 0,
}

reducer add_item(owner_id: str, item: str, qty: int) {
    let key = concat(owner_id, concat(":", item))
    let existing = inventories[key] else {
        inventories[key] = { owner: owner_id, item: item, qty: qty, slot: 0 }
        return { ok: true, qty: qty }
    }
    let new_qty = existing.qty + qty
    inventories[key].qty = new_qty
    return { ok: true, qty: new_qty }
}

reducer remove_item(owner_id: str, item: str, qty: int) {
    let key = concat(owner_id, concat(":", item))
    let existing = inventories[key] else { error("Item not in inventory") }
    if existing.qty < qty { error("Not enough quantity") }
    let new_qty = existing.qty - qty
    if new_qty == 0 {
        delete inventories[key]
    } else {
        inventories[key].qty = new_qty
    }
    return { ok: true, qty: new_qty }
}

reducer get_inventory(owner_id: str) {
    let items = find_all("inventories", "owner", owner_id)
    return { items: items, count: array_len(items) }
}
"#;

pub(crate) const VOLTRA_MOD_LEADERBOARD: &str = r#"
// ── leaderboard module ────────────────────────────────────────────────────────
table scores {
    player: str   = "",
    score:  int   = 0,
    name:   str   = "",
}

reducer submit_score(score: int) {
    let player_id = caller_id
    let existing = scores[player_id] else {
        scores[player_id] = { player: player_id, score: score, name: player_id }
        return { ok: true, score: score }
    }
    if score > existing.score {
        scores[player_id].score = score
        return { ok: true, score: score, improved: true }
    }
    return { ok: true, score: existing.score, improved: false }
}

reducer get_leaderboard() {
    let top = top_n("scores", "score", 100)
    return { leaderboard: top }
}

reducer reset_leaderboard() {
    for id, s in scores { delete scores[id] }
    return { ok: true }
}
"#;

pub(crate) const VOLTRA_MOD_ECONOMY: &str = r#"
// ── economy module ────────────────────────────────────────────────────────────
table wallets {
    player: str = "",
    gold:   int = 0,
    gems:   int = 0,
}

reducer add_gold(player_id: str, amount: int) {
    let existing = wallets[player_id] else {
        wallets[player_id] = { player: player_id, gold: amount, gems: 0 }
        return { ok: true, gold: amount }
    }
    wallets[player_id].gold += amount
    return { ok: true, gold: existing.gold + amount }
}

reducer spend_gold(player_id: str, amount: int) {
    let w = wallets[player_id] else { error("Wallet not found") }
    if w.gold < amount { error("Insufficient gold") }
    wallets[player_id].gold -= amount
    return { ok: true, gold: w.gold - amount }
}

reducer transfer_gold(to_id: str, amount: int) {
    let from_id = caller_id
    let from = wallets[from_id] else { error("Sender wallet not found") }
    let to   = wallets[to_id]   else { error("Recipient wallet not found") }
    if from.gold < amount { error("Insufficient gold") }
    wallets[from_id].gold -= amount
    wallets[to_id].gold   += amount
    return { ok: true, transferred: amount }
}

reducer get_wallet(player_id: str) {
    let w = wallets[player_id] else { return { gold: 0, gems: 0 } }
    return { gold: w.gold, gems: w.gems }
}
"#;

pub(crate) const VOLTRA_MOD_GUILDS: &str = r#"
// ── guilds module ─────────────────────────────────────────────────────────────
table guilds {
    name:         str   = "",
    owner:        str   = "",
    member_count: int   = 0,
    score:        float = 0.0,
}

table guild_members {
    guild:  str = "",
    player: str = "",
    rank:   str = "member",
}

reducer create_guild(guild_id: str, name: str) {
    let owner = caller_id
    guilds[guild_id] = { name: name, owner: owner, member_count: 1, score: 0.0 }
    let key = concat(guild_id, concat(":", owner))
    guild_members[key] = { guild: guild_id, player: owner, rank: "owner" }
    return { ok: true, guild_id: guild_id }
}

reducer join_guild(guild_id: str) {
    let player_id = caller_id
    let g = guilds[guild_id] else { error("Guild not found") }
    let key = concat(guild_id, concat(":", player_id))
    guild_members[key] = { guild: guild_id, player: player_id, rank: "member" }
    guilds[guild_id].member_count += 1
    return { ok: true }
}

reducer leave_guild(guild_id: str) {
    let player_id = caller_id
    let key = concat(guild_id, concat(":", player_id))
    delete guild_members[key]
    guilds[guild_id].member_count -= 1
    return { ok: true }
}

reducer kick_member(guild_id: str, target_id: str) {
    let requester = caller_id
    let g = guilds[guild_id] else { error("Guild not found") }
    if g.owner != requester { error("Only guild owner can kick members") }
    let key = concat(guild_id, concat(":", target_id))
    delete guild_members[key]
    guilds[guild_id].member_count -= 1
    return { ok: true, kicked: target_id }
}

reducer get_guild_members(guild_id: str) {
    let members = find_all("guild_members", "guild", guild_id)
    return { members: members, count: array_len(members) }
}
"#;

pub(crate) const VOLTRA_MOD_QUESTS: &str = r#"
// ── quests module ─────────────────────────────────────────────────────────────
table quest_progress {
    player:   str  = "",
    quest_id: str  = "",
    progress: int  = 0,
    goal:     int  = 1,
    done:     bool = false,
    claimed:  bool = false,
}

reducer accept_quest(quest_id: str, goal: int) {
    let player_id = caller_id
    let key = concat(player_id, concat(":", quest_id))
    quest_progress[key] = { player: player_id, quest_id: quest_id,
                            progress: 0, goal: goal, done: false, claimed: false }
    return { ok: true, quest_id: quest_id }
}

reducer advance_quest(quest_id: str, amount: int) {
    let player_id = caller_id
    let key = concat(player_id, concat(":", quest_id))
    let q = quest_progress[key] else { error("Quest not accepted") }
    if q.done { return { already_done: true } }
    let new_progress = min(q.progress + amount, q.goal)
    quest_progress[key].progress = new_progress
    if new_progress >= q.goal {
        quest_progress[key].done = true
        return { done: true, progress: new_progress }
    }
    return { done: false, progress: new_progress }
}

reducer claim_quest(quest_id: str) {
    let player_id = caller_id
    let key = concat(player_id, concat(":", quest_id))
    let q = quest_progress[key] else { error("Quest not found") }
    if q.done == false { error("Quest not completed") }
    if q.claimed { error("Reward already claimed") }
    quest_progress[key].claimed = true
    return { ok: true, quest_id: quest_id }
}
"#;

pub(crate) const VOLTRA_MOD_COMBAT: &str = r#"
// ── combat module ─────────────────────────────────────────────────────────────
reducer attack(target_id: str, damage: int) {
    let attacker_id = caller_id
    let target = players[target_id] else { error("Target not found") }
    let crit = rand_int(1, 100)
    let final_dmg = if crit >= 95 { damage * 2 } else { damage }
    let new_hp = max(0, target.hp - final_dmg)
    players[target_id].hp = new_hp
    if new_hp <= 0 {
        players[target_id].alive = false
        players[attacker_id].kills += 1
        return { hit: true, damage: final_dmg, crit: crit >= 95, killed: true }
    }
    return { hit: true, damage: final_dmg, crit: crit >= 95, killed: false }
}

reducer respawn(player_id: str, x: float, y: float) {
    let p = players[player_id] else { error("Player not found") }
    players[player_id].hp    = 100
    players[player_id].alive = true
    players[player_id].x     = x
    players[player_id].y     = y
    return { ok: true }
}

reducer use_ability(target_id: str, ability: str) {
    let attacker_id = caller_id
    let target = players[target_id] else { error("Target not found") }
    let dmg = if ability == "fireball" { rand_int(30, 60) } else if ability == "ice_lance" { rand_int(20, 40) } else { rand_int(10, 25) }
    let new_hp = max(0, target.hp - dmg)
    players[target_id].hp = new_hp
    return { ability: ability, damage: dmg, target_hp: new_hp }
}
"#;

pub(crate) const VOLTRA_MOD_MATCHMAKING: &str = r#"
// ── matchmaking module ────────────────────────────────────────────────────────
table mm_queue {
    player: str   = "",
    rating: float = 1000.0,
    ts:     int   = 0,
}

table matches {
    player1: str = "",
    player2: str = "",
    status:  str = "pending",
}

reducer queue_up(rating: float) {
    let player_id = caller_id
    mm_queue[player_id] = { player: player_id, rating: rating, ts: timestamp() }
    return { ok: true, position: count_rows("mm_queue") }
}

reducer leave_queue() {
    let player_id = caller_id
    delete mm_queue[player_id]
    return { ok: true }
}

reducer mm_match() {
    let waiting = sort_by("mm_queue", "ts", "asc")
    let n = array_len(waiting)
    let paired = 0
    let i = 0
    while i + 1 < n {
        let p1 = get_index(waiting, i)
        let p2 = get_index(waiting, i + 1)
        let match_id = concat(str(timestamp()), concat(":", str(i)))
        matches[match_id] = { player1: p1, player2: p2, status: "active" }
        delete mm_queue[p1]
        delete mm_queue[p2]
        paired = paired + 2
        i = i + 2
    }
    return { paired: paired, remaining: n - paired }
}
"#;

pub(crate) const VOLTRA_MOD_WORLD: &str = r#"
// ── world module ──────────────────────────────────────────────────────────────
table zones {
    name:        str = "",
    player_count: int = 0,
    max_players: int = 100,
}

table portals {
    from_zone: str = "",
    to_zone:   str = "",
    x:         float = 0.0,
    y:         float = 0.0,
}

reducer enter_zone(zone_id: str) {
    let player_id = caller_id
    let z = zones[zone_id] else { error("Zone not found") }
    if z.player_count >= z.max_players { error("Zone is full") }
    zones[zone_id].player_count += 1
    return { ok: true, zone: zone_id, count: z.player_count + 1 }
}

reducer leave_zone(zone_id: str) {
    let player_id = caller_id
    let z = zones[zone_id] else { return { ok: true } }
    zones[zone_id].player_count -= 1
    return { ok: true }
}

reducer create_zone(zone_id: str, name: str, max_players: int) {
    zones[zone_id] = { name: name, player_count: 0, max_players: max_players }
    return { ok: true, zone_id: zone_id }
}

reducer world_tick() {
    let total = count_rows("zones")
    return { zones_active: total, ts: timestamp() }
}
"#;

// ── Rust game templates ───────────────────────────────────────────────────────
pub(crate) const GAME_MAIN_RS: &str = include_str!("../../templates/r_game_main.rs.txt");
pub(crate) const R_MOD_BASIC: &str = include_str!("../../templates/r_reducers_mod_basic.rs.txt");
pub(crate) const R_SPAWN_RS: &str = include_str!("../../templates/r_spawn.rs.txt");
pub(crate) const R_MOVE_RS: &str = include_str!("../../templates/r_move.rs.txt");
pub(crate) const R_DESPAWN_RS: &str = include_str!("../../templates/r_despawn.rs.txt");
pub(crate) const R_DAMAGE_RS: &str = include_str!("../../templates/r_damage.rs.txt");
pub(crate) const R_HEAL_RS: &str = include_str!("../../templates/r_heal.rs.txt");
pub(crate) const R_BASIC_SCHEMA: &str = include_str!("../../templates/r_basic_schema.toml.txt");

// ── module reducers (voltra add <name>) ──────────────────────────────────────
pub(crate) const RM_CHAT_MOD_RS: &str = include_str!("../../templates/rm_chat_mod.rs.txt");
pub(crate) const RM_CHAT_SEND_RS: &str = include_str!("../../templates/rm_chat_send.rs.txt");
pub(crate) const RM_CHAT_JOIN_RS: &str = include_str!("../../templates/rm_chat_join.rs.txt");
pub(crate) const RM_CHAT_LEAVE_RS: &str = include_str!("../../templates/rm_chat_leave.rs.txt");
pub(crate) const RM_CHAT_CLEANUP_RS: &str = include_str!("../../templates/rm_chat_cleanup.rs.txt");
pub(crate) const RM_CHAT_SCHEMA: &str = include_str!("../../templates/rm_chat_schema.toml.txt");
pub(crate) const RM_INV_MOD_RS: &str = include_str!("../../templates/rm_inventory_mod.rs.txt");
pub(crate) const RM_INV_ADD_RS: &str = include_str!("../../templates/rm_inventory_add.rs.txt");
pub(crate) const RM_INV_REMOVE_RS: &str =
    include_str!("../../templates/rm_inventory_remove.rs.txt");
pub(crate) const RM_INV_EQUIP_RS: &str = include_str!("../../templates/rm_inventory_equip.rs.txt");
pub(crate) const RM_INV_SCHEMA: &str = include_str!("../../templates/rm_inventory_schema.toml.txt");
pub(crate) const RM_LB_MOD_RS: &str = include_str!("../../templates/rm_leaderboard_mod.rs.txt");
pub(crate) const RM_LB_SUBMIT_RS: &str =
    include_str!("../../templates/rm_leaderboard_submit.rs.txt");
pub(crate) const RM_LB_RESET_RS: &str = include_str!("../../templates/rm_leaderboard_reset.rs.txt");
pub(crate) const RM_LB_SCHEMA: &str =
    include_str!("../../templates/rm_leaderboard_schema.toml.txt");
pub(crate) const RM_MM_MOD_RS: &str = include_str!("../../templates/rm_matchmaking_mod.rs.txt");
pub(crate) const RM_MM_QUEUE_RS: &str = include_str!("../../templates/rm_matchmaking_queue.rs.txt");
pub(crate) const RM_MM_DEQUEUE_RS: &str =
    include_str!("../../templates/rm_matchmaking_dequeue.rs.txt");
pub(crate) const RM_MM_MATCH_RS: &str = include_str!("../../templates/rm_matchmaking_match.rs.txt");
pub(crate) const RM_MM_SCHEMA: &str =
    include_str!("../../templates/rm_matchmaking_schema.toml.txt");
pub(crate) const RM_GUILD_MOD_RS: &str = include_str!("../../templates/rm_guilds_mod.rs.txt");
pub(crate) const RM_GUILD_CREATE_RS: &str = include_str!("../../templates/rm_guilds_create.rs.txt");
pub(crate) const RM_GUILD_INVITE_RS: &str = include_str!("../../templates/rm_guilds_invite.rs.txt");
pub(crate) const RM_GUILD_ACCEPT_RS: &str = include_str!("../../templates/rm_guilds_accept.rs.txt");
pub(crate) const RM_GUILD_KICK_RS: &str = include_str!("../../templates/rm_guilds_kick.rs.txt");
pub(crate) const RM_GUILD_SCHEMA: &str = include_str!("../../templates/rm_guilds_schema.toml.txt");
pub(crate) const RM_QUEST_MOD_RS: &str = include_str!("../../templates/rm_quests_mod.rs.txt");
pub(crate) const RM_QUEST_ACCEPT_RS: &str = include_str!("../../templates/rm_quests_accept.rs.txt");
pub(crate) const RM_QUEST_PROGRESS_RS: &str =
    include_str!("../../templates/rm_quests_progress.rs.txt");
pub(crate) const RM_QUEST_COMPLETE_RS: &str =
    include_str!("../../templates/rm_quests_complete.rs.txt");
pub(crate) const RM_QUEST_SCHEMA: &str = include_str!("../../templates/rm_quests_schema.toml.txt");
pub(crate) const RM_ECON_MOD_RS: &str = include_str!("../../templates/rm_economy_mod.rs.txt");
pub(crate) const RM_ECON_BUY_RS: &str = include_str!("../../templates/rm_economy_buy.rs.txt");
pub(crate) const RM_ECON_SELL_RS: &str = include_str!("../../templates/rm_economy_sell.rs.txt");
pub(crate) const RM_ECON_TRANSFER_RS: &str =
    include_str!("../../templates/rm_economy_transfer.rs.txt");
pub(crate) const RM_ECON_LOOT_RS: &str = include_str!("../../templates/rm_economy_loot.rs.txt");
pub(crate) const RM_ECON_SCHEMA: &str = include_str!("../../templates/rm_economy_schema.toml.txt");
pub(crate) const RM_COMBAT_MOD_RS: &str = include_str!("../../templates/rm_combat_mod.rs.txt");
pub(crate) const RM_COMBAT_ATTACK_RS: &str =
    include_str!("../../templates/rm_combat_attack.rs.txt");
pub(crate) const RM_COMBAT_RESPAWN_RS: &str =
    include_str!("../../templates/rm_combat_respawn.rs.txt");
pub(crate) const RM_COMBAT_ABILITY_RS: &str =
    include_str!("../../templates/rm_combat_ability.rs.txt");
pub(crate) const RM_COMBAT_SCHEMA: &str = include_str!("../../templates/rm_combat_schema.toml.txt");
pub(crate) const RM_WORLD_MOD_RS: &str = include_str!("../../templates/rm_world_mod.rs.txt");
pub(crate) const RM_WORLD_TICK_RS: &str = include_str!("../../templates/rm_world_tick.rs.txt");
pub(crate) const RM_WORLD_NPC_RS: &str = include_str!("../../templates/rm_world_npc_spawn.rs.txt");
pub(crate) const RM_WORLD_CLEANUP_RS: &str =
    include_str!("../../templates/rm_world_cleanup.rs.txt");
pub(crate) const RM_WORLD_SCHEMA: &str = include_str!("../../templates/rm_world_schema.toml.txt");

// ── Voltra V1 runtime modules (voltra init --genre / --modules) ──────────────
// These fill out the remaining `RuntimeModule` ids from `voltra::runtime` that
// the legacy 9-module `voltra add` set didn't cover (TODO-V1-007). Same
// `add_module_files` wf() + append_schema() pipeline as the modules above —
// single-file-per-module instead of split into sub-files, since each is 2-3
// reducers. State is represented via ordinary TableStore rows/reducers today;
// the dedicated ECS/AOI/tick hot-path engine in `src/runtime/` is a separate,
// optional upgrade for studios that need it (see docs/voltra-v1-runtime.md).
pub(crate) const RM_SESSIONS_MOD_RS: &str = include_str!("../../templates/rm_sessions_mod.rs.txt");
pub(crate) const RM_SESSIONS_SCHEMA: &str =
    include_str!("../../templates/rm_sessions_schema.toml.txt");
pub(crate) const RM_LOBBY_MOD_RS: &str = include_str!("../../templates/rm_lobby_mod.rs.txt");
pub(crate) const RM_LOBBY_SCHEMA: &str = include_str!("../../templates/rm_lobby_schema.toml.txt");
pub(crate) const RM_TICK_MOD_RS: &str = include_str!("../../templates/rm_tick_mod.rs.txt");
pub(crate) const RM_TICK_SCHEMA: &str = include_str!("../../templates/rm_tick_schema.toml.txt");
pub(crate) const RM_ECS_MOD_RS: &str = include_str!("../../templates/rm_ecs_mod.rs.txt");
pub(crate) const RM_ECS_SCHEMA: &str = include_str!("../../templates/rm_ecs_schema.toml.txt");
pub(crate) const RM_AOI_MOD_RS: &str = include_str!("../../templates/rm_aoi_mod.rs.txt");
pub(crate) const RM_AOI_SCHEMA: &str = include_str!("../../templates/rm_aoi_schema.toml.txt");
pub(crate) const RM_DELTA_MOD_RS: &str = include_str!("../../templates/rm_delta_mod.rs.txt");
pub(crate) const RM_DELTA_SCHEMA: &str = include_str!("../../templates/rm_delta_schema.toml.txt");
pub(crate) const RM_RTPERSIST_MOD_RS: &str =
    include_str!("../../templates/rm_runtime_persistence_mod.rs.txt");
pub(crate) const RM_RTPERSIST_SCHEMA: &str =
    include_str!("../../templates/rm_runtime_persistence_schema.toml.txt");
pub(crate) const RM_MOVEMENT_MOD_RS: &str = include_str!("../../templates/rm_movement_mod.rs.txt");
pub(crate) const RM_WEAPONS_MOD_RS: &str = include_str!("../../templates/rm_weapons_mod.rs.txt");
pub(crate) const RM_WEAPONS_SCHEMA: &str =
    include_str!("../../templates/rm_weapons_schema.toml.txt");
pub(crate) const RM_HITDET_MOD_RS: &str =
    include_str!("../../templates/rm_hit_detection_mod.rs.txt");
pub(crate) const RM_HITDET_SCHEMA: &str =
    include_str!("../../templates/rm_hit_detection_schema.toml.txt");
pub(crate) const RM_EQUIPMENT_MOD_RS: &str =
    include_str!("../../templates/rm_equipment_mod.rs.txt");
pub(crate) const RM_EQUIPMENT_SCHEMA: &str =
    include_str!("../../templates/rm_equipment_schema.toml.txt");
pub(crate) const RM_PARTIES_MOD_RS: &str = include_str!("../../templates/rm_parties_mod.rs.txt");
pub(crate) const RM_PARTIES_SCHEMA: &str =
    include_str!("../../templates/rm_parties_schema.toml.txt");
pub(crate) const RM_REPLAY_MOD_RS: &str = include_str!("../../templates/rm_replay_mod.rs.txt");
pub(crate) const RM_REPLAY_SCHEMA: &str = include_str!("../../templates/rm_replay_schema.toml.txt");

// `lobby-runtime` — real ECS/AOI/tick engine (voltra::runtime) instead of
// TableStore rows. No schema file: state lives inside the LobbyRuntime, not
// in the TableStore, so there is nothing to add to schema.toml. Requires its
// own main.rs (initializes the registry + starts the tick driver) — see
// `GAME_MAIN_LOBBY_RUNTIME_RS` and `scaffold.rs::init_project_from_recipe`.
pub(crate) const RM_LOBBY_RUNTIME_MOD_RS: &str =
    include_str!("../../templates/rm_lobby_runtime_mod.rs.txt");
pub(crate) const GAME_MAIN_LOBBY_RUNTIME_RS: &str =
    include_str!("../../templates/r_game_main_lobby_runtime.rs.txt");

// ── Rust client SDK scaffold ──────────────────────────────────────────────────

pub(crate) const CLIENT_MAIN_RS: &str = r#"//! Example Rust client for a Voltra game server.
//!
//! Run the server first:  voltra start
//! Then in another terminal: cargo run --release
use voltra_client::{VoltraClient, ClientOptions};

#[tokio::main]
async fn main() {
    let opts = ClientOptions {
        url: "ws://127.0.0.1:3000".to_string(),
        api_key: None,
        call_timeout_ms: 5_000,
        reconnect: None,
    };

    let client = VoltraClient::connect(opts).await
        .expect("Failed to connect — is the server running? (voltra start)");

    println!("[client] Connected to server");

    // Subscribe to live player updates
    let (mut rx, _sub_id) = client
        .subscribe("players")
        .await
        .expect("Subscribe failed");

    tokio::spawn(async move {
        while let Some(diff) = rx.recv().await {
            println!(
                "[update] {} {} {} = {:?}",
                diff.operation, diff.table_name, diff.row_key, diff.row_data
            );
        }
    });

    // Spawn a player
    let result = client
        .call("spawn", &serde_json::json!(["rust_player", "lobby_1", "warrior"]))
        .await
        .expect("Reducer call failed");
    println!("[spawn] {:?}", result);

    // Move the player
    let result = client
        .call("move_player", &serde_json::json!(["rust_player", 10.0, 20.0]))
        .await
        .expect("Reducer call failed");
    println!("[move]  {:?}", result);

    // Keep running to receive live updates
    println!("[client] Listening for updates (Ctrl+C to stop)…");
    tokio::signal::ctrl_c().await.ok();
}
"#;

pub(crate) const CLIENT_PROTOCOL_MD: &str = r#"# Voltra Wire Protocol

Implement this to connect **any** game engine or language to Voltra.

## Transport

- **WebSocket** binary frames (not text)
- **MessagePack** encoding — structs are positional arrays (rmp_serde default)
- Auth header at upgrade: `Authorization: Bearer <api_key>`
  - Optional role suffix: `Bearer <api_key>:<role>`

## Client → Server messages

All messages are a **MessagePack map with one key** → value is a positional array.

```
{ "ReducerCall": [call_id: u64, reducer_name: str, args: bin] }
{ "Subscribe":   [sub_id: str,  query: str] }
{ "Unsubscribe": [sub_id: str] }
```

- `call_id` — any u64 you choose; matched back in the response
- `args` — MessagePack-encoded array of your reducer's positional arguments
- `query` — e.g. `"players"` or `"players WHERE zone = 'north'"` or `"players WHERE zone = 'north' ORDER BY score DESC LIMIT 10"`
- `sub_id` — any string you choose; used to route live updates back to the right handler

## Server → Client messages

### ReducerResponse (bare array — no wrapper map)
```
[call_id: u64, success: bool, result: bin | nil, error: str | nil]
```
`result` is a MessagePack-encoded value returned by the reducer.

### SubscriptionAck
```
{ "SubscriptionAck": [sub_id: str, success: bool, message: str | nil] }
```

### SubscriptionDiff (one frame per row change)
```
{ "SubscriptionDiff": [sub_id: str, table: str, row_key: str, op: str, data: map | nil] }
```
- `op` — `"insert"` | `"update"` | `"delete"` | `"initial_snapshot"`
- `data` — full row as a MessagePack map, or nil for deletes

### BatchUpdate (one frame per tick — replaces many SubscriptionDiffs)
```
{ "BatchUpdate": [compressed: bool, payload: bin] }
```
- `payload` — when `compressed = false`: MessagePack array of SubscriptionDiff arrays
- `payload` — when `compressed = true`: zstd( above )
- Each element: `[sub_id, table, row_key, op, data | nil]`
- `op` may be `"patch"` — data contains only changed fields (delta patch)

**In tick mode (default 20 Hz) the server sends BatchUpdate, not SubscriptionDiff.**
Implement BatchUpdate first — it is the primary live-update path.

### Error
```
{ "Error": { "message": str } }
```

## Minimal implementation checklist

1. Open a WebSocket to `ws://<host>:<port>` with the auth header
2. Send a `ReducerCall` to invoke game logic
3. Await a bare-array `ReducerResponse` matching your `call_id`
4. Send a `Subscribe` with a query string
5. Await `SubscriptionAck` to confirm
6. On each `BatchUpdate`: zstd-decompress if `compressed`, then MsgPack-decode the
   payload as `[[sub_id, table, row_key, op, data?], ...]` and dispatch to handlers
7. Handle `SubscriptionDiff` for servers with tick mode disabled

## MessagePack notes

- Integers: use the most compact fixint/int8/int16/int32/int64 form
- Strings: fixstr / str8 / str16
- Binary: bin8 / bin16 (used for nested args and result payloads)
- Maps: fixmap / map16 (server uses string keys)
- Arrays: fixarray / array16

Any standard MessagePack library works. The server uses Rust's `rmp-serde` in
default (array/positional) mode for struct fields.
"#;

// ── Unity + Godot SDKs ────────────────────────────────────────────────────────
pub(crate) const UNITY_CLIENT_CS: &str = include_str!("../engine_templates/unity_VoltraClient.cs");
pub(crate) const UNITY_BEHAVIOUR_CS: &str =
    include_str!("../engine_templates/unity_VoltraBehaviour.cs");
pub(crate) const UNITY_MANAGER_CS: &str = include_str!("../../templates/g_unity_Manager.cs.txt");
pub(crate) const UNITY_GAME_README: &str = include_str!("../../templates/g_unity_readme.md.txt");
pub(crate) const GODOT_CLIENT_GD: &str = include_str!("../engine_templates/godot_voltra_client.gd");
pub(crate) const GODOT_MANAGER_GD: &str = include_str!("../../templates/g_godot_Manager.gd.txt");
pub(crate) const GODOT_GAME_README: &str = include_str!("../../templates/g_godot_readme.md.txt");
