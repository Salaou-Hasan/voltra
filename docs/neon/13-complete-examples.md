# Complete Examples

Three fully working `reducers.neon` files you can copy into a new project and run immediately.

---

## Example 1: Battle Royale

A shrinking-zone battle royale. Players spawn, move, shoot each other, heal, and respawn. The server tracks kills and provides a live leaderboard.

```neon
// ============================================================
// BATTLE ROYALE — reducers.neon
// ============================================================

table players {
    name:       str   = "",
    hp:         int   = 100,
    max_hp:     int   = 100,
    alive:      bool  = false,
    x:          float = 0.0,
    y:          float = 0.0,
    zone:       str   = "lobby",
    kills:      int   = 0,
    deaths:     int   = 0,
    score:      int   = 0,
    last_heal:  int   = 0,
}

// ── Spawn ────────────────────────────────────────────────────

reducer spawn(name: str) {
    if exists("players", caller_id) {
        error("already spawned — call respawn to re-enter")
    }
    if len(trim(name)) < 2 {
        error("name must be at least 2 characters")
    }

    // Random spawn location in the 1000×1000 map
    let spawn_x = float(rand_int(-400, 400))
    let spawn_y = float(rand_int(-400, 400))

    players[caller_id] = {
        name:      trim(name),
        hp:        100,
        max_hp:    100,
        alive:     true,
        x:         spawn_x,
        y:         spawn_y,
        zone:      "arena",
        kills:     0,
        deaths:    0,
        score:     0,
        last_heal: timestamp(),
    }

    return { ok: true, x: spawn_x, y: spawn_y }
}

// ── Movement ─────────────────────────────────────────────────

reducer move_player(x: float, y: float) {
    let p = players[caller_id] else { error("not spawned") }
    if not p.alive {
        error("you are dead — call respawn")
    }

    // Keep within map bounds
    let clamped_x = clamp(x, -500.0, 500.0)
    let clamped_y = clamp(y, -500.0, 500.0)

    players[caller_id].x = clamped_x
    players[caller_id].y = clamped_y

    return { ok: true, x: clamped_x, y: clamped_y }
}

// ── Combat ───────────────────────────────────────────────────

reducer shoot(target_id: str, weapon: str) {
    let attacker = players[caller_id] else { error("attacker not found") }
    let target   = players[target_id] else { error("target not found") }

    if not attacker.alive {
        error("you are dead")
    }
    if not target.alive {
        error("target is already dead")
    }
    if caller_id == target_id {
        error("you cannot shoot yourself")
    }

    // Check range (Euclidean distance)
    let dx    = attacker.x - target.x
    let dy    = attacker.y - target.y
    let dist  = sqrt(dx * dx + dy * dy)

    // Weapon stats: damage and range
    let base_damage = 25
    let max_range   = 150.0

    if weapon == "sniper" {
        base_damage = 60
        max_range   = 500.0
    } else if weapon == "shotgun" {
        base_damage = 45
        max_range   = 50.0
    } else if weapon == "pistol" {
        base_damage = 20
        max_range   = 100.0
    }

    if dist > max_range {
        return { ok: false, reason: "out of range", distance: dist, max_range: max_range }
    }

    // Critical hit: 15% chance, 2x damage
    let damage  = base_damage
    let is_crit = rand_int(1, 100) <= 15
    if is_crit {
        damage = damage * 2
    }

    // Apply damage
    let new_hp = max(0, target.hp - damage)
    players[target_id].hp = new_hp

    let killed = false
    if new_hp == 0 {
        players[target_id].alive  = false
        players[target_id].deaths += 1
        players[caller_id].kills  += 1
        players[caller_id].score  += 100
        killed = true
    }

    return {
        ok:        true,
        damage:    damage,
        is_crit:   is_crit,
        target_hp: new_hp,
        killed:    killed,
        distance:  dist,
    }
}

// ── Healing ──────────────────────────────────────────────────

reducer heal() {
    let p   = players[caller_id] else { error("not spawned") }
    let now = timestamp()

    if not p.alive {
        error("you are dead")
    }

    // 5 second cooldown (5,000,000,000 nanoseconds)
    if now - p.last_heal < 5000000000 {
        error("heal on cooldown")
    }

    let heal_amount = 30
    let new_hp      = min(p.max_hp, p.hp + heal_amount)

    players[caller_id].hp        = new_hp
    players[caller_id].last_heal = now

    return { ok: true, healed: new_hp - p.hp, hp: new_hp }
}

// ── Respawn ──────────────────────────────────────────────────

reducer respawn() {
    let p = players[caller_id] else { error("not spawned — call spawn first") }
    if p.alive {
        error("you are not dead")
    }

    // Respawn at a random location
    let spawn_x = float(rand_int(-400, 400))
    let spawn_y = float(rand_int(-400, 400))

    players[caller_id].alive     = true
    players[caller_id].hp        = players[caller_id].max_hp
    players[caller_id].x         = spawn_x
    players[caller_id].y         = spawn_y
    players[caller_id].last_heal = timestamp()

    return { ok: true, x: spawn_x, y: spawn_y }
}

// ── Leaderboard ──────────────────────────────────────────────

reducer get_leaderboard() {
    let top  = top_n("players", "score", 10)
    let alive = find_all("players", "alive", true)
    return {
        top10:         top,
        players_alive: array_len(alive),
        total_players: count_rows("players"),
    }
}

// ── Cleanup (scheduled) ─────────────────────────────────────

reducer cleanup_dead() {
    // Scheduled: remove players who have been dead for more than 5 minutes
    // (In a real game you'd track death_time and check it here)
    let count = 0
    for id, p in players {
        if not p.alive and p.score == 0 and p.kills == 0 {
            delete players[id]
            count += 1
        }
    }
    return { ok: true, removed: count }
}
```

**Run it:**
```
voltra init battle-royale --template neon/basic
# replace reducers.neon with the above
voltra build
voltra start
voltra call spawn '["Alice"]'
voltra call shoot '["bob_player_id", "rifle"]'
voltra call get_leaderboard '[]'
```

---

## Example 2: Chat Server

Rooms with membership, message history, and moderation.

```neon
// ============================================================
// CHAT SERVER — reducers.neon
// ============================================================

table rooms {
    name:         str  = "",
    owner_id:     str  = "",
    topic:        str  = "",
    private:      bool = false,
    member_count: int  = 0,
    created_at:   int  = 0,
}

table room_members {
    room_id:   str  = "",
    player_id: str  = "",
    role:      str  = "member",
    joined_at: int  = 0,
    muted:     bool = false,
}

table messages {
    room_id:  str = "",
    author:   str = "",
    text:     str = "",
    sent_at:  int = 0,
    edited:   bool = false,
}

// ── Rooms ────────────────────────────────────────────────────

reducer create_room(room_id: str, name: str, topic: str, private: bool) {
    if len(trim(room_id)) < 2 {
        error("room ID must be at least 2 characters")
    }
    if exists("rooms", room_id) {
        error("room already exists")
    }

    let now = timestamp()
    rooms[room_id] = {
        name:         trim(name),
        owner_id:     caller_id,
        topic:        trim(topic),
        private:      private,
        member_count: 1,
        created_at:   now,
    }

    // Auto-join the creator as owner
    let member_key = concat(room_id, concat(":", caller_id))
    room_members[member_key] = {
        room_id:   room_id,
        player_id: caller_id,
        role:      "owner",
        joined_at: now,
        muted:     false,
    }

    return { ok: true, room_id: room_id }
}

reducer join_room(room_id: str) {
    let room = rooms[room_id] else { error("room not found") }

    let member_key = concat(room_id, concat(":", caller_id))

    if exists("room_members", member_key) {
        error("already in that room")
    }

    if room.private {
        error("room is private — you need an invite")
    }

    room_members[member_key] = {
        room_id:   room_id,
        player_id: caller_id,
        role:      "member",
        joined_at: timestamp(),
        muted:     false,
    }

    rooms[room_id].member_count += 1

    return { ok: true, room: room_id, members: rooms[room_id].member_count }
}

reducer leave_room(room_id: str) {
    let member_key = concat(room_id, concat(":", caller_id))

    if not exists("room_members", member_key) {
        error("you are not in that room")
    }

    let member = room_members[member_key]
    if member.role == "owner" {
        error("owner cannot leave — transfer ownership or delete the room first")
    }

    delete room_members[member_key]
    rooms[room_id].member_count = max(0, rooms[room_id].member_count - 1)

    return { ok: true }
}

reducer delete_room(room_id: str) {
    let member_key = concat(room_id, concat(":", caller_id))
    let member     = room_members[member_key] else { error("you are not in that room") }

    if member.role != "owner" and caller_role != "admin" {
        error("only the owner can delete this room")
    }

    // Remove all members
    let all_members = find_all("room_members", "room_id", room_id)
    for m in all_members {
        let key = concat(m.room_id, concat(":", m.player_id))
        delete room_members[key]
    }

    // Remove all messages
    let all_msgs = find_all("messages", "room_id", room_id)
    for msg in all_msgs {
        delete messages[msg.id]
    }

    delete rooms[room_id]
    return { ok: true }
}

// ── Messages ─────────────────────────────────────────────────

reducer send_message(room_id: str, text: str) {
    let member_key = concat(room_id, concat(":", caller_id))
    let member     = room_members[member_key] else { error("you are not in that room") }

    if member.muted {
        error("you are muted in this room")
    }

    let clean = trim(text)
    if len(clean) == 0 {
        error("message cannot be empty")
    }
    if len(clean) > 2000 {
        error("message too long (max 2000 characters)")
    }

    let now  = timestamp()
    let key  = concat(room_id, concat(":", str(now)))

    messages[key] = {
        room_id:  room_id,
        author:   caller_id,
        text:     clean,
        sent_at:  now,
        edited:   false,
    }

    return { ok: true, message_id: key }
}

reducer edit_message(message_id: str, new_text: str) {
    let msg = messages[message_id] else { error("message not found") }

    if msg.author != caller_id {
        error("you can only edit your own messages")
    }

    let clean = trim(new_text)
    if len(clean) == 0 {
        error("message cannot be empty")
    }

    messages[message_id].text   = clean
    messages[message_id].edited = true

    return { ok: true }
}

reducer delete_message(message_id: str) {
    let msg        = messages[message_id] else { error("message not found") }
    let member_key = concat(msg.room_id, concat(":", caller_id))
    let member     = room_members[member_key] else { error("you are not in that room") }

    if msg.author != caller_id and member.role == "member" and caller_role != "admin" {
        error("you cannot delete someone else's message")
    }

    delete messages[message_id]
    return { ok: true }
}

// ── Room browsing ────────────────────────────────────────────

reducer list_rooms() {
    let public_rooms = find_all("rooms", "private", false)
    return { rooms: public_rooms, count: array_len(public_rooms) }
}

reducer room_info(room_id: str) {
    let room = rooms[room_id] else { error("room not found") }
    let msgs = sort_by("messages", "sent_at", "asc")

    // Filter to only this room's messages
    let room_msgs = []
    for m in msgs {
        if m.room_id == room_id {
            push(room_msgs, m)
        }
    }

    // Return last 50 messages
    let count = array_len(room_msgs)
    let recent = slice(room_msgs, max(0, count - 50), count)

    return {
        room:     room,
        messages: recent,
        total:    count,
    }
}

reducer room_members_list(room_id: str) {
    let members = find_all("room_members", "room_id", room_id)
    return { members: members, count: array_len(members) }
}

// ── Moderation ───────────────────────────────────────────────

reducer kick_from_room(room_id: str, target_player_id: str) {
    let my_key     = concat(room_id, concat(":", caller_id))
    let me         = room_members[my_key] else { error("you are not in that room") }
    let target_key = concat(room_id, concat(":", target_player_id))

    if not exists("room_members", target_key) {
        error("target is not in this room")
    }

    if me.role != "owner" and me.role != "moderator" and caller_role != "admin" {
        error("you do not have permission to kick")
    }

    delete room_members[target_key]
    rooms[room_id].member_count = max(0, rooms[room_id].member_count - 1)

    return { ok: true, kicked: target_player_id }
}

reducer mute_player(room_id: str, target_player_id: str, muted: bool) {
    let my_key     = concat(room_id, concat(":", caller_id))
    let me         = room_members[my_key] else { error("you are not in that room") }

    if me.role != "owner" and me.role != "moderator" and caller_role != "admin" {
        error("you do not have permission to mute")
    }

    let target_key = concat(room_id, concat(":", target_player_id))
    room_members[target_key].muted = muted

    return { ok: true, muted: muted, player: target_player_id }
}
```

---

## Example 3: Trading Card Game

Players collect cards, build hands, and battle. Gold economy for buying new cards.

```neon
// ============================================================
// TRADING CARD GAME — reducers.neon
// ============================================================

table players {
    name:    str  = "",
    hp:      int  = 30,
    max_hp:  int  = 30,
    gold:    int  = 100,
    alive:   bool = false,
    wins:    int  = 0,
    losses:  int  = 0,
}

table cards {
    owner_id: str = "",
    name:     str = "",
    damage:   int = 0,
    cost:     int = 1,
    rarity:   str = "common",
    in_hand:  bool = false,
}

// ── Player Registration ──────────────────────────────────────

reducer register_player(name: str) {
    if exists("players", caller_id) {
        error("already registered")
    }
    if len(trim(name)) < 2 {
        error("name too short")
    }

    players[caller_id] = {
        name:    trim(name),
        hp:      30,
        max_hp:  30,
        gold:    100,
        alive:   true,
        wins:    0,
        losses:  0,
    }

    // Give every new player 5 starter cards
    let starter_cards = ["Fire Spark", "Water Drop", "Earth Stone", "Wind Slash", "Shadow Step"]
    let i = 0
    for card_name in starter_cards {
        let card_key = concat(caller_id, concat("_starter_", str(i)))
        cards[card_key] = {
            owner_id: caller_id,
            name:     card_name,
            damage:   rand_int(3, 7),
            cost:     rand_int(1, 3),
            rarity:   "common",
            in_hand:  false,
        }
        i += 1
    }

    return { ok: true, name: trim(name), starter_cards: 5 }
}

// ── Card Drawing ─────────────────────────────────────────────

reducer draw_card() {
    let p = players[caller_id] else { error("not registered") }

    // Cost to draw: 10 gold
    if p.gold < 10 {
        error("not enough gold (need 10)")
    }

    // Random card from the card pool
    let card_names  = ["Flame Wave", "Frost Lance", "Thunder Bolt", "Poison Dart",
                       "Holy Light", "Dark Void", "Earth Tremor", "Wind Blade",
                       "Soul Drain", "Star Fall"]
    let rarities    = ["common", "common", "common", "common", "rare",
                       "rare", "rare", "epic", "epic", "legendary"]
    let roll        = rand_int(0, 9)
    let chosen_name = get_index(card_names, roll)
    let rarity      = get_index(rarities, roll)

    // Damage scales with rarity
    let base_damage = rand_int(5, 10)
    if rarity == "rare" {
        base_damage = rand_int(8, 15)
    } else if rarity == "epic" {
        base_damage = rand_int(12, 20)
    } else if rarity == "legendary" {
        base_damage = rand_int(18, 30)
    }

    let cost = max(1, base_damage / 5)

    let card_key = concat(caller_id, concat("_", str(timestamp())))
    cards[card_key] = {
        owner_id: caller_id,
        name:     chosen_name,
        damage:   base_damage,
        cost:     cost,
        rarity:   rarity,
        in_hand:  false,
    }

    players[caller_id].gold -= 10

    return {
        ok:       true,
        card_id:  card_key,
        name:     chosen_name,
        damage:   base_damage,
        rarity:   rarity,
        gold_left: p.gold - 10,
    }
}

// ── Playing Cards ────────────────────────────────────────────

reducer play_card(card_id: str, target_id: str) {
    let p    = players[caller_id] else { error("not registered") }
    let card = cards[card_id]     else { error("card not found") }

    if card.owner_id != caller_id {
        error("that is not your card")
    }

    let target = players[target_id] else { error("target not found") }

    if not p.alive {
        error("you have been eliminated")
    }
    if not target.alive {
        error("target is already eliminated")
    }
    if caller_id == target_id {
        error("cannot target yourself")
    }

    // Apply damage to target
    let new_hp = max(0, target.hp - card.damage)
    players[target_id].hp = new_hp

    let eliminated = false
    if new_hp == 0 {
        players[target_id].alive  = false
        players[target_id].losses += 1
        players[caller_id].wins   += 1
        players[caller_id].gold   += 50    // win reward
        eliminated = true
    }

    // Card is consumed after playing
    delete cards[card_id]

    return {
        ok:         true,
        damage:     card.damage,
        target_hp:  new_hp,
        eliminated: eliminated,
    }
}

// ── Card Shop ────────────────────────────────────────────────

reducer buy_card(pack_type: str) {
    let p = players[caller_id] else { error("not registered") }

    let price = 20
    if pack_type == "premium" {
        price = 50
    }

    if p.gold < price {
        error(concat("not enough gold (need ", concat(str(price), ")")))
    }

    // Premium packs guarantee at least rare
    let min_roll = 0
    if pack_type == "premium" {
        min_roll = 4    // skip the commons in our 0-9 pool
    }

    let card_names = ["Flame Wave", "Frost Lance", "Thunder Bolt", "Poison Dart",
                      "Holy Light", "Dark Void", "Earth Tremor", "Wind Blade",
                      "Soul Drain", "Star Fall"]
    let rarities   = ["common", "common", "common", "common", "rare",
                      "rare", "rare", "epic", "epic", "legendary"]

    let roll        = rand_int(min_roll, 9)
    let chosen_name = get_index(card_names, roll)
    let rarity      = get_index(rarities, roll)

    let base_damage = rand_int(5, 10)
    if rarity == "rare" {
        base_damage = rand_int(8, 15)
    } else if rarity == "epic" {
        base_damage = rand_int(12, 20)
    } else if rarity == "legendary" {
        base_damage = rand_int(18, 30)
    }

    let card_key = concat(caller_id, concat("_bought_", str(timestamp())))
    cards[card_key] = {
        owner_id: caller_id,
        name:     chosen_name,
        damage:   base_damage,
        cost:     max(1, base_damage / 5),
        rarity:   rarity,
        in_hand:  false,
    }

    players[caller_id].gold -= price

    return {
        ok:        true,
        pack:      pack_type,
        card_id:   card_key,
        name:      chosen_name,
        rarity:    rarity,
        damage:    base_damage,
        gold_left: p.gold - price,
    }
}

// ── Viewing ──────────────────────────────────────────────────

reducer view_hand() {
    let my_cards = find_all("cards", "owner_id", caller_id)
    return {
        cards: my_cards,
        count: array_len(my_cards),
        gold:  players[caller_id].gold,
    }
}

reducer player_stats(player_id: str) {
    let p = players[player_id] else { return { found: false } }
    return {
        found:   true,
        name:    p.name,
        hp:      p.hp,
        gold:    p.gold,
        wins:    p.wins,
        losses:  p.losses,
        cards:   array_len(find_all("cards", "owner_id", player_id)),
    }
}

reducer leaderboard() {
    let top = top_n("players", "wins", 10)
    return {
        top10:          top,
        total_players:  count_rows("players"),
        total_cards:    count_rows("cards"),
    }
}
```

**Run it:**
```
voltra init card-game --template neon/basic
# replace reducers.neon with the above
voltra build
voltra start

# Register two players
voltra call register_player '["Alice"]'
# (use a different caller_id for bob in your real client)
voltra call register_player '["Bob"]'

# Alice draws a card
voltra call draw_card '[]'

# Alice views her hand
voltra call view_hand '[]'

# Alice plays a card against Bob
voltra call play_card '["<card_id_from_draw>", "<bob_caller_id>"]'

# Check the leaderboard
voltra call leaderboard '[]'
```
