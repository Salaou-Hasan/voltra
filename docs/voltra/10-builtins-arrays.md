# Array Builtins

Arrays in Voltra let you store ordered lists inside table rows — inventories, skill lists, loot pools, chat history, and more.

---

## Arrays in Voltra

An array is a JSON list. You can store arrays in table fields and return them from reducers:

```voltra
table players {
    inventory: str = "[]",   // stored as a JSON string
    skills:    str = "[]",
    name:      str = "",
    hp:        int = 100,
}
```

Create an array literal:
```voltra
let items   = ["sword", "shield", "potion"]
let numbers = [1, 2, 3, 4, 5]
let empty   = []
```

Read an array field from a row:
```voltra
let p    = players[player_id]
let inv  = p.inventory    // the array stored in that field
```

Write an array field to a row:
```voltra
push(inv, "new_item")
players[player_id].inventory = inv
```

---

## array_len(arr)

**Returns** the number of elements in the array.

```voltra
array_len(["a", "b", "c"])    // 3
array_len([])                 // 0
```

**Game use — check inventory size:**
```voltra
let p = players[player_id]
if array_len(p.inventory) >= 20 {
    error("inventory is full")
}
```

---

## get_index(arr, i)

**Returns** the element at index `i` (zero-based). Returns a null-like value if out of bounds.

```voltra
let items = ["sword", "shield", "potion"]
get_index(items, 0)    // "sword"
get_index(items, 2)    // "potion"
get_index(items, 5)    // null (out of bounds)
```

**Game use — pick a random item from a loot pool:**
```voltra
let loot_pool = ["common_ore", "iron_ore", "gold_ore", "gem", "ancient_relic"]
let roll = rand_int(0, array_len(loot_pool) - 1)
let reward = get_index(loot_pool, roll)
return { loot: reward }
```

---

## array_contains(arr, val)

**Returns** `true` if `val` is in the array.

```voltra
array_contains(["a", "b", "c"], "b")    // true
array_contains(["a", "b", "c"], "d")    // false
array_contains([], "x")                  // false
```

**Game use — check if player has an item:**
```voltra
reducer use_item(item_name: str) {
    let p   = players[caller_id] else { error("not found") }
    let inv = p.inventory
    if not array_contains(inv, item_name) {
        error("you don't have that item")
    }
    // use the item...
    return { ok: true }
}
```

**Game use — check if player has a skill unlocked:**
```voltra
if not array_contains(p.skills, "fireball") {
    error("fireball not unlocked")
}
```

---

## slice(arr, start, end)

**Returns** a new array containing elements from index `start` (inclusive) to index `end` (exclusive).

```voltra
let items = ["a", "b", "c", "d", "e"]
slice(items, 1, 3)    // ["b", "c"]
slice(items, 0, 2)    // ["a", "b"]
slice(items, 3, 5)    // ["d", "e"]
```

**Game use — get the last 5 chat messages:**
```voltra
let all_msgs = room.history
let count    = array_len(all_msgs)
let recent   = slice(all_msgs, max(0, count - 5), count)
return { messages: recent }
```

**Game use — paginate a leaderboard:**
```voltra
reducer get_leaderboard_page(page: int) {
    let all   = sort_by("leaderboard", "score", "desc")
    let start = page * 10
    let end   = min(start + 10, array_len(all))
    let page_data = slice(all, start, end)
    return { entries: page_data, page: page }
}
```

---

## array_first(arr)

**Returns** the first element of the array, or null if empty.

```voltra
array_first(["a", "b", "c"])    // "a"
array_first([42, 99, 7])        // 42
array_first([])                 // null
```

**Game use — peek at the top of a queue:**
```voltra
let queue = matchmaking_queue.players
let next_player = array_first(queue)
```

---

## array_last(arr)

**Returns** the last element of the array, or null if empty.

```voltra
array_last(["a", "b", "c"])    // "c"
array_last([42, 99, 7])        // 7
array_last([])                 // null
```

**Game use — get the most recently added item:**
```voltra
let p    = players[player_id]
let last = array_last(p.inventory)
return { last_acquired: last }
```

---

## array_reverse(arr)

**Returns** a new array with elements in reverse order.

```voltra
array_reverse(["a", "b", "c"])    // ["c", "b", "a"]
array_reverse([1, 2, 3])          // [3, 2, 1]
```

**Game use — show history in newest-first order:**
```voltra
let history  = p.action_log
let reversed = array_reverse(history)
return { recent_actions: slice(reversed, 0, 10) }
```

---

## push(arr, val)

**Modifies** `arr` by appending `val` to the end. This is a statement, not a function that returns a value.

```voltra
let items = ["sword"]
push(items, "shield")
push(items, "potion")
// items is now ["sword", "shield", "potion"]
```

**Game use — add an item to inventory:**
```voltra
reducer pickup_item(item_name: str) {
    let p   = players[caller_id] else { error("not found") }
    let inv = p.inventory

    if array_len(inv) >= 20 {
        error("inventory full")
    }

    push(inv, item_name)
    players[caller_id].inventory = inv

    return { ok: true, inventory: inv }
}
```

---

## pop(arr)

**Modifies** `arr` by removing and returning the last element.

```voltra
let items = ["sword", "shield", "potion"]
let removed = pop(items)
// removed = "potion"
// items is now ["sword", "shield"]
```

**Game use — consume from a stack:**
```voltra
reducer use_top_item() {
    let p   = players[caller_id] else { error("not found") }
    let inv = p.inventory

    if array_len(inv) == 0 {
        error("inventory empty")
    }

    let consumed = pop(inv)
    players[caller_id].inventory = inv

    return { ok: true, consumed: consumed }
}
```

---

## remove_at(arr, idx)

**Modifies** `arr` by removing the element at index `idx`. Elements after `idx` shift left.

```voltra
let items = ["sword", "shield", "potion"]
remove_at(items, 1)
// items is now ["sword", "potion"]
```

**Game use — remove a specific item from inventory:**
```voltra
reducer drop_item(item_name: str) {
    let p   = players[caller_id] else { error("not found") }
    let inv = p.inventory

    let idx = -1
    let i   = 0
    for item in inv {
        if item == item_name and idx == -1 {
            idx = i
        }
        i += 1
    }

    if idx == -1 {
        error("item not in inventory")
    }

    remove_at(inv, idx)
    players[caller_id].inventory = inv

    return { ok: true, dropped: item_name }
}
```

---

## for-array Loops

Iterate over every element in an array:

```voltra
let skills = ["fireball", "ice_shard", "lightning"]
for skill in skills {
    // skill is each element: "fireball", then "ice_shard", then "lightning"
}
```

**Game use — apply multiple effects:**
```voltra
reducer apply_status_effects(player_id: str, effects: str) {
    // effects might be passed as a JSON-encoded list
    // Here we demonstrate iterating a known list
    let debuffs = ["slow", "poison", "blind"]
    for debuff in debuffs {
        if array_contains(p.active_effects, debuff) {
            players[player_id].hp -= 5
        }
    }
    return { ok: true }
}
```

---

## Practical Example: Full Inventory System

```voltra
table players {
    name:      str  = "",
    hp:        int  = 100,
    alive:     bool = true,
    inventory: str  = "[]",    // JSON array of item name strings
    max_slots: int  = 20,
}

table items {
    item_name: str = "",
    damage:    int = 0,
    weight:    int = 1,
    rarity:    str = "common",
}

reducer pickup_item(item_id: str) {
    let p    = players[caller_id] else { error("player not found") }
    let item = items[item_id]     else { error("item not found") }
    let inv  = p.inventory

    if array_len(inv) >= p.max_slots {
        error("inventory full")
    }
    if array_contains(inv, item_id) {
        error("already carrying that item")
    }

    push(inv, item_id)
    players[caller_id].inventory = inv
    return { ok: true, carrying: array_len(inv), slots: p.max_slots }
}

reducer drop_item(item_id: str) {
    let p   = players[caller_id] else { error("player not found") }
    let inv = p.inventory

    if not array_contains(inv, item_id) {
        error("item not in inventory")
    }

    // Find and remove
    let idx = 0
    let found_idx = -1
    for id in inv {
        if id == item_id and found_idx == -1 {
            found_idx = idx
        }
        idx += 1
    }
    remove_at(inv, found_idx)
    players[caller_id].inventory = inv

    return { ok: true, carrying: array_len(inv) }
}

reducer view_inventory() {
    let p   = players[caller_id] else { error("player not found") }
    return {
        inventory: p.inventory,
        slots_used: array_len(p.inventory),
        slots_total: p.max_slots,
    }
}

reducer draw_loot(loot_pool_id: str) {
    let pool  = loot_pools[loot_pool_id] else { error("loot pool not found") }
    let items_arr = pool.items            // array of item IDs
    if array_len(items_arr) == 0 {
        error("loot pool is empty")
    }
    let roll = rand_int(0, array_len(items_arr) - 1)
    let won  = get_index(items_arr, roll)
    return { item_id: won }
}
```
