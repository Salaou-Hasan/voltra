# Cluster Builtins

When your game grows beyond what one server can handle, Voltra can run as a cluster — multiple servers working together, each owning a slice of your data.

---

## What Is a Cluster?

A single Voltra server can handle tens of thousands of concurrent players. For most games, you will never need more than one server.

When you do need more, a Voltra cluster lets you run multiple servers and split your player data across them. Each server is called a **shard**. Each shard owns a specific set of row keys, determined by a consistent hash function. The same key always maps to the same shard.

```
                    ┌─────────────────────────────────────────────────────┐
                    │              Voltra Cluster (3 shards)              │
                    │                                                     │
   Client A ────►  │  Shard 0  (owns player IDs hashing to range 0-33%) │
   Client B ────►  │  Shard 1  (owns player IDs hashing to range 34-66%) │
   Client C ────►  │  Shard 2  (owns player IDs hashing to range 67-100%)│
                    └─────────────────────────────────────────────────────┘
```

Within a shard, everything works exactly as documented elsewhere. Cluster builtins let you reach *across* shards when you need data that lives on a different server.

---

## Setting Up a Cluster

In `voltra.toml` on each server, configure the cluster:

**Server 0 (`voltra.toml`):**
```toml
[cluster]
shard_id    = 0
shard_count = 3
peers       = "shard1=http://192.168.1.2:4001,shard2=http://192.168.1.3:4001"
secret      = "your-shared-cluster-secret"
```

**Server 1 (`voltra.toml`):**
```toml
[cluster]
shard_id    = 1
shard_count = 3
peers       = "shard0=http://192.168.1.1:4001,shard2=http://192.168.1.3:4001"
secret      = "your-shared-cluster-secret"
```

Or use environment variables:
```
VOLTRA_SHARD_ID=0
VOLTRA_SHARD_COUNT=3
VOLTRA_PEERS=shard1=http://192.168.1.2:4001,shard2=http://192.168.1.3:4001
VOLTRA_CLUSTER_SECRET=your-shared-cluster-secret
```

---

## cluster_route(key)

**Returns** the shard ID (integer) that owns the given row key. Uses a consistent hash function — the same key always returns the same shard ID, regardless of which server you call it on.

```voltra
let shard = cluster_route("alice")      // e.g. 1
let shard = cluster_route("player_99")  // e.g. 0
let shard = cluster_route(player_id)    // determine the owning shard
```

The return value is an integer from `0` to `shard_count - 1`.

**Game use — check if a player's data is on this shard:**
```voltra
reducer get_player_location(player_id: str) {
    let shard = cluster_route(player_id)
    return { shard_id: shard, player_id: player_id }
}
```

**Game use — route a cross-shard action:**
```voltra
reducer attack_player(attacker_id: str, target_id: str, damage: int) {
    let my_shard     = 0   // replace with VOLTRA_SHARD_ID
    let target_shard = cluster_route(target_id)

    if target_shard != my_shard {
        // Target is on another shard — proxy the call
        let result = cross_cluster_call(target_shard, "receive_damage",
            concat("[\"", concat(target_id, concat("\",", concat(str(damage), "]")))))
        return { ok: true, proxied: true }
    }

    // Target is local
    players[target_id].hp -= damage
    return { ok: true, proxied: false }
}
```

---

## cross_cluster_call(shard_id, "reducer_name", args_json)

**Calls a reducer on a remote shard** and returns the result. The call is synchronous from the perspective of your reducer — your reducer waits for the remote shard to respond.

```voltra
let result = cross_cluster_call(1, "get_player_hp", "[\"alice\"]")
```

Arguments:
- `shard_id` — the target shard (integer, from `cluster_route`)
- `reducer_name` — the name of the reducer to call on that shard (string)
- `args_json` — the arguments as a JSON array string

**The reducer being called on the remote shard must exist with that name.** It runs on the remote shard exactly as if a client called it there.

**Game use — cross-shard trade:**
```voltra
reducer send_gold(recipient_id: str, amount: int) {
    let sender = players[caller_id] else { error("sender not found") }
    if sender.gold < amount {
        error("not enough gold")
    }

    let recipient_shard = cluster_route(recipient_id)
    let my_shard        = cluster_route(caller_id)

    if recipient_shard != my_shard {
        // Deduct gold locally, credit gold remotely
        players[caller_id].gold -= amount
        cross_cluster_call(recipient_shard, "receive_gold",
            concat("[\"", concat(recipient_id, concat("\",", concat(str(amount), "]")))))
        return { ok: true, cross_shard: true }
    }

    // Same shard — simple local transfer
    let recipient = players[recipient_id] else { error("recipient not found") }
    players[caller_id].gold  -= amount
    players[recipient_id].gold += amount
    return { ok: true, cross_shard: false }
}

reducer receive_gold(recipient_id: str, amount: int) {
    players[recipient_id].gold += amount
    return { ok: true }
}
```

---

## region_count_rows("table")

**Returns** the total number of rows in the named table across **all shards** in the cluster. On a single-node deployment, this is the same as `count_rows("table")`.

```voltra
let global_player_count = region_count_rows("players")
```

This queries every shard and sums the counts. It is slightly slower than `count_rows` (which is local only) because it must contact all peers.

**Game use — cluster-wide population:**
```voltra
reducer server_status() {
    return {
        players_online_this_shard:   count_rows("players"),
        players_online_all_shards:   region_count_rows("players"),
    }
}
```

**Game use — enforce a global cap (across all shards):**
```voltra
reducer spawn(player_id: str, name: str) {
    if region_count_rows("players") >= 100000 {
        error("all servers full")
    }
    players[player_id] = { name: name, hp: 100, alive: true }
    return { ok: true }
}
```

---

## migrate_to_cluster("table", key, shard_id)

**Moves a row** from the current shard to the specified shard. The row is deleted locally and created on the target shard.

```voltra
migrate_to_cluster("players", player_id, new_shard_id)
```

Use this when:
- A player explicitly changes server region
- You are rebalancing data after adding a new shard
- A player's key hash changes (e.g. you restructured your key scheme)

**Note:** Migration is not instant. The row is deleted locally first, then created on the target shard. During this window (microseconds), the row does not exist on either shard. Design your reducers to handle this gracefully.

**Game use — player changes region:**
```voltra
reducer change_region(target_region: int) {
    if not exists("players", caller_id) {
        error("player not spawned")
    }

    let current_shard = cluster_route(caller_id)
    if target_region == current_shard {
        return { ok: true, already_there: true }
    }

    migrate_to_cluster("players", caller_id, target_region)
    return { ok: true, moved_to_shard: target_region }
}
```

---

## Cluster Design Guidelines

### Route by player ID, not by game object

The key insight for clustering is: **put a player's data on the same shard as the player**. Use the player's ID as the shard key for all their associated data (inventory, progress, stats). This way, most reducer calls touch only one shard.

```voltra
// Good: player and their items on the same shard
let player_shard = cluster_route(player_id)
let item_key = concat(player_id, concat("_item_", item_id))
items[item_key] = { ... }  // same shard as the player

// Risky: item keyed separately — may land on different shard
items[item_id] = { owner: player_id, ... }
```

### Cross-shard calls are slower

A local call (same shard) takes microseconds. A `cross_cluster_call` takes a network round trip — typically 1-10ms on a LAN, 10-100ms across data centers. Design reducers to avoid cross-shard calls on the hot path.

### Single-node first

Start with one server. Voltra can handle 30K+ concurrent players on a single server. Only add cluster shards when you have measured that you need them.

### All shards must use the same shard_count

The `cluster_route` function uses `shard_count` to determine which shard owns a key. If different servers have different `shard_count` values, they will disagree about who owns what. All servers in a cluster must have the same `VOLTRA_SHARD_COUNT`.

---

## Example: 3-Shard Cluster Spawn Router

```voltra
// This reducer routes new players to the least-loaded shard

reducer spawn_routed(player_id: str, name: str) {
    // Determine which shard owns this player
    let target_shard = cluster_route(player_id)

    // Check if it's this shard (shard 0 in this example)
    let my_shard = 0

    if target_shard == my_shard {
        // Spawn locally
        players[player_id] = {
            name:  name,
            hp:    100,
            alive: true,
            shard: my_shard,
        }
        return { ok: true, spawned_on_shard: my_shard }
    }

    // Forward to the correct shard
    let args = concat("[\"", concat(player_id, concat("\",\"", concat(name, "\"]"))))
    cross_cluster_call(target_shard, "spawn_local", args)
    return { ok: true, spawned_on_shard: target_shard }
}

reducer spawn_local(player_id: str, name: str) {
    players[player_id] = { name: name, hp: 100, alive: true }
    return { ok: true }
}
```
