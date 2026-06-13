# game/basic — Minimal Multiplayer Game Backend

A lightweight NeonDB template with player spawn, movement, and health. Use this as the foundation for any multiplayer game.

## Layout

```
reducers/
  spawn.js      — create a player
  move.js       — update position
  despawn.js    — remove a player
  damage.js     — reduce hp
  heal.js       — restore hp
schema.toml     — players + sessions tables
```

## Run

```bash
neondb start
neondb call spawn '["player1", "lobby_1", "warrior"]'
neondb watch "players WHERE lobby = 'lobby_1'"
```

## Add Modules

Extend your backend with one command:

| Command                    | What it adds                                      |
|----------------------------|---------------------------------------------------|
| `neon add chat`            | Room-based chat with join/leave/send              |
| `neon add inventory`       | Per-player items with add/remove/equip            |
| `neon add leaderboard`     | Score tracking with auto-reset scheduler          |
| `neon add matchmaking`     | Rating-based queue with auto-pairing scheduler    |
| `neon add guilds`          | Guild creation, invites, membership, kick         |
| `neon add quests`          | Quest accept, progress tracking, claim            |
| `neon add economy`         | Gold/gem wallets, shop, transfers, loot boxes     |
| `neon add combat`          | NPC table, attack, respawn, abilities             |
| `neon add world`           | Zones, NPC spawning, world tick + session cleanup |

## Scaling

NeonDB handles thousands of concurrent players on a single node. For multi-region or 30K+ CCU, see `SCALING.md`.

## Reducer API

| Symbol         | Type                         | Description                        |
|----------------|------------------------------|------------------------------------|
| `args`         | `any[]`                      | Positional arguments from client   |
| `db.get`       | `(table, key) → obj\|null`   | Read a single row                  |
| `db.set`       | `(table, key, obj) → void`   | Write or overwrite a row           |
| `db.delete`    | `(table, key) → void`        | Remove a row                       |
| `db.all`       | `(table) → obj[]`            | Read all rows in a table           |
| `caller.id`    | `string`                     | ID of the calling player           |
| `caller.role`  | `string`                     | Role of the calling player         |
| `result`       | `any`                        | Assign this to return data to client |
