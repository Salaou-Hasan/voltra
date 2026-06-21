# game/godot — Voltra + Godot 4 Multiplayer Template

A full-stack multiplayer template: a Voltra game server with native reducers and a Godot 4 GDScript client ready to use as an autoload.

## What You Get

```
reducers/           — server-side game logic (spawn, move, damage, heal, despawn)
schema.toml         — players + sessions table definitions
godot/
  voltra_client.gd      — WebSocketPeer client with MessagePack framing
  VoltraManager.gd      — autoload: spawn, move, row_update signal
```

## Setup

1. Start the Voltra server:
   ```bash
   voltra start
   ```

2. Copy the `godot/` folder into your Godot project (e.g. `res://addons/voltra/`).

3. Add `VoltraManager.gd` as an Autoload in **Project → Project Settings → Autoloads**:
   - Path: `res://addons/voltra/VoltraManager.gd`
   - Name: `Voltra`

4. In the Inspector (or directly in the script), set:
   - `server_url`: `ws://localhost:3000`
   - `api_key`: blank for local dev; set for production
   - `lobby_id`: the lobby this client belongs to

5. Call reducers from any Node:
   ```gdscript
   # Spawn player and wait for result
   var res = await Voltra.spawn_player("player1", "lobby_1", "warrior")
   print(res)   # { ok: true, player: { ... } }

   # Move on input
   await Voltra.move_player("player1", position.x, position.z)
   ```

6. React to live player updates via the signal:
   ```gdscript
   func _ready():
       Voltra.player_updated.connect(_on_player_updated)

   func _on_player_updated(data: Dictionary):
       $Sprite2D.position = Vector2(data["x"], data["y"])
   ```

## Scaling

Voltra handles thousands of concurrent players on a single node. For multi-region or 30K+ CCU, see `SCALING.md`.

## Add Modules

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
