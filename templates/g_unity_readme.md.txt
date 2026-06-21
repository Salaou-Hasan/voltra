# game/unity — Voltra + Unity Multiplayer Template

A full-stack multiplayer template: a Voltra game server with native reducers and a Unity C# SDK ready to drop into your project.

## What You Get

```
reducers/       — server-side game logic (spawn, move, damage, heal, despawn)
schema.toml     — players + sessions table definitions
unity/
  VoltraClient.cs     — low-level WebSocket + MessagePack client
  VoltraBehaviour.cs  — MonoBehaviour that pumps callbacks on Update()
  VoltraManager.cs    — high-level game API (spawn, move, subscribe)
```

## Setup

1. Start the Voltra server:
   ```bash
   voltra start
   ```

2. Copy the `unity/` folder into your Unity project at `Assets/Scripts/Voltra/`.

3. Add `VoltraManager` to a GameObject in your scene (e.g. a GameManager object).

4. In the Inspector, set:
   - **Server URL**: `ws://localhost:3000` (or your deployed server address)
   - **Api Key**: leave blank for local dev; set for production

5. Call reducers from any MonoBehaviour:
   ```csharp
   // Spawn and subscribe
   await VoltraManager.Instance.SpawnPlayer("player1", "lobby_1", "warrior");

   // Move on input
   await VoltraManager.Instance.MovePlayer("player1", transform.position.x, transform.position.z);
   ```

6. React to live player updates:
   ```csharp
   VoltraManager.Instance.OnPlayerUpdate += (row) => {
       float x = row["x"].ToObject<float>();
       float y = row["y"].ToObject<float>();
       // move your character GameObject here
   };
   ```

## Scaling

Voltra handles thousands of concurrent players on a single node. For multi-region or 30K+ CCU, see `SCALING.md`.

## Add Modules

| Command                    | What it adds                                      |
|----------------------------|---------------------------------------------------|
| `voltra add chat`            | Room-based chat with join/leave/send              |
| `voltra add inventory`       | Per-player items with add/remove/equip            |
| `voltra add leaderboard`     | Score tracking with auto-reset scheduler          |
| `voltra add matchmaking`     | Rating-based queue with auto-pairing scheduler    |
| `voltra add guilds`          | Guild creation, invites, membership, kick         |
| `voltra add quests`          | Quest accept, progress tracking, claim            |
| `voltra add economy`         | Gold/gem wallets, shop, transfers, loot boxes     |
| `voltra add combat`          | NPC table, attack, respawn, abilities             |
| `voltra add world`           | Zones, NPC spawning, world tick + session cleanup |
