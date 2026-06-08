# NeonDB Game-Ready Template

A multi-system starter for multiplayer games: combat, economy, quests,
matchmaking, guilds, leaderboards, and world ticks.

## Layout

```
modules/world/        spawn, despawn, move, update_stats
modules/combat/       spawn_npc, attack, use_ability, apply_damage, respawn
modules/economy/      buy_item, sell_item, transfer_currency, open_loot_box
modules/quests/       accept_quest, complete_quest, update_progress
modules/matchmaking/  queue, dequeue, create_match, refresh (scheduled 5s)
modules/guilds/       guild_create, guild_invite, guild_accept, guild_kick
modules/ticks/        world_tick (1s), cleanup_sessions (60s)
modules/leaderboards/ submit_score, reset_weekly (weekly)
schema.toml           all tables
seed.json             starter data — `neondb seed seed.json`
client/               TypeScript client example
```

## Run

```bash
neondb start
neondb seed seed.json
neondb call spawn '["player1", 0, 0, "warrior"]'
neondb watch "players WHERE zone = 'zone_0_0'"
```

See `GENRE_GUIDE.md` for adapting the template to your genre.

---

## Performance

Reducers in this template run on the **Boa JS engine** (no JIT, ~50 k calls/s).
High-frequency reducers like `move`, `attack`, and `world_tick` benefit most
from upgrading to WASM before any production load test.

```bash
neondb build   # .js → .wasm via Javy; server auto-picks WASM on next start
neondb start
```

WASM runs on Wasmtime/Cranelift (~500 k calls/s, 10–50× faster).

For world-tick loops with thousands of players, or sub-millisecond combat
resolution, consider registering those reducers as native Rust functions
compiled into the server binary (2 M+ calls/s).

See **PERFORMANCE.md** in this project root for benchmark numbers, the full
three-tier guide, and native-Rust registration instructions.

---

## Reducer API Quick Reference

These globals are available in every `.js` reducer file:

| Global | Description |
|--------|-------------|
| `args` | Array of positional arguments passed by the client |
| `result` | Assign the return value here before the file ends |
| `__neondb_get(table, key)` | Read one row → `object \| null` |
| `__neondb_set(table, key, val)` | Write/upsert one row |
| `__neondb_delete(table, key)` | Delete one row |
| `__neondb_get_all(table)` | Read all rows → `object[]` |
| `__neondb_caller_id` | Identity string of the calling client |
| `__neondb_caller_role` | Role string (e.g. `"admin"`, `"player"`) |
