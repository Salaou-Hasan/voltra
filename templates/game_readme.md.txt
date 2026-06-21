# Voltra Game-Ready Template

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
seed.json             starter data — `voltra seed seed.json`
client/               TypeScript client example
```

## Run

```bash
voltra start
voltra seed seed.json
voltra call spawn '["player1", 0, 0, "warrior"]'
voltra watch "players WHERE lobby = 'lobby_1'"
```

See `GENRE_GUIDE.md` for adapting the template to your genre.

---

## Scaling

This template is designed to scale in three tiers without rewriting any reducer.

| Players      | Setup                                         |
|-------------|-----------------------------------------------|
| < 10K CCU   | `voltra start`  — single node, zero config    |
| 10K–400K    | Set `VOLTRA_SHARD_ID/COUNT/PEERS` — cluster   |
| 400K+       | Set `VOLTRA_REGION/REGIONS` — multi-region    |

Every player row stores `lobby` and `region` fields. Subscriptions are
lobby-scoped (`players WHERE lobby = 'X'`) so each client only receives
updates for its own instance — the foundation that makes sharding work.

See **SCALING.md** for the full setup, env var reference, and capacity numbers.

---

## Performance

Reducers in this template run on the **Boa JS engine** (no JIT, ~50 k calls/s).
High-frequency reducers like `move`, `attack`, and `world_tick` benefit most
from upgrading to WASM before any production load test.

```bash
voltra build   # .js → .wasm via Javy; server auto-picks WASM on next start
voltra start
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
| `__voltra_get(table, key)` | Read one row → `object \| null` |
| `__voltra_set(table, key, val)` | Write/upsert one row |
| `__voltra_delete(table, key)` | Delete one row |
| `__voltra_get_all(table)` | Read all rows → `object[]` |
| `__voltra_caller_id` | Identity string of the calling client |
| `__voltra_caller_role` | Role string (e.g. `"admin"`, `"player"`) |
