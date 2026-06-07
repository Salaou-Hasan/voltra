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
