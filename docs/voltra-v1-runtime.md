# Voltra V1 Runtime Direction

Voltra V1 turns Voltra into a low-latency game database/server/runtime while
keeping the systems that already make Voltra fast: reducers, WAL recovery,
subscriptions, SDKs, and protocol support.

The first architectural rule is that hot simulation state and durable gameplay
state are not the same thing.

- Hot simulation state belongs to lobby-owned ECS storage.
- Durable gameplay state belongs to reducer/TableStore persistence.
- Account data belongs outside the runtime behind an auth adapter.

## Hot Path

```text
client input
-> session cache
-> lobby route
-> reducer/system
-> ECS update
-> AOI
-> delta encoder
-> fanout
```

This path must avoid blocking I/O, external service calls, unbounded queues, and
avoidable allocation.

## Composition

Voltra V1 replaces preset-first templates with module-first composition. Genre
templates become recipes over reusable modules:

```text
fps = lobby + tick + ecs + aoi + movement + weapons + combat + hit-detection
mmo = lobby + tick + ecs + movement + combat + inventory + equipment + economy + quests + guilds + chat
racing = lobby + tick + ecs + movement + matchmaking + leaderboard + replay
```

The code foundation for this lives in `src/runtime/mod.rs`.
