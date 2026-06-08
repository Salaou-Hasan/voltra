# NeonDB Chat Template

A realtime chat starter: rooms, messages, threads, reactions, presence,
typing indicators, and moderation.

## Layout

```
modules/rooms/        create_room, join_room, leave_room, delete_room
modules/messages/     send_message, edit_message, delete_message, react
modules/threads/      create_thread, reply
modules/presence/     set_online, set_typing, cleanup (scheduled 30s)
modules/moderation/   ban_user, unban_user
schema.toml           all tables
```

## Run

```bash
neondb start
neondb call create_room  '["general", "General", "alice"]'
neondb call send_message '["general", "m1", "alice", "Hello!"]'
neondb watch "messages WHERE room_id = 'general' ORDER BY created_at DESC LIMIT 100"
```

Moderator/admin actions require `Authorization: Bearer <key>:moderator`
during the WebSocket upgrade.

---

## Performance

Reducers in this template run on the **Boa JS engine** (no JIT, ~50 k calls/s).
High-frequency reducers like `send_message` and `set_typing` see the largest
gains from compiling to WASM before any real load test.

```bash
neondb build   # .js → .wasm via Javy; server auto-picks WASM on next start
neondb start
```

WASM runs on Wasmtime/Cranelift (~500 k calls/s, 10–50× faster).

The `cleanup` scheduler and presence updates are good WASM candidates at scale.
For a pure message-delivery hot path that must handle tens of thousands of
concurrent users, consider native Rust registration (2 M+ calls/s).

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
| `__neondb_caller_role` | Role string (e.g. `"moderator"`, `"admin"`) |
