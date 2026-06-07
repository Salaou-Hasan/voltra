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
