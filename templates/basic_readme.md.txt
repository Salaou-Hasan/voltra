# NeonDB Basic Template

A minimal starter project with user accounts, sessions, and an inventory.

## Layout

```
modules/auth/        register, login, logout, grant_role
modules/profile/     update_profile, delete_user
modules/inventory/   add_item, remove_item
modules/sub/         sub_player (subscription helper)
schema.toml          table definitions
client/              example TypeScript client
```

## Run

```bash
neondb start
```

Then in another shell:

```bash
neondb call register '["alice", "hashed-pw"]'
neondb call login    '["alice", "hashed-pw"]'
neondb get users
```

## Next steps

- Edit `schema.toml` to add fields.
- Write new reducers in `modules/`.
- Wire up the TypeScript client in `client/`.
