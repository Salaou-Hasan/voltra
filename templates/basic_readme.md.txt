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

---

## Performance

Reducers in this template run on the **Boa JS engine** (no JIT, ~50 k calls/s).
Before any real load test, compile them to WASM with one command:

```bash
neondb build   # .js → .wasm via Javy; server auto-picks WASM on next start
neondb start
```

WASM runs on Wasmtime/Cranelift and delivers ~500 k calls/s — a 10–50× uplift
with zero code changes.  For maximum throughput (2 M+ calls/s), register
reducers as native Rust functions compiled into the server binary.

See **PERFORMANCE.md** in this project root for the full three-tier guide,
benchmark numbers, and native-Rust registration instructions.

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
