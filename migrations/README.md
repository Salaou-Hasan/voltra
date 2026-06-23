# Voltra Migrations

Place `.toml` migration files here. They are applied at startup (after WAL replay),
sorted lexicographically, so use numeric prefixes:

```
migrations/
├── 001_initial_schema.toml
├── 002_add_score_field.toml
└── 003_rename_legacy_column.toml
```

## File format

```toml
version = 1
description = "Human-readable description"

# Add a field with a default value to rows that don't have it (idempotent)
[[steps]]
table = "players"
op = "add_field"
field = "score"
default = 0

# Remove a field from all rows that have it (idempotent)
[[steps]]
table = "players"
op = "remove_field"
field = "legacy_hp"

# Rename a field in all rows that have the old name (idempotent)
[[steps]]
table = "players"
op = "rename_field"
old_field = "pts"
new_field = "points"
```

## Notes
- Migrations are **idempotent** — safe to re-apply after restart.
- `add_field` only adds the field to rows missing it (does not overwrite).
- Migrations run AFTER WAL replay so the live data is already present.
- Migrations do NOT write to the WAL — they transform in-memory state directly.
  The next WAL entry after migration will reflect the migrated data.
