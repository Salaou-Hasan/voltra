//! Schema migration support for NeonDB.
//!
//! On startup (after WAL replay), NeonDB scans the `migrations/` directory
//! for `*.toml` files sorted lexicographically and applies each migration
//! to the in-memory `TableStore`.
//!
//! ## Migration file format
//!
//! ```toml
//! # migrations/001_add_score.toml
//! version = 1
//! description = "Add score field to players"
//!
//! [[steps]]
//! table = "players"
//! op = "add_field"
//! field = "score"
//! default = 0
//!
//! [[steps]]
//! table = "counters"
//! op = "remove_field"
//! field = "legacy_field"
//!
//! [[steps]]
//! table = "players"
//! op = "rename_field"
//! old_field = "pts"
//! new_field  = "points"
//! ```
//!
//! ## Supported operations
//!
//! | `op`           | Required fields          | Description |
//! |----------------|--------------------------|-------------|
//! | `add_field`    | `table`, `field`, `default` | Add `field` with `default` value to every row that is missing it |
//! | `remove_field` | `table`, `field`         | Remove `field` from every row that has it |
//! | `rename_field` | `table`, `old_field`, `new_field` | Rename a field in every row that has it |
//!
//! ## Idempotency
//!
//! - `add_field` skips rows that already have the field.
//! - `remove_field` skips rows that don't have the field.
//! - `rename_field` skips rows where `old_field` is absent.

use crate::error::{NeonDBError, Result};
use crate::table::TableStore;
use serde::Deserialize;
use serde_json::Value;
use std::path::Path;
use std::sync::Arc;

// ── Migration file schema ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct MigrationFile {
    version: u64,
    #[allow(dead_code)]
    description: Option<String>,
    steps: Vec<MigrationStep>,
}

#[derive(Debug, Deserialize)]
struct MigrationStep {
    table: String,
    op: String,
    // add_field / remove_field
    field: Option<String>,
    // add_field
    default: Option<Value>,
    // rename_field
    old_field: Option<String>,
    new_field: Option<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// System table that tracks which migration files have already been applied.
/// Each row's key is the migration filename; the value is
/// `{"applied_at": <unix_nanos>, "version": <u64>}`.
const MIGRATIONS_TABLE: &str = "__migrations";

/// Scan `migrations_dir` for `*.toml` files, sort them lexicographically
/// (so `001_…` < `002_…`), and apply each migration to `tables`.
///
/// Migration files that already appear in the `__migrations` system table
/// are skipped — so re-running this on startup is idempotent.
///
/// Returns the number of migration files applied **this call** (skipped
/// migrations do not count).
/// Missing or empty directory is treated as "no migrations" (returns `Ok(0)`).
pub fn apply_migrations(migrations_dir: &Path, tables: &Arc<TableStore>) -> Result<usize> {
    if !migrations_dir.is_dir() {
        return Ok(0);
    }

    let mut paths: Vec<_> = std::fs::read_dir(migrations_dir)?
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("toml"))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();

    // Sort lexicographically — numeric prefixes ensure `001_…` < `002_…`.
    paths.sort();

    let mut applied = 0usize;
    for path in &paths {
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                NeonDBError::internal(format!("Migration path has no filename: {:?}", path))
            })?
            .to_string();

        // Idempotency check: skip migrations that have already been applied.
        if tables.get_row(MIGRATIONS_TABLE, &filename)?.is_some() {
            log::info!("Migration {} already applied, skipping", filename);
            continue;
        }

        let contents = std::fs::read_to_string(path).map_err(|e| {
            NeonDBError::internal(format!("Failed to read migration {:?}: {}", path, e))
        })?;
        let mig: MigrationFile = toml::from_str(&contents).map_err(|e| {
            NeonDBError::internal(format!("Failed to parse migration {:?}: {}", path, e))
        })?;
        apply_migration(&mig, tables, path)?;

        // Mark applied so the next startup skips it.
        let now_nanos: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        tables.set_row(
            MIGRATIONS_TABLE.to_string(),
            filename.clone(),
            serde_json::json!({
                "applied_at": now_nanos as u64,
                "version": mig.version,
            }),
        )?;

        applied += 1;
    }
    Ok(applied)
}

/// Apply a single migration from its TOML content string.
///
/// Returns `Ok(true)` if the migration was applied, `Ok(false)` if it was already
/// recorded in `__migrations` (skipped).  Returns `Err` on parse or apply failure.
///
/// This is the HTTP-server-facing entry point used by `POST /migrate`.  It mirrors
/// the per-file logic inside `apply_migrations()` without requiring a filesystem path.
pub fn apply_migration_str(
    filename: &str,
    content: &str,
    tables: &Arc<TableStore>,
) -> Result<bool> {
    // Idempotency: skip if already applied.
    if tables.get_row(MIGRATIONS_TABLE, filename)?.is_some() {
        log::info!("Migration {} already applied, skipping", filename);
        return Ok(false);
    }

    let mig: MigrationFile = toml::from_str(content).map_err(|e| {
        NeonDBError::internal(format!("Failed to parse migration '{}': {}", filename, e))
    })?;

    // Use a synthetic path for log messages.
    let synthetic_path = Path::new(filename);
    apply_migration(&mig, tables, synthetic_path)?;

    let now_nanos: u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    tables.set_row(
        MIGRATIONS_TABLE.to_string(),
        filename.to_string(),
        serde_json::json!({
            "applied_at": now_nanos as u64,
            "version": mig.version,
        }),
    )?;

    Ok(true)
}

// ── Internal ──────────────────────────────────────────────────────────────────

fn apply_migration(mig: &MigrationFile, tables: &Arc<TableStore>, path: &Path) -> Result<()> {
    log::info!(
        "Applying migration v{} from {:?}: {} step(s)",
        mig.version,
        path.file_name().unwrap_or_default(),
        mig.steps.len()
    );

    for step in &mig.steps {
        match step.op.as_str() {
            "add_field" => {
                let field = step
                    .field
                    .as_deref()
                    .ok_or_else(|| NeonDBError::invalid_argument("add_field requires 'field'"))?;
                let default_val = step
                    .default
                    .clone()
                    .ok_or_else(|| NeonDBError::invalid_argument("add_field requires 'default'"))?;
                add_field(tables, &step.table, field, default_val)?;
            }
            "remove_field" => {
                let field = step.field.as_deref().ok_or_else(|| {
                    NeonDBError::invalid_argument("remove_field requires 'field'")
                })?;
                remove_field(tables, &step.table, field)?;
            }
            "rename_field" => {
                let old_field = step.old_field.as_deref().ok_or_else(|| {
                    NeonDBError::invalid_argument("rename_field requires 'old_field'")
                })?;
                let new_field = step.new_field.as_deref().ok_or_else(|| {
                    NeonDBError::invalid_argument("rename_field requires 'new_field'")
                })?;
                rename_field(tables, &step.table, old_field, new_field)?;
            }
            other => {
                return Err(NeonDBError::invalid_argument(format!(
                    "Unknown migration op '{}' in {:?}",
                    other, path
                )));
            }
        }
    }
    Ok(())
}

/// Add `field` with `default_val` to every row in `table_name` that is missing it.
fn add_field(
    tables: &Arc<TableStore>,
    table_name: &str,
    field: &str,
    default_val: Value,
) -> Result<()> {
    let rows = tables.list_rows_with_keys(table_name)?;
    let mut modified = 0usize;
    for (key, mut row_data) in rows {
        if let Some(obj) = row_data.as_object_mut() {
            if !obj.contains_key(field) {
                obj.insert(field.to_string(), default_val.clone());
                tables.set_row(table_name.to_string(), key, row_data)?;
                modified += 1;
            }
        }
    }
    log::debug!(
        "  add_field {}.{}: {} rows updated",
        table_name,
        field,
        modified
    );
    Ok(())
}

/// Remove `field` from every row in `table_name` that has it.
fn remove_field(tables: &Arc<TableStore>, table_name: &str, field: &str) -> Result<()> {
    let rows = tables.list_rows_with_keys(table_name)?;
    let mut modified = 0usize;
    for (key, mut row_data) in rows {
        if let Some(obj) = row_data.as_object_mut() {
            if obj.remove(field).is_some() {
                tables.set_row(table_name.to_string(), key, row_data)?;
                modified += 1;
            }
        }
    }
    log::debug!(
        "  remove_field {}.{}: {} rows updated",
        table_name,
        field,
        modified
    );
    Ok(())
}

/// Rename `old_field` to `new_field` in every row in `table_name` that has `old_field`.
fn rename_field(
    tables: &Arc<TableStore>,
    table_name: &str,
    old_field: &str,
    new_field: &str,
) -> Result<()> {
    let rows = tables.list_rows_with_keys(table_name)?;
    let mut modified = 0usize;
    for (key, mut row_data) in rows {
        if let Some(obj) = row_data.as_object_mut() {
            if let Some(val) = obj.remove(old_field) {
                obj.insert(new_field.to_string(), val);
                tables.set_row(table_name.to_string(), key, row_data)?;
                modified += 1;
            }
        }
    }
    log::debug!(
        "  rename_field {}.{} -> {}: {} rows updated",
        table_name,
        old_field,
        new_field,
        modified
    );
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn store() -> Arc<TableStore> {
        Arc::new(TableStore::new())
    }

    #[test]
    fn test_add_field_missing_rows() {
        let ts = store();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"name": "Alice"}),
        )
        .unwrap();

        add_field(&ts, "players", "score", serde_json::json!(0)).unwrap();

        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert_eq!(row["score"], 0);
    }

    #[test]
    fn test_add_field_skips_existing() {
        let ts = store();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"score": 99}),
        )
        .unwrap();

        add_field(&ts, "players", "score", serde_json::json!(0)).unwrap();

        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert_eq!(row["score"], 99, "should not overwrite existing field");
    }

    #[test]
    fn test_remove_field() {
        let ts = store();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"name": "Alice", "legacy": true}),
        )
        .unwrap();

        remove_field(&ts, "players", "legacy").unwrap();

        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert!(row.get("legacy").is_none());
        assert_eq!(row["name"], "Alice");
    }

    #[test]
    fn test_rename_field() {
        let ts = store();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"pts": 50}),
        )
        .unwrap();

        rename_field(&ts, "players", "pts", "points").unwrap();

        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert!(row.get("pts").is_none());
        assert_eq!(row["points"], 50);
    }

    #[test]
    fn test_apply_migrations_empty_dir() {
        let ts = store();
        let tmp = std::env::temp_dir().join("neondb_mig_empty_test");
        let _ = std::fs::create_dir_all(&tmp);
        let result = apply_migrations(&tmp, &ts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_apply_migrations_from_toml() {
        let ts = store();
        // Insert a player row with old field names
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"name": "Bob", "pts": 10}),
        )
        .unwrap();

        // Write a migration file
        let tmp = std::env::temp_dir().join("neondb_mig_toml_test");
        let _ = std::fs::create_dir_all(&tmp);
        let mig_content = r#"
version = 1
description = "rename pts to points and add score"

[[steps]]
table = "players"
op = "rename_field"
old_field = "pts"
new_field = "points"

[[steps]]
table = "players"
op = "add_field"
field = "score"
default = 0
"#;
        std::fs::write(tmp.join("001_rename_pts.toml"), mig_content).unwrap();

        let applied = apply_migrations(&tmp, &ts).unwrap();
        assert_eq!(applied, 1);

        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert_eq!(row["points"], 10, "pts should be renamed to points");
        assert_eq!(row["score"], 0, "score should be added with default 0");
        assert!(row.get("pts").is_none(), "old pts field should be gone");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_apply_migrations_records_in_system_table() {
        let ts = store();
        let tmp = std::env::temp_dir().join("neondb_mig_idempotent_record_test");
        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::create_dir_all(&tmp);

        let mig_content = r#"
version = 1
description = "add score field"

[[steps]]
table = "players"
op = "add_field"
field = "score"
default = 0
"#;
        std::fs::write(tmp.join("001_add_score.toml"), mig_content).unwrap();

        let applied = apply_migrations(&tmp, &ts).unwrap();
        assert_eq!(applied, 1, "first run should apply the migration");

        // Confirm the row landed in the __migrations system table.
        let row = ts
            .get_row(MIGRATIONS_TABLE, "001_add_score.toml")
            .unwrap()
            .expect("migration should be recorded in __migrations");
        assert_eq!(row["version"], 1);
        assert!(
            row.get("applied_at").map(|v| v.as_u64().unwrap_or(0) > 0).unwrap_or(false),
            "applied_at should be a non-zero unix nanos value"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_apply_migrations_is_idempotent() {
        let ts = store();
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"name": "Bob"}),
        )
        .unwrap();

        let tmp = std::env::temp_dir().join("neondb_mig_idempotent_skip_test");
        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::create_dir_all(&tmp);

        // First migration: add `score` with default 0.
        let mig_content = r#"
version = 1

[[steps]]
table = "players"
op = "add_field"
field = "score"
default = 0
"#;
        std::fs::write(tmp.join("001_add_score.toml"), mig_content).unwrap();

        let applied_first = apply_migrations(&tmp, &ts).unwrap();
        assert_eq!(applied_first, 1, "first run applies one migration");

        // Manually mutate the row so we can detect whether the migration runs again.
        // (If it did, `add_field` would skip since the field already exists — but
        // we want to confirm the apply_migration path is not even entered.)
        ts.set_row(
            "players".to_string(),
            "p1".to_string(),
            serde_json::json!({"name": "Bob", "score": 999}),
        )
        .unwrap();

        let applied_second = apply_migrations(&tmp, &ts).unwrap();
        assert_eq!(
            applied_second, 0,
            "second run should skip already-applied migration"
        );

        // Confirm the user-set value of 999 was NOT clobbered — proving the
        // migration step truly did not re-execute.
        let row = ts.get_row("players", "p1").unwrap().unwrap();
        assert_eq!(row["score"], 999);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
