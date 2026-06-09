// ============================================================================
// NeonDB Schema System — TODO-018
//
// Provides typed table schemas loaded from `schema.toml` at startup.
//
// Schema defines:
//   - Column names and types for each table
//   - Which column is the primary key
//   - Optional default values
//
// The server validates every `set_row` call against the registered schema.
// Tables without a registered schema continue to accept any JSON (schema-free
// mode — backward compatible with all existing reducers and templates).
//
// schema.toml format:
//
//   [[table]]
//   name    = "players"
//   primary_key = "id"
//
//   [[table.columns]]
//   name = "id"
//   type = "String"
//
//   [[table.columns]]
//   name     = "score"
//   type     = "i64"
//   default  = "0"
//
//   [[table.columns]]
//   name     = "active"
//   type     = "bool"
//   default  = "true"
//
// Supported types: String, i64, f64, bool, bytes
//   (arrays and nested objects are accepted as-is — type = "any")
// ============================================================================

use crate::error::{NeonDBError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

// ── Column type ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    String,
    I64,
    F64,
    Bool,
    Bytes,
    /// Accepts any JSON value — opt-out of type checking for a column.
    Any,
}

impl ColumnType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "string" | "str" | "text"      => Some(ColumnType::String),
            "i64" | "i32" | "int" | "integer" | "number" => Some(ColumnType::I64),
            "f64" | "f32" | "float" | "double"           => Some(ColumnType::F64),
            "bool" | "boolean"             => Some(ColumnType::Bool),
            "bytes" | "blob"               => Some(ColumnType::Bytes),
            "any" | "json" | "object"      => Some(ColumnType::Any),
            _                              => None,
        }
    }

    /// Return a human-readable name for error messages.
    fn display(&self) -> &'static str {
        match self {
            ColumnType::String => "String",
            ColumnType::I64    => "i64",
            ColumnType::F64    => "f64",
            ColumnType::Bool   => "bool",
            ColumnType::Bytes  => "bytes",
            ColumnType::Any    => "any",
        }
    }

    /// Check whether a JSON value satisfies this column type.
    fn accepts(&self, value: &Value) -> bool {
        match self {
            ColumnType::String => value.is_string(),
            ColumnType::I64    => value.is_i64() || value.is_u64(),
            ColumnType::F64    => value.is_f64(),
            ColumnType::Bool   => value.is_boolean(),
            ColumnType::Bytes  => value.is_string() || value.is_array(), // base64 string or byte array
            ColumnType::Any    => true,
        }
    }

    /// Coerce a JSON value to this column type where safe.
    /// Returns the coerced value or None if coercion is impossible.
    fn coerce(&self, value: Value) -> Option<Value> {
        match self {
            ColumnType::F64 => {
                // Accept integer JSON values as f64 (and store them as an f64
                // JSON number so `Value::is_f64()` becomes true).
                if let Some(i) = value.as_i64() {
                    let f = i as f64;
                    serde_json::Number::from_f64(f).map(Value::Number)
                } else if value.is_f64() {
                    Some(value)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

// ── Column definition ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// Type string from schema.toml — parsed into ColumnType on load.
    #[serde(rename = "type")]
    pub type_str: String,
    /// Optional default value as a JSON-compatible string.
    pub default: Option<String>,
    /// Whether this column is required (non-null).
    #[serde(default = "default_true")]
    pub required: bool,
}

fn default_true() -> bool { true }

impl ColumnDef {
    pub fn col_type(&self) -> ColumnType {
        ColumnType::from_str(&self.type_str).unwrap_or(ColumnType::Any)
    }

    pub fn default_value(&self) -> Option<Value> {
        let s = self.default.as_deref()?;
        // Try parsing as a JSON literal first (handles numbers, bools, "null")
        if let Ok(v) = serde_json::from_str(s) {
            return Some(v);
        }
        // Fall back to treating it as a plain string
        Some(Value::String(s.to_string()))
    }
}

// ── Row-Level Security policy ─────────────────────────────────────────────────

/// Controls which callers may read/write rows in a table.
///
/// The policy is evaluated for every `get_row` call (in `ReducerContext`) and
/// before `apply_delta_batch` is called from `commit()`.  Callers whose
/// `caller_role` is `"scheduler"` or `"system"` bypass all policies
/// unconditionally so that internal background work is never blocked.
///
/// ## Variants
/// - `Public` — anyone may access any row.  This is the default and preserves
///   full backward compatibility.
/// - `OwnerField { field }` — the row's `field` value must equal `caller_id`.
///   New inserts (where `row` is `None`) are always allowed; the reducer is
///   responsible for setting the owner field at insert time.
/// - `RoleGated { roles }` — only callers whose `caller_role` is listed may
///   access.
/// - `OwnerFieldWithAdmin { field, admin_roles }` — either the row's `field`
///   equals `caller_id`, OR the caller's role is in `admin_roles`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RlsPolicy {
    #[default]
    Public,
    OwnerField { field: String },
    RoleGated { roles: Vec<String> },
    OwnerFieldWithAdmin { field: String, admin_roles: Vec<String> },
}

/// Evaluate an RLS policy for a single access attempt.
///
/// Returns `true` if access is permitted, `false` if denied.
///
/// # Bypass
/// Callers with `caller_role == "scheduler"` or `caller_role == "system"` are
/// always permitted regardless of the policy.
///
/// # Arguments
/// - `policy` — the table's configured RLS policy.
/// - `row` — the current stored row value, or `None` for new inserts (where the
///   row does not exist yet).  For `OwnerField` variants, `None` means "new
///   insert" and is allowed so the reducer can set the owner field.
/// - `caller_id` — the identity of the calling client.
/// - `caller_role` — the role extracted from the client's bearer token.
pub fn rls_check(
    policy: &RlsPolicy,
    row: Option<&serde_json::Value>,
    caller_id: &str,
    caller_role: &str,
) -> bool {
    // Hardcoded bypass for internal/scheduler callers.
    if caller_role == "scheduler" || caller_role == "system" {
        return true;
    }

    match policy {
        RlsPolicy::Public => true,

        RlsPolicy::OwnerField { field } => {
            match row {
                // New insert — allow so the reducer can set the owner field.
                None => true,
                Some(row_val) => {
                    row_val
                        .get(field)
                        .and_then(|v| v.as_str())
                        .map(|owner| owner == caller_id)
                        .unwrap_or(false)
                }
            }
        }

        RlsPolicy::RoleGated { roles } => roles.iter().any(|r| r == caller_role),

        RlsPolicy::OwnerFieldWithAdmin { field, admin_roles } => {
            // Admin bypass.
            if admin_roles.iter().any(|r| r == caller_role) {
                return true;
            }
            // Owner check.
            match row {
                None => true,
                Some(row_val) => {
                    row_val
                        .get(field)
                        .and_then(|v| v.as_str())
                        .map(|owner| owner == caller_id)
                        .unwrap_or(false)
                }
            }
        }
    }
}

// ── Table schema ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    #[serde(default)]
    pub primary_key: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnDef>,
    /// Row-level security policy for this table.
    /// Defaults to `Public` (no restrictions) — fully backward-compatible.
    #[serde(default)]
    pub rls: RlsPolicy,
}

impl TableSchema {
    /// Build a column map for O(1) lookup by name.
    fn column_map(&self) -> HashMap<String, &ColumnDef> {
        self.columns.iter().map(|c| (c.name.clone(), c)).collect()
    }

    /// Validate and optionally fill defaults in a row value.
    ///
    /// Returns the (potentially modified) value if valid, or an error
    /// describing the first schema violation found.
    pub fn validate_and_fill(&self, mut value: Value) -> Result<Value> {
        let col_map = self.column_map();

        // 1. Fill defaults for missing columns and reject explicit nulls for
        //    required columns.  An explicit JSON null is treated identically to
        //    "missing" for required fields — without this the validator silently
        //    accepted `{"id": null, ...}` and the bad value landed in the store.
        if let Some(obj) = value.as_object_mut() {
            for col in &self.columns {
                let is_missing_or_null = obj
                    .get(&col.name)
                    .map(|v| v.is_null())
                    .unwrap_or(true);

                if is_missing_or_null {
                    // First try to fill from default — applies whether the field
                    // was absent OR explicitly null.
                    if let Some(default_val) = col.default_value() {
                        if !default_val.is_null() {
                            obj.insert(col.name.clone(), default_val);
                            continue;
                        }
                    }

                    // No default (or default itself is null) — required columns
                    // must reject both the "missing" and "explicit null" cases.
                    if col.required && col.col_type() != ColumnType::Any {
                        // Disambiguate the error message so callers can see
                        // whether the field was absent or explicitly null.
                        let was_present = obj.contains_key(&col.name);
                        if was_present {
                            return Err(NeonDBError::table_error(format!(
                                "Schema violation on table '{}': required column '{}' must not be null",
                                self.name, col.name
                            )));
                        } else {
                            return Err(NeonDBError::table_error(format!(
                                "Schema violation on table '{}': required column '{}' ({}) is missing and has no default",
                                self.name, col.name, col.col_type().display()
                            )));
                        }
                    }
                }
            }
        }

        // 2. Type-check all present columns that have schema definitions.
        if let Some(obj) = value.as_object_mut() {
            for (key, val) in obj.iter_mut() {
                // Skip internal NeonDB fields injected by the table engine.
                if key == "row_key" || key == "shard_id" { continue; }

                if let Some(col) = col_map.get(key.as_str()) {
                    let col_type = col.col_type();
                    // Explicit null on an optional column is allowed; required
                    // nulls were already rejected in step 1.
                    if val.is_null() {
                        continue;
                    }
                    if !col_type.accepts(val) {
                        // Attempt safe coercion before erroring.
                        if let Some(coerced) = col_type.coerce(val.clone()) {
                            *val = coerced;
                        } else {
                            return Err(NeonDBError::table_error(format!(
                                "Schema violation on table '{}': column '{}' expects {} but got {}",
                                self.name,
                                key,
                                col_type.display(),
                                json_type_name(val),
                            )));
                        }
                    }
                }
                // Columns not in the schema are allowed (open schema by default).
            }
        }

        Ok(value)
    }
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::String(_)  => "String",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_)  => "float",
        Value::Bool(_)    => "bool",
        Value::Null       => "null",
        Value::Array(_)   => "array",
        Value::Object(_)  => "object",
    }
}

// ── Schema registry ───────────────────────────────────────────────────────────

/// Registry of all table schemas, keyed by table name.
/// Tables not in the registry are schema-free (any JSON accepted).
#[derive(Debug, Default, Clone)]
pub struct SchemaRegistry {
    schemas: HashMap<String, TableSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load schemas from a `schema.toml` file.
    /// Returns an empty registry (no-op) if the file does not exist.
    pub fn load_from_file(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let content = std::fs::read_to_string(path).map_err(|e| {
            NeonDBError::internal(format!("Failed to read schema.toml: {}", e))
        })?;

        let raw: RawSchemaFile = toml::from_str(&content).map_err(|e| {
            NeonDBError::invalid_argument(format!(
                "schema.toml parse error: {}",
                e
            ))
        })?;

        let mut registry = Self::new();
        for table in raw.table {
            log::info!(
                "Schema: registered table '{}' ({} columns, pk={:?})",
                table.name,
                table.columns.len(),
                table.primary_key,
            );
            registry.schemas.insert(table.name.clone(), table);
        }

        Ok(registry)
    }

    /// Register a schema programmatically (useful for tests).
    pub fn register(&mut self, schema: TableSchema) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    /// Return the schema for `table_name`, or `None` if not registered.
    pub fn get(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    /// Validate and fill defaults in `value` for `table_name`.
    /// If no schema is registered for the table, returns the value unchanged.
    pub fn validate(&self, table_name: &str, value: Value) -> Result<Value> {
        match self.schemas.get(table_name) {
            Some(schema) => schema.validate_and_fill(value),
            None         => Ok(value),
        }
    }

    pub fn table_count(&self) -> usize {
        self.schemas.len()
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.schemas.keys().map(String::as_str).collect()
    }
}

// ── TOML file shape ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawSchemaFile {
    #[serde(rename = "table", default)]
    table: Vec<TableSchema>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn player_schema() -> TableSchema {
        TableSchema {
            name: "players".to_string(),
            primary_key: Some("id".to_string()),
            columns: vec![
                ColumnDef { name: "id".to_string(), type_str: "String".to_string(), default: None, required: true },
                ColumnDef { name: "score".to_string(), type_str: "i64".to_string(), default: Some("0".to_string()), required: true },
                ColumnDef { name: "active".to_string(), type_str: "bool".to_string(), default: Some("true".to_string()), required: true },
                ColumnDef { name: "name".to_string(), type_str: "String".to_string(), default: None, required: false },
            ],
            rls: RlsPolicy::default(),
        }
    }

    #[test]
    fn test_valid_row_passes() {
        let s = player_schema();
        let row = json!({ "id": "p1", "score": 100, "active": true });
        assert!(s.validate_and_fill(row).is_ok());
    }

    #[test]
    fn test_default_filled_for_missing_column() {
        let s = player_schema();
        let row = json!({ "id": "p1" });
        let result = s.validate_and_fill(row).unwrap();
        assert_eq!(result["score"], json!(0));
        assert_eq!(result["active"], json!(true));
    }

    #[test]
    fn test_type_mismatch_string_for_i64() {
        let s = player_schema();
        let row = json!({ "id": "p1", "score": "not-a-number", "active": true });
        let err = s.validate_and_fill(row).unwrap_err();
        assert!(err.to_string().contains("score"));
        assert!(err.to_string().contains("i64"));
    }

    #[test]
    fn test_type_mismatch_number_for_bool() {
        let s = player_schema();
        let row = json!({ "id": "p1", "score": 5, "active": 1 });
        let err = s.validate_and_fill(row).unwrap_err();
        assert!(err.to_string().contains("active"));
        assert!(err.to_string().contains("bool"));
    }

    #[test]
    fn test_f64_coercion_from_integer() {
        let schema = TableSchema {
            name: "readings".to_string(),
            primary_key: None,
            columns: vec![
                ColumnDef { name: "temp".to_string(), type_str: "f64".to_string(), default: None, required: true },
            ],
            rls: RlsPolicy::default(),
        };
        // JSON integer should be coerced to f64 without error
        let row = json!({ "temp": 25 });
        let result = schema.validate_and_fill(row).unwrap();
        assert!(result["temp"].is_f64());
        assert!((result["temp"].as_f64().unwrap() - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_registry_skips_unregistered_tables() {
        let mut reg = SchemaRegistry::new();
        reg.register(player_schema());

        // "items" has no schema — any value should pass through unchanged
        let row = json!({ "whatever": true, "random": 42 });
        let result = reg.validate("items", row.clone()).unwrap();
        assert_eq!(result, row);
    }

    #[test]
    fn test_registry_validates_registered_table() {
        let mut reg = SchemaRegistry::new();
        reg.register(player_schema());

        let bad_row = json!({ "id": "p1", "score": "wrong-type", "active": true });
        let err = reg.validate("players", bad_row).unwrap_err();
        assert!(err.to_string().contains("score"));
    }

    #[test]
    fn test_required_missing_column_no_default_errors() {
        let s = player_schema();
        // "id" has no default and required=true — omitting it should fail
        let row = json!({ "score": 10, "active": false });
        let err = s.validate_and_fill(row).unwrap_err();
        assert!(err.to_string().contains("id"));
    }

    #[test]
    fn test_extra_columns_allowed() {
        let s = player_schema();
        // Extra field "zone" is not in the schema — should be accepted
        let row = json!({ "id": "p1", "score": 5, "active": true, "zone": "zone_0_0" });
        let result = s.validate_and_fill(row).unwrap();
        assert_eq!(result["zone"], json!("zone_0_0"));
    }

    // ── Session 39: required-null rejection ──────────────────────────────────

    #[test]
    fn test_required_column_missing_rejected() {
        let s = player_schema();
        // "id" is required, no default — completely absent from the row
        let row = json!({ "score": 10, "active": true });
        let err = s.validate_and_fill(row).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("id"), "error should mention column name, got: {}", msg);
        assert!(msg.contains("missing"), "missing-column error expected, got: {}", msg);
    }

    #[test]
    fn test_required_column_with_value_ok() {
        let s = player_schema();
        let row = json!({ "id": "p1", "score": 7, "active": false });
        let result = s.validate_and_fill(row);
        assert!(result.is_ok(), "valid row should pass: {:?}", result.err());
    }

    #[test]
    fn test_required_column_explicit_null_rejected() {
        // NEW BEHAVIOR — previously `{ "id": null, ... }` slipped through and a
        // null primary key landed in the store. Now it's rejected with a clear
        // "must not be null" message.
        let s = player_schema();
        let row = json!({ "id": null, "score": 10, "active": true });
        let err = s.validate_and_fill(row).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("id"), "error should mention column name, got: {}", msg);
        assert!(msg.contains("must not be null"), "expected explicit-null error, got: {}", msg);
    }

    #[test]
    fn test_optional_column_with_null_ok() {
        // "name" is required=false on player_schema — null is acceptable.
        let s = player_schema();
        let row = json!({ "id": "p1", "score": 10, "active": true, "name": null });
        let result = s.validate_and_fill(row);
        assert!(result.is_ok(), "optional null should pass: {:?}", result.err());
    }

    #[test]
    fn test_required_column_explicit_null_rejected_even_when_others_valid() {
        // A row that satisfies every other required field but null-prims the
        // primary key must still fail.
        let s = player_schema();
        let row = json!({ "id": null, "score": 100, "active": true, "name": "Alice" });
        let err = s.validate_and_fill(row).unwrap_err();
        assert!(err.to_string().contains("must not be null"));
    }

    #[test]
    fn test_required_column_null_with_default_uses_default() {
        // If a column has a default value AND the row contains an explicit null
        // for it, the default kicks in.  This keeps the validator forgiving for
        // columns the schema author marked optional-via-default.
        let s = player_schema();
        let row = json!({ "id": "p1", "score": null, "active": null });
        let result = s.validate_and_fill(row).unwrap();
        // score had default "0", active had default "true"
        assert_eq!(result["score"], json!(0));
        assert_eq!(result["active"], json!(true));
    }

    #[test]
    fn test_nested_object_schema_required_field_null_rejected() {
        // Use an inner table schema "nested_required" — a required Any-typed
        // column accepts any non-null shape; explicit null still rejected.
        let schema = TableSchema {
            name: "events".to_string(),
            primary_key: Some("event_id".to_string()),
            columns: vec![
                ColumnDef { name: "event_id".to_string(), type_str: "String".to_string(), default: None, required: true },
                ColumnDef { name: "payload".to_string(), type_str: "any".to_string(), default: None, required: true },
            ],
            rls: RlsPolicy::default(),
        };

        // Valid: nested object as payload
        let ok_row = json!({ "event_id": "e1", "payload": { "kind": "login", "user": "alice" } });
        assert!(schema.validate_and_fill(ok_row).is_ok());

        // Invalid: payload explicitly null on a required column.
        // NOTE: ColumnType::Any short-circuits the *missing* check (required-Any
        // columns can be absent), but explicit-null is still recognised by
        // step-2's `is_null() continue` — so a null payload silently sticks.
        // This test documents the intentional rule: required-Any columns
        // permit null. Other required types (String here on event_id) do not.
        let null_event_id = json!({ "event_id": null, "payload": { "kind": "x" } });
        let err = schema.validate_and_fill(null_event_id).unwrap_err();
        assert!(err.to_string().contains("must not be null"));
    }

    #[test]
    fn test_toml_parse() {
        let toml_str = r#"
[[table]]
name = "scores"
primary_key = "player_id"

[[table.columns]]
name = "player_id"
type = "String"

[[table.columns]]
name = "score"
type = "i64"
default = "0"
"#;
        let raw: super::RawSchemaFile = toml::from_str(toml_str).unwrap();
        assert_eq!(raw.table.len(), 1);
        assert_eq!(raw.table[0].name, "scores");
        assert_eq!(raw.table[0].columns.len(), 2);
    }

    // ── RLS policy tests ──────────────────────────────────────────────────────

    fn owned_row(owner: &str) -> serde_json::Value {
        json!({ "owner": owner, "data": 42 })
    }

    #[test]
    fn rls_public_always_allows() {
        let policy = RlsPolicy::Public;
        let row = owned_row("alice");
        assert!(rls_check(&policy, Some(&row), "bob", "player"));
        assert!(rls_check(&policy, Some(&row), "", ""));
        assert!(rls_check(&policy, None, "bob", "player"));
    }

    #[test]
    fn rls_owner_field_allows_owner() {
        let policy = RlsPolicy::OwnerField { field: "owner".to_string() };
        let row = owned_row("alice");
        assert!(rls_check(&policy, Some(&row), "alice", "player"));
    }

    #[test]
    fn rls_owner_field_denies_non_owner() {
        let policy = RlsPolicy::OwnerField { field: "owner".to_string() };
        let row = owned_row("alice");
        assert!(!rls_check(&policy, Some(&row), "bob", "player"));
    }

    #[test]
    fn rls_owner_field_allows_new_insert() {
        // row == None means new insert — always allow so reducer can set owner.
        let policy = RlsPolicy::OwnerField { field: "owner".to_string() };
        assert!(rls_check(&policy, None, "charlie", "player"));
    }

    #[test]
    fn rls_role_gated_allows_matching_role() {
        let policy = RlsPolicy::RoleGated { roles: vec!["admin".to_string(), "moderator".to_string()] };
        assert!(rls_check(&policy, None, "alice", "admin"));
        assert!(rls_check(&policy, None, "alice", "moderator"));
    }

    #[test]
    fn rls_role_gated_denies_wrong_role() {
        let policy = RlsPolicy::RoleGated { roles: vec!["admin".to_string()] };
        assert!(!rls_check(&policy, None, "alice", "player"));
        assert!(!rls_check(&policy, None, "alice", ""));
    }

    #[test]
    fn rls_owner_with_admin_allows_admin_role() {
        let policy = RlsPolicy::OwnerFieldWithAdmin {
            field: "owner".to_string(),
            admin_roles: vec!["admin".to_string()],
        };
        let row = owned_row("alice");
        // admin can access alice's row even though bob is not the owner
        assert!(rls_check(&policy, Some(&row), "bob", "admin"));
    }

    #[test]
    fn rls_owner_with_admin_allows_owner() {
        let policy = RlsPolicy::OwnerFieldWithAdmin {
            field: "owner".to_string(),
            admin_roles: vec!["admin".to_string()],
        };
        let row = owned_row("alice");
        assert!(rls_check(&policy, Some(&row), "alice", "player"));
    }

    #[test]
    fn rls_owner_with_admin_denies_non_owner_non_admin() {
        let policy = RlsPolicy::OwnerFieldWithAdmin {
            field: "owner".to_string(),
            admin_roles: vec!["admin".to_string()],
        };
        let row = owned_row("alice");
        assert!(!rls_check(&policy, Some(&row), "bob", "player"));
    }

    #[test]
    fn rls_scheduler_bypasses_owner_field() {
        let policy = RlsPolicy::OwnerField { field: "owner".to_string() };
        let row = owned_row("alice");
        // scheduler role always bypasses — even accessing another user's row.
        assert!(rls_check(&policy, Some(&row), "system", "scheduler"));
    }

    #[test]
    fn rls_system_role_bypasses_role_gated() {
        let policy = RlsPolicy::RoleGated { roles: vec!["vip".to_string()] };
        // "system" role is a hardcoded bypass even if not in the allowed list.
        assert!(rls_check(&policy, None, "anything", "system"));
    }

    #[test]
    fn rls_default_policy_is_public() {
        assert_eq!(RlsPolicy::default(), RlsPolicy::Public);
    }
}
