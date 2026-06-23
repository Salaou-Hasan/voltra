// ============================================================================
// Schema validation integration tests (Session 39)
//
// Exercise SchemaRegistry through the public crate API to make sure the
// validator correctly enforces type checks, defaults, required-vs-optional
// columns, and the new "required must not be null" rule introduced in
// Session 39.
//
// These tests deliberately construct schemas in code (not from `schema.toml`)
// so they don't depend on any on-disk fixture and run identically on every
// platform.
// ============================================================================

use voltra::{ColumnDef, SchemaRegistry, TableSchema};
use voltra::schema::RlsPolicy;
use serde_json::json;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn registry_with_players() -> SchemaRegistry {
    let mut reg = SchemaRegistry::new();
    reg.register(TableSchema {
        name: "players".to_string(),
        primary_key: Some("id".to_string()),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                type_str: "String".to_string(),
                default: None,
                required: true,
            },
            ColumnDef {
                name: "name".to_string(),
                type_str: "String".to_string(),
                default: None,
                required: false,
            },
            ColumnDef {
                name: "score".to_string(),
                type_str: "i64".to_string(),
                default: Some("0".to_string()),
                required: true,
            },
            ColumnDef {
                name: "active".to_string(),
                type_str: "bool".to_string(),
                default: Some("true".to_string()),
                required: true,
            },
            ColumnDef {
                name: "hp".to_string(),
                type_str: "f64".to_string(),
                default: None,
                required: false,
            },
            ColumnDef {
                name: "avatar".to_string(),
                type_str: "bytes".to_string(),
                default: None,
                required: false,
            },
        ],
        rls: RlsPolicy::Public,
    });
    reg
}

// ── Required + null rejection (Session 39) ───────────────────────────────────

#[test]
fn required_string_column_missing_rejected() {
    let reg = registry_with_players();
    let err = reg
        .validate("players", json!({ "score": 5, "active": true }))
        .unwrap_err();
    assert!(err.to_string().contains("id"));
}

#[test]
fn required_string_column_null_rejected() {
    let reg = registry_with_players();
    let err = reg
        .validate(
            "players",
            json!({ "id": null, "score": 5, "active": true }),
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("id"));
    assert!(msg.contains("must not be null"), "msg: {}", msg);
}

#[test]
fn optional_string_column_null_accepted() {
    let reg = registry_with_players();
    let result = reg.validate(
        "players",
        json!({ "id": "p1", "score": 5, "active": true, "name": null }),
    );
    assert!(result.is_ok(), "{:?}", result.err());
}

// ── Type enforcement ─────────────────────────────────────────────────────────

#[test]
fn i64_column_rejects_string() {
    let reg = registry_with_players();
    let err = reg
        .validate(
            "players",
            json!({ "id": "p1", "score": "not-a-number", "active": true }),
        )
        .unwrap_err();
    assert!(err.to_string().contains("score"));
    assert!(err.to_string().contains("i64"));
}

#[test]
fn bool_column_rejects_integer() {
    let reg = registry_with_players();
    let err = reg
        .validate(
            "players",
            json!({ "id": "p1", "score": 5, "active": 1 }),
        )
        .unwrap_err();
    assert!(err.to_string().contains("active"));
    assert!(err.to_string().contains("bool"));
}

#[test]
fn f64_column_coerces_from_integer() {
    let reg = registry_with_players();
    // hp is f64; pass an integer literal — it should be silently coerced.
    let result = reg
        .validate(
            "players",
            json!({ "id": "p1", "score": 5, "active": true, "hp": 100 }),
        )
        .unwrap();
    assert!(result["hp"].is_f64());
    assert!((result["hp"].as_f64().unwrap() - 100.0).abs() < 1e-9);
}

#[test]
fn bytes_column_accepts_string_and_array() {
    let reg = registry_with_players();
    // Base-64 encoded string form
    let r1 = reg.validate(
        "players",
        json!({ "id": "p1", "score": 5, "active": true, "avatar": "QUJDRA==" }),
    );
    assert!(r1.is_ok(), "{:?}", r1.err());

    // Raw byte array form
    let r2 = reg.validate(
        "players",
        json!({ "id": "p2", "score": 5, "active": true, "avatar": [1, 2, 3, 4] }),
    );
    assert!(r2.is_ok(), "{:?}", r2.err());
}

// ── Defaults ─────────────────────────────────────────────────────────────────

#[test]
fn defaults_filled_when_columns_absent() {
    let reg = registry_with_players();
    let result = reg.validate("players", json!({ "id": "p1" })).unwrap();
    assert_eq!(result["score"], json!(0));
    assert_eq!(result["active"], json!(true));
}

#[test]
fn defaults_fill_explicit_null() {
    // Session 39 behavior: explicit null on a column WITH a default uses the
    // default, instead of erroring out.
    let reg = registry_with_players();
    let result = reg
        .validate(
            "players",
            json!({ "id": "p1", "score": null, "active": null }),
        )
        .unwrap();
    assert_eq!(result["score"], json!(0));
    assert_eq!(result["active"], json!(true));
}

// ── Open-schema fallback ─────────────────────────────────────────────────────

#[test]
fn unregistered_table_accepts_any_shape() {
    let reg = registry_with_players();
    // "items" has no schema — any payload should pass.
    let raw = json!({ "anything": 1, "more": "ok", "deep": { "nested": [1, 2, 3] } });
    let result = reg.validate("items", raw.clone()).unwrap();
    assert_eq!(result, raw);
}

#[test]
fn extra_columns_outside_schema_are_passed_through() {
    let reg = registry_with_players();
    let result = reg
        .validate(
            "players",
            json!({ "id": "p1", "score": 5, "active": true, "zone": "z_0_0", "level": 12 }),
        )
        .unwrap();
    assert_eq!(result["zone"], json!("z_0_0"));
    assert_eq!(result["level"], json!(12));
}

// ── Any-type column ──────────────────────────────────────────────────────────

#[test]
fn any_column_accepts_arbitrary_value() {
    let mut reg = SchemaRegistry::new();
    reg.register(TableSchema {
        name: "events".to_string(),
        primary_key: Some("event_id".to_string()),
        columns: vec![
            ColumnDef {
                name: "event_id".to_string(),
                type_str: "String".to_string(),
                default: None,
                required: true,
            },
            ColumnDef {
                name: "payload".to_string(),
                type_str: "any".to_string(),
                default: None,
                required: true,
            },
        ],
        rls: RlsPolicy::Public,
    });

    // Nested object on an Any column
    let r1 = reg.validate(
        "events",
        json!({ "event_id": "e1", "payload": { "kind": "login" } }),
    );
    assert!(r1.is_ok(), "{:?}", r1.err());

    // Array on an Any column
    let r2 = reg.validate(
        "events",
        json!({ "event_id": "e2", "payload": [1, 2, 3] }),
    );
    assert!(r2.is_ok(), "{:?}", r2.err());

    // Plain string on an Any column
    let r3 = reg.validate(
        "events",
        json!({ "event_id": "e3", "payload": "hello" }),
    );
    assert!(r3.is_ok(), "{:?}", r3.err());
}
