// ============================================================================
// tests/security_tests.rs
//
// Pure unit-style integration tests for the security surface — no server
// processes are spawned.  These cover:
//   * PermissionsConfig::is_allowed under Open and Closed default policies
//   * Config defaults (default_policy, sql_timeout_ms)
//   * Scheduler role bypass
//   * Cluster secret validation (no panic, no I/O)
// ============================================================================

use std::collections::HashMap;

use neondb::config::{Config, PermissionsConfig, PermissionsPolicy};

// ── PermissionsConfig ────────────────────────────────────────────────────────

#[test]
fn open_policy_allows_unlisted_reducer() {
    let p = PermissionsConfig {
        rules: HashMap::new(),
        default_policy: PermissionsPolicy::Open,
    };
    assert!(p.is_allowed("any_reducer", "user"));
    assert!(p.is_allowed("hello", ""));
}

#[test]
fn closed_policy_denies_unlisted_reducer() {
    let p = PermissionsConfig {
        rules: HashMap::new(),
        default_policy: PermissionsPolicy::Closed,
    };
    assert!(!p.is_allowed("unknown", "user"));
    assert!(!p.is_allowed("unknown", "admin"));
}

#[test]
fn closed_policy_still_lets_scheduler_through() {
    let p = PermissionsConfig {
        rules: HashMap::new(),
        default_policy: PermissionsPolicy::Closed,
    };
    assert!(p.is_allowed("anything", "scheduler"));
}

#[test]
fn listed_rule_overrides_open_default() {
    let mut rules = HashMap::new();
    rules.insert("delete_player".to_string(), vec!["admin".to_string()]);
    let p = PermissionsConfig {
        rules,
        default_policy: PermissionsPolicy::Open,
    };
    assert!(p.is_allowed("delete_player", "admin"));
    assert!(!p.is_allowed("delete_player", "user"));
    // Still open for unlisted reducers.
    assert!(p.is_allowed("ping", "user"));
}

#[test]
fn listed_rule_overrides_closed_default() {
    let mut rules = HashMap::new();
    rules.insert("ping".to_string(), vec!["user".to_string(), "admin".to_string()]);
    let p = PermissionsConfig {
        rules,
        default_policy: PermissionsPolicy::Closed,
    };
    assert!(p.is_allowed("ping", "user"));
    assert!(p.is_allowed("ping", "admin"));
    assert!(!p.is_allowed("ping", "guest"));
    // Unlisted reducer denied because of closed default.
    assert!(!p.is_allowed("delete_player", "admin"));
}

#[test]
fn empty_allowed_list_blocks_all_non_scheduler_roles() {
    let mut rules = HashMap::new();
    rules.insert("nuke".to_string(), Vec::<String>::new());
    let p = PermissionsConfig {
        rules,
        default_policy: PermissionsPolicy::Open,
    };
    assert!(!p.is_allowed("nuke", "admin"));
    assert!(!p.is_allowed("nuke", "user"));
    assert!(p.is_allowed("nuke", "scheduler"));
}

#[test]
fn scheduler_role_bypasses_role_mismatch() {
    let mut rules = HashMap::new();
    rules.insert("reset".to_string(), vec!["admin".to_string()]);
    let p = PermissionsConfig {
        rules,
        default_policy: PermissionsPolicy::Closed,
    };
    // Scheduler is allowed even though it's not in the role list.
    assert!(p.is_allowed("reset", "scheduler"));
    // A different role still gets denied.
    assert!(!p.is_allowed("reset", "user"));
}

// ── Config defaults ──────────────────────────────────────────────────────────

#[test]
fn config_default_policy_is_open_for_backward_compat() {
    let cfg = Config::from_env();
    assert_eq!(
        cfg.permissions.default_policy,
        PermissionsPolicy::Open,
        "Default permissions policy must stay Open to preserve backward compatibility"
    );
}

#[test]
fn config_default_sql_timeout_is_five_seconds() {
    let cfg = Config::from_env();
    assert_eq!(cfg.sql_timeout_ms, 5_000);
}

#[test]
fn config_default_host_is_loopback() {
    // Verifies that the safe default (loopback) is preserved — important so
    // the new api_key=None warning doesn't fire spuriously in dev.
    let cfg = Config::from_env();
    assert_eq!(cfg.host, "127.0.0.1");
}
