/// Lobby dispatcher: routes table access to per-lobby stores for isolation.
///
/// This module implements transparent lobby-scoped storage. Tables with keys like
/// "l0_players" are automatically routed to a per-lobby store, enabling:
/// - Zero cross-lobby contention
/// - Per-lobby latency isolation
/// - Per-lobby worker pool affinity
///
/// Global tables (no "l*_" prefix) remain in a shared global store for accounts,
/// authentication, matchmaking, etc.

use crate::table::{RowDelta, TableStore};
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use crate::error::Result;

/// Parse a physical table name into (lobby_id, logical_table_name).
///
/// Returns None if the table is not lobby-scoped.
///
/// Examples:
///   "l0_players" -> Some(("0", "players"))
///   "l42_inventory" -> Some(("42", "inventory"))
///   "players" -> None
///   "__tenants" -> None
pub fn parse_lobby_key(physical_table: &str) -> Option<(String, String)> {
    // Global tables (no prefix or __ prefix) are never lobby-scoped
    if !physical_table.starts_with('l') || physical_table.starts_with("__") {
        return None;
    }

    // Find the underscore after the lobby ID
    if let Some(pos) = physical_table.find('_') {
        // Validate that the prefix is purely numeric (e.g. "l0", "l42", not "lab")
        let prefix = &physical_table[1..pos];
        if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit()) {
            let lobby_id = prefix.to_string();
            let logical_table = physical_table[pos + 1..].to_string();
            return Some((lobby_id, logical_table));
        }
    }

    None
}

/// Per-lobby storage. Each lobby gets its own TableStore to eliminate cross-lobby
/// contention. Lobbies are lazy-created on first write.
pub struct LobbyDispatcher {
    /// Per-lobby stores: lobby_id -> TableStore
    lobbies: DashMap<String, Arc<TableStore>>,
    /// Global store for non-lobby tables (auth, accounts, etc.)
    global: Arc<TableStore>,
}

impl LobbyDispatcher {
    /// Create a new dispatcher with the given global store.
    pub fn new(global: Arc<TableStore>) -> Self {
        LobbyDispatcher {
            lobbies: DashMap::new(),
            global,
        }
    }

    /// Get or create the TableStore for a given lobby ID.
    fn get_lobby_store(&self, lobby_id: &str) -> Arc<TableStore> {
        if let Some(store) = self.lobbies.get(lobby_id) {
            return store.clone();
        }
        self.lobbies
            .entry(lobby_id.to_string())
            .or_insert_with(|| Arc::new(TableStore::new()))
            .clone()
    }

    /// Route a table access to the appropriate store (lobby or global).
    fn route_table(&self, physical_table: &str) -> (Arc<TableStore>, String) {
        match parse_lobby_key(physical_table) {
            Some((lobby_id, logical_table)) => {
                let store = self.get_lobby_store(&lobby_id);
                (store, logical_table)
            }
            None => {
                (self.global.clone(), physical_table.to_string())
            }
        }
    }

    /// Get a row from the appropriate store.
    pub fn get_row(&self, table: &str, key: &str) -> Result<Option<Value>> {
        let (store, logical_table) = self.route_table(table);
        store.get_row(&logical_table, key)
    }

    /// Set a row in the appropriate store.
    pub fn set_row(&self, table: &str, key: &str, value: Value) -> Result<RowDelta> {
        let (store, logical_table) = self.route_table(table);
        store.set_row(logical_table, key.to_string(), value)
    }

    /// Delete a row from the appropriate store.
    pub fn delete_row(&self, table: &str, key: &str) -> Result<RowDelta> {
        let (store, logical_table) = self.route_table(table);
        store.delete_row(&logical_table, key)
    }

    /// Get all rows from a table (lobby or global), returned as Vec<Value>.
    pub fn list_rows(&self, table: &str) -> Result<Vec<Value>> {
        let (store, logical_table) = self.route_table(table);
        store.list_rows(&logical_table)
    }

    /// Get all rows from a table with their keys (lobby or global).
    pub fn list_rows_with_keys(&self, table: &str) -> Result<Vec<(String, Value)>> {
        let (store, logical_table) = self.route_table(table);
        store.list_rows_with_keys(&logical_table)
    }

    /// Apply a batch of deltas to the appropriate stores.
    /// Deltas are grouped by (lobby_id, logical_table) to minimize routing overhead.
    pub fn apply_delta_batch(&self, deltas: &[RowDelta]) -> Result<Vec<RowDelta>> {
        if deltas.is_empty() {
            return Ok(Vec::new());
        }

        // Group deltas by target store
        let lobby_deltas: DashMap<String, Vec<RowDelta>> = DashMap::new();
        let mut global_deltas: Vec<RowDelta> = Vec::new();

        for delta in deltas {
            if let Some((lobby_id, _)) = parse_lobby_key(&delta.table_name) {
                // Rewrite delta.table_name to logical table name
                let mut delta_copy = delta.clone();
                delta_copy.table_name = delta_copy.table_name[delta_copy.table_name.find('_').unwrap() + 1..].to_string();
                lobby_deltas
                    .entry(lobby_id)
                    .or_insert_with(Vec::new)
                    .push(delta_copy);
            } else {
                global_deltas.push(delta.clone());
            }
        }

        // Apply to global store first
        let mut results = if !global_deltas.is_empty() {
            self.global.apply_delta_batch(&global_deltas)?
        } else {
            Vec::new()
        };

        // Apply to each lobby store
        for entry in lobby_deltas.iter() {
            let lobby_id = entry.key().clone();
            let deltas_to_apply = entry.value().clone();
            let store = self.get_lobby_store(&lobby_id);
            let lobby_results = store.apply_delta_batch(&deltas_to_apply)?;
            results.extend(lobby_results);
        }

        Ok(results)
    }

    /// Get the number of active lobbies.
    pub fn lobby_count(&self) -> usize {
        self.lobbies.len()
    }

    /// Get the number of rows in a specific lobby (for monitoring).
    pub fn lobby_row_count(&self, lobby_id: &str) -> usize {
        self.get_lobby_store(lobby_id).total_row_count()
    }

    /// List all active lobby IDs.
    pub fn active_lobbies(&self) -> Vec<String> {
        self.lobbies.iter().map(|e| e.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_lobby_key_valid() {
        assert_eq!(
            parse_lobby_key("l0_players"),
            Some(("0".to_string(), "players".to_string()))
        );
        assert_eq!(
            parse_lobby_key("l42_inventory"),
            Some(("42".to_string(), "inventory".to_string()))
        );
        assert_eq!(
            parse_lobby_key("l999_npc_spawns"),
            Some(("999".to_string(), "npc_spawns".to_string()))
        );
    }

    #[test]
    fn test_parse_lobby_key_invalid() {
        assert_eq!(parse_lobby_key("players"), None);
        assert_eq!(parse_lobby_key("__tenants"), None);
        assert_eq!(parse_lobby_key("lab_players"), None);  // 'ab' is not numeric
        assert_eq!(parse_lobby_key("l_players"), None);     // empty prefix
        assert_eq!(parse_lobby_key("l0"), None);            // no underscore
    }

    #[test]
    fn test_lobby_dispatcher_routing() {
        let global = Arc::new(TableStore::new());
        let dispatcher = LobbyDispatcher::new(global.clone());

        // Lobby-scoped table should create a new store
        dispatcher.set_row("l0_players", "alice", serde_json::json!({"hp": 100})).ok();
        assert_eq!(dispatcher.lobby_count(), 1);

        // Another lobby should be separate
        dispatcher.set_row("l1_players", "bob", serde_json::json!({"hp": 50})).ok();
        assert_eq!(dispatcher.lobby_count(), 2);

        // Global table should not increment lobby count
        dispatcher.set_row("accounts", "alice", serde_json::json!({"email": "alice@game.com"})).ok();
        assert_eq!(dispatcher.lobby_count(), 2);
    }

    #[test]
    fn test_lobby_isolation() {
        let global = Arc::new(TableStore::new());
        let dispatcher = LobbyDispatcher::new(global);

        // Write to two lobbies with same player key
        dispatcher.set_row("l0_players", "alice", serde_json::json!({"hp": 100, "zone": "spawn"})).ok();
        dispatcher.set_row("l1_players", "alice", serde_json::json!({"hp": 50, "zone": "forest"})).ok();

        // Verify they're isolated
        let l0_alice = dispatcher.get_row("l0_players", "alice").ok().flatten();
        let l1_alice = dispatcher.get_row("l1_players", "alice").ok().flatten();

        let l0_zone = l0_alice.as_ref().and_then(|v| v["zone"].as_str());
        let l1_zone = l1_alice.as_ref().and_then(|v| v["zone"].as_str());

        assert_eq!(l0_zone, Some("spawn"));
        assert_eq!(l1_zone, Some("forest"));
    }

    #[test]
    fn test_lobby_dispatcher_delta_batch() {
        let global = Arc::new(TableStore::new());
        let dispatcher = LobbyDispatcher::new(global);

        // Create deltas for mixed lobbies and global
        let deltas = vec![
            RowDelta {
                table_name: "l0_players".to_string(),
                operation: "insert".to_string(),
                row_key: "alice".to_string(),
                row_id: 1,
                shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"hp": 100})),
                counter_add_amount: 0,
                counter_add_timestamp: 0,
            },
            RowDelta {
                table_name: "l1_players".to_string(),
                operation: "insert".to_string(),
                row_key: "bob".to_string(),
                row_id: 2,
                shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"hp": 50})),
                counter_add_amount: 0,
                counter_add_timestamp: 0,
            },
            RowDelta {
                table_name: "accounts".to_string(),
                operation: "insert".to_string(),
                row_key: "alice".to_string(),
                row_id: 3,
                shard_id: 0,
                payload_arc: None,
                row_data: Some(serde_json::json!({"email": "alice@game.com"})),
                counter_add_amount: 0,
                counter_add_timestamp: 0,
            },
        ];

        // Apply batch
        let result = dispatcher.apply_delta_batch(&deltas);
        assert!(result.is_ok());

        // Verify isolation
        assert!(dispatcher.get_row("l0_players", "alice").ok().flatten().is_some());
        assert!(dispatcher.get_row("l1_players", "bob").ok().flatten().is_some());
        assert!(dispatcher.get_row("accounts", "alice").ok().flatten().is_some());
        assert_eq!(dispatcher.lobby_count(), 2);
    }

    #[test]
    fn test_active_lobbies_list() {
        let global = Arc::new(TableStore::new());
        let dispatcher = LobbyDispatcher::new(global);

        dispatcher.set_row("l0_players", "alice", serde_json::json!({})).ok();
        dispatcher.set_row("l5_players", "bob", serde_json::json!({})).ok();
        dispatcher.set_row("l10_players", "charlie", serde_json::json!({})).ok();

        let mut lobbies = dispatcher.active_lobbies();
        lobbies.sort();

        assert_eq!(lobbies, vec!["0", "10", "5"]);  // sorted numerically might differ
    }
}
