// src/cluster/lobby_route.rs — Lobby-to-region routing registry
//
// Tracks which region hosts each lobby.  When a lobby is created, the game
// server calls register() to record "lobby 42 lives on region 'europe'".
// Any client that wants to join lobby 42 queries GET /cluster/lobby-route?lobby_id=42
// and receives the region's ws_url to connect to directly.
//
// Persistence: routes are stored in the __lobby_routes system table so they
// survive restarts without needing a separate DB.  In-memory DashMap is the
// fast path; the table is the durable source of truth loaded on boot.

use std::sync::Arc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::table::TableStore;

/// A single lobby → region mapping.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LobbyRoute {
    pub lobby_id: String,
    pub region_id: String,
    /// WebSocket URL for clients to connect to.
    pub ws_url: String,
}

/// In-memory + persistent lobby routing table.
pub struct LobbyRouteRegistry {
    /// Fast in-memory lookup: lobby_id → LobbyRoute.
    routes: DashMap<String, LobbyRoute>,
    tables: Arc<TableStore>,
}

impl LobbyRouteRegistry {
    pub fn new(tables: Arc<TableStore>) -> Arc<Self> {
        let reg = Arc::new(LobbyRouteRegistry {
            routes: DashMap::new(),
            tables,
        });
        reg.load_from_table();
        reg
    }

    /// Register a lobby as living on a specific region.
    /// Writes to both the in-memory map and the __lobby_routes system table.
    pub fn register(&self, lobby_id: &str, region_id: &str, ws_url: &str) {
        let route = LobbyRoute {
            lobby_id: lobby_id.to_string(),
            region_id: region_id.to_string(),
            ws_url: ws_url.to_string(),
        };
        self.routes.insert(lobby_id.to_string(), route.clone());
        // Persist to __lobby_routes table for durability across restarts.
        let _ = self.tables.set_row(
            "__lobby_routes".to_string(),
            lobby_id.to_string(),
            serde_json::json!({
                "lobby_id":  route.lobby_id,
                "region_id": route.region_id,
                "ws_url":    route.ws_url,
            }),
        );
    }

    /// Look up which region hosts a lobby.  Returns None if unknown.
    pub fn lookup(&self, lobby_id: &str) -> Option<LobbyRoute> {
        // Fast path: in-memory.
        if let Some(r) = self.routes.get(lobby_id) {
            return Some(r.clone());
        }
        // Slow path: check the persistent table (in case this node restarted).
        if let Ok(Some(row)) = self.tables.get_row("__lobby_routes", lobby_id) {
            let region_id = row["region_id"].as_str().unwrap_or("").to_string();
            let ws_url    = row["ws_url"].as_str().unwrap_or("").to_string();
            let route = LobbyRoute {
                lobby_id:  lobby_id.to_string(),
                region_id,
                ws_url,
            };
            self.routes.insert(lobby_id.to_string(), route.clone());
            return Some(route);
        }
        None
    }

    /// Remove a lobby route when the lobby is destroyed.
    pub fn unregister(&self, lobby_id: &str) {
        self.routes.remove(lobby_id);
        let _ = self.tables.delete_row("__lobby_routes", lobby_id);
    }

    /// Total number of known lobby routes.
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Load all routes from the persistent table into memory (called on startup).
    fn load_from_table(&self) {
        if let Ok(rows) = self.tables.list_rows_with_keys("__lobby_routes") {
            for (key, row) in rows {
                let region_id = row["region_id"].as_str().unwrap_or("").to_string();
                let ws_url    = row["ws_url"].as_str().unwrap_or("").to_string();
                self.routes.insert(key.clone(), LobbyRoute {
                    lobby_id: key,
                    region_id,
                    ws_url,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableStore;

    fn make_store() -> Arc<TableStore> {
        Arc::new(TableStore::new())
    }

    #[test]
    fn register_and_lookup() {
        let reg = LobbyRouteRegistry::new(make_store());
        reg.register("42", "europe", "ws://eu:3000");
        let r = reg.lookup("42").unwrap();
        assert_eq!(r.region_id, "europe");
        assert_eq!(r.ws_url, "ws://eu:3000");
    }

    #[test]
    fn lookup_unknown_returns_none() {
        let reg = LobbyRouteRegistry::new(make_store());
        assert!(reg.lookup("999").is_none());
    }

    #[test]
    fn unregister_removes_route() {
        let reg = LobbyRouteRegistry::new(make_store());
        reg.register("1", "asia", "ws://as:3000");
        reg.unregister("1");
        assert!(reg.lookup("1").is_none());
    }

    #[test]
    fn len_counts_routes() {
        let reg = LobbyRouteRegistry::new(make_store());
        reg.register("1", "europe", "ws://eu:3000");
        reg.register("2", "asia",   "ws://as:3000");
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn register_overwrites_existing() {
        let reg = LobbyRouteRegistry::new(make_store());
        reg.register("5", "europe", "ws://eu:3000");
        reg.register("5", "asia",   "ws://as:3000");
        let r = reg.lookup("5").unwrap();
        assert_eq!(r.region_id, "asia");
    }
}
