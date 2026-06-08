use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Presence status of a connected user.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PresenceStatus {
    Online,
    Idle,
    Away,
    /// Custom status with a message (e.g., "In Match", "AFK")
    Custom(String),
}

/// A single user's presence entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceEntry {
    pub user_id: String,
    pub status: PresenceStatus,
    pub last_heartbeat: u64, // Unix timestamp ms
    pub connected_at: u64,   // Unix timestamp ms
    pub metadata: Option<serde_json::Value>, // custom data (current room, character, etc.)
}

/// Manages presence state for all connected users.
///
/// Design:
/// - DashMap for lock-free concurrent access
/// - Heartbeat-based: users must heartbeat every `heartbeat_timeout_ms` or they go Idle,
///   then after `offline_timeout_ms` they are removed entirely
/// - Provides: online_users(), get(), users_with_status()
pub struct PresenceManager {
    entries: DashMap<String, PresenceEntry>,
    heartbeat_timeout_ms: u64, // After this without heartbeat → Idle
    offline_timeout_ms: u64,   // After this without heartbeat → removed
}

impl PresenceManager {
    /// Create a new PresenceManager.
    ///
    /// - `heartbeat_timeout_ms`: Duration without heartbeat before a user transitions to Idle.
    /// - `offline_timeout_ms`: Duration without heartbeat before a user is removed entirely.
    ///   Must be greater than `heartbeat_timeout_ms`.
    pub fn new(heartbeat_timeout_ms: u64, offline_timeout_ms: u64) -> Self {
        Self {
            entries: DashMap::new(),
            heartbeat_timeout_ms,
            offline_timeout_ms,
        }
    }

    /// Register a user as online. Called on WebSocket connect.
    /// If the user already exists, their status is reset to Online and heartbeat is refreshed.
    pub fn set_online(&self, user_id: &str, metadata: Option<serde_json::Value>) {
        let now_ms = current_time_ms();
        self.entries.insert(
            user_id.to_string(),
            PresenceEntry {
                user_id: user_id.to_string(),
                status: PresenceStatus::Online,
                last_heartbeat: now_ms,
                connected_at: now_ms,
                metadata,
            },
        );
    }

    /// Register a user as online with an explicit timestamp.
    /// Used internally and in tests where time must be controlled.
    pub fn set_online_at(
        &self,
        user_id: &str,
        now_ms: u64,
        metadata: Option<serde_json::Value>,
    ) {
        self.entries.insert(
            user_id.to_string(),
            PresenceEntry {
                user_id: user_id.to_string(),
                status: PresenceStatus::Online,
                last_heartbeat: now_ms,
                connected_at: now_ms,
                metadata,
            },
        );
    }

    /// Update heartbeat timestamp. Called when client sends a heartbeat message.
    /// No-op if the user is not tracked.
    pub fn heartbeat(&self, user_id: &str) {
        let now_ms = current_time_ms();
        self.heartbeat_at(user_id, now_ms);
    }

    /// Update heartbeat with an explicit timestamp (for testing).
    pub fn heartbeat_at(&self, user_id: &str, now_ms: u64) {
        if let Some(mut entry) = self.entries.get_mut(user_id) {
            entry.last_heartbeat = now_ms;
            // If user was Idle from a previous sweep, heartbeat brings them back Online
            if entry.status == PresenceStatus::Idle {
                entry.status = PresenceStatus::Online;
            }
        }
    }

    /// Set a custom status.
    pub fn set_status(&self, user_id: &str, status: PresenceStatus) {
        if let Some(mut entry) = self.entries.get_mut(user_id) {
            entry.status = status;
        }
    }

    /// Update metadata (e.g., current room, match ID).
    pub fn update_metadata(&self, user_id: &str, metadata: serde_json::Value) {
        if let Some(mut entry) = self.entries.get_mut(user_id) {
            entry.metadata = Some(metadata);
        }
    }

    /// Remove a user entirely. Called on WebSocket disconnect.
    /// Returns the removed entry if the user was tracked.
    pub fn set_offline(&self, user_id: &str) -> Option<PresenceEntry> {
        self.entries.remove(user_id).map(|(_, entry)| entry)
    }

    /// Get a single user's presence.
    pub fn get(&self, user_id: &str) -> Option<PresenceEntry> {
        self.entries.get(user_id).map(|e| e.clone())
    }

    /// Get all currently tracked users (online, idle, away, custom).
    pub fn online_users(&self) -> Vec<PresenceEntry> {
        self.entries.iter().map(|e| e.value().clone()).collect()
    }

    /// Get users matching a specific status.
    pub fn users_with_status(&self, status: &PresenceStatus) -> Vec<PresenceEntry> {
        self.entries
            .iter()
            .filter(|e| &e.value().status == status)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Count of tracked users.
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    /// Sweep stale entries: move expired heartbeats to Idle, remove fully timed-out users.
    /// Returns (newly_idle, removed) user IDs for notification purposes.
    /// Should be called periodically from a background task.
    ///
    /// Algorithm:
    /// 1. Iterate all entries.
    /// 2. For each entry, compute `elapsed = now_ms - last_heartbeat`.
    /// 3. If `elapsed >= offline_timeout_ms` → remove the entry, add user_id to `removed`.
    /// 4. Else if `elapsed >= heartbeat_timeout_ms` AND status is Online → set to Idle,
    ///    add user_id to `newly_idle`.
    /// 5. Return both vectors.
    pub fn sweep(&self, now_ms: u64) -> (Vec<String>, Vec<String>) {
        let mut newly_idle = Vec::new();
        let mut removed = Vec::new();

        // First pass: collect keys to remove (cannot remove during iteration with DashMap)
        let mut to_remove = Vec::new();
        let mut to_idle = Vec::new();

        for entry in self.entries.iter() {
            let elapsed = now_ms.saturating_sub(entry.last_heartbeat);
            if elapsed >= self.offline_timeout_ms {
                to_remove.push(entry.user_id.clone());
            } else if elapsed >= self.heartbeat_timeout_ms
                && entry.status == PresenceStatus::Online
            {
                to_idle.push(entry.user_id.clone());
            }
        }

        // Apply removals
        for user_id in to_remove {
            if self.entries.remove(&user_id).is_some() {
                removed.push(user_id);
            }
        }

        // Apply idle transitions
        for user_id in to_idle {
            if let Some(mut entry) = self.entries.get_mut(&user_id) {
                // Double-check: only transition if still Online (avoid racing with heartbeat)
                if entry.status == PresenceStatus::Online {
                    entry.status = PresenceStatus::Idle;
                    newly_idle.push(user_id);
                }
            }
        }

        (newly_idle, removed)
    }
}

/// Get current time in milliseconds since Unix epoch.
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_online_creates_entry() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);

        let entry = pm.get("alice").unwrap();
        assert_eq!(entry.user_id, "alice");
        assert_eq!(entry.status, PresenceStatus::Online);
        assert_eq!(entry.last_heartbeat, 1000);
        assert_eq!(entry.connected_at, 1000);
        assert!(entry.metadata.is_none());
    }

    #[test]
    fn test_heartbeat_updates_timestamp() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);

        pm.heartbeat_at("alice", 3000);

        let entry = pm.get("alice").unwrap();
        assert_eq!(entry.last_heartbeat, 3000);
        // connected_at should NOT change
        assert_eq!(entry.connected_at, 1000);
    }

    #[test]
    fn test_set_offline_removes_entry() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);

        let removed = pm.set_offline("alice");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().user_id, "alice");
        assert!(pm.get("alice").is_none());
        assert_eq!(pm.count(), 0);
    }

    #[test]
    fn test_set_status_changes_status() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);

        pm.set_status("alice", PresenceStatus::Away);
        assert_eq!(pm.get("alice").unwrap().status, PresenceStatus::Away);

        pm.set_status("alice", PresenceStatus::Custom("In Match".to_string()));
        assert_eq!(
            pm.get("alice").unwrap().status,
            PresenceStatus::Custom("In Match".to_string())
        );
    }

    #[test]
    fn test_update_metadata() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);
        assert!(pm.get("alice").unwrap().metadata.is_none());

        let meta = serde_json::json!({"room": "lobby", "level": 5});
        pm.update_metadata("alice", meta.clone());

        let entry = pm.get("alice").unwrap();
        assert_eq!(entry.metadata, Some(meta));
    }

    #[test]
    fn test_online_users_returns_all() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);
        pm.set_online_at("bob", 2000, None);
        pm.set_online_at("carol", 3000, None);

        let users = pm.online_users();
        assert_eq!(users.len(), 3);

        let ids: Vec<String> = users.iter().map(|e| e.user_id.clone()).collect();
        assert!(ids.contains(&"alice".to_string()));
        assert!(ids.contains(&"bob".to_string()));
        assert!(ids.contains(&"carol".to_string()));
    }

    #[test]
    fn test_sweep_marks_idle_after_timeout() {
        let pm = PresenceManager::new(5000, 15000);
        // Alice connected at t=1000
        pm.set_online_at("alice", 1000, None);
        // Bob connected at t=4000
        pm.set_online_at("bob", 4000, None);

        // Sweep at t=7000: Alice's last heartbeat was 6000ms ago (>=5000), Bob's was 3000ms ago
        let (newly_idle, removed) = pm.sweep(7000);

        assert_eq!(newly_idle.len(), 1);
        assert_eq!(newly_idle[0], "alice");
        assert!(removed.is_empty());

        assert_eq!(pm.get("alice").unwrap().status, PresenceStatus::Idle);
        assert_eq!(pm.get("bob").unwrap().status, PresenceStatus::Online);
    }

    #[test]
    fn test_sweep_removes_after_offline_timeout() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);
        pm.set_online_at("bob", 10000, None);

        // Sweep at t=20000: Alice's last heartbeat was 19000ms ago (>=15000), Bob's was 10000ms ago (>=5000 but <15000)
        let (newly_idle, removed) = pm.sweep(20000);

        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], "alice");
        assert!(pm.get("alice").is_none());

        // Bob should be marked idle (10000ms >= 5000ms heartbeat timeout)
        assert_eq!(newly_idle.len(), 1);
        assert_eq!(newly_idle[0], "bob");
        assert_eq!(pm.get("bob").unwrap().status, PresenceStatus::Idle);
    }

    #[test]
    fn test_users_with_status_filters_correctly() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);
        pm.set_online_at("bob", 1000, None);
        pm.set_online_at("carol", 1000, None);

        pm.set_status("bob", PresenceStatus::Away);
        pm.set_status("carol", PresenceStatus::Custom("In Match".to_string()));

        let online = pm.users_with_status(&PresenceStatus::Online);
        assert_eq!(online.len(), 1);
        assert_eq!(online[0].user_id, "alice");

        let away = pm.users_with_status(&PresenceStatus::Away);
        assert_eq!(away.len(), 1);
        assert_eq!(away[0].user_id, "bob");

        let custom = pm.users_with_status(&PresenceStatus::Custom("In Match".to_string()));
        assert_eq!(custom.len(), 1);
        assert_eq!(custom[0].user_id, "carol");
    }

    #[test]
    fn test_heartbeat_revives_idle_user() {
        let pm = PresenceManager::new(5000, 15000);
        pm.set_online_at("alice", 1000, None);

        // Sweep marks alice idle
        let (newly_idle, _) = pm.sweep(7000);
        assert_eq!(newly_idle.len(), 1);
        assert_eq!(pm.get("alice").unwrap().status, PresenceStatus::Idle);

        // Heartbeat brings her back online
        pm.heartbeat_at("alice", 8000);
        assert_eq!(pm.get("alice").unwrap().status, PresenceStatus::Online);
        assert_eq!(pm.get("alice").unwrap().last_heartbeat, 8000);
    }

    #[test]
    fn test_set_offline_nonexistent_returns_none() {
        let pm = PresenceManager::new(5000, 15000);
        assert!(pm.set_offline("nobody").is_none());
    }

    #[test]
    fn test_set_online_with_metadata() {
        let pm = PresenceManager::new(5000, 15000);
        let meta = serde_json::json!({"character": "warrior", "matchId": "m123"});
        pm.set_online_at("alice", 1000, Some(meta.clone()));

        let entry = pm.get("alice").unwrap();
        assert_eq!(entry.metadata, Some(meta));
    }
}
