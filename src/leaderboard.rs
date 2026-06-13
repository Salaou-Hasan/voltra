use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use tokio::sync::watch;

/// A single leaderboard entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderboardEntry {
    pub player_id: String,
    pub score: f64,
    pub metadata: Option<serde_json::Value>,
    pub updated_at_ms: u64,
}

/// Time window for a leaderboard.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeWindow {
    AllTime,
    /// Resets at midnight UTC.
    Daily,
    /// Resets on Monday midnight UTC.
    Weekly,
    /// Resets on 1st of month midnight UTC.
    Monthly,
    /// Custom duration in milliseconds.
    Custom(u64),
}

/// Sort direction for the leaderboard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    /// Highest score wins (typical).
    HighestFirst,
    /// Lowest score wins (e.g., speedrun time).
    LowestFirst,
}

/// Configuration for a leaderboard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderboardConfig {
    pub name: String,
    pub sort_order: SortOrder,
    pub time_window: TimeWindow,
    pub max_entries: usize,
}

/// A player's rank information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankInfo {
    /// 1-indexed rank.
    pub rank: usize,
    pub score: f64,
    pub total_players: usize,
    /// 0.0-100.0 (top 1% = 99.0).
    pub percentile: f64,
}

/// Encoded score for BTree ordering. Handles both HighestFirst and LowestFirst.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ScoreKey {
    /// For HighestFirst: invert the score bits so BTree ascending = highest first.
    /// For LowestFirst: use score bits directly so BTree ascending = lowest first.
    encoded: u64,
    /// Tie-breaker: earlier submission wins (lower timestamp = better rank).
    /// Stored inverted so BTree ascending order = earlier first.
    timestamp_inverted: u64,
    /// Final tie-breaker: player_id for uniqueness.
    player_id: String,
}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded
            .cmp(&other.encoded)
            .then(self.timestamp_inverted.cmp(&other.timestamp_inverted))
            .then(self.player_id.cmp(&other.player_id))
    }
}

impl ScoreKey {
    fn new(score: f64, sort_order: &SortOrder, timestamp_ms: u64, player_id: &str) -> Self {
        // Convert f64 to sortable u64 using IEEE 754 total ordering trick:
        // Positive floats: flip the sign bit (bit 63) so they sort after negatives.
        // Negative floats: flip all bits so they sort correctly (more negative = smaller).
        let bits = score.to_bits();
        let sortable = if bits & (1u64 << 63) == 0 {
            // Positive (or +0): flip sign bit
            bits ^ (1u64 << 63)
        } else {
            // Negative (or -0): flip all bits
            !bits
        };

        let encoded = match sort_order {
            SortOrder::HighestFirst => !sortable, // Invert so highest score comes first in BTree
            SortOrder::LowestFirst => sortable,   // Lowest score first naturally
        };

        ScoreKey {
            encoded,
            timestamp_inverted: timestamp_ms, // Earlier timestamp = smaller value = better rank in BTree
            player_id: player_id.to_string(),
        }
    }

    fn decode_score(encoded: u64, sort_order: &SortOrder) -> f64 {
        let sortable = match sort_order {
            SortOrder::HighestFirst => !encoded,
            SortOrder::LowestFirst => encoded,
        };

        // Reverse the IEEE 754 total ordering transform
        let bits = if sortable & (1u64 << 63) != 0 {
            // Was positive: un-flip sign bit
            sortable ^ (1u64 << 63)
        } else {
            // Was negative: un-flip all bits
            !sortable
        };

        f64::from_bits(bits)
    }
}

struct LeaderboardData {
    config: LeaderboardConfig,
    /// BTreeMap<ScoreKey, LeaderboardEntry> for O(log n) rank lookup.
    entries: BTreeMap<ScoreKey, LeaderboardEntry>,
    /// player_id -> ScoreKey for O(1) lookup of a player's current position.
    player_index: HashMap<String, ScoreKey>,
    /// Epoch start for time-windowed boards (Unix ms).
    window_start_ms: u64,
}

/// The leaderboard engine. Manages multiple named leaderboards.
pub struct LeaderboardEngine {
    boards: DashMap<String, Mutex<LeaderboardData>>,
}

impl LeaderboardEngine {
    pub fn new() -> Self {
        LeaderboardEngine {
            boards: DashMap::new(),
        }
    }

    /// Create/register a leaderboard.
    pub fn create_board(&self, config: LeaderboardConfig) {
        let name = config.name.clone();
        let data = LeaderboardData {
            config,
            entries: BTreeMap::new(),
            player_index: HashMap::new(),
            window_start_ms: 0,
        };
        self.boards.insert(name, Mutex::new(data));
    }

    /// Submit a score. Updates if better (or replaces, depending on sort order).
    /// Returns the player's new rank info.
    pub fn submit_score(
        &self,
        board_name: &str,
        player_id: &str,
        score: f64,
        metadata: Option<serde_json::Value>,
        now_ms: u64,
    ) -> Result<RankInfo, String> {
        let board_ref = self
            .boards
            .get(board_name)
            .ok_or_else(|| format!("Leaderboard '{}' not found", board_name))?;
        let mut data = board_ref.value().lock().unwrap();

        // Check if player already has an entry
        if let Some(existing_key) = data.player_index.get(player_id) {
            let existing_score = ScoreKey::decode_score(existing_key.encoded, &data.config.sort_order);
            let dominated = match data.config.sort_order {
                SortOrder::HighestFirst => score <= existing_score,
                SortOrder::LowestFirst => score >= existing_score,
            };
            if dominated {
                // Existing score is better or equal; return current rank without updating
                let rank = self.compute_rank_internal(&data, existing_key);
                let total = data.entries.len();
                return Ok(RankInfo {
                    rank,
                    score: existing_score,
                    total_players: total,
                    percentile: compute_percentile(rank, total),
                });
            }
            // Remove old entry
            let old_key = existing_key.clone();
            data.entries.remove(&old_key);
            data.player_index.remove(player_id);
        }

        // Insert new entry
        let new_key = ScoreKey::new(score, &data.config.sort_order, now_ms, player_id);
        let entry = LeaderboardEntry {
            player_id: player_id.to_string(),
            score,
            metadata,
            updated_at_ms: now_ms,
        };
        data.entries.insert(new_key.clone(), entry);
        data.player_index.insert(player_id.to_string(), new_key.clone());

        // Enforce max_entries cap
        while data.entries.len() > data.config.max_entries {
            // Remove the worst entry (last in BTree)
            if let Some((worst_key, _)) = data.entries.iter().next_back().map(|(k, v)| (k.clone(), v.clone())) {
                data.player_index.remove(&worst_key.player_id);
                data.entries.remove(&worst_key);
            } else {
                break;
            }
        }

        // Check if the newly inserted player was evicted
        if !data.player_index.contains_key(player_id) {
            return Err(format!(
                "Score submitted but player '{}' was evicted by max_entries cap",
                player_id
            ));
        }

        let rank = self.compute_rank_internal(&data, &new_key);
        let total = data.entries.len();
        Ok(RankInfo {
            rank,
            score,
            total_players: total,
            percentile: compute_percentile(rank, total),
        })
    }

    /// Get top N entries.
    pub fn top(
        &self,
        board_name: &str,
        count: usize,
    ) -> Result<Vec<(usize, LeaderboardEntry)>, String> {
        let board_ref = self
            .boards
            .get(board_name)
            .ok_or_else(|| format!("Leaderboard '{}' not found", board_name))?;
        let data = board_ref.value().lock().unwrap();

        let results: Vec<(usize, LeaderboardEntry)> = data
            .entries
            .values()
            .take(count)
            .enumerate()
            .map(|(i, entry)| (i + 1, entry.clone()))
            .collect();

        Ok(results)
    }

    /// Get a player's rank.
    pub fn get_rank(&self, board_name: &str, player_id: &str) -> Result<RankInfo, String> {
        let board_ref = self
            .boards
            .get(board_name)
            .ok_or_else(|| format!("Leaderboard '{}' not found", board_name))?;
        let data = board_ref.value().lock().unwrap();

        let key = data
            .player_index
            .get(player_id)
            .ok_or_else(|| format!("Player '{}' not found on board '{}'", player_id, board_name))?;

        let rank = self.compute_rank_internal(&data, key);
        let score = ScoreKey::decode_score(key.encoded, &data.config.sort_order);
        let total = data.entries.len();

        Ok(RankInfo {
            rank,
            score,
            total_players: total,
            percentile: compute_percentile(rank, total),
        })
    }

    /// Get entries around a player (e.g., `count` above and `count` below).
    pub fn get_around(
        &self,
        board_name: &str,
        player_id: &str,
        count: usize,
    ) -> Result<Vec<(usize, LeaderboardEntry)>, String> {
        let board_ref = self
            .boards
            .get(board_name)
            .ok_or_else(|| format!("Leaderboard '{}' not found", board_name))?;
        let data = board_ref.value().lock().unwrap();

        let key = data
            .player_index
            .get(player_id)
            .ok_or_else(|| format!("Player '{}' not found on board '{}'", player_id, board_name))?;

        let rank = self.compute_rank_internal(&data, key);
        // Calculate start rank (ranks are 1-indexed)
        let start_rank = if rank > count { rank - count } else { 1 };
        let end_rank = std::cmp::min(rank + count, data.entries.len());

        let results: Vec<(usize, LeaderboardEntry)> = data
            .entries
            .values()
            .enumerate()
            .skip(start_rank - 1)
            .take(end_rank - start_rank + 1)
            .map(|(i, entry)| (i + 1, entry.clone()))
            .collect();

        Ok(results)
    }

    /// Get entries at a specific rank range (e.g., ranks 50-60). Both inclusive, 1-indexed.
    pub fn get_range(
        &self,
        board_name: &str,
        start_rank: usize,
        end_rank: usize,
    ) -> Result<Vec<(usize, LeaderboardEntry)>, String> {
        let board_ref = self
            .boards
            .get(board_name)
            .ok_or_else(|| format!("Leaderboard '{}' not found", board_name))?;
        let data = board_ref.value().lock().unwrap();

        if start_rank == 0 || start_rank > end_rank {
            return Err("Invalid rank range: start_rank must be >= 1 and <= end_rank".to_string());
        }

        let results: Vec<(usize, LeaderboardEntry)> = data
            .entries
            .values()
            .enumerate()
            .skip(start_rank - 1)
            .take(end_rank - start_rank + 1)
            .map(|(i, entry)| (i + 1, entry.clone()))
            .collect();

        Ok(results)
    }

    /// Remove a player from a leaderboard.
    pub fn remove_player(&self, board_name: &str, player_id: &str) -> bool {
        let board_ref = match self.boards.get(board_name) {
            Some(b) => b,
            None => return false,
        };
        let mut data = board_ref.value().lock().unwrap();

        if let Some(key) = data.player_index.remove(player_id) {
            data.entries.remove(&key);
            true
        } else {
            false
        }
    }

    /// Reset a time-windowed board (called when the window expires).
    pub fn reset_board(&self, board_name: &str) {
        if let Some(board_ref) = self.boards.get(board_name) {
            let mut data = board_ref.value().lock().unwrap();
            data.entries.clear();
            data.player_index.clear();
        }
    }

    /// Check if a time window has expired and reset if needed.
    /// Returns true if the board was reset.
    pub fn check_window_expiry(&self, board_name: &str, now_ms: u64) -> bool {
        let board_ref = match self.boards.get(board_name) {
            Some(b) => b,
            None => return false,
        };
        let mut data = board_ref.value().lock().unwrap();

        let expired = match &data.config.time_window {
            TimeWindow::AllTime => false,
            TimeWindow::Daily => {
                let day_ms: u64 = 86_400_000;
                let current_day = now_ms / day_ms;
                let window_day = data.window_start_ms / day_ms;
                current_day > window_day
            }
            TimeWindow::Weekly => {
                let week_ms: u64 = 604_800_000;
                // Epoch (Jan 1 1970) was a Thursday. Monday = day 4 of that week.
                // Adjust so weeks start on Monday.
                let monday_offset_ms: u64 = 345_600_000; // 4 days in ms
                let current_week = (now_ms + monday_offset_ms) / week_ms;
                let window_week = (data.window_start_ms + monday_offset_ms) / week_ms;
                current_week > window_week
            }
            TimeWindow::Monthly => {
                // Approximate: 30 days. For production, you'd want calendar-aware logic.
                // We use a simpler heuristic: check if the month number changed.
                let month_for = |ms: u64| -> u64 {
                    // Approximate months since epoch (30.44 days avg)
                    ms / 2_629_746_000
                };
                month_for(now_ms) > month_for(data.window_start_ms)
            }
            TimeWindow::Custom(duration_ms) => {
                now_ms >= data.window_start_ms + duration_ms
            }
        };

        if expired {
            data.entries.clear();
            data.player_index.clear();
            data.window_start_ms = now_ms;
            true
        } else {
            false
        }
    }

    /// Total number of players on a board.
    pub fn player_count(&self, board_name: &str) -> usize {
        match self.boards.get(board_name) {
            Some(board_ref) => {
                let data = board_ref.value().lock().unwrap();
                data.entries.len()
            }
            None => 0,
        }
    }

    /// List all registered board names.
    pub fn list_boards(&self) -> Vec<String> {
        self.boards.iter().map(|r| r.key().clone()).collect()
    }

    /// Internal: compute the 1-indexed rank of a key within the BTree.
    fn compute_rank_internal(&self, data: &LeaderboardData, key: &ScoreKey) -> usize {
        // Count how many entries come before this key in BTree order
        data.entries
            .range(..key.clone())
            .count()
            + 1
    }
}

impl Default for LeaderboardEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute percentile from rank and total.
/// Rank 1 of 100 = 99.0 percentile (top 1%).
/// Rank 100 of 100 = 0.0 percentile.
fn compute_percentile(rank: usize, total: usize) -> f64 {
    if total <= 1 {
        return 100.0;
    }
    ((total - rank) as f64 / (total - 1) as f64) * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine_with_board(name: &str, sort_order: SortOrder, max_entries: usize) -> LeaderboardEngine {
        let engine = LeaderboardEngine::new();
        engine.create_board(LeaderboardConfig {
            name: name.to_string(),
            sort_order,
            time_window: TimeWindow::AllTime,
            max_entries,
        });
        engine
    }

    #[test]
    fn test_create_board() {
        let engine = LeaderboardEngine::new();
        engine.create_board(LeaderboardConfig {
            name: "kills".to_string(),
            sort_order: SortOrder::HighestFirst,
            time_window: TimeWindow::Daily,
            max_entries: 1000,
        });
        assert_eq!(engine.list_boards(), vec!["kills"]);
        assert_eq!(engine.player_count("kills"), 0);
    }

    #[test]
    fn test_submit_score_and_rank() {
        let engine = make_engine_with_board("scores", SortOrder::HighestFirst, 100);

        let info = engine
            .submit_score("scores", "alice", 100.0, None, 1000)
            .unwrap();
        assert_eq!(info.rank, 1);
        assert_eq!(info.score, 100.0);
        assert_eq!(info.total_players, 1);

        let info2 = engine
            .submit_score("scores", "bob", 200.0, None, 2000)
            .unwrap();
        assert_eq!(info2.rank, 1); // bob is #1 with 200
        assert_eq!(info2.total_players, 2);

        // Alice should now be rank 2
        let alice_rank = engine.get_rank("scores", "alice").unwrap();
        assert_eq!(alice_rank.rank, 2);
    }

    #[test]
    fn test_top_returns_correct_order() {
        let engine = make_engine_with_board("top_test", SortOrder::HighestFirst, 100);

        engine.submit_score("top_test", "c", 50.0, None, 1000).unwrap();
        engine.submit_score("top_test", "a", 300.0, None, 2000).unwrap();
        engine.submit_score("top_test", "b", 150.0, None, 3000).unwrap();

        let top = engine.top("top_test", 3).unwrap();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1.player_id, "a");
        assert_eq!(top[0].1.score, 300.0);
        assert_eq!(top[1].0, 2);
        assert_eq!(top[1].1.player_id, "b");
        assert_eq!(top[2].0, 3);
        assert_eq!(top[2].1.player_id, "c");
    }

    #[test]
    fn test_highest_first_ordering() {
        let engine = make_engine_with_board("hf", SortOrder::HighestFirst, 100);

        engine.submit_score("hf", "low", 10.0, None, 1000).unwrap();
        engine.submit_score("hf", "mid", 50.0, None, 2000).unwrap();
        engine.submit_score("hf", "high", 100.0, None, 3000).unwrap();

        let top = engine.top("hf", 3).unwrap();
        assert_eq!(top[0].1.player_id, "high");
        assert_eq!(top[1].1.player_id, "mid");
        assert_eq!(top[2].1.player_id, "low");
    }

    #[test]
    fn test_lowest_first_ordering() {
        let engine = make_engine_with_board("speedrun", SortOrder::LowestFirst, 100);

        engine.submit_score("speedrun", "slow", 120.0, None, 1000).unwrap();
        engine.submit_score("speedrun", "fast", 45.0, None, 2000).unwrap();
        engine.submit_score("speedrun", "mid", 80.0, None, 3000).unwrap();

        let top = engine.top("speedrun", 3).unwrap();
        assert_eq!(top[0].1.player_id, "fast");
        assert_eq!(top[0].1.score, 45.0);
        assert_eq!(top[1].1.player_id, "mid");
        assert_eq!(top[2].1.player_id, "slow");
    }

    #[test]
    fn test_submit_improves_score() {
        let engine = make_engine_with_board("improve", SortOrder::HighestFirst, 100);

        engine.submit_score("improve", "alice", 50.0, None, 1000).unwrap();
        engine.submit_score("improve", "bob", 100.0, None, 2000).unwrap();

        // Alice improves her score
        let info = engine
            .submit_score("improve", "alice", 200.0, None, 3000)
            .unwrap();
        assert_eq!(info.rank, 1); // Alice is now #1
        assert_eq!(info.score, 200.0);

        // Submitting a worse score should NOT update
        let info2 = engine
            .submit_score("improve", "alice", 10.0, None, 4000)
            .unwrap();
        assert_eq!(info2.rank, 1); // Still #1 with old score
        assert_eq!(info2.score, 200.0);
    }

    #[test]
    fn test_get_rank_returns_percentile() {
        let engine = make_engine_with_board("pct", SortOrder::HighestFirst, 100);

        // 10 players
        for i in 1..=10 {
            engine
                .submit_score("pct", &format!("p{}", i), i as f64 * 10.0, None, i * 1000)
                .unwrap();
        }

        // p10 has highest score (100.0), should be rank 1
        let info = engine.get_rank("pct", "p10").unwrap();
        assert_eq!(info.rank, 1);
        assert_eq!(info.total_players, 10);
        assert_eq!(info.percentile, 100.0); // top player

        // p1 has lowest score (10.0), should be rank 10
        let info_last = engine.get_rank("pct", "p1").unwrap();
        assert_eq!(info_last.rank, 10);
        assert!((info_last.percentile - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_get_around_player() {
        let engine = make_engine_with_board("around", SortOrder::HighestFirst, 100);

        for i in 1..=20 {
            engine
                .submit_score("around", &format!("p{}", i), i as f64, None, i * 1000)
                .unwrap();
        }

        // p10 is rank 11 (since p20=rank1, p19=rank2, ..., p10=rank11)
        let around = engine.get_around("around", "p10", 2).unwrap();
        // Should include ranks 9, 10, 11, 12, 13 (p12, p11, p10, p9, p8)
        assert_eq!(around.len(), 5);
        // The middle entry should be p10
        assert!(around.iter().any(|(_, e)| e.player_id == "p10"));
    }

    #[test]
    fn test_get_range() {
        let engine = make_engine_with_board("range", SortOrder::HighestFirst, 100);

        for i in 1..=10 {
            engine
                .submit_score("range", &format!("p{}", i), i as f64 * 10.0, None, i * 1000)
                .unwrap();
        }

        // Get ranks 3-5
        let range = engine.get_range("range", 3, 5).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, 3); // rank 3
        assert_eq!(range[1].0, 4); // rank 4
        assert_eq!(range[2].0, 5); // rank 5
    }

    #[test]
    fn test_remove_player() {
        let engine = make_engine_with_board("remove", SortOrder::HighestFirst, 100);

        engine.submit_score("remove", "alice", 100.0, None, 1000).unwrap();
        engine.submit_score("remove", "bob", 200.0, None, 2000).unwrap();

        assert_eq!(engine.player_count("remove"), 2);
        assert!(engine.remove_player("remove", "alice"));
        assert_eq!(engine.player_count("remove"), 1);
        assert!(!engine.remove_player("remove", "alice")); // already removed

        // bob should still be there
        let rank = engine.get_rank("remove", "bob").unwrap();
        assert_eq!(rank.rank, 1);
    }

    #[test]
    fn test_max_entries_cap() {
        let engine = make_engine_with_board("capped", SortOrder::HighestFirst, 5);

        for i in 1..=10 {
            engine
                .submit_score("capped", &format!("p{}", i), i as f64 * 10.0, None, i * 1000)
                .unwrap();
        }

        // Only top 5 should remain
        assert_eq!(engine.player_count("capped"), 5);

        // The top 5 are p10, p9, p8, p7, p6
        let top = engine.top("capped", 5).unwrap();
        assert_eq!(top[0].1.player_id, "p10");
        assert_eq!(top[4].1.player_id, "p6");

        // p5 and below should be gone
        assert!(engine.get_rank("capped", "p5").is_err());
    }

    #[test]
    fn test_window_expiry_resets_board() {
        let engine = LeaderboardEngine::new();
        engine.create_board(LeaderboardConfig {
            name: "daily".to_string(),
            sort_order: SortOrder::HighestFirst,
            time_window: TimeWindow::Custom(60_000), // 60 second window
            max_entries: 100,
        });

        // Set window start
        engine.check_window_expiry("daily", 1_000_000);

        engine
            .submit_score("daily", "alice", 100.0, None, 1_000_000)
            .unwrap();
        assert_eq!(engine.player_count("daily"), 1);

        // Check before window expires
        let reset = engine.check_window_expiry("daily", 1_050_000);
        assert!(!reset);
        assert_eq!(engine.player_count("daily"), 1);

        // Check after window expires
        let reset = engine.check_window_expiry("daily", 1_070_000);
        assert!(reset);
        assert_eq!(engine.player_count("daily"), 0);
    }

    #[test]
    fn test_tie_breaking_earlier_timestamp_wins() {
        let engine = make_engine_with_board("ties", SortOrder::HighestFirst, 100);

        // Same score, different timestamps
        engine.submit_score("ties", "late", 100.0, None, 5000).unwrap();
        engine.submit_score("ties", "early", 100.0, None, 1000).unwrap();

        let top = engine.top("ties", 2).unwrap();
        // Earlier timestamp should get better rank
        assert_eq!(top[0].1.player_id, "early");
        assert_eq!(top[1].1.player_id, "late");
    }

    #[test]
    fn test_nonexistent_board_returns_error() {
        let engine = LeaderboardEngine::new();
        assert!(engine.submit_score("ghost", "alice", 100.0, None, 1000).is_err());
        assert!(engine.top("ghost", 10).is_err());
        assert!(engine.get_rank("ghost", "alice").is_err());
    }

    #[test]
    fn test_metadata_stored_and_returned() {
        let engine = make_engine_with_board("meta", SortOrder::HighestFirst, 100);
        let meta = serde_json::json!({"display_name": "Alice", "avatar": "warrior.png"});

        engine
            .submit_score("meta", "alice", 100.0, Some(meta.clone()), 1000)
            .unwrap();

        let top = engine.top("meta", 1).unwrap();
        assert_eq!(top[0].1.metadata, Some(meta));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-region global leaderboard aggregation
// ─────────────────────────────────────────────────────────────────────────────

/// Response shape returned by GET /leaderboard/top on peer regions.
#[derive(Deserialize)]
struct TopResponse {
    entries: Vec<serde_json::Value>,
}

/// Pulls the top-N from every region, merges by score, writes to
/// "global_leaderboard" table.  Runs on a configurable interval.
pub struct LeaderboardAggregator {
    engine:        Arc<LeaderboardEngine>,
    regions:       Arc<crate::cluster::regions::RegionRegistry>,
    /// Name of the regional leaderboard board to aggregate.
    board_name:    String,
    interval_secs: u64,
    top_n:         usize,
}

impl LeaderboardAggregator {
    pub fn new(
        engine:        Arc<LeaderboardEngine>,
        regions:       Arc<crate::cluster::regions::RegionRegistry>,
        board_name:    String,
        interval_secs: u64,
        top_n:         usize,
    ) -> Arc<Self> {
        Arc::new(Self { engine, regions, board_name, interval_secs, top_n })
    }

    /// Spawn the background aggregation task.
    /// No-op when only one region is configured or interval is 0.
    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<()>) {
        if self.interval_secs == 0 || !self.regions.is_multi_region() {
            return;
        }
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(self.interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = self.aggregate().await {
                            log::warn!("[leaderboard-agg] {}", e);
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        });
    }

    async fn aggregate(&self) -> Result<(), String> {
        // Collect local top-N.
        let mut all: Vec<(String, f64, String)> = self
            .engine
            .top(&self.board_name, self.top_n)
            .unwrap_or_default()
            .into_iter()
            .map(|(_, e)| (e.player_id, e.score, self.regions.my_region.clone()))
            .collect();

        // Fetch from peer regions.
        for region in self.regions.peer_regions() {
            if region.metrics_url.is_empty() { continue; }
            match self.fetch_from_region(&region).await {
                Ok(entries) => all.extend(entries),
                Err(e) => log::warn!("[leaderboard-agg] region '{}': {}", region.id, e),
            }
        }

        // Sort descending by score.
        all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(self.top_n);

        // Register a "global" board if not already present.
        let global_name = format!("{}_global", self.board_name);
        if !self.engine.list_boards().contains(&global_name) {
            self.engine.create_board(LeaderboardConfig {
                name: global_name.clone(),
                sort_order: SortOrder::HighestFirst,
                time_window: TimeWindow::AllTime,
                max_entries: self.top_n,
            });
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for (player_id, score, region) in &all {
            let meta = serde_json::json!({ "region": region });
            let _ = self.engine.submit_score(&global_name, player_id, *score, Some(meta), now);
        }

        log::debug!("[leaderboard-agg] global '{}' updated: {} entries", global_name, all.len());
        Ok(())
    }

    async fn fetch_from_region(
        &self,
        region: &crate::cluster::regions::ClusterRegion,
    ) -> Result<Vec<(String, f64, String)>, String> {
        let url = format!(
            "{}/leaderboard/top?board={}&n={}",
            region.metrics_url, self.board_name, self.top_n
        );
        let region_id = region.id.clone();

        let resp = tokio::task::spawn_blocking(move || {
            reqwest::blocking::get(&url)
                .and_then(|r| r.json::<TopResponse>())
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;

        Ok(resp.entries.into_iter().filter_map(|row| {
            let player_id = row["player_id"].as_str()?.to_string();
            let score     = row["score"].as_f64()?;
            Some((player_id, score, region_id.clone()))
        }).collect())
    }
}

/// HTTP handler: return top-N from a named board as JSON.
/// Used by peer regions to fetch regional standings.
pub fn http_top_entries(engine: &LeaderboardEngine, board: &str, n: usize) -> serde_json::Value {
    match engine.top(board, n) {
        Ok(entries) => {
            let rows: Vec<serde_json::Value> = entries
                .into_iter()
                .map(|(rank, e)| serde_json::json!({
                    "rank":      rank,
                    "player_id": e.player_id,
                    "score":     e.score,
                    "metadata":  e.metadata,
                }))
                .collect();
            serde_json::json!({ "entries": rows })
        }
        Err(e) => serde_json::json!({ "error": e, "entries": [] }),
    }
}
