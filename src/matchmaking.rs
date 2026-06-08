//! Production-grade matchmaking service for NeonDB.
//!
//! Supports queue-based matching, skill-based (Elo/MMR) pairing, region awareness,
//! configurable team sizes (1v1, 2v2, 5v5, FFA), queue timeouts, and full match
//! lifecycle management (Pending -> Confirmed -> InProgress -> Finished).

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Skill Rating
// ---------------------------------------------------------------------------

/// Player skill rating using an Elo-like MMR system.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SkillRating {
    /// Matchmaking Rating (Elo-like).
    pub mmr: f64,
    /// Uncertainty — shrinks as more games are played (starts at 350).
    pub uncertainty: f64,
    /// Total games played.
    pub games_played: u32,
}

impl SkillRating {
    pub fn new(mmr: f64) -> Self {
        SkillRating {
            mmr,
            uncertainty: 350.0,
            games_played: 0,
        }
    }

    pub fn default_rating() -> Self {
        Self::new(1000.0)
    }
}

// ---------------------------------------------------------------------------
// Queue Entry
// ---------------------------------------------------------------------------

/// A player waiting in the matchmaking queue.
#[derive(Clone, Debug)]
pub struct QueueEntry {
    pub player_id: String,
    pub skill: SkillRating,
    /// Geographic region, e.g. "us-east", "eu-west".
    pub region: String,
    /// Game mode key, e.g. "ranked_1v1", "casual_5v5".
    pub game_mode: String,
    /// When this player joined the queue.
    pub joined_at: Instant,
    /// Player-specific max wait in seconds (0 = use mode default).
    pub max_wait_seconds: u64,
    /// Arbitrary caller-supplied metadata.
    pub metadata: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Match types
// ---------------------------------------------------------------------------

/// Match lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchState {
    /// Found match, waiting for all players to confirm.
    Pending,
    /// All players confirmed.
    Confirmed,
    /// Match is in progress.
    InProgress,
    /// Match ended normally.
    Finished,
    /// Match cancelled (player declined or confirmation timeout).
    Cancelled,
}

/// A formed match with team structure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Match {
    pub match_id: String,
    pub game_mode: String,
    pub region: String,
    pub state: MatchState,
    /// `teams[team_idx]` = list of player IDs on that team.
    pub teams: Vec<Vec<String>>,
    pub average_mmr: f64,
    /// Millisecond timestamp when the match was created.
    pub created_at_ms: u64,
    pub metadata: Option<serde_json::Value>,
    /// Which players have confirmed (relevant when `state == Pending`).
    #[serde(skip)]
    pub confirmed_players: Vec<String>,
}

/// Where a player currently is in the matchmaking pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlayerStatus {
    Idle,
    InQueue { game_mode: String },
    InMatch { match_id: String },
}

// ---------------------------------------------------------------------------
// Game-mode configuration
// ---------------------------------------------------------------------------

/// Configuration for one game mode's matchmaking rules.
#[derive(Clone, Debug)]
pub struct GameModeConfig {
    /// Mode key, e.g. "ranked_1v1".
    pub name: String,
    /// Players per team.
    pub team_size: usize,
    /// Number of teams (e.g. 2 for 1v1/5v5, 8 for FFA-8).
    pub team_count: usize,
    /// Initial MMR window when a player first enters the queue.
    pub mmr_range_initial: f64,
    /// How many MMR points the window expands per second of waiting.
    pub mmr_range_expansion_per_second: f64,
    /// Hard cap on MMR window expansion.
    pub mmr_range_max: f64,
    /// Whether to prefer same-region opponents (soft preference, not hard filter).
    pub prefer_same_region: bool,
    /// Default timeout in seconds if the player doesn't specify one.
    pub default_timeout_seconds: u64,
    /// Whether players must confirm before the match starts.
    pub require_confirmation: bool,
    /// Seconds players have to confirm once a match is found.
    pub confirmation_timeout_seconds: u64,
}

impl Default for GameModeConfig {
    fn default() -> Self {
        GameModeConfig {
            name: "ranked_1v1".to_string(),
            team_size: 1,
            team_count: 2,
            mmr_range_initial: 100.0,
            mmr_range_expansion_per_second: 10.0,
            mmr_range_max: 500.0,
            prefer_same_region: true,
            default_timeout_seconds: 120,
            require_confirmation: true,
            confirmation_timeout_seconds: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// Matchmaking Service
// ---------------------------------------------------------------------------

/// The core matchmaking engine.
///
/// Thread-safe — all internal state is protected by `DashMap` or `Mutex`.
/// Call [`tick`] or [`tick_all`] periodically (every 1-2 s) from a background
/// Tokio task to process queues and form matches.
pub struct MatchmakingService {
    /// Per-game-mode queues.
    queues: DashMap<String, Mutex<VecDeque<QueueEntry>>>,
    /// Game mode configurations.
    configs: DashMap<String, GameModeConfig>,
    /// Active matches keyed by match_id.
    matches: DashMap<String, Match>,
    /// player_id -> match_id (prevents double-queue while in a match).
    player_match: DashMap<String, String>,
    /// player_id -> game_mode (prevents double-queue).
    player_queue: DashMap<String, String>,
    /// Monotonic match-ID counter.
    next_match_id: AtomicU64,
}

impl MatchmakingService {
    /// Create an empty matchmaking service with no registered modes.
    pub fn new() -> Self {
        MatchmakingService {
            queues: DashMap::new(),
            configs: DashMap::new(),
            matches: DashMap::new(),
            player_match: DashMap::new(),
            player_queue: DashMap::new(),
            next_match_id: AtomicU64::new(1),
        }
    }

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    /// Register (or replace) a game-mode configuration.
    pub fn register_mode(&self, config: GameModeConfig) {
        let name = config.name.clone();
        self.configs.insert(name.clone(), config);
        self.queues
            .entry(name)
            .or_insert_with(|| Mutex::new(VecDeque::new()));
    }

    /// List all registered game mode names.
    pub fn list_modes(&self) -> Vec<String> {
        self.configs.iter().map(|r| r.key().clone()).collect()
    }

    // ------------------------------------------------------------------
    // Queue operations
    // ------------------------------------------------------------------

    /// Add a player to the matchmaking queue.
    ///
    /// Returns `Err` if the player is already queued or in an active match,
    /// or if the game mode is not registered.
    pub fn enqueue(&self, entry: QueueEntry) -> Result<(), String> {
        if !self.configs.contains_key(&entry.game_mode) {
            return Err(format!("Unknown game mode: {}", entry.game_mode));
        }
        if self.player_queue.contains_key(&entry.player_id) {
            return Err(format!(
                "Player {} is already in a queue",
                entry.player_id
            ));
        }
        if self.player_match.contains_key(&entry.player_id) {
            return Err(format!(
                "Player {} is already in an active match",
                entry.player_id
            ));
        }

        let mode = entry.game_mode.clone();
        let pid = entry.player_id.clone();

        if let Some(q) = self.queues.get(&mode) {
            q.lock().unwrap().push_back(entry);
        } else {
            return Err(format!("Queue missing for mode: {}", mode));
        }

        self.player_queue.insert(pid, mode);
        Ok(())
    }

    /// Remove a player from the queue. Returns `true` if found and removed.
    pub fn dequeue(&self, player_id: &str) -> bool {
        let mode = match self.player_queue.remove(player_id) {
            Some((_, m)) => m,
            None => return false,
        };
        if let Some(q) = self.queues.get(&mode) {
            q.lock().unwrap().retain(|e| e.player_id != player_id);
        }
        true
    }

    /// Current queue depth for a game mode.
    pub fn queue_size(&self, game_mode: &str) -> usize {
        match self.queues.get(game_mode) {
            Some(q) => q.lock().unwrap().len(),
            None => 0,
        }
    }

    // ------------------------------------------------------------------
    // Tick — the matching algorithm
    // ------------------------------------------------------------------

    /// Run one tick of the matchmaking algorithm for a single game mode.
    ///
    /// **Algorithm:**
    /// 1. Sort queue by wait time (longest-waiting first — FIFO priority).
    /// 2. For each unmatched anchor player:
    ///    a. Compute expanded MMR range: `initial + wait_secs * expansion`, capped at max.
    ///    b. Find compatible players within *both* players' ranges. Prefer same region.
    ///    c. If enough to fill all teams: create a Match, remove all from queue.
    ///    d. Otherwise: leave them; they will match next tick with wider range.
    ///
    /// Returns newly created [`Match`]es.
    pub fn tick(&self, game_mode: &str, now: Instant) -> Vec<Match> {
        let config = match self.configs.get(game_mode) {
            Some(c) => c.clone(),
            None => return Vec::new(),
        };

        let total_needed = config.team_size * config.team_count;

        // Snapshot the queue — already in FIFO order.
        let mut candidates: Vec<QueueEntry> = {
            let q = match self.queues.get(game_mode) {
                Some(q) => q,
                None => return Vec::new(),
            };
            let guard = q.lock().unwrap();
            let snapshot: Vec<QueueEntry> = guard.iter().cloned().collect();
            drop(guard);
            snapshot
        };

        // Stable sort by join time (oldest first).
        candidates.sort_by(|a, b| a.joined_at.cmp(&b.joined_at));

        let mut matched_ids: Vec<String> = Vec::new();
        let mut new_matches: Vec<Match> = Vec::new();

        let mut i = 0;
        while i < candidates.len() {
            if matched_ids.contains(&candidates[i].player_id) {
                i += 1;
                continue;
            }

            let anchor = &candidates[i];
            let wait_secs = now.duration_since(anchor.joined_at).as_secs_f64();
            let anchor_range = (config.mmr_range_initial
                + wait_secs * config.mmr_range_expansion_per_second)
                .min(config.mmr_range_max);

            // Collect compatible players. Prioritise same-region if configured.
            let mut same_region: Vec<usize> = Vec::new();
            let mut diff_region: Vec<usize> = Vec::new();

            for j in 0..candidates.len() {
                if j == i || matched_ids.contains(&candidates[j].player_id) {
                    continue;
                }
                let other = &candidates[j];

                // Both players must be within each other's expanded range.
                let other_wait =
                    now.duration_since(other.joined_at).as_secs_f64();
                let other_range = (config.mmr_range_initial
                    + other_wait * config.mmr_range_expansion_per_second)
                    .min(config.mmr_range_max);
                let diff = (anchor.skill.mmr - other.skill.mmr).abs();
                if diff > anchor_range || diff > other_range {
                    continue;
                }

                if other.region == anchor.region {
                    same_region.push(j);
                } else {
                    diff_region.push(j);
                }
            }

            // Build the group: anchor + preferred same-region first, then others.
            let mut group_indices: Vec<usize> = vec![i];
            if config.prefer_same_region {
                for &idx in &same_region {
                    group_indices.push(idx);
                    if group_indices.len() == total_needed {
                        break;
                    }
                }
            }
            if group_indices.len() < total_needed {
                let remaining: Vec<usize> = if config.prefer_same_region {
                    diff_region
                        .iter()
                        .chain(
                            same_region
                                .iter()
                                .filter(|idx| !group_indices.contains(idx)),
                        )
                        .copied()
                        .collect()
                } else {
                    same_region
                        .iter()
                        .chain(diff_region.iter())
                        .copied()
                        .collect()
                };
                for idx in remaining {
                    if !group_indices.contains(&idx) {
                        group_indices.push(idx);
                        if group_indices.len() == total_needed {
                            break;
                        }
                    }
                }
            }

            if group_indices.len() == total_needed {
                // Build teams — round-robin assignment.
                let mut teams: Vec<Vec<String>> =
                    (0..config.team_count).map(|_| Vec::new()).collect();
                let mut mmr_sum = 0.0f64;

                for (slot, &gi) in group_indices.iter().enumerate() {
                    let p = &candidates[gi];
                    teams[slot % config.team_count].push(p.player_id.clone());
                    mmr_sum += p.skill.mmr;
                }
                let avg_mmr = mmr_sum / group_indices.len() as f64;
                let region = anchor.region.clone();

                let match_id = format!(
                    "match_{}",
                    self.next_match_id.fetch_add(1, Ordering::Relaxed)
                );

                let state = if config.require_confirmation {
                    MatchState::Pending
                } else {
                    MatchState::Confirmed
                };

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let m = Match {
                    match_id: match_id.clone(),
                    game_mode: game_mode.to_string(),
                    region,
                    state,
                    teams: teams.clone(),
                    average_mmr: avg_mmr,
                    created_at_ms: now_ms,
                    metadata: None,
                    confirmed_players: Vec::new(),
                };

                for team in &teams {
                    for pid in team {
                        matched_ids.push(pid.clone());
                        self.player_queue.remove(pid.as_str());
                        self.player_match
                            .insert(pid.clone(), match_id.clone());
                    }
                }

                self.matches.insert(match_id, m.clone());
                new_matches.push(m);
            }

            i += 1;
        }

        // Remove matched players from the backing queue.
        if !matched_ids.is_empty() {
            if let Some(q) = self.queues.get(game_mode) {
                q.lock()
                    .unwrap()
                    .retain(|e| !matched_ids.contains(&e.player_id));
            }
        }

        new_matches
    }

    /// Run one tick for ALL registered game modes.
    pub fn tick_all(&self, now: Instant) -> Vec<Match> {
        let modes = self.list_modes();
        let mut all = Vec::new();
        for mode in modes {
            all.extend(self.tick(&mode, now));
        }
        all
    }

    // ------------------------------------------------------------------
    // Match lifecycle
    // ------------------------------------------------------------------

    /// Player confirms participation in a pending match.
    ///
    /// Returns `Ok(true)` when *all* players have confirmed (state -> Confirmed),
    /// `Ok(false)` when confirmation is recorded but others are still outstanding.
    pub fn confirm_player(
        &self,
        match_id: &str,
        player_id: &str,
    ) -> Result<bool, String> {
        let mut m = self
            .matches
            .get_mut(match_id)
            .ok_or_else(|| format!("Match {} not found", match_id))?;

        if m.state != MatchState::Pending {
            return Err(format!("Match {} is not in Pending state", match_id));
        }

        let is_participant = m
            .teams
            .iter()
            .any(|t| t.contains(&player_id.to_string()));
        if !is_participant {
            return Err(format!(
                "Player {} is not in match {}",
                player_id, match_id
            ));
        }

        if !m.confirmed_players.contains(&player_id.to_string()) {
            m.confirmed_players.push(player_id.to_string());
        }

        let total: usize = m.teams.iter().map(|t| t.len()).sum();
        if m.confirmed_players.len() == total {
            m.state = MatchState::Confirmed;
            return Ok(true);
        }
        Ok(false)
    }

    /// Player declines a pending match, cancelling it for everyone.
    pub fn decline_match(
        &self,
        match_id: &str,
        player_id: &str,
    ) -> Result<(), String> {
        let mut m = self
            .matches
            .get_mut(match_id)
            .ok_or_else(|| format!("Match {} not found", match_id))?;

        if m.state != MatchState::Pending {
            return Err(format!("Match {} is not in Pending state", match_id));
        }

        let is_participant = m
            .teams
            .iter()
            .any(|t| t.contains(&player_id.to_string()));
        if !is_participant {
            return Err(format!(
                "Player {} is not in match {}",
                player_id, match_id
            ));
        }

        m.state = MatchState::Cancelled;

        // Release all players.
        for team in &m.teams {
            for pid in team {
                self.player_match.remove(pid.as_str());
            }
        }
        Ok(())
    }

    /// Transition a confirmed match to InProgress.
    pub fn start_match(&self, match_id: &str) -> Result<(), String> {
        let mut m = self
            .matches
            .get_mut(match_id)
            .ok_or_else(|| format!("Match {} not found", match_id))?;

        if m.state != MatchState::Confirmed {
            return Err(format!(
                "Match {} is not Confirmed (current: {:?})",
                match_id, m.state
            ));
        }
        m.state = MatchState::InProgress;
        Ok(())
    }

    /// Transition an in-progress match to Finished and release all players.
    pub fn finish_match(&self, match_id: &str) -> Result<(), String> {
        let mut m = self
            .matches
            .get_mut(match_id)
            .ok_or_else(|| format!("Match {} not found", match_id))?;

        if m.state != MatchState::InProgress {
            return Err(format!(
                "Match {} is not InProgress (current: {:?})",
                match_id, m.state
            ));
        }
        m.state = MatchState::Finished;

        for team in &m.teams {
            for pid in team {
                self.player_match.remove(pid.as_str());
            }
        }
        Ok(())
    }

    /// Retrieve a match by ID.
    pub fn get_match(&self, match_id: &str) -> Option<Match> {
        self.matches.get(match_id).map(|r| r.clone())
    }

    // ------------------------------------------------------------------
    // Player status
    // ------------------------------------------------------------------

    /// Check whether a player is idle, queued, or in a match.
    pub fn player_status(&self, player_id: &str) -> PlayerStatus {
        if let Some(mid) = self.player_match.get(player_id) {
            return PlayerStatus::InMatch {
                match_id: mid.value().clone(),
            };
        }
        if let Some(mode) = self.player_queue.get(player_id) {
            return PlayerStatus::InQueue {
                game_mode: mode.value().clone(),
            };
        }
        PlayerStatus::Idle
    }

    // ------------------------------------------------------------------
    // Timeout sweep
    // ------------------------------------------------------------------

    /// Remove timed-out players from all queues. Returns removed player IDs.
    pub fn sweep_timeouts(&self, now: Instant) -> Vec<String> {
        let mut removed = Vec::new();

        for entry in self.queues.iter() {
            let mode = entry.key().clone();
            let default_timeout = self
                .configs
                .get(&mode)
                .map(|c| c.default_timeout_seconds)
                .unwrap_or(120);

            let mut lock = entry.value().lock().unwrap();
            lock.retain(|e| {
                let timeout = if e.max_wait_seconds > 0 {
                    e.max_wait_seconds
                } else {
                    default_timeout
                };
                let elapsed = now.duration_since(e.joined_at).as_secs();
                if elapsed >= timeout {
                    removed.push(e.player_id.clone());
                    false
                } else {
                    true
                }
            });
        }

        for pid in &removed {
            self.player_queue.remove(pid.as_str());
        }
        removed
    }

    // ------------------------------------------------------------------
    // Elo update (static helpers)
    // ------------------------------------------------------------------

    /// Update MMR for a winner/loser pair using standard Elo.
    ///
    /// ```text
    /// expected_a = 1 / (1 + 10^((rating_b - rating_a) / 400))
    /// new_a = old_a + K * (score - expected_a)
    /// ```
    ///
    /// `k_factor` controls sensitivity (typical: 32 new, 16 established).
    /// Uncertainty is reduced by 5% per game (floor 50).
    pub fn update_mmr(
        winner: &mut SkillRating,
        loser: &mut SkillRating,
        k_factor: f64,
    ) {
        let expected_winner =
            1.0 / (1.0 + 10.0_f64.powf((loser.mmr - winner.mmr) / 400.0));
        let expected_loser = 1.0 - expected_winner;

        winner.mmr += k_factor * (1.0 - expected_winner);
        loser.mmr += k_factor * (0.0 - expected_loser);

        winner.uncertainty = (winner.uncertainty * 0.95).max(50.0);
        loser.uncertainty = (loser.uncertainty * 0.95).max(50.0);
        winner.games_played += 1;
        loser.games_played += 1;
    }

    /// Update MMR for a draw between two players.
    pub fn update_mmr_draw(
        player_a: &mut SkillRating,
        player_b: &mut SkillRating,
        k_factor: f64,
    ) {
        let expected_a =
            1.0 / (1.0 + 10.0_f64.powf((player_b.mmr - player_a.mmr) / 400.0));
        let expected_b = 1.0 - expected_a;

        player_a.mmr += k_factor * (0.5 - expected_a);
        player_b.mmr += k_factor * (0.5 - expected_b);

        player_a.uncertainty = (player_a.uncertainty * 0.95).max(50.0);
        player_b.uncertainty = (player_b.uncertainty * 0.95).max(50.0);
        player_a.games_played += 1;
        player_b.games_played += 1;
    }
}

impl Default for MatchmakingService {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn default_service() -> MatchmakingService {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig::default()); // ranked_1v1
        svc
    }

    fn make_entry(id: &str, mmr: f64, region: &str, mode: &str) -> QueueEntry {
        QueueEntry {
            player_id: id.to_string(),
            skill: SkillRating::new(mmr),
            region: region.to_string(),
            game_mode: mode.to_string(),
            joined_at: Instant::now(),
            max_wait_seconds: 0,
            metadata: None,
        }
    }

    fn make_entry_at(
        id: &str,
        mmr: f64,
        region: &str,
        mode: &str,
        joined_at: Instant,
    ) -> QueueEntry {
        QueueEntry {
            player_id: id.to_string(),
            skill: SkillRating::new(mmr),
            region: region.to_string(),
            game_mode: mode.to_string(),
            joined_at,
            max_wait_seconds: 0,
            metadata: None,
        }
    }

    // ---------------------------------------------------------------
    // Queue operations
    // ---------------------------------------------------------------

    #[test]
    fn test_enqueue_and_dequeue() {
        let svc = default_service();
        let e = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        assert!(svc.enqueue(e).is_ok());
        assert_eq!(svc.queue_size("ranked_1v1"), 1);
        assert!(svc.dequeue("alice"));
        assert_eq!(svc.queue_size("ranked_1v1"), 0);
    }

    #[test]
    fn test_enqueue_prevents_double_queue() {
        let svc = default_service();
        let e1 = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        let e2 = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        assert!(svc.enqueue(e1).is_ok());
        assert!(svc.enqueue(e2).is_err());
    }

    #[test]
    fn test_enqueue_unknown_mode_rejected() {
        let svc = default_service();
        let e = make_entry("alice", 1000.0, "us-east", "unknown_mode");
        assert!(svc.enqueue(e).is_err());
    }

    #[test]
    fn test_dequeue_nonexistent_returns_false() {
        let svc = default_service();
        assert!(!svc.dequeue("nobody"));
    }

    // ---------------------------------------------------------------
    // Tick — matching
    // ---------------------------------------------------------------

    #[test]
    fn test_tick_matches_compatible_players() {
        let svc = default_service();
        let e1 = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        let e2 = make_entry("bob", 1050.0, "us-east", "ranked_1v1");
        svc.enqueue(e1).unwrap();
        svc.enqueue(e2).unwrap();

        let matches = svc.tick("ranked_1v1", Instant::now());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].state, MatchState::Pending);
        assert_eq!(matches[0].teams.len(), 2); // 1v1 = 2 teams of 1
        assert_eq!(svc.queue_size("ranked_1v1"), 0);
    }

    #[test]
    fn test_tick_respects_mmr_range() {
        let svc = default_service();
        // Initial range is 100 — these players are 200 apart.
        let e1 = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        let e2 = make_entry("bob", 1200.0, "us-east", "ranked_1v1");
        svc.enqueue(e1).unwrap();
        svc.enqueue(e2).unwrap();

        let matches = svc.tick("ranked_1v1", Instant::now());
        assert_eq!(matches.len(), 0, "Should not match — MMR 200 apart, range 100");
        assert_eq!(svc.queue_size("ranked_1v1"), 2);
    }

    #[test]
    fn test_mmr_range_expands_over_time() {
        let svc = default_service();
        let past = Instant::now() - Duration::from_secs(15);
        // After 15s at 10/s expansion: range = 100 + 150 = 250. Diff is 200.
        let e1 = make_entry_at("alice", 1000.0, "us-east", "ranked_1v1", past);
        let e2 = make_entry_at("bob", 1200.0, "us-east", "ranked_1v1", past);
        svc.enqueue(e1).unwrap();
        svc.enqueue(e2).unwrap();

        let matches = svc.tick("ranked_1v1", Instant::now());
        assert_eq!(matches.len(), 1, "Should match after range expansion");
    }

    #[test]
    fn test_tick_prefers_same_region() {
        let svc = default_service();
        // Three players: alice (us-east), bob (eu-west), carol (us-east).
        // 1v1 needs 2. Anchor is alice (us-east). Carol is same region, should be preferred.
        let now = Instant::now();
        let e1 = make_entry_at("alice", 1000.0, "us-east", "ranked_1v1", now);
        let e2 = make_entry_at("bob", 1010.0, "eu-west", "ranked_1v1", now);
        let e3 = make_entry_at("carol", 1010.0, "us-east", "ranked_1v1", now);
        svc.enqueue(e1).unwrap();
        svc.enqueue(e2).unwrap();
        svc.enqueue(e3).unwrap();

        let matches = svc.tick("ranked_1v1", Instant::now());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].region, "us-east");
        // Carol should be chosen over Bob (same region preference).
        let all_players: Vec<&String> =
            matches[0].teams.iter().flat_map(|t| t.iter()).collect();
        assert!(all_players.contains(&&"alice".to_string()));
        assert!(all_players.contains(&&"carol".to_string()));
    }

    #[test]
    fn test_tick_no_match_with_one_player() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        let matches = svc.tick("ranked_1v1", Instant::now());
        assert_eq!(matches.len(), 0);
        assert_eq!(svc.queue_size("ranked_1v1"), 1);
    }

    // ---------------------------------------------------------------
    // Timeouts
    // ---------------------------------------------------------------

    #[test]
    fn test_sweep_timeouts_removes_stale() {
        let svc = default_service();
        let past = Instant::now() - Duration::from_secs(200); // > 120s default
        let e = make_entry_at("alice", 1000.0, "us-east", "ranked_1v1", past);
        svc.enqueue(e).unwrap();
        assert_eq!(svc.queue_size("ranked_1v1"), 1);

        let removed = svc.sweep_timeouts(Instant::now());
        assert_eq!(removed, vec!["alice"]);
        assert_eq!(svc.queue_size("ranked_1v1"), 0);
        // Player queue index also cleaned.
        assert!(matches!(svc.player_status("alice"), PlayerStatus::Idle));
    }

    #[test]
    fn test_sweep_timeouts_keeps_fresh() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        let removed = svc.sweep_timeouts(Instant::now());
        assert!(removed.is_empty());
        assert_eq!(svc.queue_size("ranked_1v1"), 1);
    }

    #[test]
    fn test_sweep_timeouts_respects_player_override() {
        let svc = default_service();
        let mut e = make_entry_at(
            "alice",
            1000.0,
            "us-east",
            "ranked_1v1",
            Instant::now() - Duration::from_secs(50),
        );
        e.max_wait_seconds = 30; // Player wants a shorter timeout.
        svc.enqueue(e).unwrap();

        let removed = svc.sweep_timeouts(Instant::now());
        assert_eq!(removed, vec!["alice"]);
    }

    // ---------------------------------------------------------------
    // Match lifecycle
    // ---------------------------------------------------------------

    #[test]
    fn test_match_lifecycle_pending_to_finished() {
        let svc = default_service();
        let e1 = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        let e2 = make_entry("bob", 1050.0, "us-east", "ranked_1v1");
        svc.enqueue(e1).unwrap();
        svc.enqueue(e2).unwrap();

        let matches = svc.tick("ranked_1v1", Instant::now());
        let mid = &matches[0].match_id;

        assert_eq!(svc.confirm_player(mid, "alice").unwrap(), false);
        assert_eq!(svc.confirm_player(mid, "bob").unwrap(), true);
        assert_eq!(svc.get_match(mid).unwrap().state, MatchState::Confirmed);

        svc.start_match(mid).unwrap();
        assert_eq!(svc.get_match(mid).unwrap().state, MatchState::InProgress);

        svc.finish_match(mid).unwrap();
        assert_eq!(svc.get_match(mid).unwrap().state, MatchState::Finished);

        // Players released after finish.
        assert!(matches!(svc.player_status("alice"), PlayerStatus::Idle));
        assert!(matches!(svc.player_status("bob"), PlayerStatus::Idle));
    }

    #[test]
    fn test_confirm_player_all_confirmed() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        let matches = svc.tick("ranked_1v1", Instant::now());
        let mid = &matches[0].match_id;

        assert!(!svc.confirm_player(mid, "alice").unwrap());
        assert!(svc.confirm_player(mid, "bob").unwrap());
    }

    #[test]
    fn test_decline_cancels_match() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        let matches = svc.tick("ranked_1v1", Instant::now());
        let mid = &matches[0].match_id;

        svc.decline_match(mid, "alice").unwrap();
        assert_eq!(svc.get_match(mid).unwrap().state, MatchState::Cancelled);
        assert!(matches!(svc.player_status("alice"), PlayerStatus::Idle));
        assert!(matches!(svc.player_status("bob"), PlayerStatus::Idle));
    }

    // ---------------------------------------------------------------
    // Player status
    // ---------------------------------------------------------------

    #[test]
    fn test_player_status_tracking() {
        let svc = default_service();
        assert!(matches!(svc.player_status("alice"), PlayerStatus::Idle));

        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        assert!(matches!(
            svc.player_status("alice"),
            PlayerStatus::InQueue { .. }
        ));

        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.tick("ranked_1v1", Instant::now());
        assert!(matches!(
            svc.player_status("alice"),
            PlayerStatus::InMatch { .. }
        ));
    }

    // ---------------------------------------------------------------
    // Elo / MMR
    // ---------------------------------------------------------------

    #[test]
    fn test_update_mmr_winner_gains() {
        let mut winner = SkillRating::new(1000.0);
        let mut loser = SkillRating::new(1000.0);
        MatchmakingService::update_mmr(&mut winner, &mut loser, 32.0);
        assert!(winner.mmr > 1000.0);
        assert!(loser.mmr < 1000.0);
        assert_eq!(winner.games_played, 1);
        assert_eq!(loser.games_played, 1);
        // Uncertainty should decrease.
        assert!(winner.uncertainty < 350.0);
    }

    #[test]
    fn test_update_mmr_upset_gives_more() {
        let mut underdog = SkillRating::new(800.0);
        let mut favourite = SkillRating::new(1200.0);
        MatchmakingService::update_mmr(&mut underdog, &mut favourite, 32.0);
        let underdog_gain = underdog.mmr - 800.0;

        let mut fav2 = SkillRating::new(1200.0);
        let mut dog2 = SkillRating::new(800.0);
        MatchmakingService::update_mmr(&mut fav2, &mut dog2, 32.0);
        let fav_gain = fav2.mmr - 1200.0;

        assert!(
            underdog_gain > fav_gain,
            "Underdog should gain more: {:.2} vs {:.2}",
            underdog_gain,
            fav_gain
        );
    }

    #[test]
    fn test_update_mmr_draw_equal_players() {
        let mut a = SkillRating::new(1000.0);
        let mut b = SkillRating::new(1000.0);
        MatchmakingService::update_mmr_draw(&mut a, &mut b, 32.0);
        // Equal players draw — ratings should barely change.
        assert!((a.mmr - 1000.0).abs() < 0.01);
        assert!((b.mmr - 1000.0).abs() < 0.01);
    }

    // ---------------------------------------------------------------
    // Multiple modes
    // ---------------------------------------------------------------

    #[test]
    fn test_multiple_game_modes_independent() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "mode_a".to_string(),
            ..GameModeConfig::default()
        });
        svc.register_mode(GameModeConfig {
            name: "mode_b".to_string(),
            ..GameModeConfig::default()
        });

        svc.enqueue(make_entry("alice", 1000.0, "us-east", "mode_a"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1010.0, "us-east", "mode_b"))
            .unwrap();

        let ma = svc.tick("mode_a", Instant::now());
        assert_eq!(ma.len(), 0); // only 1 player in mode_a
        assert_eq!(svc.queue_size("mode_a"), 1);
        assert_eq!(svc.queue_size("mode_b"), 1);
    }

    // ---------------------------------------------------------------
    // Team matching 5v5
    // ---------------------------------------------------------------

    #[test]
    fn test_team_matching_5v5() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "casual_5v5".to_string(),
            team_size: 5,
            team_count: 2,
            require_confirmation: false,
            ..GameModeConfig::default()
        });

        for i in 0..10 {
            svc.enqueue(make_entry(
                &format!("player_{}", i),
                1000.0 + (i as f64) * 5.0,
                "us-east",
                "casual_5v5",
            ))
            .unwrap();
        }
        assert_eq!(svc.queue_size("casual_5v5"), 10);

        let matches = svc.tick("casual_5v5", Instant::now());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].teams.len(), 2);
        assert_eq!(matches[0].teams[0].len(), 5);
        assert_eq!(matches[0].teams[1].len(), 5);
        assert_eq!(matches[0].state, MatchState::Confirmed);
        assert_eq!(svc.queue_size("casual_5v5"), 0);
    }

    // ---------------------------------------------------------------
    // FFA mode
    // ---------------------------------------------------------------

    #[test]
    fn test_ffa_mode() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "ffa_8".to_string(),
            team_size: 1,
            team_count: 8,
            require_confirmation: false,
            ..GameModeConfig::default()
        });

        for i in 0..8 {
            svc.enqueue(make_entry(
                &format!("p{}", i),
                1000.0 + (i as f64) * 10.0,
                "eu-west",
                "ffa_8",
            ))
            .unwrap();
        }

        let matches = svc.tick("ffa_8", Instant::now());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].teams.len(), 8);
        for team in &matches[0].teams {
            assert_eq!(team.len(), 1);
        }
    }

    // ---------------------------------------------------------------
    // 2v2 mode
    // ---------------------------------------------------------------

    #[test]
    fn test_team_matching_2v2() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "ranked_2v2".to_string(),
            team_size: 2,
            team_count: 2,
            require_confirmation: false,
            ..GameModeConfig::default()
        });

        for i in 0..4 {
            svc.enqueue(make_entry(
                &format!("p{}", i),
                1000.0 + (i as f64) * 10.0,
                "us-east",
                "ranked_2v2",
            ))
            .unwrap();
        }

        let matches = svc.tick("ranked_2v2", Instant::now());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].teams.len(), 2);
        assert_eq!(matches[0].teams[0].len(), 2);
        assert_eq!(matches[0].teams[1].len(), 2);
    }

    // ---------------------------------------------------------------
    // Confirmation edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_confirm_nonexistent_match() {
        let svc = default_service();
        assert!(svc.confirm_player("fake_match", "alice").is_err());
    }

    #[test]
    fn test_start_unconfirmed_match_fails() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        let matches = svc.tick("ranked_1v1", Instant::now());
        assert!(svc.start_match(&matches[0].match_id).is_err());
    }

    #[test]
    fn test_finish_not_started_fails() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        let matches = svc.tick("ranked_1v1", Instant::now());
        assert!(svc.finish_match(&matches[0].match_id).is_err());
    }

    #[test]
    fn test_enqueue_while_in_match_rejected() {
        let svc = default_service();
        svc.enqueue(make_entry("alice", 1000.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1050.0, "us-east", "ranked_1v1"))
            .unwrap();
        svc.tick("ranked_1v1", Instant::now());

        let e = make_entry("alice", 1000.0, "us-east", "ranked_1v1");
        assert!(svc.enqueue(e).is_err());
    }

    // ---------------------------------------------------------------
    // tick_all
    // ---------------------------------------------------------------

    #[test]
    fn test_tick_all_processes_all_modes() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "mode_a".to_string(),
            require_confirmation: false,
            ..GameModeConfig::default()
        });
        svc.register_mode(GameModeConfig {
            name: "mode_b".to_string(),
            require_confirmation: false,
            ..GameModeConfig::default()
        });

        svc.enqueue(make_entry("a1", 1000.0, "us", "mode_a")).unwrap();
        svc.enqueue(make_entry("a2", 1010.0, "us", "mode_a")).unwrap();
        svc.enqueue(make_entry("b1", 1000.0, "eu", "mode_b")).unwrap();
        svc.enqueue(make_entry("b2", 1010.0, "eu", "mode_b")).unwrap();

        let all = svc.tick_all(Instant::now());
        assert_eq!(all.len(), 2);
    }

    // ---------------------------------------------------------------
    // MMR range cap
    // ---------------------------------------------------------------

    #[test]
    fn test_mmr_range_capped_at_max() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "capped".to_string(),
            mmr_range_initial: 50.0,
            mmr_range_expansion_per_second: 100.0,
            mmr_range_max: 200.0,
            ..GameModeConfig::default()
        });

        // Players are 300 apart. Even after long wait, cap is 200 so no match.
        let past = Instant::now() - Duration::from_secs(60);
        svc.enqueue(make_entry_at("alice", 1000.0, "us", "capped", past))
            .unwrap();
        svc.enqueue(make_entry_at("bob", 1300.0, "us", "capped", past))
            .unwrap();

        let matches = svc.tick("capped", Instant::now());
        assert_eq!(matches.len(), 0, "MMR diff 300 > max range 200");
    }

    // ---------------------------------------------------------------
    // Average MMR calculation
    // ---------------------------------------------------------------

    #[test]
    fn test_match_average_mmr() {
        let svc = MatchmakingService::new();
        svc.register_mode(GameModeConfig {
            name: "test_avg".to_string(),
            require_confirmation: false,
            ..GameModeConfig::default()
        });

        svc.enqueue(make_entry("alice", 1000.0, "us", "test_avg"))
            .unwrap();
        svc.enqueue(make_entry("bob", 1080.0, "us", "test_avg"))
            .unwrap();

        let matches = svc.tick("test_avg", Instant::now());
        assert_eq!(matches.len(), 1);
        let avg = matches[0].average_mmr;
        assert!((avg - 1040.0).abs() < 0.01, "Expected ~1040, got {}", avg);
    }

    // ---------------------------------------------------------------
    // list_modes
    // ---------------------------------------------------------------

    #[test]
    fn test_list_modes() {
        let svc = MatchmakingService::new();
        assert!(svc.list_modes().is_empty());
        svc.register_mode(GameModeConfig::default());
        let modes = svc.list_modes();
        assert_eq!(modes.len(), 1);
        assert!(modes.contains(&"ranked_1v1".to_string()));
    }
}
