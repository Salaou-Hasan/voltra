use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use dashmap::DashMap;

/// Per-client token bucket rate limiter.
///
/// Each client gets `capacity` tokens. Tokens refill at `refill_rate` per second.
/// A reducer call costs 1 token. When tokens are exhausted, the call is rejected
/// with a rate-limit error (not queued).
pub struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_rate: f64) -> Self {
        TokenBucket {
            tokens: capacity as f64,
            capacity: capacity as f64,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if allowed, false if rate-limited.
    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
            self.last_refill = now;
        }
    }

    /// Tokens currently available (after refill).
    pub fn available(&mut self) -> u32 {
        self.refill();
        self.tokens as u32
    }
}

/// Configuration for the per-client rate limiter.
#[derive(Clone, Debug)]
pub struct RateLimiterConfig {
    /// Maximum burst size (tokens).
    pub capacity: u32,
    /// Sustained rate (calls per second).
    pub refill_rate: f64,
    /// Whether rate limiting is enabled.
    pub enabled: bool,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        RateLimiterConfig {
            capacity: 100,     // burst of 100 calls
            refill_rate: 50.0, // sustained 50 calls/sec per client
            enabled: true,
        }
    }
}

/// Registry that maps client IDs to their individual token buckets.
///
/// Thread-safe — uses DashMap internally. Safe to share across tasks via Arc.
pub struct RateLimiterRegistry {
    config: RateLimiterConfig,
    buckets: DashMap<String, TokenBucket>,
}

impl RateLimiterRegistry {
    pub fn new(config: RateLimiterConfig) -> Self {
        RateLimiterRegistry {
            config,
            buckets: DashMap::new(),
        }
    }

    /// Check if a client is allowed to make a call. Creates bucket on first use.
    /// Returns true if the call is allowed, false if rate-limited.
    /// When rate limiting is disabled (config.enabled == false), always returns true.
    pub fn check(&self, client_id: &str) -> bool {
        if !self.config.enabled {
            return true;
        }

        let mut entry = self.buckets.entry(client_id.to_string()).or_insert_with(|| {
            TokenBucket::new(self.config.capacity, self.config.refill_rate)
        });
        entry.value_mut().try_consume()
    }

    /// Remove a client's bucket (on disconnect).
    pub fn remove(&self, client_id: &str) {
        self.buckets.remove(client_id);
    }

    /// Number of tracked clients.
    pub fn client_count(&self) -> usize {
        self.buckets.len()
    }

    /// Get a reference to the config.
    pub fn config(&self) -> &RateLimiterConfig {
        &self.config
    }
}

/// Graceful shutdown coordinator.
///
/// When shutdown is triggered:
/// 1. Stop accepting new connections
/// 2. Stop accepting new reducer calls (return "shutting down" error)
/// 3. Wait for in-flight reducers to complete (up to drain_timeout)
/// 4. Close all client connections with a Close frame
pub struct ShutdownState {
    draining: AtomicBool,
}

impl ShutdownState {
    pub fn new() -> Self {
        ShutdownState {
            draining: AtomicBool::new(false),
        }
    }

    /// Returns true if the server is in drain mode (shutting down).
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    /// Transition to draining state. Once set, new connections and calls are rejected.
    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_allows_within_capacity() {
        let mut bucket = TokenBucket::new(10, 5.0);
        for _ in 0..10 {
            assert!(bucket.try_consume(), "should allow consume within capacity");
        }
    }

    #[test]
    fn test_token_bucket_denies_over_capacity() {
        let mut bucket = TokenBucket::new(10, 5.0);
        // Exhaust all tokens
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }
        // 11th should be denied
        assert!(!bucket.try_consume(), "should deny consume over capacity");
        assert!(!bucket.try_consume(), "should continue denying");
    }

    #[test]
    fn test_token_bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(10, 50.0); // 50 tokens/sec refill
        // Exhaust all tokens
        for _ in 0..10 {
            bucket.try_consume();
        }
        assert!(!bucket.try_consume(), "should be empty");

        // Wait 200ms: should refill ~10 tokens (50 * 0.2 = 10)
        thread::sleep(Duration::from_millis(220));

        // Should have some tokens available now
        let available = bucket.available();
        assert!(
            available >= 8,
            "expected at least 8 tokens after 220ms at 50/s, got {}",
            available
        );
        assert!(bucket.try_consume(), "should allow after refill");
    }

    #[test]
    fn test_token_bucket_does_not_exceed_capacity() {
        let mut bucket = TokenBucket::new(5, 100.0); // high refill rate
        // Wait a bit to let refill try to go above capacity
        thread::sleep(Duration::from_millis(100));
        let available = bucket.available();
        assert!(
            available <= 5,
            "should not exceed capacity, got {}",
            available
        );
    }

    #[test]
    fn test_rate_limiter_registry_creates_per_client() {
        let config = RateLimiterConfig {
            capacity: 5,
            refill_rate: 1.0,
            enabled: true,
        };
        let registry = RateLimiterRegistry::new(config);

        // First check for each client creates their bucket
        assert!(registry.check("alice"));
        assert!(registry.check("bob"));
        assert_eq!(registry.client_count(), 2);

        // Exhaust alice's tokens (she started with 5, used 1 above)
        for _ in 0..4 {
            registry.check("alice");
        }
        // Alice should be rate-limited
        assert!(!registry.check("alice"), "alice should be rate-limited");

        // Bob should still have tokens (started with 5, used 1 above)
        assert!(registry.check("bob"), "bob should still have tokens");
    }

    #[test]
    fn test_rate_limiter_registry_remove_cleans_up() {
        let config = RateLimiterConfig {
            capacity: 10,
            refill_rate: 5.0,
            enabled: true,
        };
        let registry = RateLimiterRegistry::new(config);

        registry.check("alice");
        registry.check("bob");
        registry.check("carol");
        assert_eq!(registry.client_count(), 3);

        registry.remove("bob");
        assert_eq!(registry.client_count(), 2);

        registry.remove("alice");
        assert_eq!(registry.client_count(), 1);

        // Removing non-existent client is a no-op
        registry.remove("dave");
        assert_eq!(registry.client_count(), 1);
    }

    #[test]
    fn test_shutdown_state_transitions() {
        let state = ShutdownState::new();
        assert!(!state.is_draining(), "should start as not draining");

        state.start_draining();
        assert!(state.is_draining(), "should be draining after start_draining");

        // Idempotent — calling again should be fine
        state.start_draining();
        assert!(state.is_draining(), "should remain draining");
    }

    #[test]
    fn test_rate_limiter_disabled_always_allows() {
        let config = RateLimiterConfig {
            capacity: 1,       // very small capacity
            refill_rate: 0.01, // very slow refill
            enabled: false,    // but disabled!
        };
        let registry = RateLimiterRegistry::new(config);

        // Should always allow regardless of capacity
        for _ in 0..1000 {
            assert!(
                registry.check("flood_client"),
                "disabled limiter should always allow"
            );
        }
        // No bucket should be created when disabled
        assert_eq!(registry.client_count(), 0);
    }

    #[test]
    fn test_shutdown_state_default() {
        let state = ShutdownState::default();
        assert!(!state.is_draining());
    }

    #[test]
    fn test_rate_limiter_config_default() {
        let config = RateLimiterConfig::default();
        assert_eq!(config.capacity, 100);
        assert_eq!(config.refill_rate, 50.0);
        assert!(config.enabled);
    }
}
