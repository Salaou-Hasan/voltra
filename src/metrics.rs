// ============================================================================
// Voltra metrics.rs — Prometheus exposition format
//
// Provides a `Metrics` struct that holds all registered Prometheus metrics.
// Call `metrics.render()` to produce a text/plain Prometheus scrape response.
//
// All metrics are registered on a dedicated `Registry` (not the global default
// registry) so unit tests can construct independent `Metrics` instances without
// cross-test pollution.
// ============================================================================

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry, TextEncoder,
};

/// All Prometheus metrics exported by the Voltra server.
///
/// Pass `Arc<Metrics>` to any component that needs to record observations.
pub struct Metrics {
    /// The private registry — all metrics below are registered here.
    pub registry: Registry,

    // ── Reducer metrics ───────────────────────────────────────────────────
    /// Total number of reducer calls that completed successfully.
    pub reducer_calls_total: IntCounter,
    /// Total number of reducer calls that returned an error or panicked.
    pub reducer_errors_total: IntCounter,
    /// Wall-clock duration of each reducer call in seconds.
    pub reducer_duration_seconds: Histogram,

    // ── WebSocket / connection metrics ────────────────────────────────────
    /// Current number of open WebSocket connections (gauge).
    pub websocket_connections_active: IntGauge,
    /// Total WebSocket connections accepted since server start.
    pub websocket_connects_total: IntCounter,
    /// Total subscription fan-out frames dropped because a client's outbound
    /// buffer was full (transient back-pressure; stale state is shed first).
    pub subscription_frames_dropped_total: IntCounter,
    /// Total connections evicted for sustained send-buffer backlog
    /// (slow-consumer eviction — the client could not keep up with fan-out).
    pub slow_consumer_evictions_total: IntCounter,

    // ── WAL metrics ───────────────────────────────────────────────────────
    /// Total WAL entries successfully written.
    pub wal_entries_written_total: IntCounter,
    /// Current number of WAL entries pending flush (approximation).
    pub wal_queue_depth: IntGauge,

    // ── Data metrics ──────────────────────────────────────────────────────
    /// Total rows across all tables (refreshed by a background task).
    pub rows_total: IntGauge,
    /// Current number of active subscriptions (refreshed by a background task).
    pub subscriptions_active: IntGauge,

    // ── Raft metrics ──────────────────────────────────────────────────────
    /// Last committed Raft log index (refreshed by a background task).
    pub raft_log_index: IntGauge,
    /// 1 if this node is the current Raft leader, 0 otherwise.
    pub raft_is_leader: IntGauge,
}

impl Metrics {
    /// Construct and register all metrics on a fresh `Registry`.
    pub fn new() -> Self {
        let registry = Registry::new();

        let reducer_calls_total = IntCounter::with_opts(
            Opts::new("voltra_reducer_calls_total", "Total successful reducer calls"),
        )
        .expect("metric creation failed");

        let reducer_errors_total = IntCounter::with_opts(
            Opts::new("voltra_reducer_errors_total", "Total reducer errors"),
        )
        .expect("metric creation failed");

        let reducer_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "voltra_reducer_duration_seconds",
                "Reducer call wall-clock duration in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
        )
        .expect("metric creation failed");

        let websocket_connections_active = IntGauge::with_opts(Opts::new(
            "voltra_websocket_connections_active",
            "Current open WebSocket connections",
        ))
        .expect("metric creation failed");

        let websocket_connects_total = IntCounter::with_opts(Opts::new(
            "voltra_websocket_connects_total",
            "Total WebSocket connections accepted",
        ))
        .expect("metric creation failed");

        let subscription_frames_dropped_total = IntCounter::with_opts(Opts::new(
            "voltra_subscription_frames_dropped_total",
            "Total subscription fan-out frames dropped due to full client buffers",
        ))
        .expect("metric creation failed");

        let slow_consumer_evictions_total = IntCounter::with_opts(Opts::new(
            "voltra_slow_consumer_evictions_total",
            "Total connections evicted for sustained send-buffer backlog",
        ))
        .expect("metric creation failed");

        let wal_entries_written_total = IntCounter::with_opts(Opts::new(
            "voltra_wal_entries_written_total",
            "Total WAL entries written",
        ))
        .expect("metric creation failed");

        let wal_queue_depth = IntGauge::with_opts(Opts::new(
            "voltra_wal_queue_depth",
            "WAL entries pending flush",
        ))
        .expect("metric creation failed");

        let rows_total = IntGauge::with_opts(Opts::new(
            "voltra_rows_total",
            "Total rows across all tables",
        ))
        .expect("metric creation failed");

        let subscriptions_active = IntGauge::with_opts(Opts::new(
            "voltra_subscriptions_active",
            "Current active subscriptions",
        ))
        .expect("metric creation failed");

        let raft_log_index = IntGauge::with_opts(Opts::new(
            "voltra_raft_log_index",
            "Last committed Raft log index",
        ))
        .expect("metric creation failed");

        let raft_is_leader = IntGauge::with_opts(Opts::new(
            "voltra_raft_is_leader",
            "1 if this node is the Raft leader, 0 otherwise",
        ))
        .expect("metric creation failed");

        // Register everything on the private registry.
        registry.register(Box::new(reducer_calls_total.clone())).unwrap();
        registry.register(Box::new(reducer_errors_total.clone())).unwrap();
        registry.register(Box::new(reducer_duration_seconds.clone())).unwrap();
        registry.register(Box::new(websocket_connections_active.clone())).unwrap();
        registry.register(Box::new(websocket_connects_total.clone())).unwrap();
        registry.register(Box::new(subscription_frames_dropped_total.clone())).unwrap();
        registry.register(Box::new(slow_consumer_evictions_total.clone())).unwrap();
        registry.register(Box::new(wal_entries_written_total.clone())).unwrap();
        registry.register(Box::new(wal_queue_depth.clone())).unwrap();
        registry.register(Box::new(rows_total.clone())).unwrap();
        registry.register(Box::new(subscriptions_active.clone())).unwrap();
        registry.register(Box::new(raft_log_index.clone())).unwrap();
        registry.register(Box::new(raft_is_leader.clone())).unwrap();

        Metrics {
            registry,
            reducer_calls_total,
            reducer_errors_total,
            reducer_duration_seconds,
            websocket_connections_active,
            websocket_connects_total,
            subscription_frames_dropped_total,
            slow_consumer_evictions_total,
            wal_entries_written_total,
            wal_queue_depth,
            rows_total,
            subscriptions_active,
            raft_log_index,
            raft_is_leader,
        }
    }

    /// Render all metrics in Prometheus text exposition format (version 0.0.4).
    ///
    /// The returned string is suitable as the body of a `GET /metrics` response
    /// with `Content-Type: text/plain; version=0.0.4`.
    pub fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        encoder
            .encode_to_string(&families)
            .unwrap_or_else(|e| format!("# ERROR encoding metrics: {}\n", e))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// `Metrics::new()` must complete without panicking and all counters start
    /// at zero / gauges start at zero.
    #[test]
    fn test_metrics_new_succeeds() {
        let m = Metrics::new();
        assert_eq!(m.reducer_calls_total.get(), 0);
        assert_eq!(m.reducer_errors_total.get(), 0);
        assert_eq!(m.websocket_connections_active.get(), 0);
        assert_eq!(m.rows_total.get(), 0);
    }

    /// Counters must increment monotonically.
    #[test]
    fn test_counter_increments() {
        let m = Metrics::new();
        m.reducer_calls_total.inc();
        m.reducer_calls_total.inc();
        m.reducer_errors_total.inc();
        assert_eq!(m.reducer_calls_total.get(), 2);
        assert_eq!(m.reducer_errors_total.get(), 1);
    }

    /// Histograms must accept observations without panicking.
    #[test]
    fn test_histogram_observes() {
        let m = Metrics::new();
        m.reducer_duration_seconds.observe(0.001);
        m.reducer_duration_seconds.observe(0.050);
        m.reducer_duration_seconds.observe(1.5);
        // If we got here without panicking the test passes.
    }

    /// `render()` must produce non-empty text containing `# TYPE` comment lines
    /// (the standard Prometheus metric type annotation).
    #[test]
    fn test_render_produces_prometheus_text() {
        let m = Metrics::new();
        m.reducer_calls_total.inc_by(5);
        m.websocket_connections_active.set(3);
        let output = m.render();
        assert!(!output.is_empty(), "render() must not return empty string");
        assert!(
            output.contains("# TYPE"),
            "render() output must contain '# TYPE' lines, got:\n{}",
            output
        );
        // Verify at least one of our metric names appears in the output.
        assert!(
            output.contains("voltra_reducer_calls_total"),
            "Expected voltra_reducer_calls_total in output"
        );
    }

    /// Multiple independent `Metrics` instances must not conflict with each other
    /// (each uses its own registry, not the global default).
    #[test]
    fn test_multiple_instances_independent() {
        let m1 = Metrics::new();
        let m2 = Metrics::new();
        m1.reducer_calls_total.inc_by(10);
        m2.reducer_calls_total.inc_by(3);
        assert_eq!(m1.reducer_calls_total.get(), 10);
        assert_eq!(m2.reducer_calls_total.get(), 3);
    }

    /// Gauges support set, inc, and dec.
    #[test]
    fn test_gauge_operations() {
        let m = Metrics::new();
        m.websocket_connections_active.set(5);
        assert_eq!(m.websocket_connections_active.get(), 5);
        m.websocket_connections_active.inc();
        assert_eq!(m.websocket_connections_active.get(), 6);
        m.websocket_connections_active.dec();
        assert_eq!(m.websocket_connections_active.get(), 5);
    }

    /// The WAL counter and queue depth gauge work correctly.
    #[test]
    fn test_wal_metrics() {
        let m = Metrics::new();
        m.wal_entries_written_total.inc_by(100);
        m.wal_queue_depth.set(7);
        assert_eq!(m.wal_entries_written_total.get(), 100);
        assert_eq!(m.wal_queue_depth.get(), 7);
    }

    /// Raft metrics reflect leader state correctly.
    #[test]
    fn test_raft_metrics() {
        let m = Metrics::new();
        m.raft_log_index.set(42);
        m.raft_is_leader.set(1);
        assert_eq!(m.raft_log_index.get(), 42);
        assert_eq!(m.raft_is_leader.get(), 1);
        m.raft_is_leader.set(0);
        assert_eq!(m.raft_is_leader.get(), 0);
    }
}
