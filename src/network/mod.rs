pub mod message;
pub mod protocol;
pub mod rate_limiter;
pub mod tls;
pub mod websocket;

pub use message::{
    ClientMessage, ReducerCall, ReducerResponse, ServerMessage,
    SqlQuery, SqlResult,
    SubscriptionBody, SubscriptionDiff, SubscriptionRoute,
};
pub use protocol::{decode_client_message, decode_reducer_call, encode_message, encode_server_message};
pub use rate_limiter::{RateLimiterConfig, RateLimiterRegistry, ShutdownState, TokenBucket};
pub use websocket::{start_listener, PendingCall};

// ── Inline reducer fast path ──────────────────────────────────────────────────
//
// Reducers registered here bypass the kanal worker pool entirely — they execute
// directly in the WebSocket async task.  This eliminates the ~80µs OS thread
// wakeup overhead on Windows and enables 300K-500K TPS for pure-computation
// reducers (no DB writes, no blocking I/O).
//
// Rules:
//   - The closure must be Send + Sync + 'static (shared across all connections).
//   - It must NOT block (no mutex, no I/O, no sleep).
//   - It must NOT write to the database.  Only pure computation is safe here.

use std::collections::HashMap;

/// Closure type for an inline reducer.
/// `args` is the raw MessagePack argument bytes; returns serialised result bytes.
pub type InlineFn = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static>;

use std::sync::Arc;

/// Lookup table populated once at startup.  Read-only at runtime.
pub struct InlineRegistry {
    table: HashMap<String, InlineFn>,
}

impl InlineRegistry {
    pub fn new() -> Self {
        Self { table: HashMap::new() }
    }

    pub fn register(&mut self, name: impl Into<String>, f: InlineFn) {
        self.table.insert(name.into(), f);
    }

    #[inline]
    pub fn get(&self, name: &str) -> Option<&InlineFn> {
        self.table.get(name)
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
}

/// Build the default inline registry that ships with NeonDB.
/// `stress_ping` is always registered so benchmarks can bypass the worker pool.
pub fn build_inline_registry() -> Arc<InlineRegistry> {
    let mut r = InlineRegistry::new();

    // Pre-encode the response once — zero allocation per call at runtime.
    let ping_bytes: Vec<u8> = rmp_serde::to_vec(&serde_json::json!({ "ok": true }))
        .unwrap_or_default();
    let ping_bytes = Arc::new(ping_bytes);

    r.register("stress_ping", Arc::new(move |_args: &[u8]| {
        (*ping_bytes).clone()
    }));

    Arc::new(r)
}
