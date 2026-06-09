use serde::{Deserialize, Serialize};

use crate::client::ReconnectConfig;

// ── Wire types ────────────────────────────────────────────────────────────────

/// Outgoing: reducer call (rmp_serde array format).
#[derive(Serialize, Deserialize, Debug)]
pub struct ReducerCall {
    pub call_id: u64,
    pub reducer_name: String,
    pub args: Vec<u8>,
}

/// Incoming: bare ReducerResponse (rmp_serde array format).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReducerResponse {
    pub call_id: u64,
    pub success: bool,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
}

/// A row change delivered to a subscriber.
#[derive(Debug, Clone)]
pub struct RowDiff {
    pub subscription_id: String,
    pub table_name: String,
    pub row_key: String,
    /// `"insert"`, `"update"`, `"delete"`, or `"initial_snapshot"`
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

// ── ServerMessage (enum, externally tagged by rmp_serde) ─────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionDiffWire {
    pub subscription_id: String,
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionAckWire {
    pub subscription_id: String,
    pub success: bool,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionRoute {
    pub subscription_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionBody {
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

/// All messages the server can send.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServerMessage {
    ReducerResponse(ReducerResponse),
    SubscriptionAck(SubscriptionAckWire),
    SubscriptionDiff(SubscriptionDiffWire),
    SubscriptionRoute(SubscriptionRoute),
    SubscriptionBody(SubscriptionBody),
    Error { message: String },
}

/// Client commands.
#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    ReducerCall(ReducerCall),
    Subscribe {
        subscription_id: String,
        query: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
}

// ── Client API types ──────────────────────────────────────────────────────────

/// Options for creating a client.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// WebSocket URL, e.g. `"ws://localhost:3000"`.
    pub url: String,
    /// Optional API key — sent as `Authorization: Bearer <key>`.
    pub api_key: Option<String>,
    /// Call timeout in milliseconds. Default: 5000.
    pub call_timeout_ms: u64,
    /// Auto-reconnect configuration.  `None` uses [`ReconnectConfig::default()`]
    /// (reconnect enabled, infinite retries, 1 s base delay, 30 s max, jitter on).
    pub reconnect: Option<ReconnectConfig>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            url: "ws://localhost:3000".to_string(),
            api_key: None,
            call_timeout_ms: 5_000,
            reconnect: None,
        }
    }
}

/// Cached rows for a single table, keyed by `row_key`.
pub type RowCache = dashmap::DashMap<String, serde_json::Value>;
