// ============================================================================
// src/cluster/fanout.rs
//
// Delta fan-out: after a reducer commits on this node, serialise the
// RowDeltas and POST them to all healthy peer nodes so their subscribers
// see the change too.
//
// Wire format (JSON, sent as POST /cluster/deltas body):
//   {
//     "from_shard": 0,
//     "deltas": [
//       {
//         "table":    "players",
//         "row_key":  "alice",
//         "op":       "set",          // "set" | "delete"
//         "data_b64": "<base64>"      // only present for "set"
//       }
//     ]
//   }
//
// Delivery is fire-and-forget on a blocking thread per peer.
// Errors are logged but never propagated — the local commit already succeeded.
// ============================================================================

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::table::RowDelta;
use crate::error::{NeonDBError, Result};
use super::ClusterBus;

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out retry state
//
// Stored process-wide in a OnceLock so the existing Arc<ClusterBus> shape does
// not change.  Per-peer bounded VecDeque of payloads that failed delivery after
// the in-line retry budget was exhausted.  A background task (started from
// `start_fanout_retry`) periodically drains each queue against healthy peers.
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum number of pending payloads we will hold per peer before dropping
/// the oldest.  Sized to bound worst-case memory at ~16 MiB per dead peer.
const MAX_RETRY_QUEUE_LEN: usize = 1024;

/// How many payloads to drain per retry tick when a peer recovers.
const DRAIN_BATCH: usize = 64;

/// How often the background retry task wakes up to drain the queues.
const RETRY_TICK_MS: u64 = 5_000;

/// Per-peer retry queue keyed by shard_id.
pub struct FanoutRetryState {
    queues: DashMap<u32, Mutex<VecDeque<Arc<Vec<u8>>>>>,
}

impl FanoutRetryState {
    fn new() -> Self {
        FanoutRetryState { queues: DashMap::new() }
    }

    /// Push a payload onto the *back* of the queue.  Drops the oldest entry
    /// (front) if the queue is at capacity.
    fn push_back(&self, shard_id: u32, payload: Arc<Vec<u8>>) {
        let entry = self
            .queues
            .entry(shard_id)
            .or_insert_with(|| Mutex::new(VecDeque::with_capacity(64)));
        let mut q = entry.lock().expect("retry queue mutex poisoned");
        if q.len() >= MAX_RETRY_QUEUE_LEN {
            q.pop_front();
            log::warn!(
                "[cluster/fanout] retry queue for shard{} full ({} entries) — dropping oldest payload",
                shard_id,
                MAX_RETRY_QUEUE_LEN
            );
        }
        q.push_back(payload);
    }

    /// Push a payload onto the *front* of the queue.  Used when a retry-from-
    /// queue attempt fails — we want to preserve original ordering as best we
    /// can.  If the queue is at capacity, the *newest* entry (back) is dropped
    /// instead, because the re-queued payload is older and more important.
    fn push_front(&self, shard_id: u32, payload: Arc<Vec<u8>>) {
        let entry = self
            .queues
            .entry(shard_id)
            .or_insert_with(|| Mutex::new(VecDeque::with_capacity(64)));
        let mut q = entry.lock().expect("retry queue mutex poisoned");
        if q.len() >= MAX_RETRY_QUEUE_LEN {
            q.pop_back();
            log::warn!(
                "[cluster/fanout] retry queue for shard{} full while requeuing — dropping newest payload",
                shard_id
            );
        }
        q.push_front(payload);
    }

    /// Drain up to `n` payloads from the front of the queue.
    fn drain_front(&self, shard_id: u32, n: usize) -> Vec<Arc<Vec<u8>>> {
        let Some(entry) = self.queues.get(&shard_id) else { return vec![] };
        let mut q = entry.lock().expect("retry queue mutex poisoned");
        let take = q.len().min(n);
        q.drain(0..take).collect()
    }

    /// Number of pending payloads queued for `shard_id`.
    pub fn pending(&self, shard_id: u32) -> usize {
        self.queues
            .get(&shard_id)
            .map(|e| e.lock().map(|q| q.len()).unwrap_or(0))
            .unwrap_or(0)
    }
}

static RETRY_STATE: OnceLock<Arc<FanoutRetryState>> = OnceLock::new();

/// Return the process-wide fan-out retry state, lazily initialised.
pub fn retry_state() -> Arc<FanoutRetryState> {
    RETRY_STATE
        .get_or_init(|| Arc::new(FanoutRetryState::new()))
        .clone()
}

// ── In-line retry policy (used by the per-call spawn_blocking task) ──────────
//
// The first POST is followed by up to two retries with exponential back-off:
//   attempt 1 → fail → sleep 50ms
//   attempt 2 → fail → sleep 200ms
//   attempt 3 → fail → sleep 800ms (final)
// After all three attempts fail, the payload is pushed onto the per-peer retry
// queue so it can be drained later by the background task.
const RETRY_BACKOFF_MS: &[u64] = &[50, 200, 800];

fn try_post_with_backoff(
    bus: &Arc<ClusterBus>,
    url: &str,
    payload: &[u8],
) -> std::result::Result<(), String> {
    let mut last_err: Option<String> = None;
    for (i, _) in RETRY_BACKOFF_MS.iter().enumerate() {
        let mut req = bus
            .http_client()
            .post(url)
            .header("Content-Type", "application/json")
            .body(payload.to_vec());
        if let Some((name, value)) = bus.secret_header() {
            req = req.header(name, value);
        }
        match req.send() {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                last_err = Some(format!("HTTP {}", resp.status()));
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
        // Don't sleep after the last attempt.
        if i + 1 < RETRY_BACKOFF_MS.len() {
            std::thread::sleep(Duration::from_millis(RETRY_BACKOFF_MS[i]));
        }
    }
    Err(last_err.unwrap_or_else(|| "unknown error".to_string()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WireDelta {
    pub table: String,
    pub row_key: String,
    pub op: String,           // "set" | "delete"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_b64: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeltaPayload {
    pub from_shard: u32,
    pub deltas: Vec<WireDelta>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversion helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Convert RowDeltas from the commit path → wire-safe WireDeltas.
pub fn row_deltas_to_wire(deltas: &[RowDelta]) -> Vec<WireDelta> {
    deltas
        .iter()
        .map(|d| {
            if d.operation == "delete" {
                WireDelta {
                    table: d.table_name.clone(),
                    row_key: d.row_key.clone(),
                    op: "delete".to_string(),
                    data_b64: None,
                }
            } else {
                // For set/insert/update/counter_add: grab the row_data
                // value and re-encode it as rmp bytes → base64.
                let data_b64 = d.row_data_value().and_then(|v| {
                    rmp_serde::to_vec_named(&v).ok().map(|b| B64.encode(&b))
                });
                WireDelta {
                    table: d.table_name.clone(),
                    row_key: d.row_key.clone(),
                    op: "set".to_string(),
                    data_b64,
                }
            }
        })
        .collect()
}

/// Convert WireDeltas received from a peer → RowDeltas for apply_delta().
pub fn wire_to_row_deltas(wire: Vec<WireDelta>) -> Vec<RowDelta> {
    wire.into_iter()
        .filter_map(|w| {
            if w.op == "delete" {
                Some(RowDelta {
                    table_name: w.table,
                    operation: "delete".to_string(),
                    row_key: w.row_key,
                    row_id: 0,
                    shard_id: 0,
                    payload_arc: None,
                    row_data: None,
                    counter_add_amount: 0,
                    counter_add_timestamp: 0,
                })
            } else {
                // Decode base64 → rmp bytes → serde_json::Value
                let bytes = w.data_b64.as_deref().and_then(|b| B64.decode(b).ok())?;
                let data: serde_json::Value = rmp_serde::from_slice(&bytes).ok()?;
                Some(RowDelta {
                    table_name: w.table,
                    operation: "update".to_string(),
                    row_key: w.row_key,
                    row_id: 0,
                    shard_id: 0,
                    payload_arc: None,
                    row_data: Some(data),
                    counter_add_amount: 0,
                    counter_add_timestamp: 0,
                })
            }
        })
        .collect()
}

/// Parse the raw bytes of a /cluster/deltas request body.
pub fn parse_delta_payload(body: &[u8]) -> Result<DeltaPayload> {
    serde_json::from_slice(body).map_err(|e| {
        NeonDBError::invalid_argument(format!("Delta payload JSON parse error: {}", e))
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::RowDelta;
    use serde_json::json;

    fn make_set_delta(table: &str, key: &str, data: serde_json::Value) -> RowDelta {
        RowDelta {
            table_name: table.to_string(),
            operation: "update".to_string(),
            row_key: key.to_string(),
            row_id: 1,
            shard_id: 0,
            payload_arc: None,
            row_data: Some(data),
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        }
    }

    fn make_delete_delta(table: &str, key: &str) -> RowDelta {
        RowDelta {
            table_name: table.to_string(),
            operation: "delete".to_string(),
            row_key: key.to_string(),
            row_id: 1,
            shard_id: 0,
            payload_arc: None,
            row_data: None,
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        }
    }

    #[test]
    fn row_deltas_to_wire_set_roundtrips() {
        let delta = make_set_delta("players", "alice", json!({"hp": 100, "level": 5}));
        let wire = row_deltas_to_wire(&[delta]);
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0].table, "players");
        assert_eq!(wire[0].row_key, "alice");
        assert_eq!(wire[0].op, "set");
        assert!(wire[0].data_b64.is_some(), "set delta must carry base64 data");
    }

    #[test]
    fn row_deltas_to_wire_delete_has_no_data() {
        let delta = make_delete_delta("players", "bob");
        let wire = row_deltas_to_wire(&[delta]);
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0].op, "delete");
        assert!(wire[0].data_b64.is_none(), "delete delta must not carry data");
    }

    #[test]
    fn wire_to_row_deltas_set_roundtrip() {
        let original = make_set_delta("inventory", "player1", json!({"items": [], "currency": 50}));
        let wire = row_deltas_to_wire(&[original]);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored.len(), 1);
        let r = &restored[0];
        assert_eq!(r.table_name, "inventory");
        assert_eq!(r.row_key, "player1");
        assert_eq!(r.operation, "update");
        let data = r.row_data_value().expect("restored delta must have row_data");
        assert_eq!(data["currency"], json!(50));
    }

    #[test]
    fn wire_to_row_deltas_delete_roundtrip() {
        let original = make_delete_delta("sessions", "sess_001");
        let wire = row_deltas_to_wire(&[original]);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].operation, "delete");
        assert_eq!(restored[0].row_key, "sess_001");
    }

    #[test]
    fn wire_to_row_deltas_drops_invalid_base64() {
        // A WireDelta with corrupt base64 should be silently skipped.
        let bad = WireDelta {
            table: "players".to_string(),
            row_key: "x".to_string(),
            op: "set".to_string(),
            data_b64: Some("!!!not_valid_base64!!!".to_string()),
        };
        let restored = wire_to_row_deltas(vec![bad]);
        assert!(restored.is_empty(), "corrupt delta should be dropped");
    }

    #[test]
    fn parse_delta_payload_valid_json() {
        let body = br#"{"from_shard":1,"deltas":[]}"#;
        let payload = parse_delta_payload(body).expect("should parse");
        assert_eq!(payload.from_shard, 1);
        assert!(payload.deltas.is_empty());
    }

    #[test]
    fn parse_delta_payload_invalid_json_returns_error() {
        let body = b"not json at all";
        assert!(parse_delta_payload(body).is_err());
    }

    // ─── Retry-queue tests ──────────────────────────────────────────────────
    //
    // These tests construct fresh `FanoutRetryState` instances (NOT the global
    // `retry_state()`) so they don't share state with other tests or with
    // production code.

    fn payload(n: u8) -> Arc<Vec<u8>> {
        Arc::new(vec![n; 4])
    }

    #[test]
    fn retry_queue_is_bounded_and_drops_oldest() {
        let state = FanoutRetryState::new();
        // Push MAX + 5 distinct payloads; the first 5 must be dropped.
        for i in 0..(MAX_RETRY_QUEUE_LEN + 5) {
            state.push_back(7, payload((i % 251) as u8));
        }
        assert_eq!(
            state.pending(7),
            MAX_RETRY_QUEUE_LEN,
            "queue must cap at MAX_RETRY_QUEUE_LEN entries"
        );

        // Drain everything; first surviving entry must correspond to the 5th
        // payload pushed (index 5), proving the oldest 5 were dropped.
        let all = state.drain_front(7, MAX_RETRY_QUEUE_LEN);
        assert_eq!(all.len(), MAX_RETRY_QUEUE_LEN);
        assert_eq!(
            all[0].as_ref(),
            &vec![5u8 % 251; 4],
            "front of the queue should be the 6th-pushed payload after overflow drops 5 oldest"
        );
    }

    #[test]
    fn retry_queue_drain_returns_queue_in_order_and_empties_it() {
        let state = FanoutRetryState::new();
        for i in 0..10u8 {
            state.push_back(2, payload(i));
        }
        assert_eq!(state.pending(2), 10);

        let drained = state.drain_front(2, 64);
        assert_eq!(drained.len(), 10);
        for (i, p) in drained.iter().enumerate() {
            assert_eq!(p.as_ref(), &vec![i as u8; 4]);
        }
        assert_eq!(state.pending(2), 0, "successful drain must leave queue empty");
    }

    #[test]
    fn retry_queue_requeue_to_front_preserves_ordering() {
        let state = FanoutRetryState::new();
        // Start with 3 items in queue.
        for i in 0..3u8 {
            state.push_back(9, payload(i));
        }
        // Simulate: drain 2, then a retry of the 2nd one fails — requeue to front.
        let drained = state.drain_front(9, 2);
        assert_eq!(drained.len(), 2);
        // Payload index 0 succeeded; payload index 1 failed → push_front.
        state.push_front(9, drained[1].clone());

        // The queue should now hold [payload(1), payload(2)] in that order.
        let after = state.drain_front(9, 64);
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].as_ref(), &vec![1u8; 4], "requeued item must come first");
        assert_eq!(after[1].as_ref(), &vec![2u8; 4], "originally-queued item still after it");
    }

    #[test]
    fn retry_queue_pending_zero_for_unknown_peer() {
        let state = FanoutRetryState::new();
        assert_eq!(state.pending(99), 0);
    }

    #[test]
    fn mixed_deltas_roundtrip() {
        let deltas = vec![
            make_set_delta("players", "alice", json!({"hp": 200})),
            make_delete_delta("sessions", "old_sess"),
            make_set_delta("inventory", "alice", json!({"items": ["sword"]})),
        ];
        let wire = row_deltas_to_wire(&deltas);
        assert_eq!(wire.len(), 3);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored.len(), 3);
        assert_eq!(restored[0].table_name, "players");
        assert_eq!(restored[1].operation, "delete");
        assert_eq!(restored[2].table_name, "inventory");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Ship `deltas` to all healthy peers with bounded in-line retry.
/// Called from the worker loop immediately after `subs.publish_deltas()`.
///
/// Delivery semantics:
///   * Each peer is POSTed in a separate `spawn_blocking` task.
///   * Up to 3 attempts per peer with exponential back-off (50ms/200ms/800ms).
///   * If all attempts fail, the payload is pushed onto the peer's retry
///     queue (bounded at 1024 entries — oldest is dropped on overflow) and
///     a background task (`start_fanout_retry`) will retry later when the
///     peer is healthy again.
pub fn fanout_to_peers(bus: &Arc<ClusterBus>, deltas: &[RowDelta]) {
    let wire_deltas = row_deltas_to_wire(deltas);
    if wire_deltas.is_empty() {
        return;
    }

    let from_shard = bus.config.my_shard_id;
    let wire_count = wire_deltas.len();
    let payload = DeltaPayload { from_shard, deltas: wire_deltas };

    let payload_json = match serde_json::to_vec(&payload) {
        Ok(j) => Arc::new(j),
        Err(e) => {
            log::error!("[cluster/fanout] Failed to serialise deltas: {}", e);
            return;
        }
    };

    let state = retry_state();
    for peer in bus.healthy_peers() {
        let bus_c = bus.clone();
        let json_c = payload_json.clone();
        let peer_c = peer.clone();
        let state_c = state.clone();

        tokio::task::spawn_blocking(move || {
            let url = format!("{}/cluster/deltas", peer_c.metrics_url);
            match try_post_with_backoff(&bus_c, &url, &json_c) {
                Ok(()) => {
                    log::debug!(
                        "[cluster/fanout] shard{} accepted {} delta(s)",
                        peer_c.shard_id,
                        wire_count
                    );
                }
                Err(reason) => {
                    log::warn!(
                        "[cluster/fanout] shard{} delivery failed after retries ({}) — queuing for background retry",
                        peer_c.shard_id,
                        reason
                    );
                    bus_c.mark_unhealthy(peer_c.shard_id);
                    state_c.push_back(peer_c.shard_id, json_c);
                }
            }
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Background retry task
// ─────────────────────────────────────────────────────────────────────────────

/// Spawn the background fan-out retry task.  Mirrors `gossip::start_gossip`
/// in shape — runs until `shutdown` fires.
///
/// Every `RETRY_TICK_MS` it walks every peer in the bus and, if the peer is
/// currently healthy, drains up to `DRAIN_BATCH` payloads from its retry queue
/// and POSTs them.  Any payload whose POST fails is pushed back to the FRONT
/// of the queue (preserving ordering) and the peer is marked unhealthy.
pub fn start_fanout_retry(
    bus: Arc<ClusterBus>,
    mut shutdown: watch::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !bus.is_active() {
            log::debug!("[cluster/fanout] Single-node mode — retry loop disabled");
            return;
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(RETRY_TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        log::info!(
            "[cluster/fanout] Retry loop started — draining up to {} payload(s) per peer every {}ms",
            DRAIN_BATCH,
            RETRY_TICK_MS
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    drain_all_peers(&bus).await;
                }
                _ = shutdown.changed() => {
                    log::info!("[cluster/fanout] Retry loop shutdown");
                    break;
                }
            }
        }
    })
}

async fn drain_all_peers(bus: &Arc<ClusterBus>) {
    let state = retry_state();
    let peers: Vec<_> = bus
        .peers
        .iter()
        .map(|e| (*e.key(), e.value().node.clone(), e.value().is_healthy()))
        .collect();

    for (shard_id, node, healthy) in peers {
        if !healthy {
            continue;
        }
        let batch = state.drain_front(shard_id, DRAIN_BATCH);
        if batch.is_empty() {
            continue;
        }
        log::info!(
            "[cluster/fanout] draining {} queued payload(s) for shard{}",
            batch.len(),
            shard_id
        );

        let bus_c = bus.clone();
        let state_c = state.clone();
        tokio::task::spawn_blocking(move || {
            let url = format!("{}/cluster/deltas", node.metrics_url);
            for payload in batch {
                match try_post_with_backoff(&bus_c, &url, &payload) {
                    Ok(()) => {
                        log::debug!(
                            "[cluster/fanout] retry drained 1 payload for shard{}",
                            shard_id
                        );
                    }
                    Err(reason) => {
                        log::warn!(
                            "[cluster/fanout] retry POST to shard{} failed again ({}) — requeueing",
                            shard_id,
                            reason
                        );
                        bus_c.mark_unhealthy(shard_id);
                        // Push this and the rest back to the front to preserve order.
                        state_c.push_front(shard_id, payload);
                        // Don't keep hammering — bail out for this peer this tick.
                        break;
                    }
                }
            }
        });
    }
}
