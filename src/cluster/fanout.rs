// ============================================================================
// src/cluster/fanout.rs — Delta fan-out to peer nodes
//
// After a reducer commits on this node, serialise the RowDeltas and POST them
// to all healthy peer nodes so their subscribers see the change.
//
// Wire format (JSON):
//   { "from_shard": 0, "deltas": [{ "table", "row_key", "op", "data_b64"? }] }
//
// Delivery is fire-and-forget on a blocking thread per peer.
// Failed deliveries are queued for background retry.
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
// Retry state
// ─────────────────────────────────────────────────────────────────────────────

const MAX_RETRY_QUEUE_LEN: usize = 1024;
const DRAIN_BATCH: usize = 64;
const RETRY_TICK_MS: u64 = 5_000;
const RETRY_BACKOFF_MS: &[u64] = &[50, 200, 800];

pub struct FanoutRetryState {
    queues: DashMap<u32, Mutex<VecDeque<Arc<Vec<u8>>>>>,
}

impl FanoutRetryState {
    fn new() -> Self {
        FanoutRetryState { queues: DashMap::new() }
    }

    fn push_back(&self, shard_id: u32, payload: Arc<Vec<u8>>) {
        let entry = self.queues.entry(shard_id)
            .or_insert_with(|| Mutex::new(VecDeque::with_capacity(64)));
        let mut q = entry.lock().expect("retry queue poisoned");
        if q.len() >= MAX_RETRY_QUEUE_LEN {
            q.pop_front();
        }
        q.push_back(payload);
    }

    fn push_front(&self, shard_id: u32, payload: Arc<Vec<u8>>) {
        let entry = self.queues.entry(shard_id)
            .or_insert_with(|| Mutex::new(VecDeque::with_capacity(64)));
        let mut q = entry.lock().expect("retry queue poisoned");
        if q.len() >= MAX_RETRY_QUEUE_LEN {
            q.pop_back();
        }
        q.push_front(payload);
    }

    fn drain_front(&self, shard_id: u32, n: usize) -> Vec<Arc<Vec<u8>>> {
        let Some(entry) = self.queues.get(&shard_id) else { return vec![] };
        let mut q = entry.lock().expect("retry queue poisoned");
        let take = q.len().min(n);
        q.drain(0..take).collect()
    }

    pub fn pending(&self, shard_id: u32) -> usize {
        self.queues.get(&shard_id)
            .map(|e| e.lock().map(|q| q.len()).unwrap_or(0))
            .unwrap_or(0)
    }
}

static RETRY_STATE: OnceLock<Arc<FanoutRetryState>> = OnceLock::new();

pub fn retry_state() -> Arc<FanoutRetryState> {
    RETRY_STATE.get_or_init(|| Arc::new(FanoutRetryState::new())).clone()
}

fn try_post_with_backoff(bus: &Arc<ClusterBus>, url: &str, payload: &[u8]) -> std::result::Result<(), String> {
    let mut last_err: Option<String> = None;
    for (i, _) in RETRY_BACKOFF_MS.iter().enumerate() {
        let mut req = bus.http_client()
            .post(url)
            .header("Content-Type", "application/json")
            .body(payload.to_vec());
        if let Some((name, value)) = bus.secret_header() {
            req = req.header(name, value);
        }
        match req.send() {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => { last_err = Some(format!("HTTP {}", resp.status())); }
            Err(e) => { last_err = Some(e.to_string()); }
        }
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
    pub op: String,
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

pub fn row_deltas_to_wire(deltas: &[RowDelta]) -> Vec<WireDelta> {
    deltas.iter().map(|d| {
        if d.operation == "delete" {
            WireDelta { table: d.table_name.clone(), row_key: d.row_key.clone(), op: "delete".to_string(), data_b64: None }
        } else {
            let data_b64 = d.row_data_value().and_then(|v| {
                rmp_serde::to_vec_named(&v).ok().map(|b| B64.encode(&b))
            });
            WireDelta { table: d.table_name.clone(), row_key: d.row_key.clone(), op: "set".to_string(), data_b64 }
        }
    }).collect()
}

pub fn wire_to_row_deltas(wire: Vec<WireDelta>) -> Vec<RowDelta> {
    wire.into_iter().filter_map(|w| {
        if w.op == "delete" {
            Some(RowDelta {
                table_name: w.table, operation: "delete".to_string(), row_key: w.row_key,
                row_id: 0, shard_id: 0, payload_arc: None, row_data: None,
                counter_add_amount: 0, counter_add_timestamp: 0,
            })
        } else {
            let bytes = w.data_b64.as_deref().and_then(|b| B64.decode(b).ok())?;
            let data: serde_json::Value = rmp_serde::from_slice(&bytes).ok()?;
            Some(RowDelta {
                table_name: w.table, operation: "update".to_string(), row_key: w.row_key,
                row_id: 0, shard_id: 0, payload_arc: None, row_data: Some(data),
                counter_add_amount: 0, counter_add_timestamp: 0,
            })
        }
    }).collect()
}

pub fn parse_delta_payload(body: &[u8]) -> Result<DeltaPayload> {
    serde_json::from_slice(body).map_err(|e| {
        NeonDBError::invalid_argument(format!("Delta payload JSON parse error: {}", e))
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out entry point
// ─────────────────────────────────────────────────────────────────────────────

pub fn fanout_to_peers(bus: &Arc<ClusterBus>, deltas: &[RowDelta]) {
    let wire_deltas = row_deltas_to_wire(deltas);
    if wire_deltas.is_empty() { return; }

    let from_shard = bus.config.my_shard_id;
    let wire_count = wire_deltas.len();
    let payload = DeltaPayload { from_shard, deltas: wire_deltas };

    let payload_json = match serde_json::to_vec(&payload) {
        Ok(j) => Arc::new(j),
        Err(e) => { log::error!("[cluster/fanout] Serialise deltas failed: {}", e); return; }
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
                    log::debug!("[cluster/fanout] shard{} accepted {} delta(s)", peer_c.shard_id, wire_count);
                }
                Err(reason) => {
                    log::warn!(
                        "[cluster/fanout] shard{} delivery failed ({}) — queuing for retry",
                        peer_c.shard_id, reason
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

pub fn start_fanout_retry(bus: Arc<ClusterBus>, mut shutdown: watch::Receiver<()>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !bus.is_active() {
            log::debug!("[cluster/fanout] Single-node mode — retry loop disabled");
            return;
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(RETRY_TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        log::info!("[cluster/fanout] Retry loop started");

        loop {
            tokio::select! {
                _ = ticker.tick() => { drain_all_peers(&bus).await; }
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
    let peers: Vec<_> = bus.peers.iter()
        .map(|e| (*e.key(), e.value().node.clone(), e.value().is_healthy()))
        .collect();

    for (shard_id, node, healthy) in peers {
        if !healthy { continue; }
        let batch = state.drain_front(shard_id, DRAIN_BATCH);
        if batch.is_empty() { continue; }

        log::info!("[cluster/fanout] draining {} queued payload(s) for shard{}", batch.len(), shard_id);

        let bus_c = bus.clone();
        let state_c = state.clone();
        tokio::task::spawn_blocking(move || {
            let url = format!("{}/cluster/deltas", node.metrics_url);
            for payload in batch {
                match try_post_with_backoff(&bus_c, &url, &payload) {
                    Ok(()) => {
                        log::debug!("[cluster/fanout] retry drained 1 payload for shard{}", shard_id);
                    }
                    Err(reason) => {
                        log::warn!("[cluster/fanout] retry POST to shard{} failed ({}) — requeueing", shard_id, reason);
                        bus_c.mark_unhealthy(shard_id);
                        state_c.push_front(shard_id, payload);
                        break;
                    }
                }
            }
        });
    }
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
            table_name: table.to_string(), operation: "update".to_string(), row_key: key.to_string(),
            row_id: 1, shard_id: 0, payload_arc: None, row_data: Some(data),
            counter_add_amount: 0, counter_add_timestamp: 0,
        }
    }

    fn make_delete_delta(table: &str, key: &str) -> RowDelta {
        RowDelta {
            table_name: table.to_string(), operation: "delete".to_string(), row_key: key.to_string(),
            row_id: 1, shard_id: 0, payload_arc: None, row_data: None,
            counter_add_amount: 0, counter_add_timestamp: 0,
        }
    }

    #[test]
    fn row_deltas_to_wire_set_roundtrips() {
        let delta = make_set_delta("players", "alice", json!({"hp": 100}));
        let wire = row_deltas_to_wire(&[delta]);
        assert_eq!(wire[0].op, "set");
        assert!(wire[0].data_b64.is_some());
    }

    #[test]
    fn row_deltas_to_wire_delete_has_no_data() {
        let delta = make_delete_delta("players", "bob");
        let wire = row_deltas_to_wire(&[delta]);
        assert_eq!(wire[0].op, "delete");
        assert!(wire[0].data_b64.is_none());
    }

    #[test]
    fn wire_to_row_deltas_set_roundtrip() {
        let original = make_set_delta("inventory", "p1", json!({"currency": 50}));
        let wire = row_deltas_to_wire(&[original]);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored[0].table_name, "inventory");
        assert_eq!(restored[0].row_data_value().unwrap()["currency"], json!(50));
    }

    #[test]
    fn wire_to_row_deltas_delete_roundtrip() {
        let original = make_delete_delta("sessions", "sess_001");
        let wire = row_deltas_to_wire(&[original]);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored[0].operation, "delete");
    }

    #[test]
    fn wire_to_row_deltas_drops_invalid_base64() {
        let bad = WireDelta {
            table: "players".to_string(), row_key: "x".to_string(),
            op: "set".to_string(), data_b64: Some("!!!not_valid_base64!!!".to_string()),
        };
        let restored = wire_to_row_deltas(vec![bad]);
        assert!(restored.is_empty());
    }

    #[test]
    fn parse_delta_payload_valid_json() {
        let body = br#"{"from_shard":1,"deltas":[]}"#;
        let payload = parse_delta_payload(body).expect("should parse");
        assert_eq!(payload.from_shard, 1);
    }

    #[test]
    fn parse_delta_payload_invalid_json_returns_error() {
        assert!(parse_delta_payload(b"not json").is_err());
    }

    #[test]
    fn mixed_deltas_roundtrip() {
        let deltas = vec![
            make_set_delta("players", "alice", json!({"hp": 200})),
            make_delete_delta("sessions", "old"),
        ];
        let wire = row_deltas_to_wire(&deltas);
        let restored = wire_to_row_deltas(wire);
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[1].operation, "delete");
    }
}
