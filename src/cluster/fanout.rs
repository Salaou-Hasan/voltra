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

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::{Deserialize, Serialize};

use crate::table::RowDelta;
use crate::error::{NeonDBError, Result};
use super::ClusterBus;

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

/// Fire-and-forget: ship `deltas` to all healthy peers.
/// Called from the worker loop immediately after `subs.publish_deltas()`.
pub fn fanout_to_peers(bus: &Arc<ClusterBus>, deltas: &[RowDelta]) {
    let wire_deltas = row_deltas_to_wire(deltas);
    if wire_deltas.is_empty() {
        return;
    }

    let from_shard = bus.config.my_shard_id;
    let wire_count = wire_deltas.len();
    let payload = DeltaPayload { from_shard, deltas: wire_deltas };

    let payload_json = match serde_json::to_vec(&payload) {
        Ok(j) => j,
        Err(e) => {
            log::error!("[cluster/fanout] Failed to serialise deltas: {}", e);
            return;
        }
    };

    for peer in bus.healthy_peers() {
        let bus_c = bus.clone();
        let json_c = payload_json.clone();
        let peer_c = peer.clone();

        tokio::task::spawn_blocking(move || {
            let url = format!("{}/cluster/deltas", peer_c.metrics_url);
            let mut req = bus_c
                .http_client()
                .post(&url)
                .header("Content-Type", "application/json")
                .body(json_c);

            if let Some((name, value)) = bus_c.secret_header() {
                req = req.header(name, value);
            }

            match req.send() {
                Ok(resp) if resp.status().is_success() => {
                    log::debug!(
                        "[cluster/fanout] shard{} accepted {} delta(s)",
                        peer_c.shard_id,
                        wire_count
                    );
                }
                Ok(resp) => {
                    log::warn!(
                        "[cluster/fanout] shard{} rejected deltas — HTTP {}",
                        peer_c.shard_id,
                        resp.status()
                    );
                    bus_c.mark_unhealthy(peer_c.shard_id);
                }
                Err(e) => {
                    log::warn!(
                        "[cluster/fanout] shard{} unreachable during fanout — {}",
                        peer_c.shard_id,
                        e
                    );
                    bus_c.mark_unhealthy(peer_c.shard_id);
                }
            }
        });
    }
}
