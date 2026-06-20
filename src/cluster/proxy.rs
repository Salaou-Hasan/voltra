// ============================================================================
// src/cluster/proxy.rs — Proxy reducer calls to the shard owner
//
// Wire format for POST /cluster/call (JSON):
//
//   Request:  { "reducer_name", "args_b64", "caller_id", "caller_role", "target_shard_id?" }
//   Response: { "ok": true,  "result_b64": "<base64>" }
//           | { "ok": false, "error": "<message>" }
// ============================================================================

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::{Deserialize, Serialize};

use crate::error::{NeonDBError, Result};
use super::{ClusterBus, NodeInfo};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProxyCallRequest {
    pub reducer_name: String,
    pub args_b64: String,
    pub caller_id: String,
    pub caller_role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_shard_id: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProxyCallResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_b64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}


pub fn proxy_call(
    bus: &Arc<ClusterBus>,
    peer: &NodeInfo,
    reducer_name: &str,
    args: &[u8],
    caller_id: &str,
    caller_role: &str,
) -> Result<Vec<u8>> {
    let url = format!("{}/cluster/call", peer.metrics_url);

    let req_body = ProxyCallRequest {
        reducer_name: reducer_name.to_string(),
        args_b64: B64.encode(args),
        caller_id: caller_id.to_string(),
        caller_role: caller_role.to_string(),
        target_shard_id: Some(peer.shard_id),
    };

    let body_json = serde_json::to_vec(&req_body).map_err(|e| {
        NeonDBError::internal(format!("[cluster/proxy] Serialise error: {}", e))
    })?;

    let mut req = bus.http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body_json);

    if let Some((name, value)) = bus.secret_header() {
        req = req.header(name, value);
    }

    let resp = req.send().map_err(|e| {
        NeonDBError::network_error(format!("[cluster/proxy] shard{} unreachable: {}", peer.shard_id, e))
    })?;

    if !resp.status().is_success() {
        return Err(NeonDBError::network_error(format!(
            "[cluster/proxy] shard{} returned HTTP {}", peer.shard_id, resp.status()
        )));
    }

    let resp_body: ProxyCallResponse = resp.json().map_err(|e| {
        NeonDBError::internal(format!("[cluster/proxy] Deserialise response: {}", e))
    })?;

    if !resp_body.ok {
        return Err(NeonDBError::internal(
            resp_body.error.unwrap_or_else(|| "Unknown proxy error".to_string())
        ));
    }

    let result_b64 = resp_body.result_b64.unwrap_or_default();
    B64.decode(&result_b64).map_err(|e| {
        NeonDBError::internal(format!("[cluster/proxy] Base64 decode result: {}", e))
    })
}
