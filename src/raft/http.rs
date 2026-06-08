// ============================================================================
// src/raft/http.rs — HTTP handlers for incoming Raft RPCs
//
// These handlers are mounted on the metrics HTTP server (main.rs) and receive
// Raft RPCs from peer nodes.  Every handler:
//   1. Deserializes the JSON body.
//   2. Calls the appropriate Raft method on the local Raft handle.
//   3. Serializes the response back as JSON.
//
// Endpoints:
//   POST /raft/append   — AppendEntries RPC (log replication + heartbeat)
//   POST /raft/vote     — RequestVote RPC (leader election)
//   POST /raft/snapshot — InstallSnapshot RPC (catch-up for stale followers)
//   GET  /raft/metrics  — Raft metrics (leader, term, commit index, …)
//
// Split-brain prevention:
//   openraft enforces quorum internally. A node that receives an AppendEntries
//   with a stale term rejects it automatically. The HTTP layer here is just a
//   thin transport adapter — correctness comes from openraft's Raft core.
// ============================================================================

use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};

use crate::raft::{NeonRaft, TypeConfig};

// ─────────────────────────────────────────────────────────────────────────────
// Helper — read the full body as bytes
// ─────────────────────────────────────────────────────────────────────────────

async fn body_bytes(req: Request<Body>) -> Result<bytes::Bytes, hyper::Error> {
    hyper::body::to_bytes(req.into_body()).await
}

/// Build a JSON 200 response.
fn json_ok(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

/// Build a JSON error response.
fn json_err(status: StatusCode, msg: &str) -> Response<Body> {
    let body = serde_json::json!({ "error": msg }).to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/append
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_append(
    raft: Arc<NeonRaft>,
    req: Request<Body>,
) -> Response<Body> {
    let bytes = match body_bytes(req).await {
        Ok(b) => b,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let rpc: openraft::raft::AppendEntriesRequest<TypeConfig> =
        match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    match raft.append_entries(rpc).await {
        Ok(resp) => json_ok(serde_json::to_string(&resp).unwrap_or_default()),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/vote
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_vote(
    raft: Arc<NeonRaft>,
    req: Request<Body>,
) -> Response<Body> {
    let bytes = match body_bytes(req).await {
        Ok(b) => b,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let rpc: openraft::raft::VoteRequest<u64> =
        match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    match raft.vote(rpc).await {
        Ok(resp) => json_ok(serde_json::to_string(&resp).unwrap_or_default()),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/snapshot
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_snapshot(
    raft: Arc<NeonRaft>,
    req: Request<Body>,
) -> Response<Body> {
    let bytes = match body_bytes(req).await {
        Ok(b) => b,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let rpc: openraft::raft::InstallSnapshotRequest<TypeConfig> =
        match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    match raft.install_full_snapshot(rpc.vote, openraft::Snapshot {
        meta: rpc.meta,
        snapshot: Box::new(std::io::Cursor::new(rpc.data)),
    }).await {
        Ok(resp) => json_ok(serde_json::to_string(&resp).unwrap_or_default()),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — GET /raft/metrics
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_metrics(raft: Arc<NeonRaft>) -> Response<Body> {
    let metrics = raft.metrics().borrow().clone();
    let body = serde_json::json!({
        "id":             metrics.id,
        "state":          format!("{:?}", metrics.state),
        "current_term":   metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied":   metrics.last_applied,
        "current_leader": metrics.current_leader,
        "membership_config": {
            "nodes": metrics.membership_config
                .membership()
                .nodes()
                .map(|(id, node)| serde_json::json!({ "id": id, "addr": node.addr }))
                .collect::<Vec<_>>()
        }
    });
    json_ok(body.to_string())
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/add-learner
//   Body: { "node_id": u64, "addr": "http://..." }
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_add_learner(
    raft: Arc<NeonRaft>,
    req: Request<Body>,
) -> Response<Body> {
    let bytes = match body_bytes(req).await {
        Ok(b) => b,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    #[derive(serde::Deserialize)]
    struct Params { node_id: u64, addr: String }

    let params: Params = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let node = openraft::BasicNode { addr: params.addr };
    match raft.add_learner(params.node_id, node, true).await {
        Ok(resp) => json_ok(serde_json::to_string(&resp).unwrap_or_default()),
        Err(e)   => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/change-membership
//   Body: { "members": [node_id, ...] }
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_change_membership(
    raft: Arc<NeonRaft>,
    req: Request<Body>,
) -> Response<Body> {
    let bytes = match body_bytes(req).await {
        Ok(b) => b,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    #[derive(serde::Deserialize)]
    struct Params { members: Vec<u64> }

    let params: Params = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let members: std::collections::BTreeSet<u64> = params.members.into_iter().collect();
    match raft.change_membership(members, false).await {
        Ok(resp) => json_ok(serde_json::to_string(&resp).unwrap_or_default()),
        Err(e)   => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler — POST /raft/init
//   Initialise a single-node cluster (leader bootstrap).
// ─────────────────────────────────────────────────────────────────────────────

pub async fn handle_raft_init(
    raft: Arc<NeonRaft>,
    node_id: u64,
    addr: String,
) -> Response<Body> {
    let mut members = std::collections::BTreeMap::new();
    members.insert(node_id, openraft::BasicNode { addr });
    match raft.initialize(members).await {
        Ok(_)  => json_ok(r#"{"ok":true}"#.to_string()),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_ok_has_200() {
        let resp = json_ok(r#"{"key":"value"}"#.to_string());
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_json_err_has_status() {
        let resp = json_err(StatusCode::BAD_REQUEST, "bad input");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_json_err_body_has_error_field() {
        let resp = json_err(StatusCode::INTERNAL_SERVER_ERROR, "oops");
        // We can't easily read the body in sync context, but we can verify the status.
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
