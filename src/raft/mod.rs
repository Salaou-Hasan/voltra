// ============================================================================
// src/raft/mod.rs — Raft consensus layer for NeonDB
//
// Architecture:
//   NeonDB uses openraft 0.9 for distributed consensus. Every committed
//   reducer result (as a set of RowDeltas) flows through Raft before being
//   applied to the TableStore.
//
//   Write path (leader):
//     1. Reducer executes  → produces Vec<RowDelta>
//     2. Leader calls Raft::client_write(RaftRequest { deltas, … })
//     3. openraft replicates the entry to a quorum of followers
//     4. On commit, NeonStateMachine::apply() → TableStore.apply_delta_batch()
//     5. Subscription fan-out notifies connected clients
//
//   Write path (follower receives client write):
//     1. Follower detects it is not leader via Raft::current_leader()
//     2. Follower HTTP-proxies the call to the current leader
//     3. Leader executes → replicates → commits
//
//   Failure modes handled:
//     - Leader crash      : follower with highest log wins new election (quorum)
//     - Network partition : split-brain prevented — minority can't commit
//     - Follower lag      : replication catches up; snapshot transfer for stale nodes
//     - Node restart      : vote persisted to disk; log replayed from storage
// ============================================================================

pub mod http;
pub mod network;
pub mod state_machine;
pub mod storage;

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// Application data types flowing through the Raft log
// ─────────────────────────────────────────────────────────────────────────────

/// A committed reducer result — the payload that gets replicated to all nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftRequest {
    /// Reducer that produced this delta set.
    pub reducer_name: String,
    /// Serialised call arguments (MessagePack).
    pub args: Vec<u8>,
    /// The committed row deltas to apply to every node's TableStore.
    pub deltas: Vec<crate::table::RowDelta>,
    /// Wall-clock timestamp (ms since epoch) when the reducer ran on the leader.
    pub timestamp_ms: u64,
}

/// Response returned after applying a [`RaftRequest`] to the state machine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftResponse {
    /// Number of row deltas applied.
    pub applied: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// Raft type configuration
//   Ties all openraft generic parameters to NeonDB's concrete types.
// ─────────────────────────────────────────────────────────────────────────────

openraft::declare_raft_types!(
    /// NeonDB's openraft type configuration.
    pub TypeConfig:
        D            = RaftRequest,
        R            = RaftResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

// ─────────────────────────────────────────────────────────────────────────────
// Convenience aliases
// ─────────────────────────────────────────────────────────────────────────────

/// The concrete Raft handle used throughout NeonDB.
pub type NeonRaft = openraft::Raft<TypeConfig>;

/// Node ID — shard_id as u64 (openraft requires u64 NodeId).
pub type NodeId = u64;

/// Node metadata (address) stored alongside each node in the membership config.
pub type NodeAddr = BasicNode;

// ─────────────────────────────────────────────────────────────────────────────
// Raft configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Build an openraft [`Config`] with production-appropriate timeouts and
/// snapshot policy.
///
/// Timeouts:
/// - Heartbeat every 250 ms  → leader sends heartbeat 4× per second.
/// - Election timeout 750–1500 ms → reduces split-vote probability.
/// - Up to 300 log entries per AppendEntries RPC for throughput.
///
/// Snapshot: trigger after 10 000 entries since last snapshot → roughly
/// every 2 seconds at 5k writes/sec, keeping crash-recovery replay short.
pub fn build_raft_config() -> std::sync::Arc<openraft::Config> {
    let config = openraft::Config {
        heartbeat_interval:        250,
        election_timeout_min:      750,
        election_timeout_max:      1500,
        max_payload_entries:       300,
        replication_lag_threshold: 1000,
        snapshot_policy:           openraft::SnapshotPolicy::LogsSinceLast(10_000),
        ..Default::default()
    };
    std::sync::Arc::new(config.validate().expect("invalid Raft config"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_request_roundtrip() {
        let req = RaftRequest {
            reducer_name: "increment".to_string(),
            args:         vec![1, 2, 3],
            deltas:       vec![],
            timestamp_ms: 1_234_567_890,
        };
        let json  = serde_json::to_string(&req).unwrap();
        let back: RaftRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.reducer_name, "increment");
        assert_eq!(back.args, vec![1u8, 2, 3]);
        assert_eq!(back.timestamp_ms, 1_234_567_890);
        assert!(back.deltas.is_empty());
    }

    #[test]
    fn test_raft_response_roundtrip() {
        let resp  = RaftResponse { applied: 42 };
        let json  = serde_json::to_string(&resp).unwrap();
        let back: RaftResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.applied, 42);
    }

    #[test]
    fn test_build_raft_config_validates() {
        let cfg = build_raft_config();
        assert!(cfg.heartbeat_interval > 0);
        assert!(cfg.election_timeout_max > cfg.election_timeout_min);
        assert_eq!(cfg.max_payload_entries, 300);
    }

    #[test]
    fn test_raft_request_empty_args_and_deltas() {
        let req = RaftRequest {
            reducer_name: "noop".to_string(),
            args:         vec![],
            deltas:       vec![],
            timestamp_ms: 0,
        };
        assert!(req.args.is_empty());
        assert!(req.deltas.is_empty());
    }

    #[test]
    fn test_node_id_is_u64() {
        let id: NodeId = u64::MAX;
        assert_eq!(id, u64::MAX);
    }
}
