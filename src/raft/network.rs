// ============================================================================
// src/raft/network.rs — Raft network layer (HTTP transport)
//
// Implements openraft's RaftNetwork + RaftNetworkFactory traits using
// async reqwest HTTP calls.
//
// Each RPC (AppendEntries, InstallSnapshot, Vote) is a POST to the peer's
// metrics server at:
//   POST http://<peer_addr>/raft/append
//   POST http://<peer_addr>/raft/snapshot
//   POST http://<peer_addr>/raft/vote
//
// The receiving side is handled by src/raft/http.rs which is wired into
// the hyper metrics server in main.rs.
//
// Security: all Raft RPC requests carry the cluster secret header
//   x-neondb-cluster-secret: <NEONDB_CLUSTER_SECRET>
// ============================================================================

use std::sync::Arc;

use openraft::{
    error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse},
    BasicNode, Snapshot, Vote,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::raft::TypeConfig;

// ─────────────────────────────────────────────────────────────────────────────
// NeonNetworkFactory — creates per-target network connections
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct NeonNetworkFactory {
    client: Arc<reqwest::Client>,
    /// Optional cluster secret for header injection.
    cluster_secret: Option<String>,
}

impl NeonNetworkFactory {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build reqwest client for Raft network");
        Self {
            client: Arc::new(client),
            cluster_secret: std::env::var("NEONDB_CLUSTER_SECRET").ok(),
        }
    }
}

impl Default for NeonNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for NeonNetworkFactory {
    type Network = NeonNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> NeonNetwork {
        NeonNetwork {
            target,
            target_addr: node.addr.clone(),
            client: self.client.clone(),
            cluster_secret: self.cluster_secret.clone(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NeonNetwork — per-connection RPC sender
// ─────────────────────────────────────────────────────────────────────────────

pub struct NeonNetwork {
    /// The target node's ID.
    pub target: u64,
    /// HTTP address of the target's metrics server (e.g. "http://10.0.0.2:3001").
    pub target_addr: String,
    pub client: Arc<reqwest::Client>,
    pub cluster_secret: Option<String>,
}

/// A simple newtype that wraps an error message string and implements `std::error::Error`.
#[derive(Debug)]
struct NetworkError(String);
impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for NetworkError {}

impl NeonNetwork {
    /// Send a JSON POST to `<target_addr>/<path>` and decode the response.
    async fn send<Req, Resp>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp, NetworkError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let url = format!("{}/{}", self.target_addr.trim_end_matches('/'), path);
        let mut builder = self.client.post(&url).json(req);
        if let Some(secret) = &self.cluster_secret {
            builder = builder.header("x-neondb-cluster-secret", secret);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| NetworkError(e.to_string()))?
            .json::<Resp>()
            .await
            .map_err(|e| NetworkError(e.to_string()))?;
        Ok(resp)
    }

    /// Convert a NetworkError to an openraft `Unreachable` RPCError.
    fn to_unreachable(e: NetworkError) -> RPCError<u64, BasicNode, RaftError<u64>> {
        RPCError::Unreachable(Unreachable::new(&e))
    }

    /// Convert a NetworkError to a snapshot RPCError.
    fn to_unreachable_snap(e: NetworkError) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
        RPCError::Unreachable(Unreachable::new(&e))
    }
}

impl RaftNetwork<TypeConfig> for NeonNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send::<_, AppendEntriesResponse<u64>>("raft/append", &rpc)
            .await
            .map_err(Self::to_unreachable)
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        self.send::<_, openraft::raft::InstallSnapshotResponse<u64>>("raft/snapshot", &rpc)
            .await
            .map_err(Self::to_unreachable_snap)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send::<_, VoteResponse<u64>>("raft/vote", &rpc)
            .await
            .map_err(Self::to_unreachable)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl futures::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        use openraft::network::snapshot_transport::{Chunked, SnapshotTransport};
        let resp = Chunked::send_snapshot(self, vote, snapshot, cancel, option).await?;
        Ok(resp)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_creates_network_instance() {
        // Just verify NeonNetworkFactory::new() doesn't panic.
        let factory = NeonNetworkFactory::new();
        // The client is shared via Arc — verify it's reachable.
        assert!(Arc::strong_count(&factory.client) >= 1);
    }

    #[tokio::test]
    async fn test_new_client_sets_target_addr() {
        let mut factory = NeonNetworkFactory::new();
        let node = BasicNode { addr: "http://127.0.0.1:3001".to_string() };
        let net = factory.new_client(1, &node).await;
        assert_eq!(net.target, 1);
        assert_eq!(net.target_addr, "http://127.0.0.1:3001");
    }

    #[test]
    fn test_url_construction() {
        let net = NeonNetwork {
            target: 2,
            target_addr: "http://10.0.0.2:3001".to_string(),
            client: Arc::new(reqwest::Client::new()),
            cluster_secret: None,
        };
        // Verify the trailing-slash trimming logic.
        let url = format!("{}/{}", net.target_addr.trim_end_matches('/'), "raft/vote");
        assert_eq!(url, "http://10.0.0.2:3001/raft/vote");
    }

    #[test]
    fn test_trailing_slash_trimmed() {
        let addr = "http://10.0.0.2:3001/";
        let url = format!("{}/{}", addr.trim_end_matches('/'), "raft/append");
        assert_eq!(url, "http://10.0.0.2:3001/raft/append");
    }
}
