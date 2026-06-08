// ============================================================================
// tests/raft_consensus_test.rs — P0 Raft consensus integration tests
//
// Tests run entirely in-process using a channel-based "fake" network that
// forwards RPCs directly to other Raft node instances.  No real HTTP, no
// subprocesses, no ports — pure in-memory consensus.
//
// Coverage:
//   1. Single-node bootstrap → leader election → client_write → apply
//   2. Three-node cluster — add_learner + change_membership
//   3. Log replication — write on leader, verify all nodes see it
//   4. Failover — kill leader, verify new leader elected
//   5. Split-brain prevention — minority partition cannot commit
// ============================================================================

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dashmap::DashMap;
use openraft::{
    BasicNode,
    error::{Fatal, InstallSnapshotError, RaftError, RPCError, RemoteError, StreamingError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    Snapshot, Vote,
};

use neondb::{
    raft::{
        NeonRaft, RaftRequest, TypeConfig,
        build_raft_config,
        state_machine::NeonStateMachine,
        storage::MemLogStore,
    },
    subscriptions::SubscriptionManager,
    table::{RowDelta, TableStore},
};

// ─────────────────────────────────────────────────────────────────────────────
// In-process network (no HTTP, no I/O)
// ─────────────────────────────────────────────────────────────────────────────

/// Shared registry that maps NodeId → live Raft instance.
type NodeRegistry = Arc<DashMap<u64, Arc<NeonRaft>>>;

/// Factory that creates an `InProcNetwork` per target node.
#[derive(Clone)]
struct InProcFactory {
    nodes: NodeRegistry,
}

impl RaftNetworkFactory<TypeConfig> for InProcFactory {
    type Network = InProcNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> InProcNetwork {
        InProcNetwork {
            target,
            nodes: self.nodes.clone(),
        }
    }
}

/// Per-connection in-process forwarder.
struct InProcNetwork {
    target: u64,
    nodes: NodeRegistry,
}

impl InProcNetwork {
    fn get_target(&self) -> Option<Arc<NeonRaft>> {
        self.nodes.get(&self.target).map(|r| r.value().clone())
    }
}

impl RaftNetwork<TypeConfig> for InProcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self
            .get_target()
            .ok_or_else(|| RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::new(std::io::ErrorKind::NotFound, "node not found"),
            )))?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError {
                target:      self.target,
                target_node: None,
                source:      e,
            }))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self
            .get_target()
            .ok_or_else(|| RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::new(std::io::ErrorKind::NotFound, "node not found"),
            )))?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError {
                target:      self.target,
                target_node: None,
                source:      e,
            }))
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let raft = self
            .get_target()
            .ok_or_else(|| RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::new(std::io::ErrorKind::NotFound, "node not found"),
            )))?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError {
                target:      self.target,
                target_node: None,
                source:      e,
            }))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl futures::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        use openraft::network::snapshot_transport::{Chunked, SnapshotTransport};
        Chunked::send_snapshot(self, vote, snapshot, cancel, option).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

async fn make_node(
    id: u64,
    tables: Arc<TableStore>,
    subs: Arc<SubscriptionManager>,
    nodes: NodeRegistry,
) -> Arc<NeonRaft> {
    let config   = build_raft_config();
    let log      = MemLogStore::new(None);
    let sm       = NeonStateMachine::new(tables, subs);
    let factory  = InProcFactory { nodes };
    Arc::new(
        NeonRaft::new(id, config, factory, log, sm)
            .await
            .expect("Raft::new failed"),
    )
}

fn node_addr(id: u64) -> BasicNode {
    BasicNode { addr: format!("inproc://node{}", id) }
}

fn make_delta(table: &str, key: &str, value: serde_json::Value) -> RowDelta {
    RowDelta {
        table_name:            table.to_string(),
        row_key:               key.to_string(),
        operation:             "insert".to_string(),
        row_data:              Some(value),
        row_id:                0,
        shard_id:              0,
        payload_arc:           None,
        counter_add_amount:    0,
        counter_add_timestamp: 0,
    }
}

fn make_request(table: &str, key: &str, value: serde_json::Value) -> RaftRequest {
    RaftRequest {
        reducer_name: "test".to_string(),
        args:         vec![],
        deltas:       vec![make_delta(table, key, value)],
        timestamp_ms: 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Single-node bootstrap, leader election, client_write, apply
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_single_node_leader_election_and_write() {
    let tables  = Arc::new(TableStore::new());
    let subs    = Arc::new(SubscriptionManager::new());
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    let raft = make_node(1, tables.clone(), subs, nodes.clone()).await;
    nodes.insert(1, raft.clone());

    // Bootstrap single-node cluster.
    let mut members = BTreeMap::new();
    members.insert(1u64, node_addr(1));
    raft.initialize(members).await.expect("initialize failed");

    // Wait for this node to become leader.
    raft.wait(None)
        .current_leader(1, "waiting for node 1 to become leader")
        .await
        .expect("leader election timed out");

    // Write a row via Raft consensus.
    let req = make_request("players", "alice", serde_json::json!({"hp": 100}));
    raft.client_write(req).await.expect("client_write failed");

    // Verify the row appeared in the shared TableStore.
    let row = tables.get_row("players", "alice").expect("get_row failed");
    assert!(row.is_some(), "row was not applied by the state machine");

    raft.shutdown().await.expect("shutdown failed");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Single node: multiple writes commit in order
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_single_node_multiple_writes_ordered() {
    let tables  = Arc::new(TableStore::new());
    let subs    = Arc::new(SubscriptionManager::new());
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    let raft = make_node(1, tables.clone(), subs, nodes.clone()).await;
    nodes.insert(1, raft.clone());

    let mut members = BTreeMap::new();
    members.insert(1u64, node_addr(1));
    raft.initialize(members).await.unwrap();
    raft.wait(None).current_leader(1, "leader").await.unwrap();

    for i in 0u32..5 {
        let req = make_request(
            "counters",
            &format!("counter_{}", i),
            serde_json::json!({"value": i}),
        );
        raft.client_write(req).await.expect("write failed");
    }

    for i in 0u32..5 {
        let row = tables
            .get_row("counters", &format!("counter_{}", i))
            .unwrap();
        assert!(row.is_some(), "counter_{} missing", i);
    }

    raft.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Three-node cluster: add_learner, change_membership
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_three_node_membership_change() {
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    // Create 3 nodes with independent state machines.
    let tables: Vec<Arc<TableStore>> = (0..3).map(|_| Arc::new(TableStore::new())).collect();
    let subs: Vec<Arc<SubscriptionManager>> = (0..3).map(|_| Arc::new(SubscriptionManager::new())).collect();

    for i in 0..3usize {
        let id = (i + 1) as u64;
        let raft = make_node(id, tables[i].clone(), subs[i].clone(), nodes.clone()).await;
        nodes.insert(id, raft);
    }

    // Bootstrap node 1 as a single-node cluster first.
    let n1 = nodes.get(&1).unwrap().value().clone();
    let mut single = BTreeMap::new();
    single.insert(1u64, node_addr(1));
    n1.initialize(single).await.expect("init failed");
    n1.wait(None).current_leader(1, "node1 leader").await.expect("node1 not leader");

    // Add nodes 2 and 3 as learners, then promote them.
    for id in [2u64, 3u64] {
        n1.add_learner(id, node_addr(id), true)
            .await
            .expect("add_learner failed");
    }

    // Promote to voters: {1, 2, 3}.
    let all_voters: BTreeSet<u64> = [1u64, 2, 3].iter().cloned().collect();
    n1.change_membership(all_voters, true)
        .await
        .expect("change_membership failed");

    // Verify the cluster has a leader in the {1,2,3} voter set.
    let metrics = n1.metrics().borrow().clone();
    assert!(
        metrics.current_leader.is_some(),
        "no leader after change_membership"
    );

    // Shut down all nodes.
    for id in [1u64, 2, 3] {
        let raft = nodes.get(&id).unwrap().value().clone();
        raft.shutdown().await.unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Three-node log replication: write on leader, follower sees it
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_three_node_log_replication() {
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    let tables: Vec<Arc<TableStore>> = (0..3).map(|_| Arc::new(TableStore::new())).collect();
    let subs:   Vec<Arc<SubscriptionManager>> = (0..3).map(|_| Arc::new(SubscriptionManager::new())).collect();

    for i in 0..3usize {
        let id = (i + 1) as u64;
        let raft = make_node(id, tables[i].clone(), subs[i].clone(), nodes.clone()).await;
        nodes.insert(id, raft);
    }

    let n1 = nodes.get(&1).unwrap().value().clone();

    // Bootstrap + promote to 3-voter cluster.
    let mut single = BTreeMap::new();
    single.insert(1u64, node_addr(1));
    n1.initialize(single).await.unwrap();
    n1.wait(None).current_leader(1, "initial leader").await.unwrap();

    for id in [2u64, 3] {
        n1.add_learner(id, node_addr(id), true).await.unwrap();
    }
    let voters: BTreeSet<u64> = [1u64, 2, 3].iter().cloned().collect();
    n1.change_membership(voters, true).await.unwrap();

    // Write via the leader.
    let req = make_request("scores", "player_x", serde_json::json!({"score": 9999}));
    n1.client_write(req).await.expect("client_write failed");

    // Wait for replication: check that the entry is applied on all 3 state machines.
    // Give Raft up to ~2 seconds to replicate (heartbeat = 250 ms → 8 cycles max).
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
    let mut all_replicated = false;
    while tokio::time::Instant::now() < deadline {
        let replicated = (0..3).all(|i| {
            tables[i]
                .get_row("scores", "player_x")
                .unwrap_or(None)
                .is_some()
        });
        if replicated {
            all_replicated = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(all_replicated, "write was not replicated to all 3 nodes within 2 s");

    for id in [1u64, 2, 3] {
        nodes.get(&id).unwrap().value().shutdown().await.unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Failover: kill leader, verify new leader elected
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_failover_new_leader_elected_after_leader_dies() {
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    let tables: Vec<Arc<TableStore>> = (0..3).map(|_| Arc::new(TableStore::new())).collect();
    let subs:   Vec<Arc<SubscriptionManager>> = (0..3).map(|_| Arc::new(SubscriptionManager::new())).collect();

    for i in 0..3usize {
        let id = (i + 1) as u64;
        let raft = make_node(id, tables[i].clone(), subs[i].clone(), nodes.clone()).await;
        nodes.insert(id, raft);
    }

    let n1 = nodes.get(&1).unwrap().value().clone();

    let mut single = BTreeMap::new();
    single.insert(1u64, node_addr(1));
    n1.initialize(single).await.unwrap();
    n1.wait(None).current_leader(1, "initial leader").await.unwrap();
    for id in [2u64, 3] {
        n1.add_learner(id, node_addr(id), true).await.unwrap();
    }
    let voters: BTreeSet<u64> = [1u64, 2, 3].iter().cloned().collect();
    n1.change_membership(voters, true).await.unwrap();

    // Write one entry before killing the leader.
    let req = make_request("pre_fail", "k1", serde_json::json!({"ok": true}));
    n1.client_write(req).await.unwrap();

    // Shut down node 1 (the current leader) to simulate a crash.
    n1.shutdown().await.unwrap();
    // Remove from registry so network layer returns "unreachable" for node 1.
    nodes.remove(&1);

    // Nodes 2 and 3 form a quorum → one of them must elect a new leader.
    // Wait up to 4 s (covers worst-case election timeout of 1.5 s × 2 rounds).
    let n2 = nodes.get(&2).unwrap().value().clone();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(4);
    let mut new_leader_found = false;
    while tokio::time::Instant::now() < deadline {
        let m = n2.metrics().borrow().clone();
        if m.current_leader.is_some() && m.current_leader != Some(1) {
            new_leader_found = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(new_leader_found, "no new leader elected after old leader crashed");

    // Write via the surviving cluster (should succeed with quorum of 2).
    let new_leader_id = n2.metrics().borrow().current_leader.unwrap();
    let new_leader = nodes.get(&new_leader_id).unwrap().value().clone();
    let req2 = make_request("post_fail", "k2", serde_json::json!({"survived": true}));
    new_leader.client_write(req2).await.expect("post-failover write failed");

    for id in [2u64, 3] {
        if let Some(r) = nodes.get(&id) {
            r.value().shutdown().await.unwrap();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6 — Split-brain prevention: minority (1 of 3) cannot commit
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_minority_cannot_commit_without_quorum() {
    let nodes: NodeRegistry = Arc::new(DashMap::new());

    let tables: Vec<Arc<TableStore>> = (0..3).map(|_| Arc::new(TableStore::new())).collect();
    let subs:   Vec<Arc<SubscriptionManager>> = (0..3).map(|_| Arc::new(SubscriptionManager::new())).collect();

    for i in 0..3usize {
        let id = (i + 1) as u64;
        let raft = make_node(id, tables[i].clone(), subs[i].clone(), nodes.clone()).await;
        nodes.insert(id, raft);
    }

    let n1 = nodes.get(&1).unwrap().value().clone();

    let mut single = BTreeMap::new();
    single.insert(1u64, node_addr(1));
    n1.initialize(single).await.unwrap();
    n1.wait(None).current_leader(1, "initial leader").await.unwrap();
    for id in [2u64, 3] {
        n1.add_learner(id, node_addr(id), true).await.unwrap();
    }
    let voters: BTreeSet<u64> = [1u64, 2, 3].iter().cloned().collect();
    n1.change_membership(voters, true).await.unwrap();

    // Partition: remove nodes 2 and 3 from the registry so node 1 cannot reach quorum.
    nodes.remove(&2);
    nodes.remove(&3);

    // Shut down nodes 2 and 3 to make the partition permanent.
    // (They're already removed from the network registry; shutdown is cleanup only.)

    // client_write on the now-partitioned leader should NOT commit (no quorum).
    // openraft will either return an error or hang indefinitely waiting for quorum.
    // We use a timeout to detect the non-commit.
    let req = make_request("partition_test", "should_not_land", serde_json::json!({"bad": true}));
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(1500),
        n1.client_write(req),
    )
    .await;

    // Either timeout (Err from tokio::time::timeout) or a Raft error — both are acceptable.
    // What must NOT happen is Ok(Ok(_)) within the timeout window.
    match result {
        Err(_timeout)       => { /* expected: write did not commit in time */ }
        Ok(Err(_raft_err))  => { /* also acceptable: Raft refused without quorum */ }
        Ok(Ok(_))           => panic!(
            "minority partition should not be able to commit, but client_write succeeded"
        ),
    }

    // The row must NOT be in the local state machine (no commit occurred).
    let row = tables[0].get_row("partition_test", "should_not_land").unwrap();
    assert!(row.is_none(), "row was applied without quorum — split-brain bug!");

    n1.shutdown().await.unwrap();
}
