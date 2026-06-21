# Clustering

---

## Single-Node vs Cluster

Voltra starts in single-node mode by default. In single-node mode the Raft consensus layer still runs but auto-bootstraps as a one-member cluster — writes commit immediately without any quorum round-trip overhead. Clustering is opt-in.

When to use clustering:

- You need fault tolerance: the cluster survives the loss of a minority of nodes.
- You have more write throughput than a single machine can serve.
- You want replicated in-memory state across multiple availability zones.

When NOT to use clustering:

- You are running a game server for a single-region, single-instance deployment. Single-node with WAL + snapshots is simpler and faster.
- Your data fits in memory on one machine and you can tolerate a brief restart to restore from WAL.

---

## Raft Consensus

Voltra uses openraft 0.9 for Raft consensus. Each node runs a full Raft state machine.

Guarantees provided:

- **Quorum writes**: an entry is only committed after it has been durably stored on a majority of nodes (`floor(N/2) + 1`). A 3-node cluster tolerates one node failure; a 5-node cluster tolerates two.
- **Leader election**: if the current leader becomes unreachable, remaining nodes hold an election (randomized timeout, 750–1500 ms by default). The new leader is elected within 2–3 seconds in typical LAN conditions.
- **Split-brain prevention**: a node with a stale term rejects AppendEntries from old leaders. A minority partition cannot elect a leader and cannot commit writes.
- **Log conflict resolution**: followers that diverge from the leader's log are corrected via the truncate path before new entries are appended.
- **Snapshot transfer**: nodes that fall too far behind receive a full state machine snapshot via InstallSnapshot, then catch up with incremental log entries.

---

## Bootstrapping a 3-Node Cluster

### 1. Start all three nodes

Start each node with a distinct WAL path and metrics port. The WebSocket port is client-facing; the metrics port is used for Raft and admin HTTP.

Node 1 (`http://node1:3001`):

```bash
VOLTRA_HOST=0.0.0.0 \
VOLTRA_METRICS_PORT=3001 \
VOLTRA_WAL_PATH=/data/node1/voltra.wal \
VOLTRA_API_KEY=changeme \
voltra start
```

Node 2 (`http://node2:3001`):

```bash
VOLTRA_HOST=0.0.0.0 \
VOLTRA_METRICS_PORT=3001 \
VOLTRA_WAL_PATH=/data/node2/voltra.wal \
VOLTRA_API_KEY=changeme \
voltra start
```

Node 3 (`http://node3:3001`):

```bash
VOLTRA_HOST=0.0.0.0 \
VOLTRA_METRICS_PORT=3001 \
VOLTRA_WAL_PATH=/data/node3/voltra.wal \
VOLTRA_API_KEY=changeme \
voltra start
```

### 2. Initialize the first node as Raft leader

```bash
curl -X POST http://node1:3001/raft/init \
  -H "Content-Type: application/json" \
  -d '{"node_id": 1, "addr": "http://node1:3001"}'
```

### 3. Add nodes 2 and 3 as learners

```bash
curl -X POST http://node1:3001/raft/add-learner \
  -H "Content-Type: application/json" \
  -d '{"node_id": 2, "addr": "http://node2:3001"}'

curl -X POST http://node1:3001/raft/add-learner \
  -H "Content-Type: application/json" \
  -d '{"node_id": 3, "addr": "http://node3:3001"}'
```

### 4. Promote all three to voters

```bash
curl -X POST http://node1:3001/raft/change-membership \
  -H "Content-Type: application/json" \
  -d '[1, 2, 3]'
```

The cluster is now live. Writes can be sent to any node. Nodes that are not the current leader will forward the call to the leader automatically.

### 5. Verify

```bash
curl http://node1:3001/raft/metrics
```

Expected response includes `"current_leader": 1` (or whichever node won the election) and `"membership": {"voters": [1, 2, 3]}`.

---

## Leader Forwarding

Clients connect to any node's WebSocket port. If that node is not the current Raft leader, it detects the `ForwardToLeader` error from `client_write()` and transparently forwards the reducer call to the leader via `POST /cluster/call`. The response is then relayed back to the client.

This means clients do not need to track which node is the leader.

---

## HLC and Last-Write-Wins Conflict Resolution

Every row written through a reducer carries a Hybrid Logical Clock (HLC) timestamp packed as a 64-bit integer: 48 bits of wall-clock milliseconds and 16 bits of logical counter. The HLC advances monotonically even when the wall clock does not.

When a delta arrives from another node via cluster fanout:

1. `apply_delta_batch` compares the delta's `hlc_ts` to the stored row's `hlc_ts`.
2. If the delta is older (lower `hlc_ts`), the write is silently skipped.
3. If the delta is newer or equal, the write is applied.

This implements last-write-wins conflict resolution without coordination. In a properly functioning Raft cluster, conflicts should be rare because all writes are serialized by the leader. HLC conflict resolution is primarily a safety net for cluster fanout edge cases (e.g., a node that receives a replayed write from a snapshot install).

---

## Sharding

Voltra does not implement transparent server-side sharding. Each Raft cluster is a single logical unit that holds the full dataset in memory across all nodes.

If your dataset does not fit in memory on any single machine, you need to shard across multiple Voltra clusters. The canonical shard assignment function is:

```
shard = fnv1a_64(row_key) % shard_count
```

This is implemented in `src/cluster/mod.rs` as `shard_for_key(key, shard_count) -> u32`. Clients and application code are responsible for routing writes and reads to the correct cluster based on this function. There is no transparent proxy layer yet — this is a known limitation.

---

## Cluster Security

Set `VOLTRA_CLUSTER_SECRET` to a shared secret on all nodes. The secret is injected as the `x-voltra-cluster-secret` header on all inter-node Raft HTTP requests. Requests without the correct secret are rejected.

```bash
VOLTRA_CLUSTER_SECRET=your-shared-secret voltra start
```
