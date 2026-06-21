# Raft Consensus Architecture Proposal for Voltra

**Author:** Agent 5 (Distributed Systems Architect)
**Date:** 2026-06-08
**Status:** PROPOSAL -- requires approval before implementation

---

## 1. Current State Audit

This section evaluates every distributed-systems primitive in the existing
Voltra cluster layer against what Raft consensus requires.

### 1.1 Leader Election -- MISSING

There is no leader election. Every node is a peer. The `ClusterBus`
(`src/cluster/mod.rs`) treats all peers equally -- any node can accept
writes for the rows it owns (based on `shard_for_key()`). There is no
concept of a leader, no term counter, no vote request, and no election
timeout.

**Evidence:** `ClusterConfig` has `my_shard_id` and `shard_count` but no
`leader_id`, `current_term`, or `voted_for` fields. `ClusterBus` has
`fanout_deltas()` and `proxy_call()` but no `request_vote()` or
`append_entries()`.

### 1.2 Consensus (Log Agreement) -- MISSING

There is no log agreement protocol. Writes are committed locally first
(via `apply_delta_batch()` in `src/table/mod.rs`), then fanned out
asynchronously to peers via `POST /cluster/deltas` (`src/cluster/fanout.rs`).
This is fire-and-forget replication -- the local commit succeeds regardless
of whether any peer acknowledges it.

**Evidence:** `fanout_to_peers()` (fanout.rs:486) spawns blocking tasks
per peer and never awaits their result. The caller (worker loop) continues
immediately after calling `fanout_deltas()`.

### 1.3 Cluster Membership -- PROTOTYPE

Node discovery exists via `VOLTRA_PEERS` env var and `VOLTRA_SEED_NODE`
dynamic join. However, membership changes are static (restart required for
`VOLTRA_PEERS`) or uncoordinated (seed join adds to a local DashMap without
cluster-wide agreement).

**Evidence:** `ClusterConfig::from_env()` (mod.rs:186) reads `VOLTRA_PEERS`
once at startup. `cluster_seed()` (main.rs:763) adds peers locally via HTTP
but there is no two-phase membership change.

### 1.4 Heartbeats -- COMPLETE

The gossip system (`src/cluster/gossip.rs`) pings each peer's
`GET /cluster/health` endpoint every `VOLTRA_GOSSIP_INTERVAL_MS` (default
5000ms). After 3 consecutive failures, a peer is marked unhealthy and
excluded from fan-out. Recovery is automatic when a ping succeeds.

**Evidence:** `start_gossip()` (gossip.rs:26), `PeerEntry::mark_failure()`
(mod.rs:143, threshold=3), `PeerEntry::mark_healthy()` (mod.rs:135).

### 1.5 Quorum Handling -- MISSING

No quorum concept exists. A single node can accept writes unilaterally.
There is no majority requirement for commits.

### 1.6 Split-Brain Prevention -- MISSING

Nothing prevents two partitioned halves of the cluster from both accepting
writes to the same logical shard. Since writes commit locally first and
fan-out is best-effort, a network partition creates divergent state that
is never reconciled.

### 1.7 Replica Promotion -- MISSING

There is no concept of primary vs. replica for a given shard. Each shard
is owned by exactly one node (determined by `shard_for_key()`). If that
node dies, its data is unavailable until it restarts.

### 1.8 Failover -- MISSING

No automatic failover. If a node crashes, its shards are unreachable.
`proxy_call()` (proxy.rs:80) will return a network error. There is no
mechanism to reassign shard ownership.

### 1.9 State Synchronization -- PARTIAL

Delta fan-out exists but is lossy. The retry queue (fanout.rs) is bounded
at 1024 entries per peer -- overflow drops the oldest payload. A peer that
is down for an extended period will miss deltas permanently with no way
to catch up.

**Evidence:** `MAX_RETRY_QUEUE_LEN = 1024` (fanout.rs:50). `push_back()`
drops front on overflow (fanout.rs:76).

### 1.10 Recovery After Node Loss -- PARTIAL

Snapshots (`src/wal/snapshot.rs`) and WAL replay exist for single-node
crash recovery. A node that restarts will reload its own snapshot + WAL.
However, there is no mechanism to receive missed deltas from peers -- if
the node was down during fan-out, those updates are lost unless the
peer's retry queue still held them.

### 1.11 Replication Correctness -- MISSING

No guarantees. The system provides at-most-once async replication. Deltas
can be lost (retry queue overflow), duplicated (retry delivers twice),
or reordered (parallel `spawn_blocking` tasks per peer). There is no
sequence number on replicated deltas to detect gaps or duplicates.

---

### Summary Table

| Primitive                 | Status    | Raft Requirement |
|---------------------------|-----------|------------------|
| Leader election           | MISSING   | Core             |
| Consensus (log agreement) | MISSING   | Core             |
| Cluster membership        | PROTOTYPE | Core             |
| Heartbeats                | COMPLETE  | Core (reusable)  |
| Quorum handling           | MISSING   | Core             |
| Split-brain prevention    | MISSING   | Core             |
| Replica promotion         | MISSING   | Core             |
| Failover                  | MISSING   | Core             |
| State synchronization     | PARTIAL   | Core             |
| Recovery after node loss  | PARTIAL   | Core             |
| Replication correctness   | MISSING   | Core             |

---

## 2. Architecture Proposal

### 2.1 Raft Integration Strategy

#### Option A: Embedded Raft (Build It)

Implement the Raft consensus algorithm directly in Voltra from scratch.
The existing WAL (`src/wal/batch_writer.rs`) becomes the Raft log. The
existing `TableStore` (`src/table/mod.rs`) becomes the Raft state machine.

**Effort estimate:** 16-24 engineer-weeks.

**Pros:**
- Zero external dependencies.
- Full control over every aspect of the protocol.
- Can optimize for Voltra's specific access patterns (high fan-out,
  game-tick workloads, per-row deltas).

**Cons:**
- Raft is deceptively complex. The paper describes ~15 pages of protocol,
  but production-grade implementations handle dozens of edge cases:
  pre-vote protocol, leadership transfer, read-index, learner nodes,
  joint consensus for membership, snapshot streaming, log compaction.
- Testing Raft correctly requires deterministic simulation (like
  Jepsen/FoundationDB-style testing). Voltra has no simulation harness.
- A single subtle bug in term handling or log matching can cause
  silent data loss or split-brain -- the exact problems Raft is
  supposed to prevent.
- Every Raft implementation in production (etcd, CockroachDB, TiKV)
  took years of hardening. Voltra cannot afford that timeline.

**Risk:** HIGH. The probability of shipping a correct, production-grade
Raft from scratch within 6 months is low.

---

#### Option B: `openraft` Crate (Use a Library)

Use the `openraft` crate (https://github.com/databendlabs/openraft),
the most mature and actively maintained Raft library in Rust.

- Implement `RaftLogStorage` + `RaftStateMachine` traits on top of
  existing WAL + TableStore.
- Implement `RaftNetwork` trait on top of existing cluster HTTP bus.
- `openraft` handles: election, log replication, snapshotting protocol,
  membership changes, and all the subtle edge cases.

**Effort estimate:** 6-10 engineer-weeks.

**Pros:**
- Battle-tested consensus core (used by Databend, Metasrv, etc.).
- Correct by construction -- `openraft` has extensive property-based
  tests and Jepsen-style verification.
- Ships with: pre-vote protocol, leadership transfer, learner nodes,
  joint consensus, log compaction, and snapshot transfer.
- Active maintenance with regular releases.
- Pure Rust, no C/C++ FFI -- matches Voltra's dependency philosophy.

**Cons:**
- Additional dependency (~15k LOC).
- Must adapt to `openraft`'s trait API -- some impedance mismatch with
  the existing WAL format and TableStore API.
- `openraft`'s async API requires careful integration with Voltra's
  `spawn_blocking` worker model.
- Breaking changes between `openraft` versions require tracking upstream.

**Risk:** MEDIUM. The main risk is integration complexity, not
correctness -- `openraft` provides the correctness guarantees.

**Recommended crate version:** `openraft = "0.10"` (latest stable as of
2026-06, uses `async` storage traits, supports `serde` for log entries).

---

### 2.2 Data Flow

#### 2.2.1 Write Path (Client -> Leader -> Followers -> Commit -> Response)

```
                                          +------------------+
  Client ──WebSocket──> Node X            |  Is Node X the   |
  (ReducerCall)                           |  Raft leader?    |
                                          +--------+---------+
                                                   |
                                      YES          |          NO
                                  +----------------+----------------+
                                  |                                 |
                                  v                                 v
                          +-------+--------+              +---------+-------+
                          | 1. Propose     |              | Forward to      |
                          |    log entry   |              | current leader  |
                          |    (RowDeltas) |              | via HTTP        |
                          +-------+--------+              | POST /raft/call |
                                  |                       +-----------------+
                                  v
                          +-------+--------+
                          | 2. Replicate   |
                          |    to quorum   |
                          |    (N/2 + 1)   |
                          +-------+--------+
                                  |
                      +-----------+-----------+
                      |           |           |
                      v           v           v
                  Follower A  Follower B  Follower C
                  AppendEntry AppendEntry AppendEntry
                  (persist    (persist    (persist
                   to WAL)     to WAL)     to WAL)
                      |           |           |
                      +-----+-----+-----+----+
                            |           |
                       ACK quorum   (async)
                            |
                            v
                    +-------+--------+
                    | 3. Commit      |
                    |    (leader     |
                    |     advances   |
                    |     commit     |
                    |     index)     |
                    +-------+--------+
                            |
                            v
                    +-------+--------+
                    | 4. Apply to    |
                    |    TableStore  |
                    |    (state      |
                    |     machine)   |
                    +-------+--------+
                            |
                    +-------+--------+
                    | 5. Publish to  |
                    |    local subs  |
                    |    + respond   |
                    |    to client   |
                    +----------------+
```

**Detailed steps:**

1. Client sends `ReducerCall` over WebSocket to any node.
2. If the node is not the Raft leader, it proxies the call to the leader
   via `POST /raft/call` (replaces current `POST /cluster/call`).
3. Leader executes the reducer in a `spawn_blocking` worker, producing
   `Vec<RowDelta>`.
4. Leader wraps the deltas into a Raft `LogEntry` and proposes it to the
   Raft group via `openraft::Raft::client_write()`.
5. `openraft` replicates the entry to followers via `RaftNetwork::append_entries()`.
   Each follower persists the entry to its WAL via `RaftLogStorage::append()`.
6. Once a quorum (N/2 + 1) of nodes have persisted the entry, `openraft`
   advances the commit index.
7. On each node, `RaftStateMachine::apply()` is called for committed entries.
   This calls `TableStore::apply_delta_batch()` and
   `SubscriptionManager::publish_deltas()`.
8. Leader responds to the client with the reducer result.

**Key change from current architecture:** Writes are no longer committed
locally before replication. The commit only happens after quorum
acknowledgment. This increases write latency by one network round-trip
but provides strong consistency.

#### 2.2.2 Read Path (Follower Reads)

Two modes, configurable per-subscription or per-call:

**Mode 1: Leader reads (strong consistency)**
```
Client ──> Any Node ──> Forward to Leader ──> Read TableStore ──> Respond
```
Guarantees linearizable reads. The leader confirms it is still the leader
(via `openraft::Raft::ensure_linearizable()` which sends a heartbeat
round) before serving the read.

**Mode 2: Follower reads (eventual consistency, lower latency)**
```
Client ──> Local Node ──> Read local TableStore ──> Respond
```
May return stale data (up to one heartbeat interval behind). Suitable
for game state that tolerates slight staleness (e.g., leaderboard,
presence).

**Implementation:** Add a `read_consistency` field to the subscription
filter: `"players WHERE zone='z1' READ_CONSISTENCY strong"`. Default
to follower reads for backward compatibility and performance.

#### 2.2.3 Leader Crash and Recovery

```
Time ─────────────────────────────────────────────────────────>

Leader (Node A)    Follower B         Follower C
    |                   |                   |
    X (crash)           |                   |
                        |                   |
                  election timeout     election timeout
                  fires (150-300ms)    fires (150-300ms)
                        |                   |
                  RequestVote ──────> receives vote
                  (term T+1)          request
                        |                   |
                        | <──────── VoteGranted
                        |                   |
                  BECOMES LEADER            |
                  (term T+1)                |
                        |                   |
                  Sends heartbeat ────────> |
                  (empty AppendEntries)     |
                        |                   |
                  Catches up any      Acknowledges new
                  uncommitted         leader
                  entries from old
                  term
                        |                   |
                  READY: accepts            |
                  client writes             |
```

**Key properties:**
- Election timeout: 150-300ms (randomized per node to avoid split votes).
- Heartbeat interval: 50ms (reuses existing gossip infrastructure,
  but tightened from 5000ms to 50ms for Raft heartbeats).
- A new leader never loses committed entries (Raft safety property).
- Uncommitted entries from the old leader may be lost -- this is
  expected and correct. Clients whose reducer calls were in-flight
  during the crash will receive a timeout error and must retry.

---

### 2.3 State Machine

The Raft state machine IS `TableStore` (`src/table/mod.rs`).

```
+------------------------------------------------------------------+
|                     Raft State Machine                           |
|                                                                  |
|  +-------------------+    +---------------------------+          |
|  | RaftStateMachine   |    | TableStore (DashMap)      |          |
|  | impl for Voltra   |--->| apply_delta_batch(deltas) |          |
|  +-------------------+    +---------------------------+          |
|         |                           |                            |
|         |  apply(entries)           |  For each committed entry: |
|         |                           |  1. Deserialize RowDeltas  |
|         v                           |  2. apply_delta_batch()    |
|  +-------------------+              |  3. publish_deltas()       |
|  | Committed Raft    |              +---------------------------+
|  | Log Entry         |                                           |
|  | {                 |                                           |
|  |   term: u64,      |                                           |
|  |   index: u64,     |                                           |
|  |   payload: {      |                                           |
|  |     reducer: str,  |                                           |
|  |     deltas: [...], |                                           |
|  |     caller_id: str |                                           |
|  |   }               |                                           |
|  | }                 |                                           |
|  +-------------------+                                           |
+------------------------------------------------------------------+
```

**Log entry format:**

```rust
// New type: src/raft/log_entry.rs
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VoltraLogEntry {
    /// Reducer that produced these deltas.
    pub reducer_name: String,
    /// The committed row deltas.
    pub deltas: Vec<RowDelta>,
    /// Who called the reducer (for audit trail).
    pub caller_id: String,
    /// Timestamp of the original call.
    pub timestamp: u64,
}
```

**WAL and Raft log merge:**

The current `BatchedWalWriter` (`src/wal/batch_writer.rs`) writes
`WalEntry` structs that contain `ReducerCallEntry { reducer_id, args, deltas }`.
Under Raft, this WAL becomes the Raft log storage:

- Each `WalEntry` becomes a Raft `LogEntry` with a term and index.
- The `WalHeader.sequence_number` maps directly to the Raft log index.
- The `WalHeader.checksum` (CRC32) is preserved for corruption detection.
- New fields: `term: u64` added to `WalHeader`.

The `BatchedWalWriter` interface stays the same -- `append()` and
`truncate_before()` already match what `openraft` needs from
`RaftLogStorage`.

**Snapshot = Raft snapshot:**

The existing `save_snapshot()` / `load_snapshot()` (`src/wal/snapshot.rs`)
already serialize the full `TableStore` state. Under Raft:

- `SnapshotMeta.last_sequence` becomes the `last_applied_log` index.
- Add `last_applied_term` to `SnapshotMeta`.
- `openraft` calls `RaftStateMachine::build_snapshot()` which delegates
  to the existing `save_snapshot()`.
- `openraft` calls `RaftStateMachine::install_snapshot()` which
  delegates to `load_snapshot()`.

---

### 2.4 Membership Changes

**Current state:** Static `VOLTRA_PEERS` env var + dynamic `VOLTRA_SEED_NODE`.

**Under Raft:** Membership changes are Raft log entries, committed through
consensus like any other write. This prevents split-brain during
membership transitions.

```
Operator wants to add Node D to a 3-node cluster (A, B, C):

1. Operator starts Node D with VOLTRA_SEED_NODE=http://A:3001
2. Node D contacts Node A's /raft/join endpoint
3. Node A (if leader) proposes a membership change:
   AddLearner(D)  -- D receives log entries but cannot vote
4. Once D has caught up (log index within threshold of leader):
   Node A proposes: ChangeMembership({A, B, C, D})
5. openraft handles joint consensus internally:
   - Old config {A, B, C} and new config {A, B, C, D} both
     must agree during the transition.
   - Once committed, D becomes a voting member.
6. Node D is now a full peer.

Removing a node works similarly:
1. Leader proposes ChangeMembership({A, B, C} \ {D})
2. After commit, D is no longer a voting member.
3. D can be shut down safely.
```

**API changes:**

| Current Endpoint      | Raft Equivalent         | Notes                          |
|-----------------------|-------------------------|--------------------------------|
| `POST /cluster/join`  | `POST /raft/join`       | Proposes AddLearner + Change   |
| `VOLTRA_PEERS`        | Initial cluster config  | Used only for first bootstrap  |
| `VOLTRA_SEED_NODE`    | Discovery mechanism     | Points to any existing member  |

**Bootstrap:** The very first node starts with a single-node Raft group
(itself as the only voter). Subsequent nodes join via `/raft/join`.

---

### 2.5 Snapshot Transfer

When a new node joins or a follower falls too far behind:

```
Leader                              New Node (Learner)
  |                                       |
  |  1. New node calls /raft/join         |
  | <─────────────────────────────────────|
  |                                       |
  |  2. Leader adds as Learner            |
  |     (non-voting, receives log)        |
  |                                       |
  |  3. Leader detects: new node's        |
  |     log is too far behind             |
  |     (log entries already compacted)   |
  |                                       |
  |  4. Leader builds snapshot            |
  |     (save_snapshot to temp file)      |
  |                                       |
  |  5. Leader streams snapshot via       |
  |     RaftNetwork::install_snapshot()   |
  | ─────────────────────────────────────>|
  |     (chunked HTTP transfer)           |
  |                                       |
  |  6. New node installs snapshot        |
  |     (load_snapshot into TableStore)   |
  |                                       |
  |  7. Leader sends remaining log        |
  |     entries (append_entries)          |
  | ─────────────────────────────────────>|
  |                                       |
  |  8. New node catches up               |
  |                                       |
  |  9. Leader promotes to voter          |
  |     (ChangeMembership)                |
  |                                       |
```

**Snapshot wire format:**

Re-use the existing `SnapshotFile` MessagePack format from
`src/wal/snapshot.rs`. For large clusters with GBs of state, chunk the
transfer:

```rust
// New: src/raft/snapshot_transport.rs
pub struct SnapshotChunk {
    pub snapshot_id: String,    // unique ID for this snapshot
    pub offset: u64,            // byte offset in the snapshot file
    pub data: Vec<u8>,          // chunk bytes (1MB default)
    pub done: bool,             // true for the last chunk
    pub meta: Option<SnapshotMeta>, // included in first chunk only
}
```

**Implementation:** `openraft` drives snapshot transfer via
`RaftNetwork::install_snapshot()`. We implement this as a streaming
`POST /raft/snapshot` endpoint that accepts chunks and assembles
them into a `.tmp` file, then renames atomically (same pattern as
`save_snapshot()`).

---

## 3. Risk Analysis

### 3.1 Split-Brain During Network Partition

**Likelihood:** HIGH in any distributed system without consensus.
**Impact:** CRITICAL -- divergent state, data loss, broken game state.
**Mitigation:** Raft guarantees that at most one leader exists per term.
Only the partition with a majority of voters can elect a leader and
commit writes. The minority partition becomes read-only (stale reads)
or unavailable until the partition heals.

**Residual risk:** If Voltra is deployed with only 2 nodes (no quorum
possible if one fails), a partition makes the cluster fully unavailable.
Minimum recommended deployment: 3 nodes.

### 3.2 Data Loss During Leader Failover

**Likelihood:** MEDIUM (requires in-flight writes during crash).
**Impact:** HIGH -- reducer calls that were proposed but not yet committed
are lost.
**Mitigation:**
- Committed entries are never lost (Raft safety property).
- Uncommitted entries: clients will receive a timeout error and must
  retry. The TS/Rust SDKs already have timeout handling.
- `openraft` pre-vote protocol prevents unnecessary leader disruption.

**Residual risk:** A reducer call that has side effects outside Voltra
(e.g., sending an email via a native reducer) cannot be rolled back if
the Raft entry is not committed. Solution: make such reducers idempotent.

### 3.3 Stale Reads From Followers

**Likelihood:** CERTAIN (by design, if follower reads are enabled).
**Impact:** LOW-MEDIUM -- game state may be one heartbeat interval behind.
**Mitigation:**
- Default to follower reads for performance (game backends tolerate
  50-100ms staleness for leaderboard, presence, inventory display).
- Offer `READ_CONSISTENCY strong` for operations that require
  linearizability (e.g., checking if a player has enough currency
  before a purchase).
- Document the consistency model clearly in the SDK.

### 3.4 Snapshot Transfer Corruption

**Likelihood:** LOW (requires bit-flip during network transfer).
**Impact:** HIGH -- corrupted snapshot could produce wrong state.
**Mitigation:**
- CRC32 checksum on each snapshot chunk.
- Full snapshot checksum verified after assembly.
- If verification fails, discard and request a fresh snapshot.
- Existing `SnapshotMeta` already has integrity fields.

### 3.5 Log Divergence (Raft Safety Property Violation)

**Likelihood:** VERY LOW if using `openraft` (Option B). HIGH if
building from scratch (Option A).
**Impact:** CRITICAL -- violates the fundamental correctness guarantee.
**Mitigation:**
- Use `openraft` (Option B). Its implementation has been verified
  against the Raft spec with extensive property-based tests.
- Add Jepsen-style testing in Phase E (see Section 5).
- Never modify `openraft` internals.

### 3.6 Performance Impact of Synchronous Replication

**Likelihood:** CERTAIN.
**Impact:** MEDIUM -- write latency increases by 1 network RTT.
**Mitigation:**
- In-datacenter RTT is typically 0.1-0.5ms, so the overhead is small
  for co-located nodes.
- `openraft` pipelines AppendEntries RPCs, so throughput is not
  bottlenecked on sequential round-trips.
- Batch multiple reducer calls into a single Raft log entry when
  they arrive within the same tick (reuse the existing
  `BatchedWalWriter` batching concept).
- For latency-sensitive game ticks: use follower reads + optimistic
  updates (already implemented in TS/Rust SDKs).

**Latency budget (estimated, 3-node cluster, same datacenter):**

| Phase                  | Current (ms) | With Raft (ms) |
|------------------------|-------------|----------------|
| WebSocket receive      | 0.05        | 0.05           |
| Reducer execution      | 0.1-1.0     | 0.1-1.0        |
| WAL write (local)      | 0.05        | 0.05           |
| Raft replication       | --          | 0.2-0.5        |
| TableStore apply       | 0.02        | 0.02           |
| Subscription fan-out   | 0.01        | 0.01           |
| WebSocket respond      | 0.05        | 0.05           |
| **Total**              | **~0.3-1.2**| **~0.5-1.7**   |

---

## 4. Failure Simulations (Test Plan)

### 4.1 Leader Crash -> New Election

**Setup:** 3-node cluster (A=leader, B, C).
**Action:** Kill node A (SIGKILL).
**Expected:**
- B or C wins election within 300ms (2x election timeout).
- New leader accepts writes.
- Committed entries from A are present on the new leader.
- Uncommitted entries from A may be lost (correct behavior).
**Validation:** Client connects to B, calls a reducer, receives success.
Query confirms all previously committed data is intact.

### 4.2 Follower Crash -> Cluster Continues

**Setup:** 3-node cluster.
**Action:** Kill one follower.
**Expected:**
- Cluster continues serving reads and writes (2/3 quorum).
- No election triggered (leader is still alive).
- Dead follower's local subscribers see no updates.
**Validation:** Client writes succeed. `GET /raft/status` shows
2/3 nodes healthy.

### 4.3 Two-of-Five Crash

**Setup:** 5-node cluster.
**Action:** Kill 2 followers simultaneously.
**Expected:** Cluster continues (3/5 quorum). Performance may degrade
slightly (fewer replication targets).
**Validation:** Write succeeds, read succeeds on all 3 surviving nodes.

### 4.4 Three-of-Five Crash -> No Quorum

**Setup:** 5-node cluster.
**Action:** Kill 3 nodes (including or excluding the leader).
**Expected:**
- If leader survives: leader cannot commit new writes (no quorum).
  Client write calls return a "no quorum" error after timeout.
  Follower reads of already-committed data still work.
- If leader dies: no election succeeds (only 2 voters). Same outcome.
**Validation:** Write returns error within `reducer_timeout_ms`. Read
of existing data succeeds.

### 4.5 Network Partition (2+3 Split)

**Setup:** 5-node cluster. Partition into {A, B} and {C, D, E}.
**Action:** Drop all network traffic between the two groups.
**Expected:**
- {C, D, E} elects a new leader (if the old leader was in {A, B}).
- {C, D, E} continues serving writes (3/5 quorum).
- {A, B} cannot elect a leader (2/5 < quorum). Read-only or unavailable.
**Validation:** Write to {C, D, E} succeeds. Write to {A, B} fails.

### 4.6 Partition Heals -> Minority Re-Syncs

**Setup:** Continuing from 4.5.
**Action:** Restore network between the two groups.
**Expected:**
- {A, B} discover the new leader and new term.
- {A, B} receive AppendEntries from the new leader and catch up.
- Any conflicting uncommitted entries on {A, B} are truncated (Raft
  log matching property).
**Validation:** All 5 nodes converge to the same state. Read from any
node returns the same data.

### 4.7 New Node Joins -> Snapshot Transfer

**Setup:** 3-node cluster with 100k rows.
**Action:** Start a 4th node with `VOLTRA_SEED_NODE`.
**Expected:**
- Node 4 joins as a learner.
- Leader detects Node 4 has no log and initiates snapshot transfer.
- Node 4 installs snapshot, replays suffix log, becomes a voter.
- Total time: < 30 seconds for 100k rows.
**Validation:** `GET /raft/status` on Node 4 shows it as a voter.
Read from Node 4 returns all 100k rows.

### 4.8 Leader Steps Down -> Orderly Handoff

**Setup:** 3-node cluster.
**Action:** Call `POST /raft/transfer_leadership` on the leader.
**Expected:**
- Leader sends `TimeoutNow` to the target follower.
- Target immediately starts election, wins (since it has the most
  recent log).
- Old leader becomes a follower.
- No writes are lost (all committed entries transferred before handoff).
**Validation:** New leader accepts writes within 100ms of handoff.

### 4.9 Implementation Approach for Tests

```rust
// tests/raft_simulation.rs
//
// Uses in-process Raft nodes (no real network) with an injectable
// transport layer that can:
// - Drop messages (simulate partition)
// - Delay messages (simulate slow network)
// - Kill/restart nodes (simulate crashes)
//
// Each test creates N RaftNode instances sharing an in-memory
// transport. The transport is a DashMap<NodeId, mpsc::Sender<RaftMsg>>.
// Partitions are simulated by removing entries from the map.
```

---

## 5. Implementation Plan

### Phase A: Raft Election + Heartbeat

**Goal:** Leader election works. No log replication yet -- writes still
go through the existing local-commit path.

**Files to create:**
- `src/raft/mod.rs` -- Module root, re-exports.
- `src/raft/storage.rs` -- `RaftLogStorage` impl backed by `BatchedWalWriter`.
- `src/raft/state_machine.rs` -- `RaftStateMachine` impl wrapping `TableStore`.
- `src/raft/network.rs` -- `RaftNetwork` impl using existing cluster HTTP bus.
- `src/raft/types.rs` -- `VoltraLogEntry`, `VoltraNodeId`, type aliases.

**Files to modify:**
- `src/cluster/mod.rs` -- Add `RaftNode` to `ClusterBus`.
- `src/main.rs` -- Initialize Raft node at startup, wire into `ClusterBus`.
- `src/config.rs` -- Add `raft_election_timeout_ms`,
  `raft_heartbeat_interval_ms`, `raft_enabled` fields.
- `Cargo.toml` -- Add `openraft = "0.10"` dependency.

**Estimated effort:** 2 engineer-weeks.

**Dependencies:** None.

**Validation criteria:**
- 3-node cluster starts, one node becomes leader.
- `GET /raft/status` returns `{ "state": "leader", "term": 1 }` on leader.
- Kill leader, another node becomes leader within 500ms.
- Old writes still work (local commit path unchanged).

---

### Phase B: Log Replication

**Goal:** Reducer calls go through Raft. Writes are committed only after
quorum acknowledgment.

**Files to create:**
- `src/raft/log_entry.rs` -- `VoltraLogEntry` serialization.

**Files to modify:**
- `src/network/websocket.rs` -- Worker loop changes: instead of
  `ctx.commit()` + `wal.append()` + `subs.publish()`, the worker
  calls `raft.client_write(log_entry)` and waits for commit.
- `src/wal/batch_writer.rs` -- Adapt to serve as Raft log backend.
  Add `truncate_suffix()` (needed when Raft truncates conflicting entries).
- `src/wal/entry.rs` -- Add `term` field to `WalHeader`.
- `src/main.rs` -- Wire the Raft write path into the worker loop.
- `src/cluster/fanout.rs` -- Disable direct fan-out (Raft replication
  replaces it). Keep retry queue for snapshot transfer.

**Estimated effort:** 3-4 engineer-weeks.

**Dependencies:** Phase A.

**Validation criteria:**
- Client sends reducer call to leader, receives response.
- Follower's TableStore contains the committed data.
- Kill leader mid-write, new leader does not have the uncommitted entry.
- Write to non-leader node is forwarded to leader and succeeds.
- `cargo bench` shows < 2x latency regression for single-client writes.

---

### Phase C: Snapshot Transfer

**Goal:** New nodes and far-behind followers catch up via snapshot.

**Files to create:**
- `src/raft/snapshot_transport.rs` -- Chunked snapshot streaming.

**Files to modify:**
- `src/raft/state_machine.rs` -- Implement `build_snapshot()` and
  `install_snapshot()` using existing `save_snapshot()`/`load_snapshot()`.
- `src/raft/network.rs` -- Implement `install_snapshot()` RPC using
  `POST /raft/snapshot` endpoint.
- `src/main.rs` -- Add `/raft/snapshot` HTTP handler.

**Estimated effort:** 1-2 engineer-weeks.

**Dependencies:** Phase B.

**Validation criteria:**
- New node joins 3-node cluster, receives snapshot, catches up.
- Follower that was offline for 1 hour re-syncs via snapshot + log suffix.
- Snapshot transfer of 1M rows completes in < 60 seconds.

---

### Phase D: Membership Changes

**Goal:** Add/remove nodes without downtime.

**Files to modify:**
- `src/raft/network.rs` -- Wire `openraft::Raft::change_membership()`.
- `src/main.rs` -- Replace `/cluster/join` with `/raft/join` that
  proposes a membership change.
- `src/config.rs` -- `VOLTRA_PEERS` becomes bootstrap-only (not
  ongoing membership source).

**Estimated effort:** 1 engineer-week.

**Dependencies:** Phase C (new nodes need snapshot transfer to catch up).

**Validation criteria:**
- Add 4th node to 3-node cluster. Node becomes voter.
- Remove a node from 4-node cluster. Cluster continues with 3 nodes.
- Add + remove during active writes. No data loss.

---

### Phase E: Follower Reads + Hardening

**Goal:** Read scaling, production hardening, Jepsen-style testing.

**Files to create:**
- `tests/raft_simulation.rs` -- In-process simulation tests.
- `tests/raft_jepsen.rs` -- Network-partition and crash tests.

**Files to modify:**
- `src/network/websocket.rs` -- Add `ReadConsistency` to subscription
  queries. Follower reads bypass Raft for `eventual` consistency.
- `src/subscriptions.rs` -- Add `read_consistency` to `SubscriptionFilter`.
- SDK files (`voltra-client-ts/`, `voltra-client-rust/`) -- Add
  `readConsistency` option to `subscribe()`.

**Estimated effort:** 2-3 engineer-weeks.

**Dependencies:** Phase D.

**Validation criteria:**
- Follower read latency < 0.5ms (no Raft round-trip).
- Strong read on leader returns data committed within the last heartbeat.
- Jepsen tests pass: no linearizability violations under partitions.

---

### Phase Summary

| Phase | Scope                    | Effort        | Depends On | Key Risk                    |
|-------|--------------------------|---------------|------------|-----------------------------|
| A     | Election + heartbeat     | 2 weeks       | --         | `openraft` API learning     |
| B     | Log replication          | 3-4 weeks     | A          | Latency regression          |
| C     | Snapshot transfer        | 1-2 weeks     | B          | Large state transfer speed  |
| D     | Membership changes       | 1 week        | C          | Joint consensus edge cases  |
| E     | Follower reads + testing | 2-3 weeks     | D          | Subtle consistency bugs     |
| **Total** |                      | **9-12 weeks**|            |                             |

---

## 6. Recommendation

**Recommendation: Option B -- use the `openraft` crate.**

**Justification:**

1. **Correctness is non-negotiable.** Raft consensus is the hardest
   component to get right in a distributed system. A single bug in
   term handling, log truncation, or snapshot installation can cause
   silent data loss or split-brain -- the exact problems we are trying
   to solve. `openraft` has years of testing and production use.
   Building from scratch would require 6+ months of hardening that
   Voltra cannot afford.

2. **The existing architecture maps cleanly onto openraft's API.**
   `BatchedWalWriter` already has `append()` and `truncate_before()` --
   these map directly to `RaftLogStorage::append()` and `purge()`.
   `TableStore` is already the state machine. `save_snapshot()` /
   `load_snapshot()` map to `build_snapshot()` / `install_snapshot()`.
   The cluster HTTP bus provides the transport layer for `RaftNetwork`.
   The impedance mismatch is small.

3. **Effort is 3x lower.** Option B is estimated at 9-12 engineer-weeks
   vs. 16-24 for Option A. The savings come from not implementing
   election, log matching, snapshot protocol, membership changes, or
   pre-vote -- all provided by `openraft`.

4. **The dependency risk is manageable.** `openraft` is pure Rust
   (~15k LOC), actively maintained by the Databend team, and has no
   transitive C/C++ dependencies. It aligns with Voltra's "no V8,
   no external C++ runtime" philosophy. Version pinning in `Cargo.toml`
   prevents surprise breakage.

**Crate version:** `openraft = "0.10"` (latest stable, async storage
traits, serde support).

**Estimated total effort:** 9-12 engineer-weeks across Phases A-E.

**The single biggest risk:** Write latency regression in Phase B. Every
reducer call will pay an additional network round-trip for quorum
replication. Mitigation: batch multiple calls per Raft entry, use
follower reads for read-heavy workloads, and benchmark early in Phase B
to catch regressions before they compound.

---

## Appendix A: File Dependency Graph

```
src/raft/              (NEW -- all files created in Phases A-C)
  mod.rs
  types.rs
  storage.rs           depends on: src/wal/batch_writer.rs, src/wal/entry.rs
  state_machine.rs     depends on: src/table/mod.rs, src/subscriptions.rs
  network.rs           depends on: src/cluster/mod.rs (HTTP client)
  log_entry.rs         depends on: src/table/mod.rs (RowDelta)
  snapshot_transport.rs depends on: src/wal/snapshot.rs

src/cluster/mod.rs     MODIFIED: add Arc<RaftNode> field to ClusterBus
src/cluster/fanout.rs  MODIFIED: disable direct fan-out in Raft mode
src/cluster/gossip.rs  UNCHANGED (Raft heartbeats are separate; gossip
                       continues for /cluster/health monitoring)
src/cluster/proxy.rs   MODIFIED: forward to Raft leader instead of shard owner

src/main.rs            MODIFIED: initialize Raft, wire into worker loop
src/config.rs          MODIFIED: add raft_* config fields
src/wal/batch_writer.rs MODIFIED: add truncate_suffix() for Raft log truncation
src/wal/entry.rs       MODIFIED: add term field to WalHeader

src/network/websocket.rs MODIFIED: worker loop uses raft.client_write()
src/subscriptions.rs   MODIFIED: add read_consistency to SubscriptionFilter
```

## Appendix B: Config Fields to Add

```toml
[server]
# Enable Raft consensus (default: false for backward compat)
raft_enabled = true

# Election timeout range in ms (randomized per node)
raft_election_timeout_min_ms = 150
raft_election_timeout_max_ms = 300

# Heartbeat interval in ms (must be < election_timeout_min / 2)
raft_heartbeat_interval_ms = 50

# Maximum entries per AppendEntries RPC
raft_max_entries_per_rpc = 256

# Snapshot trigger: build snapshot after this many committed entries
raft_snapshot_threshold = 100000

# Maximum in-flight AppendEntries RPCs per follower
raft_max_in_flight = 4
```

Corresponding env vars: `VOLTRA_RAFT_ENABLED`, `VOLTRA_RAFT_ELECTION_TIMEOUT_MIN_MS`, etc.
