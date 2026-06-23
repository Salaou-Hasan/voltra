// ============================================================================
// replication/mod.rs — WAL streaming replication (primary → replica)
//
// DESIGN (asynchronous log-shipping, single primary):
//   - The PRIMARY serves committed WAL entries over HTTP:
//       GET <metrics_port>/replication/wal?from_seq=N&max=M
//     Response: { "entries": ["<base64(rmp(WalEntry))>", ...], "last_seq": N }
//   - A REPLICA starts with VOLTRA_ROLE=replica + VOLTRA_PRIMARY_URL set.
//     It polls the primary every `poll_ms`, applies each entry's deltas to its
//     local TableStore, fans out to its own subscribers, and appends the entry
//     to its own WAL (so a replica crash recovers locally without re-syncing
//     the full history).
//   - Replicas REJECT reducer calls ("read-only replica") — clients can still
//     subscribe and read.  On primary failure, promote the replica:
//       POST /replication/promote
//     which atomically flips the read-only flag and stops the pull loop.
//
// CONSISTENCY MODEL: asynchronous replication.  A write acknowledged by the
// primary may be lost if the primary dies before the replica's next poll
// (bounded by poll_ms).  This is the same model as default PostgreSQL
// streaming replication.  For game backends this is the right trade-off:
// writes stay fast (no cross-node round trip) and failover loses at most
// poll_ms worth of data.
// ============================================================================

use crate::error::Result;
use crate::subscriptions::SubscriptionManager;
use crate::table::TableStore;
use crate::wal::{BatchedWalWriter, WalEntry, WalReader};
use base64::Engine as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

// ── Global role flag ──────────────────────────────────────────────────────────

static IS_REPLICA: AtomicBool = AtomicBool::new(false);
static REPLICA_LAST_APPLIED_SEQ: AtomicU64 = AtomicU64::new(0);
static REPLICA_LAG_ENTRIES: AtomicU64 = AtomicU64::new(0);
/// True when this node has detected its own health degrading and is draining
/// existing connections before a graceful shutdown/restart.
/// Set by the health monitor in main.rs; read by /cluster/health.
static DRAINING: AtomicBool = AtomicBool::new(false);
// Primary metrics URL — set once when the replica pull loop starts.
static PRIMARY_URL_CACHE: OnceLock<String> = OnceLock::new();

// ── WAL push bus (primary side) ───────────────────────────────────────────────
//
// After each committed WAL batch the primary calls `wal_push_bus().broadcast(batch)`.
// Relay processes that connect via `GET /replication/wal-push` register a receiver
// and receive WAL entries as they are committed — zero polling latency.
//
// ponytail: tokio broadcast, 64-entry buffer per relay.  Drop-on-lagged is fine:
//           the relay falls back to `/replication/wal?from_seq=N` for the gap.

use tokio::sync::broadcast;

pub struct WalPushBus {
    tx: broadcast::Sender<Vec<String>>, // base64(rmp(WalEntry)) per batch
}

impl WalPushBus {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        WalPushBus { tx }
    }

    /// Publish a freshly committed batch to all connected relays.
    pub fn broadcast(&self, encoded: Vec<String>) {
        if !encoded.is_empty() {
            let _ = self.tx.send(encoded);
        }
    }

    /// Subscribe a new relay to the push stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Vec<String>> {
        self.tx.subscribe()
    }

    /// Count of currently connected relay listeners.
    pub fn relay_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

static WAL_PUSH_BUS: OnceLock<WalPushBus> = OnceLock::new();

pub fn wal_push_bus() -> &'static WalPushBus {
    WAL_PUSH_BUS.get_or_init(WalPushBus::new)
}

/// True when this node is a read-only replica.  Checked by the reducer worker
/// loop before executing any write.
pub fn is_replica() -> bool {
    IS_REPLICA.load(Ordering::Relaxed)
}

/// Set the replica flag.  `set_replica(false)` promotes this node to primary.
pub fn set_replica(replica: bool) {
    IS_REPLICA.store(replica, Ordering::Relaxed);
}

/// True when this node is in graceful drain mode (health thresholds exceeded).
/// New client connections should be routed elsewhere by the load balancer.
pub fn is_draining() -> bool {
    DRAINING.load(Ordering::Relaxed)
}

/// Set/clear the drain flag.  Called by the health monitor in main.rs.
pub fn set_draining(draining: bool) {
    DRAINING.store(draining, Ordering::Relaxed);
    if draining {
        log::warn!("[health] Node entering DRAIN mode — health thresholds exceeded. Route new connections elsewhere.");
    } else {
        log::info!("[health] Node leaving drain mode — health recovered.");
    }
}

/// Metrics URL of the primary node this replica pulls from.
/// Set on first call to `run_replica_loop`; used by worker threads to proxy
/// reducer calls transparently instead of rejecting them.
pub fn primary_url() -> Option<&'static str> {
    PRIMARY_URL_CACHE.get().map(String::as_str)
}

/// Forward a reducer call to the primary and return its `ReducerResponse`.
///
/// Called whenever `is_replica()` is true and a reducer call arrives.  Replicas
/// become transparent relay nodes — the client sees a normal success/error reply
/// with its original call_id.  Latency added = 1 intra-AZ HTTP round-trip.
pub fn relay_reducer_call(call: &crate::network::PendingCall) -> crate::network::ReducerResponse {
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    use crate::cluster::proxy::{ProxyCallRequest, ProxyCallResponse};
    use crate::network::ReducerResponse;

    let Some(purl) = primary_url() else {
        return ReducerResponse::error(call.call_id, "relay: no primary configured (set VOLTRA_PRIMARY_URL)".into());
    };

    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    let client = CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default()
    });

    let url = format!("{}/cluster/call", purl.trim_end_matches('/'));
    let req = ProxyCallRequest {
        reducer_name: call.reducer_name.clone(),
        args_b64: B64.encode(&call.args),
        caller_id: call.caller_id.clone(),
        caller_role: call.caller_role.clone(),
        target_shard_id: None,
    };

    match client.post(&url).json(&req).send() {
        Err(e) => ReducerResponse::error(call.call_id, format!("relay: primary unreachable: {e}")),
        Ok(resp) => match resp.json::<ProxyCallResponse>() {
            Err(e) => ReducerResponse::error(call.call_id, format!("relay: bad response: {e}")),
            Ok(pr) if !pr.ok => ReducerResponse::error(
                call.call_id,
                pr.error.unwrap_or_else(|| "primary rejected call".into()),
            ),
            Ok(pr) => {
                let bytes = pr.result_b64
                    .and_then(|s| B64.decode(&s).ok())
                    .unwrap_or_default();
                ReducerResponse::success(call.call_id, bytes)
            }
        },
    }
}

/// Last WAL sequence number applied from the primary (replica only).
pub fn last_applied_seq() -> u64 {
    REPLICA_LAST_APPLIED_SEQ.load(Ordering::Relaxed)
}

/// How many entries behind the primary this replica was at the last poll.
pub fn replication_lag() -> u64 {
    REPLICA_LAG_ENTRIES.load(Ordering::Relaxed)
}

// ── Primary side: serve WAL entries ───────────────────────────────────────────

/// Read committed WAL entries with sequence_number > `from_seq`, up to `max`
/// entries.  Returns (entries, highest_seq_in_wal).
///
/// Reads the on-disk WAL file directly — entries there are durably committed.
/// Snapshot rotation truncates the WAL; a replica that falls behind a rotation
/// must bootstrap from a backup/snapshot first (logged as a gap warning on the
/// replica side).
pub fn serve_wal_entries(wal_path: &Path, from_seq: u64, max: usize) -> Result<(Vec<WalEntry>, u64)> {
    if !wal_path.exists() {
        return Ok((Vec::new(), from_seq));
    }
    let mut reader = WalReader::open(wal_path)?;
    let all = reader.read_all_entries()?;
    let last_seq = all.iter().map(|e| e.header.sequence_number).max().unwrap_or(from_seq);
    let entries: Vec<WalEntry> = all
        .into_iter()
        .filter(|e| e.header.sequence_number > from_seq)
        .take(max)
        .collect();
    Ok((entries, last_seq))
}

/// Encode entries for the HTTP wire: base64(rmp(WalEntry)) per entry.
pub fn encode_entries(entries: &[WalEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| rmp_serde::to_vec(e).ok())
        .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes))
        .collect()
}

/// Decode entries from the HTTP wire.  Corrupt entries are skipped (the
/// checksum on each WalEntry catches payload corruption separately).
pub fn decode_entries(encoded: &[String]) -> Vec<WalEntry> {
    encoded
        .iter()
        .filter_map(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
        .filter_map(|bytes| rmp_serde::from_slice::<WalEntry>(&bytes).ok())
        .collect()
}

// ── Replica side: pull loop ───────────────────────────────────────────────────

/// Apply a batch of replicated entries locally.  Returns the number applied.
///
/// Each entry: verify checksum → apply deltas → fan out to local subscribers
/// → append to local WAL (for local crash recovery).
pub fn apply_replicated_entries(
    entries: &[WalEntry],
    tables: &TableStore,
    subs: &SubscriptionManager,
    wal_writer: &BatchedWalWriter,
    global_seq: &AtomicU64,
) -> usize {
    let mut applied = 0usize;
    let mut last = REPLICA_LAST_APPLIED_SEQ.load(Ordering::Relaxed);

    for entry in entries {
        let seq = entry.header.sequence_number;
        if seq <= last {
            continue; // duplicate from overlapping poll
        }
        if !entry.verify_checksum() {
            log::warn!("[replication] entry seq={} failed checksum, skipping", seq);
            continue;
        }
        if last > 0 && seq > last + 1 {
            log::warn!(
                "[replication] sequence gap: last applied {} but received {} — \
                 primary may have rotated its WAL; consider re-seeding this replica from a backup",
                last, seq
            );
        }

        let mut ok = true;
        for delta in &entry.payload.deltas {
            if let Err(e) = tables.apply_delta(delta) {
                log::error!("[replication] apply_delta failed at seq={}: {}", seq, e);
                ok = false;
                break;
            }
        }
        if !ok { continue; }

        if !entry.payload.deltas.is_empty() {
            subs.publish_deltas(&entry.payload.deltas);
        }
        if let Err(e) = wal_writer.append(entry, seq) {
            log::warn!("[replication] local WAL append failed at seq={}: {}", seq, e);
        }

        last = seq;
        applied += 1;
    }

    REPLICA_LAST_APPLIED_SEQ.store(last, Ordering::Relaxed);
    // Keep the local seq counter ahead of everything we've applied so that a
    // post-promotion write does not reuse a replicated sequence number.
    global_seq.fetch_max(last + 1, Ordering::Relaxed);
    applied
}

/// Long-running replica pull loop.  Polls the primary until shutdown fires or
/// the node is promoted (`set_replica(false)`).
///
/// When `auto_failover` is true, `failover_miss_count` consecutive unreachable
/// polls trigger an automatic self-promotion: the node flips to primary and
/// the loop exits.  A poll that reaches the primary (even with an HTTP error
/// or a malformed body) resets the miss counter — only genuine connectivity
/// failures count toward failover, so a transient bad response won't promote.
///
/// CAUTION: with a single replica this is last-write-wins on a network
/// partition — if the primary is alive but unreachable, both nodes accept
/// writes and diverge.  Acceptable for the single-replica HA case; use the
/// Raft cluster path if you need partition-safe quorum.
#[allow(clippy::too_many_arguments)]
pub async fn run_replica_loop(
    primary_url: String,
    tables: Arc<TableStore>,
    subs: Arc<SubscriptionManager>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<AtomicU64>,
    poll_ms: u64,
    auto_failover: bool,
    failover_miss_count: u32,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) {
    let client = reqwest::Client::new();
    let base = primary_url.trim_end_matches('/').to_string();
    // Cache for worker threads so they can proxy reducer calls to this primary.
    let _ = PRIMARY_URL_CACHE.set(base.clone());
    let miss_limit = failover_miss_count.max(1);
    log::info!("[replication] replica mode: pulling from {} every {}ms", base, poll_ms);
    if auto_failover {
        log::info!(
            "[replication] auto-failover ENABLED: promoting after {} consecutive unreachable polls (~{}ms)",
            miss_limit, miss_limit as u64 * poll_ms.max(50)
        );
    }

    let mut consecutive_misses: u32 = 0;

    loop {
        if !is_replica() {
            log::info!("[replication] promoted to primary — stopping pull loop");
            break;
        }

        let from_seq = REPLICA_LAST_APPLIED_SEQ.load(Ordering::Relaxed);
        let url = format!("{}/replication/wal?from_seq={}&max=2048", base, from_seq);

        match client.get(&url).timeout(std::time::Duration::from_secs(10)).send().await {
            Ok(resp) if resp.status().is_success() => {
                consecutive_misses = 0; // reachable
                match resp.json::<serde_json::Value>().await {
                    Ok(body) => {
                        let encoded: Vec<String> = body
                            .get("entries")
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect())
                            .unwrap_or_default();
                        let primary_last = body.get("last_seq").and_then(|v| v.as_u64()).unwrap_or(0);

                        if !encoded.is_empty() {
                            let entries = decode_entries(&encoded);
                            let n = apply_replicated_entries(&entries, &tables, &subs, &wal_writer, &global_seq);
                            if n > 0 {
                                log::debug!("[replication] applied {} entries (now at seq {})", n, last_applied_seq());
                            }
                        }
                        let lag = primary_last.saturating_sub(REPLICA_LAST_APPLIED_SEQ.load(Ordering::Relaxed));
                        REPLICA_LAG_ENTRIES.store(lag, Ordering::Relaxed);
                    }
                    Err(e) => log::warn!("[replication] bad response from primary: {}", e),
                }
            }
            // Reachable but returned an error status — not a connectivity failure.
            Ok(resp) => {
                consecutive_misses = 0;
                log::warn!("[replication] primary returned HTTP {}", resp.status());
            }
            // Genuine connectivity failure — counts toward auto-failover.
            Err(e) => {
                consecutive_misses += 1;
                log::warn!(
                    "[replication] cannot reach primary at {} ({}/{}): {}",
                    base, consecutive_misses, miss_limit, e
                );
                if auto_failover && consecutive_misses >= miss_limit {
                    log::warn!(
                        "[replication] AUTO-FAILOVER: primary unreachable for {} consecutive polls \
                         — promoting this node to PRIMARY (now accepting writes)",
                        consecutive_misses
                    );
                    set_replica(false);
                    // Ensure post-promotion writes never reuse a replicated seq.
                    global_seq.fetch_max(last_applied_seq() + 1, Ordering::Relaxed);
                    break;
                }
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(poll_ms.max(50))) => {}
            _ = shutdown.changed() => break,
        }
    }
}

/// Initialise replica state from the local WAL's highest sequence so a
/// restarted replica resumes from where it left off instead of re-pulling
/// the entire history.
pub fn init_replica_from_local_wal(highest_local_seq: u64) {
    REPLICA_LAST_APPLIED_SEQ.store(highest_local_seq, Ordering::Relaxed);
}

/// Returns a JSON status blob for the /replication/status endpoint.
pub fn status_json() -> serde_json::Value {
    serde_json::json!({
        "role": if is_replica() { "replica" } else { "primary" },
        "last_applied_seq": last_applied_seq(),
        "lag_entries": replication_lag(),
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::RowDelta;
    use crate::wal::WalWriter;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{}_{}_{}.wal", name, std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
        ))
    }

    fn delta(table: &str, key: &str, val: serde_json::Value) -> RowDelta {
        RowDelta {
            table_name: table.to_string(),
            operation: "insert".to_string(),
            row_key: key.to_string(),
            row_id: 0,
            shard_id: 0,
            payload_arc: None,
            row_data: Some(val),
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let entries = vec![
            WalEntry::new(1000, 1, "spawn".into(), vec![1, 2], vec![delta("players", "a", serde_json::json!({"hp": 10}))]),
            WalEntry::new(1001, 2, "spawn".into(), vec![3, 4], vec![delta("players", "b", serde_json::json!({"hp": 20}))]),
        ];
        let wire = encode_entries(&entries);
        assert_eq!(wire.len(), 2);
        let back = decode_entries(&wire);
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].header.sequence_number, 1);
        assert_eq!(back[1].header.sequence_number, 2);
        assert!(back[0].verify_checksum());
        assert!(back[1].verify_checksum());
    }

    #[test]
    fn test_decode_skips_garbage() {
        let wire = vec!["!!!not-base64!!!".to_string(), base64::engine::general_purpose::STANDARD.encode(b"not msgpack")];
        let back = decode_entries(&wire);
        assert!(back.is_empty());
    }

    #[test]
    fn test_serve_wal_entries_filters_by_seq() {
        let path = tmp_path("test_repl_serve");
        let mut w = WalWriter::open(&path).unwrap();
        for seq in 1..=5u64 {
            w.append(&WalEntry::new(1000 + seq, seq, "inc".into(), vec![], vec![])).unwrap();
        }
        w.fsync().unwrap();
        drop(w);

        let (entries, last) = serve_wal_entries(&path, 2, 100).unwrap();
        assert_eq!(entries.len(), 3); // seqs 3, 4, 5
        assert_eq!(entries[0].header.sequence_number, 3);
        assert_eq!(last, 5);

        let (entries2, _) = serve_wal_entries(&path, 2, 2).unwrap();
        assert_eq!(entries2.len(), 2); // max cap respected

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_serve_missing_wal_returns_empty() {
        let path = tmp_path("test_repl_missing");
        let (entries, last) = serve_wal_entries(&path, 7, 100).unwrap();
        assert!(entries.is_empty());
        assert_eq!(last, 7);
    }

    #[test]
    fn test_apply_replicated_entries() {
        let tables = TableStore::new();
        let subs = SubscriptionManager::new();
        let wal_path = tmp_path("test_repl_apply");
        let wal_w = BatchedWalWriter::open(&wal_path, 50, 16, false).unwrap();
        let seq = AtomicU64::new(0);

        let entries = vec![
            WalEntry::new(1000, 1, "spawn".into(), vec![], vec![delta("players", "alice", serde_json::json!({"hp": 100}))]),
            WalEntry::new(1001, 2, "spawn".into(), vec![], vec![delta("players", "bob", serde_json::json!({"hp": 90}))]),
        ];

        // Reset globals (tests share process state).
        REPLICA_LAST_APPLIED_SEQ.store(0, Ordering::Relaxed);
        let n = apply_replicated_entries(&entries, &tables, &subs, &wal_w, &seq);
        assert_eq!(n, 2);
        assert_eq!(last_applied_seq(), 2);
        assert!(seq.load(Ordering::Relaxed) >= 3);

        let alice = tables.get_row("players", "alice").unwrap().unwrap();
        assert_eq!(alice["hp"], 100);

        // Re-applying the same batch is a no-op (idempotent).
        let n2 = apply_replicated_entries(&entries, &tables, &subs, &wal_w, &seq);
        assert_eq!(n2, 0);

        std::fs::remove_file(&wal_path).ok();
    }

    #[test]
    fn test_promote_flips_role() {
        set_replica(true);
        assert!(is_replica());
        set_replica(false);
        assert!(!is_replica());
    }
}
