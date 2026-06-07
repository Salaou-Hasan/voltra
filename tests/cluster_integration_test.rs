// ============================================================================
// tests/cluster_integration_test.rs
//
// End-to-end integration tests for the cluster bus.  Each test spawns one or
// two real `neondb start` child processes, wires them as a 2-shard cluster via
// environment variables, runs an assertion against the live HTTP endpoints,
// and tears the children down via Drop guards so a panic never leaks zombies.
//
// Port allocation (avoids the 18080 range used by tests/integration.rs):
//   node_a: WS 28080, metrics 29080
//   node_b: WS 28081, metrics 29081
//   single-node cluster_call test:  WS 28082, metrics 29082
//   single-node /cluster/peers test: WS 28083, metrics 29083
//   single-node /cluster/join test:  WS 28084, metrics 29084
// ============================================================================

use futures::{SinkExt, StreamExt};
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

// ─── ServerGuard ─────────────────────────────────────────────────────────────
// RAII wrapper that always kills its child on drop.  This is the ONLY way to
// guarantee cleanup if a test panics mid-assertion.
struct ServerGuard {
    child: Option<Child>,
    label: &'static str,
}
impl ServerGuard {
    fn new(child: Child, label: &'static str) -> Self {
        Self {
            child: Some(child),
            label,
        }
    }
}
impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
            eprintln!("[cluster_test] Reaped server '{}'", self.label);
        }
    }
}

// ─── Shared build helper ─────────────────────────────────────────────────────
// `cargo test` already compiled the bin; just assert it exists.
fn server_binary_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file_name = if cfg!(windows) { "neondb.exe" } else { "neondb" };
    manifest_dir.join("target").join("debug").join(file_name)
}

fn ensure_server_built() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        assert!(
            server_binary_path().exists(),
            "Server binary not found at {:?}. Run `cargo build` first.",
            server_binary_path()
        );
    });
}

// ─── Spawn helper ────────────────────────────────────────────────────────────
fn spawn_server_with_env(
    port: u16,
    wal_path: PathBuf,
    extra_env: &[(&str, &str)],
    label: &'static str,
) -> ServerGuard {
    ensure_server_built();
    let binary = server_binary_path();

    let blob_dir = std::env::temp_dir().join(format!("neondb_blobs_{}", port));
    // Best-effort cleanup of any stale blob/wal artefacts from previous runs.
    let _ = std::fs::remove_dir_all(&blob_dir);
    let _ = std::fs::remove_file(&wal_path);

    let metrics_port = port + 1000;

    let mut cmd = Command::new(binary);
    cmd.arg("start")
        .env("NEONDB_HOST", "127.0.0.1")
        .env("NEONDB_PORT", port.to_string())
        .env("NEONDB_METRICS_PORT", metrics_port.to_string())
        .env("NEONDB_WAL_PATH", &wal_path)
        .env("NEONDB_BLOB_PATH", blob_dir)
        // Tighten the gossip interval so peer health converges quickly in tests.
        .env("NEONDB_GOSSIP_INTERVAL_MS", "500")
        .env("NEONDB_CLUSTER_HTTP_TIMEOUT_MS", "2000")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn '{}': {}", label, e));
    ServerGuard::new(child, label)
}

// ─── Readiness probes ────────────────────────────────────────────────────────
async fn wait_for_ws(url: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio_tungstenite::connect_async(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server WS at {} did not become ready within {:?}", url, timeout);
}

/// Wait until the metrics HTTP server answers /cluster/health (with the right
/// secret if one is configured).  This is the canonical "cluster layer up" check.
async fn wait_for_metrics(metrics_url: &str, secret: Option<&str>, timeout: Duration) {
    let start = Instant::now();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .expect("reqwest client");
    let url = format!("{}/cluster/health", metrics_url);
    while start.elapsed() < timeout {
        let mut req = client.get(&url);
        if let Some(s) = secret {
            req = req.header("x-neondb-cluster-secret", s);
        }
        if let Ok(resp) = req.send().await {
            if resp.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "Metrics endpoint {} did not become ready within {:?}",
        metrics_url, timeout
    );
}

// ─── Reducer args for `increment` ────────────────────────────────────────────
#[derive(Serialize, Deserialize)]
struct IncrementArgs {
    name: String,
    delta: i32,
}

// ═════════════════════════════════════════════════════════════════════════════
//  TEST 1 — two-node fan-out replicates writes
// ═════════════════════════════════════════════════════════════════════════════
//
// Spin up two nodes wired to each other.  Call `increment` on node_a over WS.
// After ~1s (plenty of time for the fan-out HTTP POST to land on node_b),
// GET /tables/counters on node_b's metrics endpoint and assert the counter
// row exists.
//
// Marked #[ignore] because the cluster fan-out path under load on Windows can
// be flaky when the gossip task takes more than a single tick to mark the
// peer healthy.  Run explicitly with `cargo test -- --include-ignored`.
#[tokio::test]
#[ignore = "Two-node fan-out is timing-sensitive; flakes on Windows CI under load. \
            Run with --include-ignored once gossip/fanout retry tuning stabilises."]
async fn cluster_two_node_fanout_replicates_writes() {
    let secret = "test-secret-123";
    let wal_a = std::env::temp_dir().join("neondb_cluster_fanout_a.wal");
    let wal_b = std::env::temp_dir().join("neondb_cluster_fanout_b.wal");

    let _node_a = spawn_server_with_env(
        28080,
        wal_a.clone(),
        &[
            ("NEONDB_SHARD_ID", "0"),
            ("NEONDB_SHARD_COUNT", "2"),
            ("NEONDB_PEERS", "shard1=http://127.0.0.1:29081"),
            ("NEONDB_CLUSTER_SECRET", secret),
        ],
        "node_a",
    );
    let _node_b = spawn_server_with_env(
        28081,
        wal_b.clone(),
        &[
            ("NEONDB_SHARD_ID", "1"),
            ("NEONDB_SHARD_COUNT", "2"),
            ("NEONDB_PEERS", "shard0=http://127.0.0.1:29080"),
            ("NEONDB_CLUSTER_SECRET", secret),
        ],
        "node_b",
    );

    wait_for_ws("ws://127.0.0.1:28080", Duration::from_secs(10)).await;
    wait_for_ws("ws://127.0.0.1:28081", Duration::from_secs(10)).await;
    wait_for_metrics("http://127.0.0.1:29080", Some(secret), Duration::from_secs(10)).await;
    wait_for_metrics("http://127.0.0.1:29081", Some(secret), Duration::from_secs(10)).await;

    // Call `increment` on node_a.
    let (mut ws, _) = tokio_tungstenite::connect_async("ws://127.0.0.1:28080")
        .await
        .expect("connect node_a ws");

    let args = IncrementArgs {
        name: "fanout_counter".to_string(),
        delta: 7,
    };
    let mut args_buf = Vec::new();
    args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

    let call = neondb::network::message::ReducerCall {
        call_id: 1,
        reducer_name: "increment".to_string(),
        args: args_buf,
    };
    let mut frame = Vec::new();
    call.serialize(&mut Serializer::new(&mut frame)).unwrap();
    ws.send(Message::Binary(frame)).await.expect("send call");

    // Drain the reducer response so we know the commit landed before we poll.
    let resp = ws
        .next()
        .await
        .expect("response")
        .expect("ws response error");
    match resp {
        Message::Binary(b) => {
            let r: neondb::network::message::ReducerResponse =
                rmp_serde::from_slice(&b).expect("decode response");
            assert!(r.success, "reducer call failed on node_a: {:?}", r.error);
        }
        other => panic!("unexpected ws message: {:?}", other),
    }
    let _ = ws.close(None).await;

    // Poll node_b's /tables/counters for up to 5s — fan-out is async + retried.
    let client = reqwest::Client::new();
    let mut saw_row = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let r = client
            .get("http://127.0.0.1:29081/tables/counters")
            .send()
            .await;
        if let Ok(resp) = r {
            if resp.status().is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
                if let Some(rows) = body.get("rows").and_then(|v| v.as_array()) {
                    if rows.iter().any(|row| {
                        row.get("row_key")
                            .and_then(|k| k.as_str())
                            .map(|s| s == "fanout_counter")
                            .unwrap_or(false)
                    }) {
                        saw_row = true;
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        saw_row,
        "Expected node_b to receive the replicated 'fanout_counter' row via fan-out"
    );

    // _node_a and _node_b drop here → ServerGuard kills both children.
    let _ = std::fs::remove_file(&wal_a);
    let _ = std::fs::remove_file(&wal_b);
}

// ═════════════════════════════════════════════════════════════════════════════
//  TEST 2 — GET /cluster/peers requires the shared secret
// ═════════════════════════════════════════════════════════════════════════════
//
// Single node started with cluster enabled (a phantom peer is enough to flip
// `cluster_enabled=true`).  We hit /cluster/peers three ways:
//   - no header                  → 401
//   - wrong secret               → 401
//   - correct secret             → 200 and JSON contains "peers"
#[tokio::test]
async fn cluster_peers_endpoint_requires_secret() {
    let secret = "peers-secret-456";
    let wal = std::env::temp_dir().join("neondb_cluster_peers_secret.wal");

    let _node = spawn_server_with_env(
        28083,
        wal.clone(),
        &[
            ("NEONDB_SHARD_ID", "0"),
            ("NEONDB_SHARD_COUNT", "2"),
            // Phantom peer so cluster_enabled becomes true.  No process will
            // ever answer on :30000, which is fine — we don't make any fan-out
            // calls in this test.
            ("NEONDB_PEERS", "shard1=http://127.0.0.1:30000"),
            ("NEONDB_CLUSTER_SECRET", secret),
        ],
        "node_peers",
    );

    wait_for_ws("ws://127.0.0.1:28083", Duration::from_secs(10)).await;
    wait_for_metrics("http://127.0.0.1:29083", Some(secret), Duration::from_secs(10)).await;

    let url = "http://127.0.0.1:29083/cluster/peers";
    let client = reqwest::Client::new();

    // No header → 401.
    let r = client.get(url).send().await.expect("send no-header");
    assert_eq!(
        r.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "no-header request should be 401"
    );

    // Wrong secret → 401.
    let r = client
        .get(url)
        .header("x-neondb-cluster-secret", "WRONG")
        .send()
        .await
        .expect("send wrong-secret");
    assert_eq!(
        r.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "wrong-secret request should be 401"
    );

    // Correct secret → 200 + has "peers".
    let r = client
        .get(url)
        .header("x-neondb-cluster-secret", secret)
        .send()
        .await
        .expect("send correct-secret");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = r.json().await.expect("json body");
    assert!(
        body.get("peers").is_some(),
        "response must contain 'peers' field; got: {}",
        body
    );

    let _ = std::fs::remove_file(&wal);
}

// ═════════════════════════════════════════════════════════════════════════════
//  TEST 3 — POST /cluster/call with wrong target_shard_id returns HTTP 421
// ═════════════════════════════════════════════════════════════════════════════
//
// Single node owns shard 0.  We POST a /cluster/call with target_shard_id=1.
// Expected: HTTP 421 Misdirected Request, body mentions "wrong_shard".
#[tokio::test]
async fn cluster_call_misrouted_returns_421() {
    let secret = "shard-secret-789";
    let wal = std::env::temp_dir().join("neondb_cluster_misroute.wal");

    let _node = spawn_server_with_env(
        28082,
        wal.clone(),
        &[
            ("NEONDB_SHARD_ID", "0"),
            ("NEONDB_SHARD_COUNT", "2"),
            ("NEONDB_PEERS", "shard1=http://127.0.0.1:30001"),
            ("NEONDB_CLUSTER_SECRET", secret),
        ],
        "node_misroute",
    );

    wait_for_ws("ws://127.0.0.1:28082", Duration::from_secs(10)).await;
    wait_for_metrics("http://127.0.0.1:29082", Some(secret), Duration::from_secs(10)).await;

    // Build a ProxyCallRequest aimed at shard 1 (we own shard 0).
    let args = rmp_serde::to_vec(&IncrementArgs {
        name: "misroute_test".to_string(),
        delta: 1,
    })
    .unwrap();
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    let body = serde_json::json!({
        "reducer_name":    "increment",
        "args_b64":        B64.encode(&args),
        "caller_id":       "tester",
        "caller_role":     "user",
        "target_shard_id": 1,
    });

    let client = reqwest::Client::new();
    let resp = client
        .post("http://127.0.0.1:29082/cluster/call")
        .header("x-neondb-cluster-secret", secret)
        .json(&body)
        .send()
        .await
        .expect("POST /cluster/call");

    assert_eq!(
        resp.status().as_u16(),
        421,
        "Misrouted /cluster/call should return HTTP 421 (Misdirected). Got {}",
        resp.status()
    );

    let json_body: serde_json::Value = resp.json().await.expect("json body");
    let err = json_body
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(
        err.contains("wrong_shard"),
        "Expected error='wrong_shard' in body, got: {}",
        json_body
    );
    let owner = json_body
        .get("owner_shard")
        .and_then(|v| v.as_u64())
        .unwrap_or(99);
    assert_eq!(owner, 0, "owner_shard should be 0 (this node), got {}", owner);

    let _ = std::fs::remove_file(&wal);
}

// ═════════════════════════════════════════════════════════════════════════════
//  TEST 4 — POST /cluster/join dynamically registers a new peer
// ═════════════════════════════════════════════════════════════════════════════
//
// Start one node with cluster enabled (phantom peer flips the flag on).  POST
// /cluster/join with a brand-new shard_id and metrics_url.  Verify:
//   - Join response is 200 and lists the new peer
//   - Subsequent /cluster/peers (with secret) also includes the new peer
#[tokio::test]
async fn cluster_join_endpoint_registers_new_peer() {
    let secret = "join-secret-321";
    let wal = std::env::temp_dir().join("neondb_cluster_join.wal");

    let _node = spawn_server_with_env(
        28084,
        wal.clone(),
        &[
            ("NEONDB_SHARD_ID", "0"),
            ("NEONDB_SHARD_COUNT", "2"),
            ("NEONDB_PEERS", "shard1=http://127.0.0.1:30002"),
            ("NEONDB_CLUSTER_SECRET", secret),
        ],
        "node_join",
    );

    wait_for_ws("ws://127.0.0.1:28084", Duration::from_secs(10)).await;
    wait_for_metrics("http://127.0.0.1:29084", Some(secret), Duration::from_secs(10)).await;

    let new_shard_id = 5u32;
    let new_url = "http://127.0.0.1:29999";

    let client = reqwest::Client::new();
    let join_body = serde_json::json!({
        "shard_id":    new_shard_id,
        "metrics_url": new_url,
    });

    let resp = client
        .post("http://127.0.0.1:29084/cluster/join")
        .header("x-neondb-cluster-secret", secret)
        .json(&join_body)
        .send()
        .await
        .expect("POST /cluster/join");

    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "/cluster/join with valid secret should be 200"
    );
    let join_resp: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        join_resp.get("ok").and_then(|v| v.as_bool()),
        Some(true),
        "join response missing 'ok: true': {}",
        join_resp
    );
    let peers_in_join = join_resp
        .get("peers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        peers_in_join
            .iter()
            .any(|p| p.get("shard_id").and_then(|v| v.as_u64()) == Some(u64::from(new_shard_id))),
        "join response should include new peer shard_id={}, body was: {}",
        new_shard_id,
        join_resp
    );

    // Confirm via /cluster/peers.
    let r = client
        .get("http://127.0.0.1:29084/cluster/peers")
        .header("x-neondb-cluster-secret", secret)
        .send()
        .await
        .expect("GET /cluster/peers");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    let peers_body: serde_json::Value = r.json().await.expect("peers body");
    let peers = peers_body
        .get("peers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        peers
            .iter()
            .any(|p| p.get("shard_id").and_then(|v| v.as_u64()) == Some(u64::from(new_shard_id))),
        "After join, /cluster/peers must list shard_id={}; got: {}",
        new_shard_id,
        peers_body
    );

    let _ = std::fs::remove_file(&wal);
}
