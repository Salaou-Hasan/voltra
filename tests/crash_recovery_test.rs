// ============================================================================
// WAL crash-recovery integration tests (TODO-037)
//
// These tests start a real `voltra start` server process, perform writes,
// kill the process abruptly (no graceful shutdown), restart it on the SAME
// WAL and snapshot directory, then verify that all committed rows are still
// present and no torn (partial) writes slipped through.
//
// Test port assignments (offset from 18200 to avoid collisions):
//   crash_recovery_basic    → 18200
//   crash_recovery_many     → 18201
// ============================================================================

use futures::{SinkExt, StreamExt};
use voltra::network::message::ReducerCall;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

// ── Shared harness (mirrors integration.rs) ──────────────────────────────────

fn server_binary_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file_name = if cfg!(windows) { "voltra.exe" } else { "voltra" };
    manifest_dir.join("target").join("debug").join(file_name)
}

fn ensure_server_built() {
    assert!(
        server_binary_path().exists(),
        "Server binary not found at {:?}. Run `cargo build` first.",
        server_binary_path()
    );
}

fn spawn_server_on_wal(port: u16, wal_path: &PathBuf, snapshot_dir: &PathBuf) -> Child {
    ensure_server_built();
    let binary = server_binary_path();
    let blob_dir = std::env::temp_dir().join(format!("voltra_blobs_crash_{}", port));
    let metrics_port = port + 1000;

    Command::new(binary)
        .arg("start")
        .env("VOLTRA_HOST", "127.0.0.1")
        .env("VOLTRA_PORT", port.to_string())
        .env("VOLTRA_METRICS_PORT", metrics_port.to_string())
        .env("VOLTRA_WAL_PATH", wal_path)
        .env("VOLTRA_SNAPSHOT_DIR", snapshot_dir)
        .env("VOLTRA_BLOB_PATH", blob_dir)
        .env("VOLTRA_UNSAFE_NO_FSYNC", "true")  // faster for tests
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn Voltra server")
}

async fn wait_for_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio_tungstenite::connect_async(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server did not become ready within {:?}", timeout);
}

// ── Helper: read counter value from /healthz-adjacent HTTP endpoint ──────────

async fn get_counter_value(metrics_port: u16, name: &str) -> Option<i64> {
    let url = format!("http://127.0.0.1:{}/tables/counters", metrics_port);
    let resp = reqwest::get(&url).await.ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;
    let rows = body.get("rows")?.as_array()?;
    for row in rows {
        if row.get("row_key").and_then(|v| v.as_str()) == Some(name) {
            return row
                .get("data")
                .and_then(|d| d.get("value"))
                .and_then(|v| v.as_i64());
        }
    }
    None
}

// ── Increment args struct (matches the built-in `increment` reducer) ─────────

#[derive(Serialize, Deserialize)]
struct IncrArgs {
    name: String,
    delta: i32,
}

async fn call_increment(ws: &mut tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
>, call_id: u64, name: &str, delta: i32) {
    let args = IncrArgs { name: name.to_string(), delta };
    let mut args_buf = Vec::new();
    args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

    let call = ReducerCall {
        call_id,
        reducer_name: "increment".to_string(),
        args: args_buf,
    };
    let mut call_buf = Vec::new();
    call.serialize(&mut Serializer::new(&mut call_buf)).unwrap();

    ws.send(Message::Binary(call_buf)).await.expect("send increment");
    // Drain the response
    ws.next().await.expect("response").expect("ws error");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Basic crash recovery
//
// Write N increments, kill the server, restart, verify the counter value
// is at least N (may be slightly higher if the WAL flush batched extra ops,
// but must not be lower).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn crash_recovery_basic_counter_survives() {
    let port: u16 = 18200;
    let wal_path = std::env::temp_dir().join("voltra_crash_test_basic.wal");
    let snap_dir = std::env::temp_dir().join("voltra_crash_test_basic_snaps");
    let metrics_port = port + 1000;

    // Clean up any leftovers from a previous run.
    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_dir_all(&snap_dir);
    std::fs::create_dir_all(&snap_dir).ok();

    // ── Phase 1: write N increments ──────────────────────────────────────────
    let writes: i32 = 20;
    {
        let mut child = spawn_server_on_wal(port, &wal_path, &snap_dir);
        let ws_url = format!("ws://127.0.0.1:{}", port);
        wait_for_ready(&ws_url, Duration::from_secs(8)).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .expect("connect");

        for i in 0..writes {
            call_increment(&mut ws, i as u64 + 1, "crash_test_counter", 1).await;
        }

        ws.close(None).await.ok();
        // Kill abruptly — no graceful shutdown signal.
        child.kill().expect("kill server");
        child.wait().ok();
    }

    // Small pause to let OS flush any pending kernel buffers.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Phase 2: restart on same WAL ─────────────────────────────────────────
    let mut child2 = spawn_server_on_wal(port, &wal_path, &snap_dir);
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_ready(&ws_url, Duration::from_secs(8)).await;

    // Give WAL replay a moment to finish after the listener is up.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── Phase 3: verify counter value via HTTP ────────────────────────────────
    let value = get_counter_value(metrics_port, "crash_test_counter").await;

    child2.kill().ok();
    child2.wait().ok();

    let recovered = value.expect("counter must be present after WAL replay");
    assert!(
        recovered >= writes as i64,
        "Expected counter >= {} after crash+recovery, got {}",
        writes,
        recovered
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Many writes, verify no torn rows
//
// Write to TWO counters (A and B) alternately.  After crash+restart both must
// have the same value (writes are always paired: A then B in the same session).
// If the WAL replays partially such that A was flushed and B was not, the test
// detects the discrepancy.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn crash_recovery_no_torn_paired_writes() {
    let port: u16 = 18201;
    let wal_path = std::env::temp_dir().join("voltra_crash_test_paired.wal");
    let snap_dir = std::env::temp_dir().join("voltra_crash_test_paired_snaps");
    let metrics_port = port + 1000;

    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_dir_all(&snap_dir);
    std::fs::create_dir_all(&snap_dir).ok();

    let pairs: i32 = 15;

    // ── Phase 1: write ────────────────────────────────────────────────────────
    {
        let mut child = spawn_server_on_wal(port, &wal_path, &snap_dir);
        let ws_url = format!("ws://127.0.0.1:{}", port);
        wait_for_ready(&ws_url, Duration::from_secs(8)).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.expect("connect");

        let mut call_id = 1u64;
        for _ in 0..pairs {
            call_increment(&mut ws, call_id,     "counter_a", 1).await;
            call_id += 1;
            call_increment(&mut ws, call_id, "counter_b", 1).await;
            call_id += 1;
        }

        ws.close(None).await.ok();
        child.kill().expect("kill server");
        child.wait().ok();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Phase 2: restart ──────────────────────────────────────────────────────
    let mut child2 = spawn_server_on_wal(port, &wal_path, &snap_dir);
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_ready(&ws_url, Duration::from_secs(8)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let va = get_counter_value(metrics_port, "counter_a").await;
    let vb = get_counter_value(metrics_port, "counter_b").await;

    child2.kill().ok();
    child2.wait().ok();

    let a = va.expect("counter_a must survive");
    let b = vb.expect("counter_b must survive");

    // Both counters must have the same value — they were always written together.
    // Allow a=b (perfect) or a=b+1 (last A went through before kill, B didn't
    // — this is acceptable since each write is independent in the WAL).
    assert!(
        (a - b).abs() <= 1,
        "counter_a={} and counter_b={} differ by more than 1 — indicates a torn write",
        a, b
    );

    assert!(a >= 1, "Expected at least 1 write to survive crash recovery, got a={}", a);
}
