use futures::{future::join_all, SinkExt, StreamExt};
use neondb::network::message::{ClientMessage, ReducerCall, ServerMessage};
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Serialize, Deserialize)]
struct IncrementArgs {
    name: String,
    delta: i32,
}

#[derive(Serialize, Deserialize)]
struct IncrementResult {
    new_value: i32,
    timestamp: i64,
}

fn server_binary_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file_name = if cfg!(windows) {
        "neondb.exe"
    } else {
        "neondb"
    };
    manifest_dir.join("target").join("debug").join(file_name)
}

fn ensure_server_built() {
    let binary = server_binary_path();
    if binary.exists() {
        return;
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("cargo")
        .arg("build")
        .current_dir(&manifest_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("Failed to invoke cargo build");

    assert!(status.success(), "cargo build failed");
    assert!(binary.exists(), "Server binary not found after build");
}

fn spawn_server(port: u16, wal_path: PathBuf) -> Child {
    spawn_server_with_env(port, wal_path, &[])
}

fn spawn_server_with_env(port: u16, wal_path: PathBuf, extra_env: &[(&str, &str)]) -> Child {
    ensure_server_built();
    let binary = server_binary_path();

    let mut cmd = Command::new(binary);
    cmd.arg("start")
        .env("NEONDB_HOST", "127.0.0.1")
        .env("NEONDB_PORT", port.to_string())
        .env("NEONDB_WAL_PATH", wal_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    cmd.spawn().expect("Failed to spawn NeonDB server")
}

/// Build a proper WebSocket upgrade request with an `Authorization: Bearer` header.
fn bearer_request(url: &str, api_key: &str) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut req = url.into_client_request().expect("valid ws url");
    req.headers_mut().insert(
        "authorization",
        format!("Bearer {}", api_key)
            .parse()
            .expect("valid header value"),
    );
    req
}

/// Wait for the server to accept WebSocket connections that include a Bearer token.
async fn wait_for_server_ready_with_auth(url: &str, timeout: Duration, api_key: &str) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio_tungstenite::connect_async(bearer_request(url, api_key))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server did not become ready within {:?}", timeout);
}

async fn wait_for_server_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio_tungstenite::connect_async(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Server did not become ready within {:?}", timeout);
}

#[tokio::test]
async fn integration_basic_increment_via_websocket() {
    let port = 18080;
    let wal_path = std::env::temp_dir().join("neondb_integration_test.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);

    wait_for_server_ready(&ws_url, Duration::from_secs(5)).await;

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("Failed to connect to NeonDB server");

    let args = IncrementArgs {
        name: "score".to_string(),
        delta: 5,
    };

    let mut args_buf = Vec::new();
    args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

    let call = neondb::network::message::ReducerCall {
        call_id: 1,
        reducer_name: "increment".to_string(),
        args: args_buf,
    };

    let mut call_buf = Vec::new();
    call.serialize(&mut Serializer::new(&mut call_buf)).unwrap();

    ws_stream
        .send(Message::Binary(call_buf))
        .await
        .expect("Failed to send reducer call");

    let msg = ws_stream
        .next()
        .await
        .expect("Expected a response")
        .expect("Failed to read response");

    let response_bytes = match msg {
        Message::Binary(data) => data,
        _ => panic!("Unexpected message type"),
    };

    let response: neondb::network::message::ReducerResponse =
        rmp_serde::from_slice(&response_bytes).expect("Failed to deserialize response");

    assert!(
        response.success,
        "Server returned error: {:?}",
        response.error
    );

    let result_bytes = response.result.expect("Expected serialized result bytes");

    let result: IncrementResult =
        rmp_serde::from_slice(&result_bytes).expect("Failed to deserialize reducer result");
    assert_eq!(result.new_value, 5);
    assert!(result.timestamp > 0, "Expected positive timestamp");

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn integration_invalid_message_returns_error_but_server_stays_alive() {
    let port = 18082;
    let wal_path = std::env::temp_dir().join("neondb_integration_invalid.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready(&ws_url, Duration::from_secs(5)).await;

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("Failed to connect to NeonDB server");

    ws_stream
        .send(Message::Binary(vec![0x01, 0x02, 0x03, 0x04]))
        .await
        .expect("Failed to send invalid payload");

    let msg = ws_stream
        .next()
        .await
        .expect("Expected an error response")
        .expect("Failed to read response");

    let response_bytes = match msg {
        Message::Binary(data) => data,
        _ => panic!("Unexpected message type"),
    };

    let response: ServerMessage =
        rmp_serde::from_slice(&response_bytes).expect("Failed to deserialize response");

    match response {
        ServerMessage::Error { message } => {
            assert!(
                message.len() > 0,
                "Expected error message on invalid payload"
            );
        }
        other => panic!("Expected error response, got: {:?}", other),
    }

    let args = IncrementArgs {
        name: "resilient".to_string(),
        delta: 1,
    };
    let mut args_buf = Vec::new();
    args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

    let call = neondb::network::message::ReducerCall {
        call_id: 2,
        reducer_name: "increment".to_string(),
        args: args_buf,
    };

    let mut call_buf = Vec::new();
    call.serialize(&mut Serializer::new(&mut call_buf)).unwrap();

    ws_stream
        .send(Message::Binary(call_buf))
        .await
        .expect("Failed to send valid reducer call after invalid payload");

    let msg = ws_stream
        .next()
        .await
        .expect("Expected success response")
        .expect("Failed to read response");

    let response_bytes = match msg {
        Message::Binary(data) => data,
        _ => panic!("Unexpected message type"),
    };

    let response: neondb::network::message::ReducerResponse =
        rmp_serde::from_slice(&response_bytes).expect("Failed to deserialize response");

    assert!(response.success, "Expected success after invalid payload");

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn integration_parallel_clients() {
    let port = 18081;
    let wal_path = std::env::temp_dir().join("neondb_integration_parallel.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready(&ws_url, Duration::from_secs(5)).await;

    let ws_url_clone = ws_url.clone();
    let client_task = move |id: u64| {
        let ws_url = ws_url_clone.clone();
        async move {
            let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
                .await
                .expect("Failed to connect to NeonDB server");

            let args = IncrementArgs {
                name: format!("counter_{}", id),
                delta: 7,
            };
            let mut args_buf = Vec::new();
            args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

            let call = neondb::network::message::ReducerCall {
                call_id: id,
                reducer_name: "increment".to_string(),
                args: args_buf,
            };

            let mut call_buf = Vec::new();
            call.serialize(&mut Serializer::new(&mut call_buf)).unwrap();

            ws_stream
                .send(Message::Binary(call_buf))
                .await
                .expect("Failed to send reducer call");

            let msg = ws_stream
                .next()
                .await
                .expect("Expected a response")
                .expect("Failed to read response");

            let response_bytes = match msg {
                Message::Binary(data) => data,
                _ => panic!("Unexpected message type"),
            };

            let response: neondb::network::message::ReducerResponse =
                rmp_serde::from_slice(&response_bytes).expect("Failed to deserialize response");

            assert!(
                response.success,
                "Client {} received error: {:?}",
                id, response.error
            );
        }
    };

    let tasks = vec![client_task(1), client_task(2)];
    join_all(tasks).await;

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn integration_subscription_notifications() {
    let port = 18083;
    let wal_path = std::env::temp_dir().join("neondb_integration_subscription.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready(&ws_url, Duration::from_secs(5)).await;

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("Failed to connect to NeonDB server");

    let subscribe_message = ClientMessage::Subscribe {
        subscription_id: "sub1".to_string(),
        query: "counters where row_key == player1".to_string(),
    };
    let subscribe_bytes = rmp_serde::to_vec(&subscribe_message).unwrap();
    ws_stream
        .send(Message::Binary(subscribe_bytes))
        .await
        .expect("Failed to send subscribe command");

    let ack_msg = ws_stream
        .next()
        .await
        .expect("Expected subscribe ack")
        .expect("Failed to read subscribe ack");

    let ack_bytes = match ack_msg {
        Message::Binary(data) => data,
        _ => panic!("Unexpected message type for subscribe ack"),
    };

    let ack: ServerMessage =
        rmp_serde::from_slice(&ack_bytes).expect("Failed to decode subscribe ack");
    match ack {
        ServerMessage::SubscriptionAck {
            subscription_id,
            success,
            message,
        } => {
            assert_eq!(subscription_id, "sub1");
            assert!(success, "Subscription ack reported failure: {:?}", message);
        }
        _ => panic!("Expected SubscriptionAck, got: {:?}", ack),
    }

    let args = IncrementArgs {
        name: "player1".to_string(),
        delta: 2,
    };
    let mut args_buf = Vec::new();
    args.serialize(&mut Serializer::new(&mut args_buf)).unwrap();

    let call = ReducerCall {
        call_id: 1,
        reducer_name: "increment".to_string(),
        args: args_buf,
    };
    let mut call_buf = Vec::new();
    call.serialize(&mut Serializer::new(&mut call_buf)).unwrap();

    ws_stream
        .send(Message::Binary(call_buf))
        .await
        .expect("Failed to send reducer call");

    let mut found_diff = false;
    for _ in 0..5 {
        if let Some(msg) = ws_stream.next().await {
            let msg = msg.expect("Failed to read message");
            if let Message::Binary(data) = msg {
                if let Ok(ServerMessage::SubscriptionDiff(diff)) = rmp_serde::from_slice(&data) {
                    assert_eq!(diff.subscription_id, "sub1");
                    assert_eq!(diff.row_key, "player1");
                    assert_eq!(diff.table_name, "counters");
                    assert_eq!(diff.operation, "insert");
                    assert!(diff.row_data.is_some());
                    found_diff = true;
                    break;
                }
            }
        }
    }

    assert!(found_diff, "Did not receive subscription diff notification");

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

/// A server configured with NEONDB_API_KEY must reject connections that do not
/// supply a matching `Authorization: Bearer <key>` header.
#[tokio::test]
async fn integration_api_key_rejects_unauthorized() {
    let port = 18084;
    let wal_path = std::env::temp_dir().join("neondb_integration_auth.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child =
        spawn_server_with_env(port, wal_path.clone(), &[("NEONDB_API_KEY", "supersecret")]);
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready_with_auth(&ws_url, Duration::from_secs(5), "supersecret").await;

    // Connection with no Authorization header must be rejected.
    let plain = tokio_tungstenite::connect_async(&ws_url).await;
    assert!(
        plain.is_err(),
        "Connection without API key should be rejected, but succeeded"
    );

    // Connection with wrong key must also be rejected.
    let wrong = tokio_tungstenite::connect_async(bearer_request(&ws_url, "wrongkey")).await;
    assert!(
        wrong.is_err(),
        "Connection with wrong API key should be rejected, but succeeded"
    );

    // Connection with correct key must succeed.
    let good = tokio_tungstenite::connect_async(bearer_request(&ws_url, "supersecret")).await;
    assert!(
        good.is_ok(),
        "Connection with correct API key should succeed"
    );

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

/// A server with no API key set must accept all connections (open access).
#[tokio::test]
async fn integration_no_api_key_accepts_all() {
    let port = 18085;
    let wal_path = std::env::temp_dir().join("neondb_integration_noauth.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready(&ws_url, Duration::from_secs(5)).await;

    // Plain connection (no headers) must succeed.
    let plain = tokio_tungstenite::connect_async(&ws_url).await;
    assert!(
        plain.is_ok(),
        "Connection without key should succeed when no API key is configured"
    );

    // Connection with any Authorization header must also succeed.
    let keyed = tokio_tungstenite::connect_async(bearer_request(&ws_url, "whatever")).await;
    assert!(
        keyed.is_ok(),
        "Connection with extra auth header should still succeed when no API key is configured"
    );

    child.kill().expect("Failed to kill server process");
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

/// End-to-end throughput smoke test.
/// Spawns the server, runs 5 clients × 100 calls, asserts > 100 TPS.
/// Skipped in normal `cargo test` — run with `cargo test -- --include-ignored`.
#[tokio::test]
#[ignore = "e2e perf test — run explicitly with --include-ignored"]
async fn integration_e2e_throughput_benchmark() {
    use std::time::Instant;

    let port = 18090u16;
    let wal_path = std::env::temp_dir().join("neondb_e2e_bench_test.wal");
    let _ = std::fs::remove_file(&wal_path);

    let mut child = spawn_server(port, wal_path.clone());
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready(&ws_url, Duration::from_secs(10)).await;

    let num_clients = 5usize;
    let calls_per_client = 100usize;
    let total_expected = num_clients * calls_per_client;

    let start = Instant::now();
    let tasks: Vec<_> = (0..num_clients)
        .map(|id| {
            let url = ws_url.clone();
            tokio::spawn(async move {
                let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
                let args = rmp_serde::to_vec(&IncrementArgs {
                    name: "bench_test".to_string(),
                    delta: 1,
                })
                .unwrap();
                let mut ok = 0usize;
                for i in 0..calls_per_client {
                    let call_id = (id as u64) * 10_000 + i as u64;
                    let call = neondb::ReducerCall {
                        call_id,
                        reducer_name: "increment".to_string(),
                        args: args.clone(),
                    };
                    let frame = rmp_serde::to_vec(&call).unwrap();
                    if ws.send(Message::Binary(frame)).await.is_ok() {
                        if let Ok(Some(Ok(Message::Binary(_)))) =
                            tokio::time::timeout(Duration::from_secs(5), ws.next()).await
                        {
                            ok += 1;
                        }
                    }
                }
                ok
            })
        })
        .collect();

    let mut total_success = 0usize;
    for t in tasks {
        if let Ok(n) = t.await {
            total_success += n;
        }
    }
    let elapsed = start.elapsed();
    let tps = total_success as f64 / elapsed.as_secs_f64();

    child.kill().ok();
    child.wait().ok();
    let _ = std::fs::remove_file(&wal_path);

    println!(
        "\ne2e benchmark: {}/{} calls in {:.2}s = {:.0} TPS",
        total_success,
        total_expected,
        elapsed.as_secs_f64(),
        tps
    );

    assert!(
        total_success == total_expected,
        "Expected {} successes, got {}",
        total_expected,
        total_success
    );
    assert!(tps > 100.0, "Expected > 100 TPS, got {:.0}", tps);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TODO-022: Role-based permissions integration tests (Session 30)
// ═══════════════════════════════════════════════════════════════════════════════

/// Helper: send one ReducerCall over an open WebSocket and return the response.
async fn send_call(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    call_id: u64,
    reducer: &str,
    args_bytes: Vec<u8>,
) -> neondb::network::message::ReducerResponse {
    use rmp_serde::Serializer;
    use serde::Serialize;

    let call = neondb::network::message::ReducerCall {
        call_id,
        reducer_name: reducer.to_string(),
        args: args_bytes,
    };
    let mut buf = Vec::new();
    call.serialize(&mut Serializer::new(&mut buf)).unwrap();
    ws.send(Message::Binary(buf))
        .await
        .expect("send reducer call");

    let msg = ws
        .next()
        .await
        .expect("expected response")
        .expect("ws error");
    let bytes = match msg {
        Message::Binary(b) => b,
        _ => panic!("expected binary response"),
    };
    rmp_serde::from_slice(&bytes).expect("deserialize ReducerResponse")
}

/// A caller with no role must be REJECTED when the server restricts `increment`
/// to the "admin" role.
///
/// Setup: server has `NEONDB_API_KEY=perm_key` and
///        `NEONDB_PERMISSIONS={"increment":["admin"]}`.
/// Client connects with `Bearer perm_key` (no :<role> suffix → role = "").
/// Expected: response.success == false with a "Permission denied" error.
#[tokio::test]
async fn integration_permissions_unauthorized_call_rejected() {
    let port = 18091u16;
    let wal_path = std::env::temp_dir().join("neondb_perms_reject.wal");
    let _ = std::fs::remove_file(&wal_path);

    // Restrict "increment" to admin role only.
    let perms_json = r#"{"increment":["admin"]}"#;
    let mut child = spawn_server_with_env(
        port,
        wal_path.clone(),
        &[
            ("NEONDB_API_KEY", "perm_key"),
            ("NEONDB_PERMISSIONS", perms_json),
        ],
    );
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready_with_auth(&ws_url, Duration::from_secs(5), "perm_key").await;

    // Connect with correct key but NO role suffix.
    let (mut ws, _) = tokio_tungstenite::connect_async(bearer_request(&ws_url, "perm_key"))
        .await
        .expect("connect");

    let args = rmp_serde::to_vec(&IncrementArgs {
        name: "perms_test".to_string(),
        delta: 1,
    })
    .unwrap();

    let resp = send_call(&mut ws, 1, "increment", args).await;

    assert!(
        !resp.success,
        "Call should be rejected: role='' not in [admin], got success=true"
    );
    let err = resp.error.unwrap_or_default();
    assert!(
        err.to_lowercase().contains("permission denied")
            || err.to_lowercase().contains("not allowed"),
        "Expected 'permission denied' in error, got: {}",
        err
    );

    child.kill().ok();
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

/// A caller with the correct role must be ALLOWED even when the reducer is
/// restricted.
///
/// Setup: same server config as above.
/// Client connects with `Bearer perm_key:admin` → role = "admin".
/// Expected: response.success == true.
#[tokio::test]
async fn integration_permissions_authorized_call_passes() {
    let port = 18092u16;
    let wal_path = std::env::temp_dir().join("neondb_perms_allow.wal");
    let _ = std::fs::remove_file(&wal_path);

    let perms_json = r#"{"increment":["admin"]}"#;
    let mut child = spawn_server_with_env(
        port,
        wal_path.clone(),
        &[
            ("NEONDB_API_KEY", "perm_key2"),
            ("NEONDB_PERMISSIONS", perms_json),
        ],
    );
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready_with_auth(&ws_url, Duration::from_secs(5), "perm_key2").await;

    // Connect with key:role suffix — role = "admin".
    let (mut ws, _) =
        tokio_tungstenite::connect_async(bearer_request(&ws_url, "perm_key2:admin"))
            .await
            .expect("connect with role");

    let args = rmp_serde::to_vec(&IncrementArgs {
        name: "perms_admin".to_string(),
        delta: 5,
    })
    .unwrap();

    let resp = send_call(&mut ws, 1, "increment", args).await;

    assert!(
        resp.success,
        "Admin role should be allowed to call increment; error: {:?}",
        resp.error
    );

    child.kill().ok();
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}

/// An unrestricted reducer must be callable regardless of the caller's role
/// (including no role at all).
///
/// Setup: server restricts only `delete_user` to ["admin"].
/// Client calls `increment` (not in permissions map) with no role.
/// Expected: response.success == true — open reducers are always permitted.
#[tokio::test]
async fn integration_permissions_open_reducer_always_allowed() {
    let port = 18093u16;
    let wal_path = std::env::temp_dir().join("neondb_perms_open.wal");
    let _ = std::fs::remove_file(&wal_path);

    // Only "delete_user" is restricted; "increment" is open.
    let perms_json = r#"{"delete_user":["admin"]}"#;
    let mut child = spawn_server_with_env(
        port,
        wal_path.clone(),
        &[
            ("NEONDB_API_KEY", "perm_key3"),
            ("NEONDB_PERMISSIONS", perms_json),
        ],
    );
    let ws_url = format!("ws://127.0.0.1:{}", port);
    wait_for_server_ready_with_auth(&ws_url, Duration::from_secs(5), "perm_key3").await;

    // Connect with no role.
    let (mut ws, _) = tokio_tungstenite::connect_async(bearer_request(&ws_url, "perm_key3"))
        .await
        .expect("connect");

    let args = rmp_serde::to_vec(&IncrementArgs {
        name: "open_test".to_string(),
        delta: 3,
    })
    .unwrap();

    let resp = send_call(&mut ws, 1, "increment", args).await;

    assert!(
        resp.success,
        "Open reducer 'increment' must succeed with no role; error: {:?}",
        resp.error
    );

    child.kill().ok();
    let _ = child.wait();
    let _ = std::fs::remove_file(&wal_path);
}
