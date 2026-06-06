use futures::{future::join_all, SinkExt, StreamExt};
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use neondb::network::message::{ClientMessage, ReducerCall, ServerMessage};

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
    ensure_server_built();
    let binary = server_binary_path();

    let mut cmd = Command::new(binary);
    cmd.arg("start")
        .env("NEONDB_HOST", "127.0.0.1")
        .env("NEONDB_PORT", port.to_string())
        .env("NEONDB_WAL_PATH", wal_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    cmd.spawn().expect("Failed to spawn NeonDB server")
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
            assert!(message.len() > 0, "Expected error message on invalid payload");
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

    let ack: ServerMessage = rmp_serde::from_slice(&ack_bytes).expect("Failed to decode subscribe ack");
    match ack {
        ServerMessage::SubscriptionAck { subscription_id, success, message } => {
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
