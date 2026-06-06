use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use neondb_client::{NeonDbClient, NeonDbClientOptions};
use neondb_client::types::{ClientMessage, ServerMessage};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn sends_authorization_header_when_api_key_set() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let seen_auth: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let seen_auth_s = seen_auth.clone();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ws_stream = tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, resp: Response| {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            *seen_auth_s.lock().unwrap() = auth;
            Ok(resp)
        }).await.unwrap();

        let (mut w, mut r) = ws_stream.split();
        if let Some(Ok(Message::Binary(bytes))) = r.next().await {
            let msg: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
            if let ClientMessage::ReducerCall(call) = msg {
                let result_bytes = rmp_serde::to_vec(&serde_json::json!({"ok": true})).unwrap();
                let resp = neondb_client::types::ReducerResponse {
                    call_id: call.call_id,
                    success: true,
                    result: Some(result_bytes),
                    error: None,
                };
                let out = rmp_serde::to_vec(&resp).unwrap();
                let _ = w.send(Message::Binary(out)).await;
            }
        }
    });

    let client = NeonDbClient::connect(NeonDbClientOptions {
        url: format!("ws://{}", addr),
        api_key: Some("secret".to_string()),
        identity: None,
        call_timeout: std::time::Duration::from_millis(1_000),
    })
    .await
    .unwrap();

    let out = client.call_args("increment", &("score", 1i32)).await.unwrap();
    assert!(out.is_some());

    assert_eq!(
        seen_auth.lock().unwrap().clone().unwrap(),
        "Bearer secret".to_string()
    );
}

#[tokio::test]
async fn supports_two_frame_subscription_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (mut w, mut r) = ws_stream.split();

        // Expect a Subscribe
        let sub_id = if let Some(Ok(Message::Binary(bytes))) = r.next().await {
            let msg: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
            match msg {
                ClientMessage::Subscribe {
                    subscription_id, ..
                } => subscription_id,
                _ => panic!("expected Subscribe"),
            }
        } else {
            panic!("no message");
        };

        // Send two-frame: route then body
        let route = ServerMessage::SubscriptionRoute(neondb_client::types::SubscriptionRoute {
            subscription_ids: vec![sub_id.clone()],
        });
        let body = ServerMessage::SubscriptionBody(neondb_client::types::SubscriptionBody {
            table_name: "players".to_string(),
            row_key: "p1".to_string(),
            operation: "update".to_string(),
            row_data: Some(serde_json::json!({"hp": 100})),
        });
        w.send(Message::Binary(rmp_serde::to_vec(&route).unwrap()))
            .await
            .unwrap();
        w.send(Message::Binary(rmp_serde::to_vec(&body).unwrap()))
            .await
            .unwrap();
    });

    let client = NeonDbClient::connect(NeonDbClientOptions {
        url: format!("ws://{}", addr),
        api_key: None,
        identity: None,
        call_timeout: std::time::Duration::from_millis(1_000),
    })
    .await
    .unwrap();

    let mut sub = client.subscribe("players").await.unwrap();
    let diff = tokio::time::timeout(std::time::Duration::from_millis(1_000), sub.rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(diff.subscription_id, sub.id);
    assert_eq!(diff.table_name, "players");
    assert_eq!(diff.row_key, "p1");
    assert_eq!(diff.operation, "update");
}
