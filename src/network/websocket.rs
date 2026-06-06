// ============================================================================
// NeonDB websocket.rs — high-throughput rewrite
//
// Session 7 — TODO-003: pass Arc<TableStore> into subscribe handler so new
//   clients receive initial_snapshot frames for all existing matching rows.
//
// Previous sessions:
//  1. SubscriptionManager in Arc (no Mutex) — DashMap inside.
//  2. kanal::AsyncSender<PendingCall> — true async send with back-pressure.
//  3. Arc<Bytes> pre-encoded frames — zero re-encoding per subscriber.
//  4. Dedicated write task owns the sink — no AsyncMutex contention.
// ============================================================================

use super::message::{ClientMessage, ReducerResponse, ServerMessage};
use super::protocol;
use crate::error::{NeonDBError, Result};
use crate::subscriptions::{OutboundFrames, SubscriptionManager};
use crate::table::TableStore;
use futures::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch::Receiver};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;

/// A pending reducer call with response channel.
pub struct PendingCall {
    pub call_id: u64,
    pub reducer_name: String,
    pub args: Vec<u8>,
    pub caller_id: String,
    pub response_tx: mpsc::UnboundedSender<ReducerResponse>,
}

struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Start the WebSocket listener.
pub async fn start_listener(
    addr: String,
    port: u16,
    reducer_tx: kanal::AsyncSender<PendingCall>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    max_connections: usize,
    api_key: Option<String>,
    active_connections: Arc<AtomicUsize>,
    mut shutdown: Receiver<()>,
) -> Result<()> {
    let bind_addr = format!("{}:{}", addr, port);
    let listener = TcpListener::bind(&bind_addr).await?;
    log::info!("WebSocket listener started on {}", bind_addr);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer_addr)) => {
                        log::debug!("New connection from {}", peer_addr);

                        if active_connections.load(Ordering::SeqCst) >= max_connections {
                            log::warn!("Connection limit reached: {}", max_connections);
                            drop(stream);
                            continue;
                        }

                        let tx      = reducer_tx.clone();
                        let subs    = subscription_manager.clone();
                        let tbl     = tables.clone();
                        let api_key = api_key.clone();
                        let conns   = active_connections.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, tx, subs, tbl, api_key, conns, peer_addr.to_string()).await {
                                log::warn!("Client error: {}", e);
                            }
                        });
                    }
                    Err(e) => log::error!("Accept error: {}", e),
                }
            }
            _ = shutdown.changed() => {
                log::info!("WebSocket listener shutdown requested");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_client(
    stream: TcpStream,
    reducer_tx: kanal::AsyncSender<PendingCall>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    api_key: Option<String>,
    active_connections: Arc<AtomicUsize>,
    peer_addr: String,
) -> Result<()> {
    // ── WebSocket handshake with optional Bearer auth ─────────────────────────
    // Capture the X-NeonDB-Identity header value from the upgrade request.
    let caller_id_cell = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_id_capture = caller_id_cell.clone();

    let auth_key = api_key.clone();
    let ws_stream = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &Request, response: Response| {
            if let Some(key) = auth_key.as_ref() {
                let auth = request
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if auth != format!("Bearer {}", key) {
                    return Err(ErrorResponse::new(Some("Unauthorized".to_string())));
                }
            }
            // Extract per-connection identity from the X-NeonDB-Identity header.
            // Falls back to the TCP peer address if not provided.
            if let Some(id) = request
                .headers()
                .get("x-neondb-identity")
                .and_then(|v| v.to_str().ok())
            {
                if let Ok(mut cell) = caller_id_capture.lock() {
                    *cell = id.to_string();
                }
            }
            Ok(response)
        },
    )
    .await
    .map_err(|e| NeonDBError::network_error(format!("WebSocket handshake error: {}", e)))?;

    // Resolve caller_id: X-NeonDB-Identity header if supplied, else TCP peer address.
    let caller_id: String = {
        let guard = caller_id_cell.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_empty() {
            peer_addr.clone()
        } else {
            guard.clone()
        }
    };

    let _conn_guard = ConnectionGuard(active_connections.clone());
    let current = active_connections.fetch_add(1, Ordering::SeqCst);
    log::debug!("Active WebSocket clients: {}", current + 1);

    let (ws_sink, mut ws_rx) = ws_stream.split();

    // ── Dedicated write task ──────────────────────────────────────────────────
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Message>();

    let write_task = {
        let mut sink = ws_sink;
        tokio::spawn(async move {
            while let Some(msg) = write_rx.recv().await {
                if let Err(e) = sink.send(msg).await {
                    log::warn!("WebSocket write error: {}", e);
                    break;
                }
            }
        })
    };

    // ── Reducer response task ─────────────────────────────────────────────────
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<ReducerResponse>();
    let write_tx_response = write_tx.clone();
    let response_task = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            match protocol::encode_response(&response) {
                Ok(data) => {
                    let _ = write_tx_response.send(Message::Binary(data));
                }
                Err(e) => log::warn!("Failed to encode response: {}", e),
            }
        }
    });

    // ── Register client ───────────────────────────────────────────────────────
    let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<OutboundFrames>();
    let client_id = subscription_manager.register_client(sub_tx);

    let write_tx_sub = write_tx.clone();
    let sub_task = tokio::spawn(async move {
        while let Some(frames) = sub_rx.recv().await {
            match frames {
                OutboundFrames::One(bytes) => {
                    let _ = write_tx_sub.send(Message::Binary(bytes.to_vec()));
                }
                OutboundFrames::Two { first, second } => {
                    let _ = write_tx_sub.send(Message::Binary(first.to_vec()));
                    let _ = write_tx_sub.send(Message::Binary(second.to_vec()));
                }
            }
        }
    });

    // ── Main read loop ────────────────────────────────────────────────────────
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                match protocol::decode_client_message(&data) {
                    Ok(ClientMessage::ReducerCall(call)) => {
                        let pending = PendingCall {
                            call_id: call.call_id,
                            reducer_name: call.reducer_name,
                            args: call.args,
                            caller_id: caller_id.clone(),
                            response_tx: response_tx.clone(),
                        };
                        if let Err(e) = reducer_tx.send(pending).await {
                            log::warn!("Reducer queue send failed: {}", e);
                        }
                    }
                    Ok(ClientMessage::Subscribe {
                        subscription_id,
                        query,
                    }) => {
                        // TODO-003: pass the live TableStore so the subscriber
                        // immediately receives all currently matching rows as
                        // "initial_snapshot" frames before any future deltas.
                        let result = subscription_manager.subscribe_with_snapshot(
                            client_id,
                            subscription_id.clone(),
                            query,
                            Some(&tables),
                        );
                        let ack = match result {
                            Ok(_) => ServerMessage::SubscriptionAck {
                                subscription_id,
                                success: true,
                                message: None,
                            },
                            Err(e) => ServerMessage::SubscriptionAck {
                                subscription_id,
                                success: false,
                                message: Some(e.to_string()),
                            },
                        };
                        if let Ok(encoded) = protocol::encode_server_message(&ack) {
                            let _ = write_tx.send(Message::Binary(encoded));
                        }
                    }
                    Ok(ClientMessage::Unsubscribe { subscription_id }) => {
                        let result = subscription_manager.unsubscribe(client_id, &subscription_id);
                        let ack = match result {
                            Ok(true) => ServerMessage::SubscriptionAck {
                                subscription_id,
                                success: true,
                                message: None,
                            },
                            Ok(false) => ServerMessage::SubscriptionAck {
                                subscription_id,
                                success: false,
                                message: Some("Subscription not found".to_string()),
                            },
                            Err(e) => ServerMessage::SubscriptionAck {
                                subscription_id,
                                success: false,
                                message: Some(e.to_string()),
                            },
                        };
                        if let Ok(encoded) = protocol::encode_server_message(&ack) {
                            let _ = write_tx.send(Message::Binary(encoded));
                        }
                    }
                    Err(_) => match protocol::decode_reducer_call(&data) {
                        Ok(call) => {
                            let pending = PendingCall {
                                call_id: call.call_id,
                                reducer_name: call.reducer_name,
                                args: call.args,
                                caller_id: caller_id.clone(),
                                response_tx: response_tx.clone(),
                            };
                            if let Err(e) = reducer_tx.send(pending).await {
                                log::warn!("Reducer queue send failed: {}", e);
                            }
                        }
                        Err(e) => {
                            log::warn!("Failed to decode client message: {}", e);
                            let error = ServerMessage::Error {
                                message: format!("Decode error: {}", e),
                            };
                            if let Ok(encoded) = protocol::encode_server_message(&error) {
                                let _ = write_tx.send(Message::Binary(encoded));
                            }
                        }
                    },
                }
            }
            Ok(Message::Close(_)) => {
                log::debug!("Client closed connection");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                log::warn!("WebSocket error: {}", e);
                break;
            }
        }
    }

    log::debug!("Client disconnected");
    subscription_manager.unregister_client(client_id);

    drop(write_tx);
    let _ = write_task.await;
    let _ = response_task.await;
    let _ = sub_task.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_call_creation() {
        let (_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let call = PendingCall {
            call_id: 1,
            reducer_name: "increment".to_string(),
            args: vec![],
            caller_id: String::new(),
            response_tx: _tx,
        };
        assert_eq!(call.call_id, 1);
    }
}
