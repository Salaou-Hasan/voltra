// ============================================================================
// NeonDB websocket.rs — high-throughput rewrite
//
// Session 7 — TODO-003: pass Arc<TableStore> into subscribe handler so new
//   clients receive initial_snapshot frames for all existing matching rows.
//
// Session 28 — TODO-022: Role-based auth / permissions.
//   - Bearer token now accepts `Bearer <key>` (unchanged) OR
//     `Bearer <key>:<role>` (new — role extracted from the suffix after the
//     last colon).  The key validation uses only the part before the colon.
//   - Parsed role is stored in `PendingCall.caller_role` and threaded into
//     `ReducerContext.caller_role` by the worker loop in main.rs.
//   - Before enqueuing a ReducerCall, the server checks
//     `permissions.is_allowed(reducer_name, caller_role)`.  Unauthorized
//     calls get an immediate error response without touching the reducer queue.
//   - `start_listener` now accepts `permissions: Arc<PermissionsConfig>`.
//
// Session 33 — Full SQL engine wired in.
//   - New `ClientMessage::SqlQuery` variant handled here.
//   - SQL is executed synchronously in a `spawn_blocking` task so the async
//     WebSocket loop is never blocked.
//   - Results serialised as `ServerMessage::SqlResult` and sent back.
//   - Rows are converted to `serde_json::Value::Object` for the wire format.
// ============================================================================

use super::message::{ClientMessage, ReducerResponse, ServerMessage, SqlResult};
use super::protocol;
use crate::config::PermissionsConfig;
use crate::error::{NeonDBError, Result};
use crate::sql::{Executor as SqlExecutor};
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
    /// Identity of the caller (X-NeonDB-Identity header or TCP peer address).
    pub caller_id: String,
    /// Role of the caller, parsed from `Bearer <key>:<role>`.
    /// Empty string when no role suffix was provided.
    pub caller_role: String,
    pub response_tx: mpsc::UnboundedSender<ReducerResponse>,
}

struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Parse a Bearer token into (api_key_part, role_part).
///
/// Format: `Bearer <key>` → key = full value, role = ""
/// Format: `Bearer <key>:<role>` → key = part before last ':', role = part after
fn parse_bearer(header: &str) -> Option<(String, String)> {
    let token = header.strip_prefix("Bearer ")?;
    match token.rsplit_once(':') {
        Some((key, role)) if !role.is_empty() && !role.contains('/') => {
            Some((key.to_string(), role.to_string()))
        }
        _ => Some((token.to_string(), String::new())),
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
    permissions: Arc<PermissionsConfig>,
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
                        let perms   = permissions.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, tx, subs, tbl, api_key, conns, perms, peer_addr.to_string()).await {
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
    permissions: Arc<PermissionsConfig>,
    peer_addr: String,
) -> Result<()> {
    // ── WebSocket handshake with optional Bearer auth ─────────────────────────
    let caller_id_cell   = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_role_cell = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_id_capture   = caller_id_cell.clone();
    let caller_role_capture = caller_role_cell.clone();

    let auth_key = api_key.clone();
    let ws_stream = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &Request, response: Response| {
            let auth_header = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if let Some(required_key) = auth_key.as_ref() {
                let (presented_key, role) = parse_bearer(auth_header)
                    .unwrap_or_else(|| (String::new(), String::new()));
                if &presented_key != required_key {
                    return Err(ErrorResponse::new(Some("Unauthorized".to_string())));
                }
                if let Ok(mut cell) = caller_role_capture.lock() { *cell = role; }
            } else if !auth_header.is_empty() {
                if let Some((_key, role)) = parse_bearer(auth_header) {
                    if let Ok(mut cell) = caller_role_capture.lock() { *cell = role; }
                }
            }

            if let Some(id) = request
                .headers()
                .get("x-neondb-identity")
                .and_then(|v| v.to_str().ok())
            {
                if let Ok(mut cell) = caller_id_capture.lock() { *cell = id.to_string(); }
            }
            Ok(response)
        },
    )
    .await
    .map_err(|e| NeonDBError::network_error(format!("WebSocket handshake error: {}", e)))?;

    let caller_id: String = {
        let g = caller_id_cell.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_empty() { peer_addr.clone() } else { g.clone() }
    };
    let caller_role: String = {
        caller_role_cell.lock().unwrap_or_else(|e| e.into_inner()).clone()
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
                Ok(data) => { let _ = write_tx_response.send(Message::Binary(data)); }
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
                        // ── Permission check ──────────────────────────────────
                        if !permissions.is_allowed(&call.reducer_name, &caller_role) {
                            log::warn!(
                                "Permission denied: caller_role='{}' tried to call '{}'",
                                caller_role, call.reducer_name
                            );
                            let denied = ReducerResponse::error(
                                call.call_id,
                                format!(
                                    "Permission denied: role '{}' is not allowed to call '{}'",
                                    caller_role, call.reducer_name
                                ),
                            );
                            if let Ok(encoded) = protocol::encode_response(&denied) {
                                let _ = write_tx.send(Message::Binary(encoded));
                            }
                            continue;
                        }

                        let pending = PendingCall {
                            call_id: call.call_id,
                            reducer_name: call.reducer_name,
                            args: call.args,
                            caller_id: caller_id.clone(),
                            caller_role: caller_role.clone(),
                            response_tx: response_tx.clone(),
                        };
                        if let Err(e) = reducer_tx.send(pending).await {
                            log::warn!("Reducer queue send failed: {}", e);
                        }
                    }

                    Ok(ClientMessage::Subscribe { subscription_id, query }) => {
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

                    // ── Full SQL query ────────────────────────────────────────
                    Ok(ClientMessage::SqlQuery(sq)) => {
                        let query_id  = sq.query_id;
                        let sql       = sq.sql.clone();
                        let tables_q  = tables.clone();
                        let write_tx_q = write_tx.clone();

                        // Run the SQL in a blocking thread so we don't hold the
                        // async executor while doing potentially expensive work.
                        tokio::task::spawn_blocking(move || {
                            let result = execute_sql_query(&sql, &tables_q, query_id);
                            let msg = ServerMessage::SqlResult(result);
                            match protocol::encode_server_message(&msg) {
                                Ok(bytes) => { let _ = write_tx_q.send(Message::Binary(bytes)); }
                                Err(e)    => log::warn!("SQL result encode error: {}", e),
                            }
                        });
                    }

                    Err(_) => {
                        // Fallback: try old ReducerCall-only decode path
                        match protocol::decode_reducer_call(&data) {
                            Ok(call) => {
                                if !permissions.is_allowed(&call.reducer_name, &caller_role) {
                                    log::warn!(
                                        "Permission denied (fallback): role='{}' tried '{}'",
                                        caller_role, call.reducer_name
                                    );
                                    let denied = ReducerResponse::error(
                                        call.call_id,
                                        format!(
                                            "Permission denied: role '{}' is not allowed to call '{}'",
                                            caller_role, call.reducer_name
                                        ),
                                    );
                                    if let Ok(encoded) = protocol::encode_response(&denied) {
                                        let _ = write_tx.send(Message::Binary(encoded));
                                    }
                                    continue;
                                }

                                let pending = PendingCall {
                                    call_id: call.call_id,
                                    reducer_name: call.reducer_name,
                                    args: call.args,
                                    caller_id: caller_id.clone(),
                                    caller_role: caller_role.clone(),
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
                        }
                    }
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

// ── SQL execution helper ──────────────────────────────────────────────────────

/// Parse and execute a SQL string against the live TableStore.
/// Returns a `SqlResult` ready to send over the wire.
fn execute_sql_query(
    sql: &str,
    tables: &Arc<TableStore>,
    query_id: u64,
) -> SqlResult {
    // Parse
    let stmt = match crate::sql::parser::parse(sql) {
        Ok(s) => s,
        Err(e) => return SqlResult::err(query_id, format!("Parse error: {}", e)),
    };

    // Execute
    let exec = SqlExecutor::new(tables.clone());
    match exec.execute_statement(&stmt) {
        Err(e) => SqlResult::err(query_id, format!("Execution error: {}", e)),
        Ok(result) => {
            // Convert each Row (Map<String, Value>) into a plain JSON object Value
            let rows: Vec<serde_json::Value> = result.rows
                .into_iter()
                .map(serde_json::Value::Object)
                .collect();
            SqlResult::ok(query_id, result.columns, rows, result.rows_affected)
        }
    }
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
            caller_role: String::new(),
            response_tx: _tx,
        };
        assert_eq!(call.call_id, 1);
        assert_eq!(call.caller_role, "");
    }

    #[test]
    fn test_parse_bearer_no_role() {
        let (key, role) = parse_bearer("Bearer mysecretkey").unwrap();
        assert_eq!(key, "mysecretkey");
        assert_eq!(role, "");
    }

    #[test]
    fn test_parse_bearer_with_role() {
        let (key, role) = parse_bearer("Bearer mysecretkey:admin").unwrap();
        assert_eq!(key, "mysecretkey");
        assert_eq!(role, "admin");
    }

    #[test]
    fn test_parse_bearer_role_user() {
        let (key, role) = parse_bearer("Bearer abc123:user").unwrap();
        assert_eq!(key, "abc123");
        assert_eq!(role, "user");
    }

    #[test]
    fn test_parse_bearer_invalid_prefix() {
        assert!(parse_bearer("Token abc").is_none());
        assert!(parse_bearer("").is_none());
    }

    #[test]
    fn test_parse_bearer_key_with_colons() {
        let (key, role) = parse_bearer("Bearer key:with:colons:admin").unwrap();
        assert_eq!(key, "key:with:colons");
        assert_eq!(role, "admin");
    }

    #[test]
    fn test_execute_sql_query_select() {
        let tables = Arc::new(TableStore::new());
        tables.set_row(
            "players".into(),
            "alice".into(),
            serde_json::json!({"id": "alice", "score": 200}),
        ).unwrap();
        let result = execute_sql_query(
            "SELECT * FROM players WHERE id = 'alice'",
            &tables,
            1,
        );
        assert!(result.success, "{:?}", result.error);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_execute_sql_query_parse_error() {
        let tables = Arc::new(TableStore::new());
        let result = execute_sql_query("NOT VALID SQL %%", &tables, 1);
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_execute_sql_query_insert_select() {
        let tables = Arc::new(TableStore::new());
        let ins = execute_sql_query(
            "INSERT INTO items (id, name, power) VALUES ('sword', 'Iron Sword', 30)",
            &tables,
            1,
        );
        assert!(ins.success, "{:?}", ins.error);
        assert_eq!(ins.rows_affected, 1);

        let sel = execute_sql_query("SELECT * FROM items", &tables, 2);
        assert!(sel.success);
        assert_eq!(sel.rows.len(), 1);
    }
}
