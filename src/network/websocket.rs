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
use super::rate_limiter::RateLimiterRegistry;
use crate::auth::{AuthResult, AuthValidator};
use crate::config::PermissionsConfig;
use crate::error::{NeonDBError, Result};
use crate::metrics::Metrics;
use crate::presence::PresenceManager;
use crate::ttl::TtlManager;
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

/// Maximum queued outbound frames per client before the connection is forcibly closed.
/// 4096 frames × ~512 bytes average ≈ 2 MB per slow client (bounded).
pub const CLIENT_SEND_BUFFER_CAPACITY: usize = 4096;

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

/// Returns true if `role` is a syntactically valid role name.
///
/// Constraints:
/// - 1..=32 characters
/// - Only ASCII alphanumerics, underscore, or dash
/// - No control characters, no `/`, `\`, `:`, `..`, no whitespace.
#[cfg(test)]
fn is_valid_role(role: &str) -> bool {
    if role.is_empty() || role.len() > 32 {
        return false;
    }
    role.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
    })
}

/// Parse a Bearer token into (api_key_part, role_part).
///
/// Format: `Bearer <key>` → key = full value, role = ""
/// Format: `Bearer <key>:<role>` → key = part before LAST ':' (so the key may
///   itself contain colons), role = part after.
///
/// The role suffix is only honoured when it passes [`is_valid_role`].  If the
/// role part fails validation (bad chars, too long, etc.) the entire token is
/// treated as the api_key and the role is set to empty — degrade gracefully
/// rather than refusing the connection.
#[cfg(test)]
fn parse_bearer(header: &str) -> Option<(String, String)> {
    let token = header.strip_prefix("Bearer ")?;
    if let Some((key, role)) = token.rsplit_once(':') {
        if is_valid_role(role) {
            return Some((key.to_string(), role.to_string()));
        }
    }
    Some((token.to_string(), String::new()))
}

/// Start the WebSocket listener.
///
/// `sql_timeout_ms` caps how long a single SQL query may run on the blocking
/// pool before the result is replaced with a timeout error.  Use 0 to disable
/// the timeout (not recommended in production).
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
    sql_timeout_ms: u64,
    auth_validator: Arc<AuthValidator>,
    rate_limiter: Arc<RateLimiterRegistry>,
    presence: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    mut shutdown: Receiver<()>,
    metrics: Arc<Metrics>,
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
                        let sql_to  = sql_timeout_ms;
                        let auth_v  = auth_validator.clone();
                        let rl      = rate_limiter.clone();
                        let pres    = presence.clone();
                        let ttl     = ttl_manager.clone();
                        let met     = metrics.clone();

                        // Record connection metrics immediately on accept.
                        metrics.websocket_connects_total.inc();
                        metrics.websocket_connections_active.inc();

                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, tx, subs, tbl, api_key, conns, perms, sql_to, auth_v, rl, pres, ttl, peer_addr.to_string(), met).await {
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
    _api_key: Option<String>,
    active_connections: Arc<AtomicUsize>,
    permissions: Arc<PermissionsConfig>,
    sql_timeout_ms: u64,
    auth_validator: Arc<AuthValidator>,
    rate_limiter: Arc<RateLimiterRegistry>,
    presence: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    peer_addr: String,
    metrics: Arc<Metrics>,
) -> Result<()> {
    // ── WebSocket handshake with JWT / API-key auth ───────────────────────────
    let caller_id_cell   = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_role_cell = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_id_capture   = caller_id_cell.clone();
    let caller_role_capture = caller_role_cell.clone();

    let auth_v = auth_validator.clone();
    let ws_stream = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &Request, response: Response| {
            let auth_header = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            // Use AuthValidator for all auth modes (JWT, API key, or open)
            if auth_header.is_empty() {
                // No auth header provided.
                // If auth is configured (api_key is Some, or AuthValidator is not in None mode),
                // check if we should reject or allow anonymous.
                match auth_v.validate("Bearer ") {
                    AuthResult::Anonymous => {
                        // No auth configured — allow anonymous
                    }
                    _ => {
                        // Auth is configured but no header provided — reject.
                        return Err(ErrorResponse::new(Some("Unauthorized: missing Authorization header".to_string())));
                    }
                }
            } else {
                match auth_v.validate(auth_header) {
                    AuthResult::Authenticated { user_id, role, .. } => {
                        if let Ok(mut cell) = caller_id_capture.lock() { *cell = user_id; }
                        if let Ok(mut cell) = caller_role_capture.lock() { *cell = role; }
                    }
                    AuthResult::Denied(reason) => {
                        return Err(ErrorResponse::new(Some(format!("Unauthorized: {}", reason))));
                    }
                    AuthResult::Anonymous => {
                        // No auth configured — allow with default identity
                    }
                }
            }

            // Also check X-NeonDB-Identity header as override for caller_id
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

    // ── Presence: mark user online ───────────────────────────────────────────
    presence.set_online(&caller_id, None);

    let _conn_guard = ConnectionGuard(active_connections.clone());
    let current = active_connections.fetch_add(1, Ordering::SeqCst);
    log::debug!("Active WebSocket clients: {}", current + 1);

    let (ws_sink, mut ws_rx) = ws_stream.split();

    // ── Dedicated write task ──────────────────────────────────────────────────
    let (write_tx, mut write_rx) = mpsc::channel::<Message>(CLIENT_SEND_BUFFER_CAPACITY);

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
                    if let Err(mpsc::error::TrySendError::Full(_)) = write_tx_response.try_send(Message::Binary(data)) {
                        log::warn!("Client send buffer full (response task), dropping connection");
                        break;
                    }
                }
                Err(e) => log::warn!("Failed to encode response: {}", e),
            }
        }
    });

    // ── Register client ───────────────────────────────────────────────────────
    let (sub_tx, mut sub_rx) = mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
    let client_id = subscription_manager.register_client(sub_tx);

    let write_tx_sub = write_tx.clone();
    let sub_task = tokio::spawn(async move {
        while let Some(frames) = sub_rx.recv().await {
            let full = match frames {
                OutboundFrames::One(bytes) => {
                    matches!(
                        write_tx_sub.try_send(Message::Binary(bytes.to_vec())),
                        Err(mpsc::error::TrySendError::Full(_))
                    )
                }
                OutboundFrames::Two { first, second } => {
                    if let Err(mpsc::error::TrySendError::Full(_)) =
                        write_tx_sub.try_send(Message::Binary(first.to_vec()))
                    {
                        true
                    } else {
                        matches!(
                            write_tx_sub.try_send(Message::Binary(second.to_vec())),
                            Err(mpsc::error::TrySendError::Full(_))
                        )
                    }
                }
            };
            if full {
                log::warn!("Client send buffer full (subscription task), dropping connection");
                break;
            }
        }
    });

    // ── Main read loop ────────────────────────────────────────────────────────
    while let Some(msg) = ws_rx.next().await {
        // Implicit heartbeat: any message from the client refreshes presence.
        presence.heartbeat(&caller_id);

        match msg {
            Ok(Message::Binary(data)) => {
                match protocol::decode_client_message(&data) {
                    Ok(ClientMessage::ReducerCall(call)) => {
                        // ── Rate limit check ─────────────────────────────────
                        if !rate_limiter.check(&caller_id) {
                            log::debug!("Rate limited: caller_id='{}'", caller_id);
                            let limited = ReducerResponse::error(
                                call.call_id,
                                "Rate limited".to_string(),
                            );
                            if let Ok(encoded) = protocol::encode_response(&limited) {
                                if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                    log::warn!("Client send buffer full, disconnecting slow client");
                                    break;
                                }
                            }
                            continue;
                        }

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
                                if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                        log::warn!("Client send buffer full, disconnecting slow client");
                                        break;
                                    }
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
                            if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                        log::warn!("Client send buffer full, disconnecting slow client");
                                        break;
                                    }
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
                            if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                        log::warn!("Client send buffer full, disconnecting slow client");
                                        break;
                                    }
                        }
                    }

                    // ── Full SQL query ────────────────────────────────────────
                    Ok(ClientMessage::SqlQuery(sq)) => {
                        let query_id  = sq.query_id;
                        let sql       = sq.sql.clone();
                        let tables_q  = tables.clone();
                        let write_tx_q = write_tx.clone();
                        let timeout_ms = sql_timeout_ms;

                        // Run the SQL in a blocking thread so we don't hold the
                        // async executor while doing potentially expensive work,
                        // then race it against `timeout_ms` so a runaway query
                        // can't tie up an executor task forever.
                        tokio::spawn(async move {
                            let work = tokio::task::spawn_blocking(move || {
                                execute_sql_query(&sql, &tables_q, query_id)
                            });
                            let result = if timeout_ms == 0 {
                                match work.await {
                                    Ok(r) => r,
                                    Err(e) => SqlResult::err(
                                        query_id,
                                        format!("SQL task join error: {}", e),
                                    ),
                                }
                            } else {
                                match tokio::time::timeout(
                                    std::time::Duration::from_millis(timeout_ms),
                                    work,
                                ).await {
                                    Ok(Ok(r)) => r,
                                    Ok(Err(e)) => SqlResult::err(
                                        query_id,
                                        format!("SQL task join error: {}", e),
                                    ),
                                    Err(_) => {
                                        log::warn!(
                                            "SQL query {} cancelled after {}ms timeout",
                                            query_id, timeout_ms
                                        );
                                        SqlResult::err(
                                            query_id,
                                            format!(
                                                "SQL query exceeded timeout of {}ms",
                                                timeout_ms
                                            ),
                                        )
                                    }
                                }
                            };
                            let msg = ServerMessage::SqlResult(result);
                            match protocol::encode_server_message(&msg) {
                                Ok(bytes) => {
                                if let Err(mpsc::error::TrySendError::Full(_)) = write_tx_q.try_send(Message::Binary(bytes)) {
                                    log::warn!("Client send buffer full (SQL result), frame dropped");
                                }
                            }
                                Err(e)    => log::warn!("SQL result encode error: {}", e),
                            }
                        });
                    }

                    // ── Heartbeat ────────────────────────────────────
                    Ok(ClientMessage::Heartbeat) => {
                        // Presence heartbeat already handled at top of loop.
                        // Nothing else to do.
                    }

                    // ── SetPresence ──────────────────────────────────
                    Ok(ClientMessage::SetPresence { status, metadata }) => {
                        presence.set_online(&caller_id, metadata);
                        log::debug!("Presence update: caller='{}' status='{}'", caller_id, status);
                    }

                    // ── SetTtl ───────────────────────────────────────
                    Ok(ClientMessage::SetTtl { table_name, row_key, ttl_ms }) => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        ttl_manager.set_ttl(&table_name, &row_key, now_ms, ttl_ms);
                        log::debug!("TTL set: {}.{} expires in {}ms", table_name, row_key, ttl_ms);
                    }

                    // ── CancelTtl ────────────────────────────────────
                    Ok(ClientMessage::CancelTtl { table_name, row_key }) => {
                        ttl_manager.cancel_ttl(&table_name, &row_key);
                        log::debug!("TTL cancelled: {}.{}", table_name, row_key);
                    }

                    Err(_) => {
                        // Fallback: try old ReducerCall-only decode path
                        match protocol::decode_reducer_call(&data) {
                            Ok(call) => {
                                // ── Rate limit check (fallback path) ──────────
                                if !rate_limiter.check(&caller_id) {
                                    log::debug!("Rate limited (fallback): caller_id='{}'", caller_id);
                                    let limited = ReducerResponse::error(
                                        call.call_id,
                                        "Rate limited".to_string(),
                                    );
                                    if let Ok(encoded) = protocol::encode_response(&limited) {
                                        if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                            log::warn!("Client send buffer full, disconnecting slow client");
                                            break;
                                        }
                                    }
                                    continue;
                                }

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
                                        if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                        log::warn!("Client send buffer full, disconnecting slow client");
                                        break;
                                    }
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
                                    if let Err(mpsc::error::TrySendError::Full(_)) = write_tx.try_send(Message::Binary(encoded)) {
                                        log::warn!("Client send buffer full, disconnecting slow client");
                                        break;
                                    }
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
    presence.set_offline(&caller_id);
    rate_limiter.remove(&caller_id);
    metrics.websocket_connections_active.dec();

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

    // ── role validation tests ────────────────────────────────────────────────

    #[test]
    fn test_is_valid_role_basic() {
        assert!(is_valid_role("admin"));
        assert!(is_valid_role("user"));
        assert!(is_valid_role("guest_1"));
        assert!(is_valid_role("svc-bot"));
        assert!(is_valid_role("A"));
        assert!(is_valid_role("a1b2c3"));
    }

    #[test]
    fn test_is_valid_role_rejects_bad_chars() {
        assert!(!is_valid_role(""));
        assert!(!is_valid_role(".."));
        assert!(!is_valid_role("admin/root"));
        assert!(!is_valid_role("ad\\min"));
        assert!(!is_valid_role("ad min"));
        assert!(!is_valid_role("ad:min"));
        assert!(!is_valid_role("ad\nmin"));
        assert!(!is_valid_role("emoji😀"));
        assert!(!is_valid_role("../etc"));
    }

    #[test]
    fn test_is_valid_role_length_cap() {
        // 32 chars = max allowed.
        let max = "a".repeat(32);
        assert!(is_valid_role(&max));
        // 33 chars = rejected.
        let over = "a".repeat(33);
        assert!(!is_valid_role(&over));
    }

    #[test]
    fn test_parse_bearer_invalid_role_chars_degrades_to_full_key() {
        // Role contains '/', so the colon is NOT treated as a role separator;
        // the whole thing becomes the key.
        let (key, role) = parse_bearer("Bearer mykey:bad/role").unwrap();
        assert_eq!(key, "mykey:bad/role");
        assert_eq!(role, "");
    }

    #[test]
    fn test_parse_bearer_too_long_role_degrades() {
        let long_role = "a".repeat(50);
        let header = format!("Bearer mykey:{}", long_role);
        let (key, role) = parse_bearer(&header).unwrap();
        assert_eq!(key, format!("mykey:{}", long_role));
        assert_eq!(role, "");
    }

    #[test]
    fn test_parse_bearer_no_colon() {
        let (key, role) = parse_bearer("Bearer plainkey").unwrap();
        assert_eq!(key, "plainkey");
        assert_eq!(role, "");
    }

    #[test]
    fn test_parse_bearer_multiple_colons_takes_last() {
        // rsplit_once → split on the LAST colon.  Role is "admin".
        let (key, role) = parse_bearer("Bearer aa:bb:cc:admin").unwrap();
        assert_eq!(key, "aa:bb:cc");
        assert_eq!(role, "admin");
    }

    #[test]
    fn test_parse_bearer_trailing_colon_empty_role() {
        // Empty role suffix fails validation → whole token becomes key.
        let (key, role) = parse_bearer("Bearer mykey:").unwrap();
        assert_eq!(key, "mykey:");
        assert_eq!(role, "");
    }

    #[test]
    fn test_parse_bearer_control_char_in_role() {
        let (key, role) = parse_bearer("Bearer mykey:ad\x01min").unwrap();
        assert_eq!(key, "mykey:ad\x01min");
        assert_eq!(role, "");
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

    // ── Backpressure tests ───────────────────────────────────────────────────

    #[test]
    fn test_bounded_channel_capacity_constant() {
        // Sanity check: capacity must be at least 1024 to avoid premature disconnects
        // under normal bursty game traffic.
        assert!(
            CLIENT_SEND_BUFFER_CAPACITY >= 1024,
            "CLIENT_SEND_BUFFER_CAPACITY must be >= 1024, got {}",
            CLIENT_SEND_BUFFER_CAPACITY
        );
    }

    #[test]
    fn test_slow_client_handling() {
        // Create a bounded channel with CLIENT_SEND_BUFFER_CAPACITY and fill it
        // to capacity, then verify the next try_send returns Full.
        let (tx, _rx) = mpsc::channel::<Message>(CLIENT_SEND_BUFFER_CAPACITY);

        // Fill the channel to capacity
        for i in 0..CLIENT_SEND_BUFFER_CAPACITY {
            let msg = Message::Binary(vec![i as u8]);
            assert!(
                tx.try_send(msg).is_ok(),
                "send {} should succeed (capacity={})",
                i,
                CLIENT_SEND_BUFFER_CAPACITY
            );
        }

        // The next send should fail with Full
        let overflow_msg = Message::Binary(vec![0xFF]);
        match tx.try_send(overflow_msg) {
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Expected: channel is full, slow client would be disconnected
            }
            Ok(_) => panic!("Expected channel to be full after {} sends", CLIENT_SEND_BUFFER_CAPACITY),
            Err(mpsc::error::TrySendError::Closed(_)) => panic!("Channel unexpectedly closed"),
        }
    }
}
