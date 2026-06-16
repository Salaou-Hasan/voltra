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
use super::InlineRegistry;
use crate::auth::{AuthResult, AuthValidator, IdentityIssuer};
use crate::config::PermissionsConfig;
use crate::error::{NeonDBError, Result};
use crate::metrics::Metrics;
use crate::presence::PresenceManager;
use crate::tenant::TenantRegistry;
use crate::ttl::TtlManager;
use crate::sql::{Executor as SqlExecutor};
use crate::subscriptions::{OutboundFrames, SubscriptionManager};
use crate::table::TableStore;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch::Receiver};
use tokio_rustls::TlsAcceptor;
use socket2::{Domain, Protocol, Socket, Type};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;

/// Maximum queued outbound frames per client before the connection is forcibly closed.
/// Each queued frame is an owned `Vec<u8>` copy (see sub_task), so this directly
/// bounds per-connection channel memory: 1024 frames × ~128 bytes ≈ 128 KiB.
/// Combined with the 512 KiB tungstenite write-buffer cap, total worst-case
/// outbound buffering per connection is well under 1 MiB — and a client that
/// stays maxed is dropped (slow-consumer eviction) rather than buffered forever.
/// Lowered from 4096 in the 15-20K CCU memory pass: at 15K connections the old
/// cap allowed multi-GB of channel copies to accumulate for slow clients.
pub const CLIENT_SEND_BUFFER_CAPACITY: usize = 1024;

/// Create N TCP listeners all bound to the same address.
///
/// On Linux, each socket gets `SO_REUSEPORT` so the kernel load-balances
/// incoming connections across all N accept queues — one per NIC RX queue.
/// On other platforms (Windows, macOS) we fall back to a single listener;
/// `SO_REUSEPORT` either doesn't exist or doesn't provide the same guarantee.
fn create_listeners(bind_addr: &str, _count: usize) -> std::io::Result<Vec<TcpListener>> {
    let addr: std::net::SocketAddr = bind_addr.parse()
        .map_err(|e: std::net::AddrParseError| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

    #[cfg(target_os = "linux")]
    let n = _count;
    #[cfg(not(target_os = "linux"))]
    let n = 1usize;

    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        let domain  = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
        let socket  = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        #[cfg(target_os = "linux")]
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(4096)?;
        listeners.push(TcpListener::from_std(socket.into())?);
    }
    Ok(listeners)
}

/// All shared state passed to each accept-loop task.
/// Using a struct + Arc lets us spawn N accept tasks without cloning 20 params.
struct AcceptState {
    reducer_tx:           kanal::AsyncSender<PendingCall>,
    subscription_manager: Arc<crate::subscriptions::SubscriptionManager>,
    tables:               Arc<crate::table::TableStore>,
    api_key:              Option<String>,
    active_connections:   Arc<AtomicUsize>,
    permissions:          Arc<crate::config::PermissionsConfig>,
    sql_timeout_ms:       u64,
    auth_validator:       Arc<crate::auth::AuthValidator>,
    rate_limiter:         Arc<super::RateLimiterRegistry>,
    presence:             Arc<crate::presence::PresenceManager>,
    ttl_manager:          Arc<crate::ttl::TtlManager>,
    identity_issuer:      Arc<crate::auth::IdentityIssuer>,
    metrics:              Arc<crate::metrics::Metrics>,
    tenant_registry:      Arc<crate::tenant::TenantRegistry>,
    inline_registry:      Arc<super::InlineRegistry>,
    lobby_router:         Option<Arc<crate::worker_pool::LobbyRouter>>,
    drain_flag:           Arc<AtomicBool>,
    max_connections:      usize,
}

async fn run_accept_loop(
    listener:     TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    s:            Arc<AcceptState>,
    mut shutdown: Receiver<()>,
) {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer_addr)) => {
                        let _ = stream.set_nodelay(true);
                        log::debug!("New connection from {}", peer_addr);

                        if s.active_connections.load(Ordering::SeqCst) >= s.max_connections {
                            log::warn!("Connection limit reached: {}", s.max_connections);
                            drop(stream);
                            continue;
                        }

                        if s.drain_flag.load(Ordering::Relaxed) {
                            log::debug!("Drain active — rejecting new connection from {}", peer_addr);
                            let _ = tokio::io::AsyncWriteExt::write_all(
                                &mut tokio::io::BufWriter::new(stream),
                                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 30\r\nX-NeonDB-Draining: true\r\n\r\n",
                            ).await;
                            continue;
                        }

                        let tx      = s.reducer_tx.clone();
                        let subs    = s.subscription_manager.clone();
                        let tbl     = s.tables.clone();
                        let api_key = s.api_key.clone();
                        let conns   = s.active_connections.clone();
                        let perms   = s.permissions.clone();
                        let sql_to  = s.sql_timeout_ms;
                        let auth_v  = s.auth_validator.clone();
                        let rl      = s.rate_limiter.clone();
                        let pres    = s.presence.clone();
                        let ttl     = s.ttl_manager.clone();
                        let sd      = shutdown.clone();
                        let met     = s.metrics.clone();
                        let iss     = s.identity_issuer.clone();
                        let peer    = peer_addr.to_string();
                        let tls_acc = tls_acceptor.clone();
                        let ten     = s.tenant_registry.clone();
                        let inl     = s.inline_registry.clone();
                        let lr      = s.lobby_router.clone();

                        s.metrics.websocket_connects_total.inc();
                        s.metrics.websocket_connections_active.inc();

                        tokio::spawn(async move {
                            if let Some(acceptor) = tls_acc {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        if let Err(e) = handle_client(
                                            tls_stream, tx, subs, tbl, api_key, conns, perms,
                                            sql_to, auth_v, rl, pres, ttl, iss, peer, sd, met, ten, inl, lr,
                                        ).await { log::warn!("TLS client error: {}", e); }
                                    }
                                    Err(e) => log::warn!("TLS accept error from {}: {}", peer, e),
                                }
                            } else {
                                if let Err(e) = handle_client(
                                    stream, tx, subs, tbl, api_key, conns, perms,
                                    sql_to, auth_v, rl, pres, ttl, iss, peer, sd, met, ten, inl, lr,
                                ).await { log::warn!("Client error: {}", e); }
                            }
                        });
                    }
                    Err(e) => log::error!("Accept error: {}", e),
                }
            }
            _ = shutdown.changed() => {
                log::info!("WebSocket accept loop shutdown");
                break;
            }
        }
    }
}

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
    /// Tenant ID if the connection authenticated with a tenant API key (`ndbt_…`).
    /// None for non-tenant connections (global or scheduler calls).
    pub tenant_id: Option<String>,
    /// Lobby hint for per-lobby worker routing. When set, the call is dispatched
    /// to a dedicated worker thread for this lobby instead of the global pool.
    /// Parsed from the first argument's key prefix (e.g. `"l42_p123"` → `"42"`).
    pub lobby_hint: Option<String>,
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

/// Returns `true` when the token string looks like a JWT (has exactly 2 dots).
///
/// A raw API key never contains dots in this format; JWTs always have the
/// structure `header.payload.signature`.
fn is_jwt(token: &str) -> bool {
    token.chars().filter(|&c| c == '.').count() == 2
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
    identity_issuer: Arc<IdentityIssuer>,
    mut shutdown: Receiver<()>,
    metrics: Arc<Metrics>,
    tls: Option<Arc<rustls::ServerConfig>>,
    tenant_registry: Arc<TenantRegistry>,
    inline_registry: Arc<InlineRegistry>,
    lobby_router: Option<Arc<crate::worker_pool::LobbyRouter>>,
    drain_flag: Arc<AtomicBool>,
) -> Result<()> {
    let bind_addr = format!("{}:{}", addr, port);
    let tls_acceptor: Option<TlsAcceptor> = tls.map(TlsAcceptor::from);

    // Spawn N accept tasks: one per logical core (capped at 8) on Linux with
    // SO_REUSEPORT, exactly 1 everywhere else (create_listeners handles this).
    let accept_count = num_cpus::get().min(8).max(1);
    let listeners = create_listeners(&bind_addr, accept_count)
        .map_err(|e| crate::error::NeonDBError::network_error(format!("bind {}: {}", bind_addr, e)))?;

    if tls_acceptor.is_some() {
        log::info!("WebSocket listener (WSS/TLS) on {} ({} accept queue(s))", bind_addr, listeners.len());
    } else {
        log::info!("WebSocket listener on {} ({} accept queue(s))", bind_addr, listeners.len());
    }

    let state = Arc::new(AcceptState {
        reducer_tx:           reducer_tx,
        subscription_manager: subscription_manager,
        tables:               tables,
        api_key:              api_key,
        active_connections:   active_connections,
        permissions:          permissions,
        sql_timeout_ms:       sql_timeout_ms,
        auth_validator:       auth_validator,
        rate_limiter:         rate_limiter,
        presence:             presence,
        ttl_manager:          ttl_manager,
        identity_issuer:      identity_issuer,
        metrics:              metrics,
        tenant_registry:      tenant_registry,
        inline_registry:      inline_registry,
        lobby_router:         lobby_router,
        drain_flag:           drain_flag,
        max_connections:      max_connections,
    });

    let mut handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let s   = state.clone();
        let tls = tls_acceptor.clone();
        let sd  = shutdown.clone();
        handles.push(tokio::spawn(run_accept_loop(listener, tls, s, sd)));
    }

    // Wait for the shutdown signal, then abort all accept tasks.
    let _ = shutdown.changed().await;
    log::info!("WebSocket listener shutdown requested");
    for h in &handles { h.abort(); }
    Ok(())
}

async fn handle_client<S>(
    stream: S,
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
    identity_issuer: Arc<IdentityIssuer>,
    peer_addr: String,
    mut shutdown: Receiver<()>,
    metrics: Arc<Metrics>,
    tenant_registry: Arc<TenantRegistry>,
    inline_registry: Arc<InlineRegistry>,
    lobby_router: Option<Arc<crate::worker_pool::LobbyRouter>>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // ── WebSocket handshake with JWT / API-key auth ───────────────────────────
    let caller_id_cell    = Arc::new(std::sync::Mutex::new(String::new()));
    let caller_role_cell  = Arc::new(std::sync::Mutex::new(String::new()));
    let tenant_id_cell    = Arc::new(std::sync::Mutex::new(Option::<String>::None));
    let caller_id_capture   = caller_id_cell.clone();
    let caller_role_capture = caller_role_cell.clone();
    let tenant_id_capture   = tenant_id_cell.clone();

    let auth_v  = auth_validator.clone();
    let iss_v   = identity_issuer.clone();
    let tenant_v = tenant_registry.clone();

    // Bound per-connection memory. tungstenite's default max_write_buffer_size
    // is usize::MAX — under subscription fan-out a slow client's outbound buffer
    // grows without limit, so total server memory climbs proportionally to
    // backed-up frames. Capping it converts that unbounded climb into a hard
    // per-connection ceiling: once a client is this far behind, feed() returns
    // WriteBufferFull, the write task breaks, and the connection is dropped
    // (slow-consumer eviction — stale game state is shed first, exactly right
    // for state-sync semantics). Game frames are tiny, so message/frame caps
    // also tighten the worst-case single-allocation.
    let ws_config = {
        let mut c = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
        c.write_buffer_size     = 64 * 1024;        // flush threshold (64 KiB)
        c.max_write_buffer_size  = 512 * 1024;       // hard per-conn cap (512 KiB)
        c.max_message_size       = Some(1 << 20);    // 1 MiB inbound message cap
        c.max_frame_size         = Some(1 << 20);    // 1 MiB inbound frame cap
        c
    };

    let ws_stream = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        move |request: &Request, response: Response| {
            let auth_header = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if auth_header.is_empty() {
                // No auth header provided.  Allow only when NO auth is
                // configured (dev mode).  Checking the mode directly — calling
                // validate() with an empty token would return Denied even in
                // None mode, locking out all anonymous dev connections.
                if !matches!(auth_v.mode(), crate::auth::AuthMode::None) {
                    return Err(ErrorResponse::new(Some("Unauthorized: missing Authorization header".to_string())));
                }
            } else {
                // Strip "Bearer " prefix to get the raw token.
                let raw_token = if let Some(s) = auth_header.strip_prefix("Bearer ") {
                    s.trim()
                } else if let Some(s) = auth_header.strip_prefix("bearer ") {
                    s.trim()
                } else {
                    auth_header
                };

                if is_jwt(raw_token) {
                    // ── Ed25519 JWT path ──────────────────────────────────────
                    match iss_v.verify(raw_token) {
                        Ok(claims) => {
                            if let Ok(mut cell) = caller_id_capture.lock() {
                                *cell = claims.sub.clone();
                            }
                            if let Ok(mut cell) = caller_role_capture.lock() {
                                *cell = claims.roles.first().cloned().unwrap_or_default();
                            }
                        }
                        Err(e) => {
                            return Err(ErrorResponse::new(Some(format!(
                                "Unauthorized: invalid JWT — {}", e
                            ))));
                        }
                    }
                } else if raw_token.starts_with("ndbt_") {
                    // ── Tenant API key path ───────────────────────────────────
                    match tenant_v.resolve_key(raw_token) {
                        Some(tid) => {
                            if let Ok(mut cell) = caller_id_capture.lock() {
                                *cell = format!("tenant:{}", tid);
                            }
                            if let Ok(mut cell) = caller_role_capture.lock() {
                                *cell = "tenant".to_string();
                            }
                            if let Ok(mut cell) = tenant_id_capture.lock() {
                                *cell = Some(tid);
                            }
                        }
                        None => {
                            return Err(ErrorResponse::new(Some(
                                "Unauthorized: invalid tenant API key".to_string()
                            )));
                        }
                    }
                } else {
                    // ── Legacy API key / HMAC JWT path ───────────────────────
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
        Some(ws_config),
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
    let tenant_id: Option<String> = {
        tenant_id_cell.lock().unwrap_or_else(|e| e.into_inner()).clone()
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
            // Group commit for outbound frames: drain everything queued,
            // feed without flushing, then flush ONCE. Under subscription
            // fan-out this turns hundreds of per-frame flush syscalls into
            // one — the difference between 4K and 30K+ TPS at high CCU.
            'conn: while let Some(msg) = write_rx.recv().await {
                if sink.feed(msg).await.is_err() {
                    break 'conn;
                }
                let mut batched = 1usize;
                while batched < 256 {
                    match write_rx.try_recv() {
                        Ok(m) => {
                            if sink.feed(m).await.is_err() {
                                break 'conn;
                            }
                            batched += 1;
                        }
                        Err(_) => break,
                    }
                }
                if let Err(e) = sink.flush().await {
                    log::warn!("WebSocket write error: {}", e);
                    break 'conn;
                }
                // feed()+flush() on a writable socket can complete without
                // ever returning Pending — under sustained fan-out this loop
                // would never yield and starves the runtime (accepts, reads,
                // every other task). Force a scheduler yield per batch.
                tokio::task::yield_now().await;
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
                    // Reducer responses must NEVER be dropped — a lost response
                    // is a 5s client timeout. When subscription fan-out fills
                    // the shared write queue, responses wait (backpressure on
                    // this client only); fan-out frames stay droppable.
                    if write_tx_response.send(Message::Binary(data)).await.is_err() {
                        break; // connection closed
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
    let sub_tenant_id = tenant_id.clone();
    let sub_task = tokio::spawn(async move {
        while let Some(frames) = sub_rx.recv().await {
            // For tenant connections, strip the physical table prefix from
            // outbound subscription frames so clients see logical table names.
            let frames = match &sub_tenant_id {
                Some(tid) => strip_tenant_frames(frames, tid),
                None => frames,
            };
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
    let mut send_close_on_exit = false;
    loop {
        let msg_opt = tokio::select! {
            msg = ws_rx.next() => msg,
            _ = shutdown.changed() => {
                // Server is shutting down — ask the client to close gracefully.
                send_close_on_exit = true;
                break;
            }
        };
        let msg = match msg_opt {
            Some(m) => m,
            None    => break,
        };
        // Implicit heartbeat: any message from the client refreshes presence.
        presence.heartbeat(&caller_id);

        match msg {
            Ok(Message::Binary(data)) => {
                match protocol::decode_client_message(&data) {
                    Ok(ClientMessage::ReducerCall(call)) => {
                        // ── Rate limit check ─────────────────────────────────
                        let rate_ok = rate_limiter.check(&caller_id)
                            && tenant_id.as_deref()
                                .map(|tid| tenant_registry.check_rate(tid))
                                .unwrap_or(true);
                        if !rate_ok {
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

                        let call_id = call.call_id;

                        // ── Inline fast path ─────────────────────────────────────
                        // Reducers in the inline registry are pure-computation with
                        // no DB writes.  Execute them directly in this async task —
                        // zero channel hops, zero OS thread wakeups.  This path
                        // enables 300K-500K TPS for ping/pong style reducers.
                        if let Some(inline_fn) = inline_registry.get(&call.reducer_name) {
                            let result_bytes = inline_fn(&call.args);
                            let resp = ReducerResponse::success(call_id, result_bytes);
                            let _ = response_tx.send(resp);
                            metrics.reducer_calls_total.inc();
                            continue;
                        }

                        // Extract lobby hint from first arg (e.g. "l42_p123" → "42").
                        let lobby_hint = extract_lobby_hint(&call.args);
                        let pending = PendingCall {
                            call_id,
                            reducer_name: call.reducer_name,
                            args: call.args,
                            caller_id: caller_id.clone(),
                            caller_role: caller_role.clone(),
                            tenant_id: tenant_id.clone(),
                            lobby_hint,
                            response_tx: response_tx.clone(),
                        };
                        let dispatched = match &lobby_router {
                            Some(lr) => lr.try_dispatch(pending),
                            None => reducer_tx.try_send(pending).is_ok(),
                        };
                        if !dispatched {
                            log::warn!("Reducer queue full, rejecting call_id={}", call_id);
                            let overloaded = ReducerResponse::error(
                                call_id,
                                "server overloaded, retry later".to_string(),
                            );
                            let _ = response_tx.send(overloaded);
                        }
                    }

                    Ok(ClientMessage::Subscribe { subscription_id, query }) => {
                        // Rewrite query to use the physical table name for tenant clients.
                        let physical_query = match &tenant_id {
                            Some(tid) => rewrite_query_for_tenant(&query, tid),
                            None => query,
                        };
                        let result = subscription_manager.subscribe_with_snapshot(
                            client_id,
                            subscription_id.clone(),
                            physical_query,
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

                                let call_id = call.call_id;
                                let lobby_hint = extract_lobby_hint(&call.args);
                                let pending = PendingCall {
                                    call_id,
                                    reducer_name: call.reducer_name,
                                    args: call.args,
                                    caller_id: caller_id.clone(),
                                    caller_role: caller_role.clone(),
                                    tenant_id: tenant_id.clone(),
                                    lobby_hint,
                                    response_tx: response_tx.clone(),
                                };
                                let dispatched = match &lobby_router {
                                    Some(lr) => lr.try_dispatch(pending),
                                    None => reducer_tx.try_send(pending).is_ok(),
                                };
                                if !dispatched {
                                    log::warn!("Reducer queue full, rejecting call_id={}", call_id);
                                    let overloaded = ReducerResponse::error(
                                        call_id,
                                        "server overloaded, retry later".to_string(),
                                    );
                                    let _ = response_tx.send(overloaded);
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

    // ── Graceful close frame on server shutdown ───────────────────────────────
    if send_close_on_exit {
        log::debug!("Sending WebSocket Close frame to {}", peer_addr);
        // Queue a Close frame through the write channel; the write task will
        // deliver it before dropping the connection.
        let _ = write_tx.try_send(Message::Close(None));
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

// ── Tenant helpers ────────────────────────────────────────────────────────────

/// Rewrite a subscription query's first token (table name) to the physical name
/// for a tenant.  System tables (`__*`) and already-prefixed names pass through.
fn rewrite_query_for_tenant(query: &str, tenant_id: &str) -> String {
    let trimmed = query.trim();
    let (table_name, rest) = match trimmed.find(|c: char| c.is_whitespace()) {
        Some(i) => (&trimmed[..i], &trimmed[i..]),
        None => (trimmed, ""),
    };
    if table_name.starts_with("__") || table_name.starts_with("tn:") {
        return query.to_string();
    }
    format!("tn:{}:{}{}", tenant_id, table_name, rest)
}

/// Strip the tenant prefix from `table_name` in an outbound subscription frame.
/// Returns the original `Arc<Bytes>` unchanged if no rewrite is needed.
fn strip_tenant_prefix_from_frame(bytes: &Arc<Bytes>, prefix: &str) -> Arc<Bytes> {
    let msg: ServerMessage = match rmp_serde::from_slice(bytes) {
        Ok(m) => m,
        Err(_) => return bytes.clone(),
    };
    let modified = match msg {
        ServerMessage::SubscriptionDiff(mut diff) => {
            if let Some(logical) = diff.table_name.strip_prefix(prefix) {
                diff.table_name = logical.to_string();
                ServerMessage::SubscriptionDiff(diff)
            } else {
                return bytes.clone();
            }
        }
        ServerMessage::SubscriptionBody(mut body) => {
            if let Some(logical) = body.table_name.strip_prefix(prefix) {
                body.table_name = logical.to_string();
                ServerMessage::SubscriptionBody(body)
            } else {
                return bytes.clone();
            }
        }
        _ => return bytes.clone(),
    };
    match rmp_serde::to_vec(&modified) {
        Ok(b) => Arc::new(Bytes::from(b)),
        Err(_) => bytes.clone(),
    }
}

/// Strip tenant prefix from all frames in an `OutboundFrames` envelope.
fn strip_tenant_frames(frames: OutboundFrames, tenant_id: &str) -> OutboundFrames {
    let prefix = format!("tn:{}:", tenant_id);
    match frames {
        OutboundFrames::One(bytes) => {
            OutboundFrames::One(strip_tenant_prefix_from_frame(&bytes, &prefix))
        }
        OutboundFrames::Two { first, second } => OutboundFrames::Two {
            first: strip_tenant_prefix_from_frame(&first, &prefix),
            second: strip_tenant_prefix_from_frame(&second, &prefix),
        },
    }
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

/// Extract a lobby hint from reducer args (MsgPack-encoded array).
///
/// Looks at the first element of the args array. If it's a string matching
/// `l{digits}_...` (e.g. `"l42_p123"`), returns `Some("42")`.
/// Returns `None` on decode failure or when no lobby prefix is present.
fn extract_lobby_hint(args: &[u8]) -> Option<String> {
    let arr: Vec<serde_json::Value> = rmp_serde::from_slice(args).ok()?;
    let first = arr.first()?.as_str()?;
    // Reuse parse_lobby_key logic: "l42_p123" has the shape of a lobby key.
    crate::table::parse_lobby_key(first).map(|(lid, _)| lid)
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
            tenant_id: None,
            lobby_hint: None,
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

    // ── is_jwt helper tests ──────────────────────────────────────────────────

    #[test]
    fn test_is_jwt_with_three_segments() {
        // A real JWT (three base64url segments separated by dots)
        assert!(is_jwt("eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJhbGljZSJ9.SIGNATURE"));
    }

    #[test]
    fn test_is_jwt_rejects_api_key() {
        assert!(!is_jwt("my-secret-api-key"));
    }

    #[test]
    fn test_is_jwt_rejects_key_with_role_suffix() {
        // API key with role: "key:role" — one colon but no dots → not JWT
        assert!(!is_jwt("my-key:admin"));
    }

    #[test]
    fn test_is_jwt_rejects_two_segment_string() {
        // Only one dot → 2 segments, not 3 → not a JWT
        assert!(!is_jwt("header.payload"));
    }

    #[test]
    fn test_is_jwt_rejects_four_segment_string() {
        // Three dots → 4 segments — also not a valid JWT
        assert!(!is_jwt("a.b.c.d"));
    }

    #[test]
    fn test_is_jwt_empty_string_not_jwt() {
        assert!(!is_jwt(""));
    }
}
