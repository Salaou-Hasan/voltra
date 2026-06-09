// ============================================================================
// NeonDB Rust Client SDK — NeonDBClient
// Session 31 — TODO-021: Optimistic updates
//   client.call_optimistic(reducer, args, |cache| new_cache) applies a
//   speculative cache update immediately and rolls back on server error.
// Session (auto-reconnect) — exponential-backoff reconnect with:
//   - pending call queue (calls made while disconnected are buffered)
//   - subscription re-issue after reconnect
//   - optimistic rollback on disconnect (pitfall #22)
//   - ClientEvent broadcast channel
// ============================================================================

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, Message},
};

use crate::{
    error::{NeonDBError, Result},
    protocol::{decode_server_frame, encode_args, encode_client_message},
    types::{ClientMessage, ClientOptions, ReducerCall, RowCache, RowDiff, ServerMessage},
};

// ── Reconnect configuration ───────────────────────────────────────────────────

/// Configuration for the exponential-backoff auto-reconnect logic.
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    /// Whether to attempt reconnects at all.  Default: `true`.
    pub enabled: bool,
    /// Maximum number of consecutive reconnect attempts before giving up.
    /// `None` means retry forever.  Default: `None`.
    pub max_attempts: Option<u32>,
    /// Starting delay for the exponential backoff.  Default: 1 second.
    pub base_delay: Duration,
    /// Upper cap on delay between reconnect attempts.  Default: 30 seconds.
    pub max_delay: Duration,
    /// Add ±25% random jitter to each computed delay.  Default: `true`.
    pub jitter: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: None,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: true,
        }
    }
}

/// Compute the backoff delay for a given attempt number (0-based).
///
/// Formula: `min(max_delay, base_delay * 2^attempt)` ± 25% jitter when enabled.
pub fn compute_backoff_delay(cfg: &ReconnectConfig, attempt: u32) -> Duration {
    let base_ms = cfg.base_delay.as_millis() as f64;
    let max_ms = cfg.max_delay.as_millis() as f64;
    let raw = (base_ms * 2_f64.powi(attempt as i32)).min(max_ms);
    let final_ms = if cfg.jitter {
        // Cheap pseudo-random jitter using current time nanoseconds.
        // Produces a multiplier in [0.75, 1.25].
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_nanos(0))
            .subsec_nanos();
        // Map nanos into [0, 1) then into [0.75, 1.25].
        let frac = (nanos as f64) / 1_000_000_000.0;
        let factor = 0.75 + frac * 0.5;
        (raw * factor).round()
    } else {
        raw
    };
    Duration::from_millis(final_ms as u64)
}

// ── Client events ─────────────────────────────────────────────────────────────

/// Events broadcast by the client for observability.
#[derive(Clone, Debug)]
pub enum ClientEvent {
    /// The WebSocket closed unexpectedly (before any reconnect attempt).
    Disconnected,
    /// A reconnect attempt succeeded.
    Reconnected {
        /// 1-based attempt number that succeeded.
        attempt: u32,
    },
    /// All reconnect attempts were exhausted (`max_attempts` was finite).
    ReconnectFailed,
}

// ── Internal command channel ──────────────────────────────────────────────────

/// Type alias for a boxed, replayable optimistic mutation function.
///
/// Unlike `FnOnce`, these can be called multiple times so the cache can be
/// re-computed after any layer is removed (see TODO-036).
pub type OptimisticMutation =
    Arc<dyn Fn(CacheSnapshot) -> CacheSnapshot + Send + Sync + 'static>;

enum Command {
    Call {
        call_id: u64,
        reducer_name: String,
        args: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>>>,
    },
    Subscribe {
        sub_id: String,
        query: String,
        tx: mpsc::UnboundedSender<RowDiff>,
    },
    Unsubscribe {
        sub_id: String,
    },
    /// Push an optimistic mutation layer onto the background task's layer stack.
    RegisterLayer {
        call_id: u64,
        mutation: OptimisticMutation,
    },
    /// Signal the background task to disconnect (user-initiated close).
    Shutdown,
}

/// A handle to a subscription.  Drop or call `.unsubscribe()` to cancel.
pub struct Subscription {
    pub id: String,
    cmd_tx: mpsc::UnboundedSender<Command>,
}

impl Subscription {
    pub async fn unsubscribe(self) {
        let _ = self.cmd_tx.send(Command::Unsubscribe { sub_id: self.id });
    }
}

// ── Cache snapshot helpers ────────────────────────────────────────────────────

/// A plain serializable snapshot of the row cache for one instant.
pub type CacheSnapshot = HashMap<String, HashMap<String, serde_json::Value>>;

/// Convert the DashMap cache into a plain HashMap snapshot.
#[allow(dead_code)]
fn snapshot_dashmap_cache(cache: &Arc<DashMap<String, RowCache>>) -> CacheSnapshot {
    let mut snap: CacheSnapshot = HashMap::new();
    for table_ref in cache.iter() {
        let mut rows: HashMap<String, serde_json::Value> = HashMap::new();
        for row_ref in table_ref.value().iter() {
            rows.insert(row_ref.key().clone(), row_ref.value().clone());
        }
        snap.insert(table_ref.key().clone(), rows);
    }
    snap
}

/// Apply a snapshot back to the DashMap cache, replacing all contents.
fn apply_snapshot_to_cache(cache: &Arc<DashMap<String, RowCache>>, snap: &CacheSnapshot) {
    cache.clear();
    for (table, rows) in snap {
        let table_map: RowCache = DashMap::new();
        for (k, v) in rows {
            table_map.insert(k.clone(), v.clone());
        }
        cache.insert(table.clone(), table_map);
    }
}

/// Recompute the derived cache = `server_base` + all `layers` applied in order,
/// then write the result to the shared DashMap.
///
/// This is the core of the TODO-036 race fix: removing any single layer and
/// calling this function automatically re-applies all remaining layers on top of
/// the clean server-confirmed base, without clobbering sibling pending calls.
fn recompute_and_apply(
    server_base: &CacheSnapshot,
    layers: &[(u64, OptimisticMutation)],
    cache: &Arc<DashMap<String, RowCache>>,
) {
    if layers.is_empty() {
        apply_snapshot_to_cache(cache, server_base);
        return;
    }
    let mut current = server_base.clone();
    for (_, mutation) in layers {
        current = mutation(current);
    }
    apply_snapshot_to_cache(cache, &current);
}

// ── NeonDBClient ──────────────────────────────────────────────────────────────

/// Async Rust client for NeonDB.
///
/// # Example
/// ```no_run
/// use neondb_client::{NeonDBClient, ClientOptions, ReconnectConfig};
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = NeonDBClient::connect(ClientOptions {
///         url: "ws://localhost:3000".to_string(),
///         reconnect: Some(ReconnectConfig {
///             max_attempts: Some(10),
///             base_delay: Duration::from_millis(500),
///             ..Default::default()
///         }),
///         ..Default::default()
///     }).await?;
///
///     // Subscribe to connection lifecycle events.
///     let mut events = client.events();
///     tokio::spawn(async move {
///         while let Ok(ev) = events.recv().await {
///             println!("event: {:?}", ev);
///         }
///     });
///
///     // Standard call
///     let result_bytes = client.call("increment", &("score", 5_i32)).await?;
///
///     // Optimistic call — speculative UI update then server reconcile
///     client.call_optimistic(
///         "move_player",
///         &("alice", 5_i32, 3_i32),
///         |mut cache| {
///             if let Some(players) = cache.get_mut("players") {
///                 players.insert("alice".to_string(),
///                     serde_json::json!({"x": 5, "y": 3}));
///             }
///             cache
///         },
///     ).await?;
///
///     client.disconnect().await;
///     Ok(())
/// }
/// ```
pub struct NeonDBClient {
    cmd_tx: mpsc::UnboundedSender<Command>,
    next_call_id: Arc<AtomicU64>,
    next_sub_id: Arc<AtomicU64>,
    /// Local row cache populated by subscription diffs and optimistic updates.
    pub cache: Arc<DashMap<String, RowCache>>,
    opts: ClientOptions,
    /// Broadcast channel for connection lifecycle events.
    event_tx: broadcast::Sender<ClientEvent>,
    /// Signals to the background task that the user has requested a disconnect.
    shutdown_flag: Arc<AtomicBool>,
}

impl NeonDBClient {
    /// Connect to NeonDB and return a client handle.
    pub async fn connect(opts: ClientOptions) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
        let cache: Arc<DashMap<String, RowCache>> = Arc::new(DashMap::new());
        let cache_c = cache.clone();
        let (event_tx, _) = broadcast::channel::<ClientEvent>(64);
        let event_tx_c = event_tx.clone();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_c = shutdown_flag.clone();

        let reconnect = opts.reconnect.clone().unwrap_or_default();
        let url = opts.url.clone();
        let api_key = opts.api_key.clone();

        // Make the initial WebSocket connection.
        let ws_stream = make_ws_connection(&url, api_key.as_deref()).await?;

        tokio::spawn(async move {
            run_connection_with_reconnect(
                ws_stream,
                cmd_rx,
                cache_c,
                event_tx_c,
                shutdown_flag_c,
                reconnect,
                url,
                api_key,
            )
            .await;
        });

        Ok(NeonDBClient {
            cmd_tx,
            next_call_id: Arc::new(AtomicU64::new(1)),
            next_sub_id: Arc::new(AtomicU64::new(1)),
            cache,
            opts,
            event_tx,
            shutdown_flag,
        })
    }

    /// Subscribe to connection lifecycle events.
    ///
    /// Returns a `broadcast::Receiver` that delivers `ClientEvent` values.
    /// Multiple callers can call `events()` independently; each gets its own receiver.
    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    /// Call a reducer and return the raw result bytes.
    pub async fn call<A: serde::Serialize>(&self, reducer_name: &str, args: &A) -> Result<Vec<u8>> {
        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let args_bytes = encode_args(args)?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Call {
                call_id,
                reducer_name: reducer_name.to_string(),
                args: args_bytes,
                reply: reply_tx,
            })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        tokio::time::timeout(Duration::from_millis(self.opts.call_timeout_ms), reply_rx)
            .await
            .map_err(|_| NeonDBError::Timeout(self.opts.call_timeout_ms))?
            .map_err(|_| NeonDBError::ConnectionClosed)?
    }

    /// Call a reducer with an **optimistic** cache update.
    ///
    /// `optimistic_fn` receives the current cache snapshot and returns a
    /// speculative updated snapshot.  The background task pushes it as a layer
    /// on top of the server-confirmed base so that `get_row()` / `get_rows()`
    /// reflect the change immediately.
    ///
    /// **Concurrent-call race fix (TODO-036):** unlike the previous snapshot-
    /// restore approach, the background task maintains an ordered layer stack.
    /// Rolling back call #1 removes its layer and re-applies call #2's layer on
    /// top of the clean server base — so call #2's speculative state is
    /// preserved correctly.
    ///
    /// On server success: the layer is removed; server subscription diffs
    ///   update `server_base_cache` and the derived cache is recomputed.
    /// On server error: the layer is removed (rolled back) and the remaining
    ///   layers are re-applied automatically.
    pub async fn call_optimistic<A, F>(
        &self,
        reducer_name: &str,
        args: &A,
        optimistic_fn: F,
    ) -> Result<Vec<u8>>
    where
        A: serde::Serialize,
        F: Fn(CacheSnapshot) -> CacheSnapshot + Send + Sync + 'static,
    {
        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let args_bytes = encode_args(args)?;

        // 1. Send RegisterLayer — background task applies mutation and writes
        //    the speculative state to the shared DashMap (pitfall #22: ordered
        //    channel guarantees RegisterLayer is processed before Call reply).
        let mutation: OptimisticMutation = Arc::new(optimistic_fn);
        self.cmd_tx
            .send(Command::RegisterLayer { call_id, mutation })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        // 2. Enqueue the actual network call.
        let (inner_tx, inner_rx) = oneshot::channel::<Result<Vec<u8>>>();
        self.cmd_tx
            .send(Command::Call {
                call_id,
                reducer_name: reducer_name.to_string(),
                args: args_bytes,
                reply: inner_tx,
            })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        // 3. Await the network result.
        let result = tokio::time::timeout(
            Duration::from_millis(self.opts.call_timeout_ms),
            inner_rx,
        )
        .await
        .map_err(|_| NeonDBError::Timeout(self.opts.call_timeout_ms))?
        .map_err(|_| NeonDBError::ConnectionClosed)??;

        Ok(result)
    }

    /// Decode raw result bytes from `call()` into a typed value.
    pub fn decode_result<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        crate::protocol::decode_result(bytes)
    }

    /// Subscribe to a table query.
    pub async fn subscribe(
        &self,
        query: &str,
    ) -> Result<(Subscription, mpsc::UnboundedReceiver<RowDiff>)> {
        let sub_id = format!(
            "rs_sub_{}",
            self.next_sub_id.fetch_add(1, Ordering::Relaxed)
        );
        let (diff_tx, diff_rx) = mpsc::unbounded_channel::<RowDiff>();

        self.cmd_tx
            .send(Command::Subscribe {
                sub_id: sub_id.clone(),
                query: query.to_string(),
                tx: diff_tx,
            })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        let sub = Subscription {
            id: sub_id,
            cmd_tx: self.cmd_tx.clone(),
        };
        Ok((sub, diff_rx))
    }

    /// Get the cached rows for a table.
    pub fn get_rows(
        &self,
        table_name: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, RowCache>> {
        self.cache.get(table_name)
    }

    /// Get a single cached row.
    pub fn get_row(&self, table_name: &str, row_key: &str) -> Option<serde_json::Value> {
        self.cache.get(table_name)?.get(row_key).map(|v| v.clone())
    }

    /// Disconnect from NeonDB.
    pub async fn disconnect(self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        let _ = self.cmd_tx.send(Command::Shutdown);
    }
}

// ── WebSocket connection helper ───────────────────────────────────────────────

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn make_ws_connection(url: &str, api_key: Option<&str>) -> Result<WsStream> {
    let mut request = url
        .into_client_request()
        .map_err(NeonDBError::WebSocket)?;
    if let Some(key) = api_key {
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", key)
                .parse()
                .expect("valid header value"),
        );
    }
    let (ws_stream, _) = connect_async_with_config(request, None, false)
        .await
        .map_err(NeonDBError::WebSocket)?;
    Ok(ws_stream)
}

// ── Background connection task with reconnect ─────────────────────────────────

/// Top-level reconnect loop.  Runs the inner connection loop, and on
/// unexpected close schedules reconnect attempts with exponential backoff.
async fn run_connection_with_reconnect(
    initial_ws: WsStream,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    cache: Arc<DashMap<String, RowCache>>,
    event_tx: broadcast::Sender<ClientEvent>,
    shutdown_flag: Arc<AtomicBool>,
    reconnect: ReconnectConfig,
    url: String,
    api_key: Option<String>,
) {
    // Track all active subscriptions: sub_id → (query, diff sender).
    let mut active_subs: HashMap<String, (String, mpsc::UnboundedSender<RowDiff>)> =
        HashMap::new();
    // Calls buffered while the connection is down: (call_id, reducer, args, reply).
    let mut pending_queue: Vec<(u64, String, Vec<u8>, oneshot::Sender<Result<Vec<u8>>>)> =
        Vec::new();
    let user_shutdown = run_connection_inner(
        initial_ws,
        &mut cmd_rx,
        &cache,
        &event_tx,
        &mut active_subs,
        &mut pending_queue,
    )
    .await;

    if user_shutdown || !reconnect.enabled {
        drain_pending_queue(&mut pending_queue);
        return;
    }

    // Broadcast disconnect event.
    let _ = event_tx.send(ClientEvent::Disconnected);

    let mut attempt: u32 = 0;
    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            drain_pending_queue(&mut pending_queue);
            break;
        }

        if let Some(max) = reconnect.max_attempts {
            if attempt >= max {
                log::warn!(
                    "[neondb-client] Reconnect exhausted after {} attempts",
                    attempt
                );
                let _ = event_tx.send(ClientEvent::ReconnectFailed);
                drain_pending_queue(&mut pending_queue);
                break;
            }
        }

        let delay = compute_backoff_delay(&reconnect, attempt);
        log::debug!(
            "[neondb-client] Reconnecting in {}ms (attempt {})",
            delay.as_millis(),
            attempt + 1
        );
        tokio::time::sleep(delay).await;
        attempt += 1;

        if shutdown_flag.load(Ordering::SeqCst) {
            drain_pending_queue(&mut pending_queue);
            break;
        }

        match make_ws_connection(&url, api_key.as_deref()).await {
            Err(e) => {
                log::warn!(
                    "[neondb-client] Reconnect attempt {} failed: {}",
                    attempt,
                    e
                );
                // Loop and try again.
            }
            Ok(ws) => {
                let _ = event_tx.send(ClientEvent::Reconnected { attempt });
                log::info!("[neondb-client] Reconnected (attempt {})", attempt);
                attempt = 0; // reset backoff counter after successful connect

                let was_user_shutdown = run_connection_inner(
                    ws,
                    &mut cmd_rx,
                    &cache,
                    &event_tx,
                    &mut active_subs,
                    &mut pending_queue,
                )
                .await;

                if was_user_shutdown || !reconnect.enabled {
                    drain_pending_queue(&mut pending_queue);
                    break;
                }
                let _ = event_tx.send(ClientEvent::Disconnected);
            }
        }
    }
}

fn drain_pending_queue(
    queue: &mut Vec<(u64, String, Vec<u8>, oneshot::Sender<Result<Vec<u8>>>)>,
) {
    for (_, _, _, reply) in queue.drain(..) {
        let _ = reply.send(Err(NeonDBError::ConnectionClosed));
    }
}

/// Inner connection loop.  Returns `true` if shutdown was user-initiated.
///
/// On entry: re-issues all `active_subs` and flushes any `pending_queue` calls.
/// On exit: all still-pending calls are added to `pending_queue` for the
/// reconnect loop to re-send after the next successful connection.
async fn run_connection_inner(
    ws: WsStream,
    cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
    cache: &Arc<DashMap<String, RowCache>>,
    _event_tx: &broadcast::Sender<ClientEvent>,
    active_subs: &mut HashMap<String, (String, mpsc::UnboundedSender<RowDiff>)>,
    pending_queue: &mut Vec<(u64, String, Vec<u8>, oneshot::Sender<Result<Vec<u8>>>)>,
) -> bool {
    let (mut sink, mut stream) = ws.split();

    // Server-confirmed row state — only updated by subscription diffs/snapshots.
    let mut server_base_cache: CacheSnapshot = HashMap::new();
    // Ordered stack of in-flight optimistic mutations.
    let mut optimistic_layers: Vec<(u64, OptimisticMutation)> = Vec::new();

    // Re-issue active subscriptions so the server delivers fresh snapshots.
    for (sub_id, (query, _)) in active_subs.iter() {
        let msg = ClientMessage::Subscribe {
            subscription_id: sub_id.clone(),
            query: query.clone(),
        };
        if let Ok(bytes) = encode_client_message(&msg) {
            let _ = sink.send(Message::Binary(bytes)).await;
        }
    }

    // Flush calls that were queued while disconnected.
    let mut pending_calls: HashMap<u64, oneshot::Sender<Result<Vec<u8>>>> = HashMap::new();
    let queued = std::mem::take(pending_queue);
    for (call_id, reducer_name, args, reply) in queued {
        let msg = ClientMessage::ReducerCall(ReducerCall {
            call_id,
            reducer_name,
            args,
        });
        match encode_client_message(&msg) {
            Ok(bytes) => {
                pending_calls.insert(call_id, reply);
                let _ = sink.send(Message::Binary(bytes)).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    let mut pending_route: Option<Vec<String>> = None;
    let mut user_shutdown = false;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break,
                    Some(Command::Shutdown) => {
                        user_shutdown = true;
                        break;
                    }

                    Some(Command::Call { call_id, reducer_name, args, reply }) => {
                        let msg = ClientMessage::ReducerCall(ReducerCall {
                            call_id,
                            reducer_name,
                            args,
                        });
                        match encode_client_message(&msg) {
                            Ok(bytes) => {
                                pending_calls.insert(call_id, reply);
                                let _ = sink.send(Message::Binary(bytes)).await;
                            }
                            Err(e) => { let _ = reply.send(Err(e)); }
                        }
                    }

                    Some(Command::RegisterLayer { call_id, mutation }) => {
                        // Push the optimistic mutation onto the layer stack and
                        // recompute the derived cache (server_base + all layers).
                        optimistic_layers.push((call_id, mutation));
                        recompute_and_apply(&server_base_cache, &optimistic_layers, cache);
                    }

                    Some(Command::Subscribe { sub_id, query, tx }) => {
                        let msg = ClientMessage::Subscribe {
                            subscription_id: sub_id.clone(),
                            query: query.clone(),
                        };
                        if let Ok(bytes) = encode_client_message(&msg) {
                            let _ = sink.send(Message::Binary(bytes)).await;
                        }
                        active_subs.insert(sub_id.clone(), (query, tx));
                    }

                    Some(Command::Unsubscribe { sub_id }) => {
                        active_subs.remove(&sub_id);
                        let msg = ClientMessage::Unsubscribe {
                            subscription_id: sub_id,
                        };
                        if let Ok(bytes) = encode_client_message(&msg) {
                            let _ = sink.send(Message::Binary(bytes)).await;
                        }
                    }
                }
            }

            frame = stream.next() => {
                match frame {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("[neondb-client] WebSocket error: {}", e);
                        break;
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if let Some(msg) = decode_server_frame(&data) {
                            dispatch_message(
                                msg,
                                &mut pending_calls,
                                active_subs,
                                &mut pending_route,
                                &mut server_base_cache,
                                &mut optimistic_layers,
                                cache,
                            );
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }

    // Any calls still awaiting a response: reject them so their oneshots don't hang.
    for (_, reply) in pending_calls.drain() {
        let _ = reply.send(Err(NeonDBError::ConnectionClosed));
    }

    user_shutdown
}

fn dispatch_message(
    msg: ServerMessage,
    pending_calls: &mut HashMap<u64, oneshot::Sender<Result<Vec<u8>>>>,
    subscriptions: &mut HashMap<String, (String, mpsc::UnboundedSender<RowDiff>)>,
    pending_route: &mut Option<Vec<String>>,
    server_base_cache: &mut CacheSnapshot,
    optimistic_layers: &mut Vec<(u64, OptimisticMutation)>,
    cache: &Arc<DashMap<String, RowCache>>,
) {
    match msg {
        ServerMessage::ReducerResponse(resp) => {
            if let Some(reply) = pending_calls.remove(&resp.call_id) {
                // Remove the optimistic layer for this call regardless of success/failure.
                // On failure this IS the rollback: recomputing without the failed layer
                // re-applies any remaining sibling layers on top of server_base (TODO-036).
                let had_layer = {
                    let before = optimistic_layers.len();
                    optimistic_layers.retain(|(id, _)| *id != resp.call_id);
                    optimistic_layers.len() < before
                };
                if had_layer {
                    recompute_and_apply(server_base_cache, optimistic_layers, cache);
                }

                let result = if resp.success {
                    Ok(resp.result.unwrap_or_default())
                } else {
                    Err(NeonDBError::ReducerError(
                        resp.error.unwrap_or_else(|| "Unknown error".to_string()),
                    ))
                };
                let _ = reply.send(result);
            }
        }

        ServerMessage::SubscriptionAck(ack) => {
            if !ack.success {
                log::warn!(
                    "[neondb-client] Subscription '{}' failed: {:?}",
                    ack.subscription_id,
                    ack.message
                );
                subscriptions.remove(&ack.subscription_id);
            }
        }

        ServerMessage::SubscriptionDiff(diff) => {
            let row_diff = RowDiff {
                subscription_id: diff.subscription_id.clone(),
                table_name: diff.table_name.clone(),
                row_key: diff.row_key.clone(),
                operation: diff.operation.clone(),
                row_data: diff.row_data.clone(),
            };
            // Apply to server_base_cache (source of truth), then recompute derived cache.
            apply_to_base(
                server_base_cache,
                &diff.table_name,
                &diff.row_key,
                &diff.operation,
                diff.row_data,
            );
            recompute_and_apply(server_base_cache, optimistic_layers, cache);
            if let Some((_, tx)) = subscriptions.get(&diff.subscription_id) {
                let _ = tx.send(row_diff);
            }
        }

        ServerMessage::SubscriptionRoute(route) => {
            *pending_route = Some(route.subscription_ids);
        }

        ServerMessage::SubscriptionBody(body) => {
            if let Some(ids) = pending_route.take() {
                apply_to_base(
                    server_base_cache,
                    &body.table_name,
                    &body.row_key,
                    &body.operation,
                    body.row_data.clone(),
                );
                recompute_and_apply(server_base_cache, optimistic_layers, cache);
                for sub_id in &ids {
                    let diff = RowDiff {
                        subscription_id: sub_id.clone(),
                        table_name: body.table_name.clone(),
                        row_key: body.row_key.clone(),
                        operation: body.operation.clone(),
                        row_data: body.row_data.clone(),
                    };
                    if let Some((_, tx)) = subscriptions.get(sub_id) {
                        let _ = tx.send(diff);
                    }
                }
            }
        }

        ServerMessage::Error { message } => {
            log::warn!("[neondb-client] Server error: {}", message);
        }
    }
}

/// Apply a single row diff to the server-base cache (plain HashMap).
/// After calling this, call `recompute_and_apply` to update the visible DashMap cache.
fn apply_to_base(
    base: &mut CacheSnapshot,
    table_name: &str,
    row_key: &str,
    operation: &str,
    row_data: Option<serde_json::Value>,
) {
    let table = base
        .entry(table_name.to_string())
        .or_insert_with(HashMap::new);
    if operation == "delete" {
        table.remove(row_key);
    } else if let Some(data) = row_data {
        table.insert(row_key.to_string(), data);
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ReconnectConfig defaults ──────────────────────────────────────────────

    #[test]
    fn reconnect_config_default_enabled() {
        let cfg = ReconnectConfig::default();
        assert!(cfg.enabled, "reconnect should be enabled by default");
    }

    #[test]
    fn reconnect_config_default_max_attempts_is_none() {
        let cfg = ReconnectConfig::default();
        assert!(
            cfg.max_attempts.is_none(),
            "max_attempts should be None (infinite) by default"
        );
    }

    #[test]
    fn reconnect_config_default_delays() {
        let cfg = ReconnectConfig::default();
        assert_eq!(cfg.base_delay, Duration::from_secs(1));
        assert_eq!(cfg.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn reconnect_config_default_jitter_enabled() {
        let cfg = ReconnectConfig::default();
        assert!(cfg.jitter, "jitter should be enabled by default");
    }

    // ── Exponential backoff math ──────────────────────────────────────────────

    #[test]
    fn backoff_no_jitter_exact_exponential() {
        let cfg = ReconnectConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: false,
            ..Default::default()
        };
        assert_eq!(compute_backoff_delay(&cfg, 0), Duration::from_millis(1_000));
        assert_eq!(compute_backoff_delay(&cfg, 1), Duration::from_millis(2_000));
        assert_eq!(compute_backoff_delay(&cfg, 2), Duration::from_millis(4_000));
        assert_eq!(compute_backoff_delay(&cfg, 3), Duration::from_millis(8_000));
    }

    #[test]
    fn backoff_capped_at_max_delay() {
        let cfg = ReconnectConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            jitter: false,
            ..Default::default()
        };
        // attempt 10 → 2^10 * 1000ms = 1_024_000ms >> 10_000ms
        assert_eq!(
            compute_backoff_delay(&cfg, 10),
            Duration::from_millis(10_000)
        );
    }

    #[test]
    fn backoff_jitter_within_25_percent() {
        let cfg = ReconnectConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: true,
            ..Default::default()
        };
        for attempt in 0u32..5 {
            let raw_ms = (1_000_f64 * 2_f64.powi(attempt as i32)).min(60_000_f64);
            let low = Duration::from_millis((raw_ms * 0.75).floor() as u64);
            let high = Duration::from_millis((raw_ms * 1.25).ceil() as u64);
            for _ in 0..50 {
                let d = compute_backoff_delay(&cfg, attempt);
                assert!(
                    d >= low && d <= high,
                    "attempt {attempt}: delay {d:?} not in [{low:?}, {high:?}]"
                );
            }
        }
    }

    // ── ClientEvent variants ──────────────────────────────────────────────────

    #[test]
    fn client_event_disconnected_is_clone_and_debug() {
        let ev = ClientEvent::Disconnected;
        let ev2 = ev.clone();
        assert!(format!("{ev2:?}").contains("Disconnected"));
    }

    #[test]
    fn client_event_reconnected_carries_attempt_number() {
        let ev = ClientEvent::Reconnected { attempt: 3 };
        match ev {
            ClientEvent::Reconnected { attempt } => assert_eq!(attempt, 3),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_event_reconnect_failed_debug() {
        let ev = ClientEvent::ReconnectFailed;
        assert!(format!("{ev:?}").contains("ReconnectFailed"));
    }
}
