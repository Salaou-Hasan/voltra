// ============================================================================
// NeonDB Rust Client SDK — NeonDBClient
// Session 31 — TODO-021: Optimistic updates
//   client.call_optimistic(reducer, args, |cache| new_cache) applies a
//   speculative cache update immediately and rolls back on server error.
// ============================================================================

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, Message},
};

use crate::{
    error::{NeonDBError, Result},
    protocol::{decode_server_frame, encode_args, encode_client_message},
    types::{ClientMessage, ClientOptions, ReducerCall, RowCache, RowDiff, ServerMessage},
};

// ── Internal command channel ──────────────────────────────────────────────────

#[derive(Debug)]
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
    /// Apply an optimistic cache snapshot (before the call is sent).
    ApplyOptimistic {
        call_id: u64,
        snapshot: HashMap<String, HashMap<String, serde_json::Value>>,
        optimistic: HashMap<String, HashMap<String, serde_json::Value>>,
        reply: oneshot::Sender<Result<Vec<u8>>>,
        inner_call_id: u64,
    },
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

// ── NeonDBClient ──────────────────────────────────────────────────────────────

/// Async Rust client for NeonDB.
///
/// # Example
/// ```no_run
/// use neondb_client::{NeonDBClient, ClientOptions};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let client = NeonDBClient::connect(ClientOptions {
///         url: "ws://localhost:3000".to_string(),
///         ..Default::default()
///     }).await?;
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
}

impl NeonDBClient {
    /// Connect to NeonDB and return a client handle.
    pub async fn connect(opts: ClientOptions) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
        let cache: Arc<DashMap<String, RowCache>> = Arc::new(DashMap::new());
        let cache_c = cache.clone();

        let mut request = opts
            .url
            .as_str()
            .into_client_request()
            .map_err(NeonDBError::WebSocket)?;
        if let Some(key) = &opts.api_key {
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

        tokio::spawn(async move {
            run_connection(ws_stream, cmd_rx, cache_c).await;
        });

        Ok(NeonDBClient {
            cmd_tx,
            next_call_id: Arc::new(AtomicU64::new(1)),
            next_sub_id: Arc::new(AtomicU64::new(1)),
            cache,
            opts,
        })
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
    /// speculative updated snapshot.  The client immediately applies it so
    /// that `get_row()` / `get_rows()` reflect the change.
    ///
    /// On server success: server subscription diffs reconcile naturally.
    /// On server error: the cache is automatically rolled back and an
    /// `Err(NeonDBError::ReducerError(_))` is returned.
    ///
    /// ```rust,no_run
    /// client.call_optimistic(
    ///     "move_player",
    ///     &("alice", 5_i32, 3_i32),
    ///     |mut cache| {
    ///         if let Some(players) = cache.get_mut("players") {
    ///             players.insert("alice".to_string(),
    ///                 serde_json::json!({"x": 5, "y": 3}));
    ///         }
    ///         cache
    ///     },
    /// ).await?;
    /// ```
    pub async fn call_optimistic<A, F>(
        &self,
        reducer_name: &str,
        args: &A,
        optimistic_fn: F,
    ) -> Result<Vec<u8>>
    where
        A: serde::Serialize,
        F: FnOnce(CacheSnapshot) -> CacheSnapshot + Send + 'static,
    {
        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
        let args_bytes = encode_args(args)?;

        // 1. Snapshot the current cache.
        let snapshot = snapshot_dashmap_cache(&self.cache);

        // 2. Apply the speculative state to the live cache.
        let optimistic_state = optimistic_fn(snapshot.clone());
        apply_snapshot_to_cache(&self.cache, &optimistic_state);

        // 3. Send the actual reducer call; background task handles rollback.
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::ApplyOptimistic {
                call_id,
                snapshot: snapshot.clone(),
                optimistic: optimistic_state,
                reply: reply_tx,
                inner_call_id: call_id,
            })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        // Also enqueue the actual network call so the worker sends the frame.
        let (inner_tx, inner_rx) = oneshot::channel::<Result<Vec<u8>>>();
        self.cmd_tx
            .send(Command::Call {
                call_id,
                reducer_name: reducer_name.to_string(),
                args: args_bytes,
                reply: inner_tx,
            })
            .map_err(|_| NeonDBError::ConnectionClosed)?;

        // Await the network result.
        let result = tokio::time::timeout(
            Duration::from_millis(self.opts.call_timeout_ms),
            inner_rx,
        )
        .await
        .map_err(|_| NeonDBError::Timeout(self.opts.call_timeout_ms))?
        .map_err(|_| NeonDBError::ConnectionClosed)??;

        // Complete the optimistic bookkeeping channel (it's a oneshot drain).
        let _ = reply_rx;

        Ok(result)
    }

    /// Roll back the cache to `snapshot`, used after a failed optimistic call.
    fn rollback_cache(&self, snapshot: &CacheSnapshot) {
        apply_snapshot_to_cache(&self.cache, snapshot);
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
        let _ = self.cmd_tx.send(Command::Shutdown);
    }
}

// ── Background connection task ────────────────────────────────────────────────

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn run_connection(
    ws: WsStream,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    cache: Arc<DashMap<String, RowCache>>,
) {
    let (mut sink, mut stream) = ws.split();

    let mut pending_calls: HashMap<u64, oneshot::Sender<Result<Vec<u8>>>> = HashMap::new();
    let mut subscriptions: HashMap<String, mpsc::UnboundedSender<RowDiff>> = HashMap::new();
    let mut pending_route: Option<Vec<String>> = None;
    // Track rollback snapshots for optimistic calls indexed by call_id.
    let mut optimistic_snapshots: HashMap<u64, CacheSnapshot> = HashMap::new();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    None | Some(Command::Shutdown) => break,

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

                    Some(Command::ApplyOptimistic { call_id, snapshot, .. }) => {
                        // Register the rollback snapshot; actual Call follows.
                        optimistic_snapshots.insert(call_id, snapshot);
                    }

                    Some(Command::Subscribe { sub_id, query, tx }) => {
                        let msg = ClientMessage::Subscribe {
                            subscription_id: sub_id.clone(),
                            query,
                        };
                        if let Ok(bytes) = encode_client_message(&msg) {
                            let _ = sink.send(Message::Binary(bytes)).await;
                        }
                        subscriptions.insert(sub_id, tx);
                    }

                    Some(Command::Unsubscribe { sub_id }) => {
                        subscriptions.remove(&sub_id);
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
                                &mut subscriptions,
                                &mut pending_route,
                                &mut optimistic_snapshots,
                                &cache,
                            );
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }

    for (_, reply) in pending_calls.drain() {
        let _ = reply.send(Err(NeonDBError::ConnectionClosed));
    }
}

fn dispatch_message(
    msg: ServerMessage,
    pending_calls: &mut HashMap<u64, oneshot::Sender<Result<Vec<u8>>>>,
    subscriptions: &mut HashMap<String, mpsc::UnboundedSender<RowDiff>>,
    pending_route: &mut Option<Vec<String>>,
    optimistic_snapshots: &mut HashMap<u64, CacheSnapshot>,
    cache: &Arc<DashMap<String, RowCache>>,
) {
    match msg {
        ServerMessage::ReducerResponse(resp) => {
            if let Some(reply) = pending_calls.remove(&resp.call_id) {
                let result = if resp.success {
                    // Clean up any optimistic snapshot — server confirmed.
                    optimistic_snapshots.remove(&resp.call_id);
                    Ok(resp.result.unwrap_or_default())
                } else {
                    // Roll back the optimistic cache if we have a snapshot.
                    if let Some(snap) = optimistic_snapshots.remove(&resp.call_id) {
                        apply_snapshot_to_cache(cache, &snap);
                    }
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
            apply_to_cache(
                cache,
                &diff.table_name,
                &diff.row_key,
                &diff.operation,
                diff.row_data,
            );
            if let Some(tx) = subscriptions.get(&diff.subscription_id) {
                let _ = tx.send(row_diff);
            }
        }

        ServerMessage::SubscriptionRoute(route) => {
            *pending_route = Some(route.subscription_ids);
        }

        ServerMessage::SubscriptionBody(body) => {
            if let Some(ids) = pending_route.take() {
                apply_to_cache(
                    cache,
                    &body.table_name,
                    &body.row_key,
                    &body.operation,
                    body.row_data.clone(),
                );
                for sub_id in &ids {
                    let diff = RowDiff {
                        subscription_id: sub_id.clone(),
                        table_name: body.table_name.clone(),
                        row_key: body.row_key.clone(),
                        operation: body.operation.clone(),
                        row_data: body.row_data.clone(),
                    };
                    if let Some(tx) = subscriptions.get(sub_id) {
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

fn apply_to_cache(
    cache: &Arc<DashMap<String, RowCache>>,
    table_name: &str,
    row_key: &str,
    operation: &str,
    row_data: Option<serde_json::Value>,
) {
    let table = cache
        .entry(table_name.to_string())
        .or_insert_with(DashMap::new);
    if operation == "delete" {
        table.remove(row_key);
    } else if let Some(data) = row_data {
        table.insert(row_key.to_string(), data);
    }
}
