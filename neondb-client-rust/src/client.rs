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
///     // Call the built-in increment reducer
///     let result_bytes = client.call("increment", &("score", 5_i32)).await?;
///     println!("result bytes: {:?}", result_bytes);
///
///     client.disconnect().await;
///     Ok(())
/// }
/// ```
pub struct NeonDBClient {
    cmd_tx: mpsc::UnboundedSender<Command>,
    next_call_id: Arc<AtomicU64>,
    next_sub_id: Arc<AtomicU64>,
    /// Local row cache populated by subscription diffs.
    pub cache: Arc<DashMap<String, RowCache>>,
    opts: ClientOptions,
}

impl NeonDBClient {
    /// Connect to NeonDB and return a client handle.
    pub async fn connect(opts: ClientOptions) -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
        let cache: Arc<DashMap<String, RowCache>> = Arc::new(DashMap::new());
        let cache_c = cache.clone();

        // Build the WebSocket request (add auth header if needed)
        let mut request = opts
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| NeonDBError::WebSocket(e))?;
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

        // Spawn the background reader/writer task
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
    ///
    /// ```rust,no_run
    /// // Built-in increment reducer (positional args matching Rust struct):
    /// let bytes = client.call("increment", &("score", 5_i32)).await?;
    /// ```
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

    /// Decode raw result bytes from `call()` into a typed value.
    pub fn decode_result<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        crate::protocol::decode_result(bytes)
    }

    /// Subscribe to a table query.  Returns a receiver channel for incoming diffs.
    ///
    /// Supported predicates: `WHERE field op value`, `WHERE field IN (...)`,
    /// `WHERE pred1 AND pred2`.
    ///
    /// The first messages will be `"initial_snapshot"` diffs for rows that existed
    /// at subscription time.
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

    /// Get the cached rows for a table (populated by subscriptions).
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

    // pending_calls: call_id → reply channel
    let mut pending_calls: HashMap<u64, oneshot::Sender<Result<Vec<u8>>>> = HashMap::new();
    // subscriptions: sub_id → diff sender
    let mut subscriptions: HashMap<String, mpsc::UnboundedSender<RowDiff>> = HashMap::new();
    // Two-frame state
    let mut pending_route: Option<Vec<String>> = None;

    loop {
        tokio::select! {
            // Outgoing commands from the application
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

            // Incoming frames from the server
            frame = stream.next() => {
                match frame {
                    None => break, // connection closed
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
                                &cache,
                            );
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {} // ping/pong/text — ignore
                }
            }
        }
    }

    // Reject all outstanding calls
    for (_, reply) in pending_calls.drain() {
        let _ = reply.send(Err(NeonDBError::ConnectionClosed));
    }
}

fn dispatch_message(
    msg: ServerMessage,
    pending_calls: &mut HashMap<u64, oneshot::Sender<Result<Vec<u8>>>>,
    subscriptions: &mut HashMap<String, mpsc::UnboundedSender<RowDiff>>,
    pending_route: &mut Option<Vec<String>>,
    cache: &Arc<DashMap<String, RowCache>>,
) {
    match msg {
        ServerMessage::ReducerResponse(resp) => {
            if let Some(reply) = pending_calls.remove(&resp.call_id) {
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
