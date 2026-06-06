//! Interactive CLI client commands for NeonDB.
//!
//! These commands connect to a *running* NeonDB server:
//!   - Read-only inspection (`status`, `tables`, `get`) uses the HTTP admin
//!     endpoint on the metrics port.
//!   - Interactive commands (`call`, `watch`) use the WebSocket port.
//!
//! All commands print human-friendly output and are designed to be usable by
//! beginners (sensible defaults) and experts (full flags).

use crate::error::{NeonDBError, Result};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Build a WebSocket request, optionally adding a Bearer auth header.
fn ws_request(
    url: &str,
    api_key: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut req = url.into_client_request().map_err(|e| {
        NeonDBError::network_error(format!("Invalid WebSocket URL '{}': {}", url, e))
    })?;
    if let Some(key) = api_key {
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {}", key)
                .parse()
                .map_err(|_| NeonDBError::invalid_argument("Invalid API key header value"))?,
        );
    }
    Ok(req)
}

/// Perform a simple HTTP GET and return the response body as a string.
async fn http_get(url: &str) -> Result<String> {
    let uri: hyper::Uri = url
        .parse()
        .map_err(|e| NeonDBError::network_error(format!("Invalid URL '{}': {}", url, e)))?;
    let client = hyper::Client::new();
    let resp = client.get(uri).await.map_err(|e| {
        NeonDBError::network_error(format!(
            "HTTP request failed: {}. Is the server running?",
            e
        ))
    })?;
    let status = resp.status();
    let bytes = hyper::body::to_bytes(resp.into_body())
        .await
        .map_err(|e| NeonDBError::network_error(format!("Failed to read response: {}", e)))?;
    let body = String::from_utf8_lossy(&bytes).to_string();
    if !status.is_success() {
        return Err(NeonDBError::network_error(format!(
            "Server returned {}: {}",
            status, body
        )));
    }
    Ok(body)
}

/// Pretty-print a JSON string (falls back to raw on parse failure).
fn print_json_pretty(raw: &str) {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| raw.to_string())
        ),
        Err(_) => println!("{}", raw),
    }
}

// ── status ─────────────────────────────────────────────────────────────────────

/// `neondb status` — show server health and metrics via the admin HTTP port.
pub async fn cmd_status(metrics_url: &str) -> Result<()> {
    let health_url = format!("{}/healthz", metrics_url.trim_end_matches('/'));
    let metrics_endpoint = format!("{}/metrics", metrics_url.trim_end_matches('/'));

    println!("Checking NeonDB at {} …\n", metrics_url);

    match http_get(&health_url).await {
        Ok(body) => {
            println!("● Server is UP");
            print_json_pretty(&body);
            println!();
            if let Ok(metrics) = http_get(&metrics_endpoint).await {
                println!("Metrics:");
                println!("{}", metrics);
            }
            Ok(())
        }
        Err(e) => {
            println!("● Server appears DOWN");
            println!("  {}", e);
            Err(e)
        }
    }
}

// ── tables ───────────────────────────────────────────────────────────────────

/// `neondb tables` — list all tables with row counts.
pub async fn cmd_tables(metrics_url: &str) -> Result<()> {
    let url = format!("{}/tables", metrics_url.trim_end_matches('/'));
    let body = http_get(&url).await?;
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    let empty = vec![];
    let tables = parsed
        .get("tables")
        .and_then(|t| t.as_array())
        .unwrap_or(&empty);
    if tables.is_empty() {
        println!("No tables yet.");
        return Ok(());
    }

    println!("{:<24} {:>10}", "TABLE", "ROWS");
    println!("{:<24} {:>10}", "─────", "────");
    for t in tables {
        let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let rows = t.get("rows").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("{:<24} {:>10}", name, rows);
    }
    let total = parsed
        .get("total_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    println!("{:<24} {:>10}", "─────", "────");
    println!("{:<24} {:>10}", "TOTAL", total);
    Ok(())
}

// ── get ────────────────────────────────────────────────────────────────────────

/// `neondb get <table> [key]` — read rows from a table.
pub async fn cmd_get(metrics_url: &str, table: &str, key: Option<&str>) -> Result<()> {
    let url = format!("{}/tables/{}", metrics_url.trim_end_matches('/'), table);
    let body = http_get(&url).await?;
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    let empty = vec![];
    let rows = parsed
        .get("rows")
        .and_then(|r| r.as_array())
        .unwrap_or(&empty);

    match key {
        Some(k) => {
            // Filter to a single row_key
            let found = rows
                .iter()
                .find(|row| row.get("row_key").and_then(|v| v.as_str()) == Some(k));
            match found {
                Some(row) => {
                    let data = row.get("data").cloned().unwrap_or(serde_json::Value::Null);
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&data).unwrap_or_default()
                    );
                }
                None => println!("Row '{}' not found in table '{}'", k, table),
            }
        }
        None => {
            if rows.is_empty() {
                println!("Table '{}' is empty.", table);
                return Ok(());
            }
            println!("Table '{}' ({} rows):\n", table, rows.len());
            for row in rows {
                let rk = row.get("row_key").and_then(|v| v.as_str()).unwrap_or("?");
                let data = row.get("data").cloned().unwrap_or(serde_json::Value::Null);
                println!(
                    "  [{}] {}",
                    rk,
                    serde_json::to_string(&data).unwrap_or_default()
                );
            }
        }
    }
    Ok(())
}

// ── call ───────────────────────────────────────────────────────────────────────

/// `neondb call <reducer> [args_json]` — invoke a reducer once and print the result.
///
/// `args_json` is a JSON value passed to the reducer.  For the built-in
/// `increment` reducer, pass a positional array: `'["my_counter", 5]'`.
pub async fn cmd_call(
    ws_url: &str,
    reducer: &str,
    args_json: Option<&str>,
    api_key: Option<&str>,
) -> Result<()> {
    // Parse args JSON → MessagePack
    let args_value: serde_json::Value = match args_json {
        Some(s) => serde_json::from_str(s)
            .map_err(|e| NeonDBError::invalid_argument(format!("Invalid args JSON: {}", e)))?,
        None => serde_json::json!([]),
    };
    let args_bytes = rmp_serde::to_vec(&args_value)
        .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    let request = ws_request(ws_url, api_key)?;
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| {
            NeonDBError::network_error(format!(
                "Connect failed: {}. Is the server running at {}?",
                e, ws_url
            ))
        })?;

    let call_id: u64 = 1;
    // {"ReducerCall": [call_id, reducer, args_bytes]}
    let frame = rmp_serde::to_vec(&CallWire {
        reducer_call: (call_id, reducer.to_string(), args_bytes),
    })
    .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| NeonDBError::network_error(e.to_string()))?;

    // Await one response
    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| NeonDBError::network_error("Timed out waiting for response".to_string()))?;

    match resp {
        Some(Ok(Message::Binary(data))) => {
            // [call_id, success, result|nil, error|nil]
            match rmp_serde::from_slice::<(u64, bool, Option<Vec<u8>>, Option<String>)>(&data) {
                Ok((_cid, success, result, error)) => {
                    if success {
                        println!("✓ Reducer '{}' succeeded.", reducer);
                        if let Some(bytes) = result {
                            // Try to decode result bytes as a JSON-ish value
                            match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
                                Ok(v) => println!(
                                    "Result: {}",
                                    serde_json::to_string_pretty(&v).unwrap_or_default()
                                ),
                                Err(_) => println!("Result: {} bytes (binary)", bytes.len()),
                            }
                        }
                    } else {
                        println!(
                            "✗ Reducer '{}' failed: {}",
                            reducer,
                            error.unwrap_or_default()
                        );
                    }
                }
                Err(e) => println!("Could not decode response: {}", e),
            }
        }
        Some(Ok(_)) => println!("Unexpected non-binary response"),
        Some(Err(e)) => return Err(NeonDBError::network_error(e.to_string())),
        None => {
            return Err(NeonDBError::network_error(
                "Connection closed without a response".to_string(),
            ))
        }
    }

    let _ = ws.close(None).await;
    Ok(())
}

// ── watch ──────────────────────────────────────────────────────────────────────

/// `neondb watch <query>` — subscribe to a table query and print live updates
/// until interrupted (Ctrl-C).
pub async fn cmd_watch(ws_url: &str, query: &str, api_key: Option<&str>) -> Result<()> {
    let request = ws_request(ws_url, api_key)?;
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| {
            NeonDBError::network_error(format!(
                "Connect failed: {}. Is the server running at {}?",
                e, ws_url
            ))
        })?;

    let sub_id = "cli_watch".to_string();
    let frame = rmp_serde::to_vec(&SubscribeWire {
        subscribe: (sub_id.clone(), query.to_string()),
    })
    .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| NeonDBError::network_error(e.to_string()))?;

    println!("Watching '{}' (Ctrl-C to stop)…\n", query);

    // Two-frame protocol routing state.
    let mut pending_route: Option<Vec<String>> = None;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nStopping watch.");
                break;
            }
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        handle_watch_frame(&data, &mut pending_route);
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        println!("Connection closed.");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        eprintln!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    let _ = ws.close(None).await;
    Ok(())
}

fn ts() -> String {
    // Simple HH:MM:SS.mmm-ish marker using elapsed wall clock.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{}", now % 100_000_000)
}

fn handle_watch_frame(data: &[u8], pending_route: &mut Option<Vec<String>>) {
    // Try ServerMessage enum forms first.
    if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(data) {
        // ServerMessage variants are encoded as {"Variant": [...]}
        if let Some(obj) = val.as_object() {
            if let Some((variant, content)) = obj.iter().next() {
                let fields = content.as_array().cloned().unwrap_or_default();
                match variant.as_str() {
                    "SubscriptionAck" => {
                        let ok = fields.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                        if ok {
                            println!("[{}] subscribed ✓", ts());
                        } else {
                            let msg = fields.get(2).and_then(|v| v.as_str()).unwrap_or("");
                            println!("[{}] subscription failed: {}", ts(), msg);
                        }
                        return;
                    }
                    "SubscriptionDiff" => {
                        let table = fields.get(1).and_then(|v| v.as_str()).unwrap_or("?");
                        let key = fields.get(2).and_then(|v| v.as_str()).unwrap_or("?");
                        let op = fields.get(3).and_then(|v| v.as_str()).unwrap_or("?");
                        let row = fields.get(4).cloned().unwrap_or(serde_json::Value::Null);
                        println!("[{}] {:<16} {} {} = {}", ts(), op, table, key, row);
                        return;
                    }
                    "SubscriptionRoute" => {
                        let ids = fields
                            .get(0)
                            .and_then(|v| v.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        *pending_route = Some(ids);
                        return;
                    }
                    "SubscriptionBody" => {
                        let table = fields.get(0).and_then(|v| v.as_str()).unwrap_or("?");
                        let key = fields.get(1).and_then(|v| v.as_str()).unwrap_or("?");
                        let op = fields.get(2).and_then(|v| v.as_str()).unwrap_or("?");
                        let row = fields.get(3).cloned().unwrap_or(serde_json::Value::Null);
                        let n = pending_route.take().map(|r| r.len()).unwrap_or(1);
                        println!(
                            "[{}] {:<16} {} {} = {} (×{} sub)",
                            ts(),
                            op,
                            table,
                            key,
                            row,
                            n
                        );
                        return;
                    }
                    _ => {}
                }
            }
        }
    }
}

// ── Wire structs (rmp_serde array-tagged enum encoding) ─────────────────────────
//
// serde serializes a newtype enum variant `ClientMessage::ReducerCall(x)` as a
// map `{"ReducerCall": x}`.  We mirror that here with `rename`d single-field
// structs that serialize to the same MessagePack map shape.

#[derive(serde::Serialize)]
struct CallWire {
    #[serde(rename = "ReducerCall")]
    reducer_call: (u64, String, Vec<u8>),
}

#[derive(serde::Serialize)]
struct SubscribeWire {
    #[serde(rename = "Subscribe")]
    subscribe: (String, String),
}
