//! Interactive CLI client commands for NeonDB.
//!
//! Session 25 fixes:
//!   - CallWire: send ClientMessage::ReducerCall directly (struct variant).
//!   - SubscribeWire: send ClientMessage::Subscribe directly (struct variant).
//!   - ts(): format as HH:MM:SS.mmm.
//!
//! Session 27 fixes:
//!   - cmd_call: PowerShell-safe args parsing (pass 1-3 fallback chain).
//!
//! Session 28 fixes:
//!   - parse_args_json Pass 3: PowerShell strips ALL quotes, leaving bare words
//!     like `[general, alice]`.  We now detect bare unquoted tokens inside
//!     `[...]` and re-quote them as JSON strings before parsing.  Numbers,
//!     booleans, and null are left unquoted to preserve their types.
//!   - Error message rewritten: shows the raw input received and two concrete
//!     working examples instead of the misleading old tip.

use crate::error::{NeonDBError, Result};
use crate::network::message::{ClientMessage, ReducerCall};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

// ── Helpers ────────────────────────────────────────────────────────────────────

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

fn print_json_pretty(raw: &str) {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| raw.to_string())
        ),
        Err(_) => println!("{}", raw),
    }
}

fn ts() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let total_secs = (ms / 1000) as u64;
    let millis = ms % 1000;
    let secs = total_secs % 60;
    let mins = (total_secs / 60) % 60;
    let hours = (total_secs / 3600) % 24;
    format!("{:02}:{:02}:{:02}.{:03}", hours, mins, secs, millis)
}

/// Parse reducer args JSON with a three-pass PowerShell-resilient fallback.
///
/// PowerShell has two quote-stripping levels depending on how arguments are
/// passed and which version of PowerShell is in use:
///
///   Level 1 — outer single-quotes stripped, inner double-quotes kept:
///     User types:   `'["general", "alice"]'`
///     CLI receives: `["general", "alice"]`    ← valid JSON, Pass 1 succeeds
///
///   Level 2 — ALL quotes stripped (PowerShell re-parses unquoted tokens):
///     User types:   `'["general", "alice"]'`
///     CLI receives: `[general, alice]`         ← bare words, NOT valid JSON
///
/// Pass 1 — try the input exactly as received.
/// Pass 2 — wrap in `[...]` and retry (handles forgotten brackets).
/// Pass 3 — split on commas, re-quote bare unquoted tokens as JSON strings,
///           reassemble as a JSON array.  Numbers / booleans / null keep their
///           original types.
fn parse_args_json(raw: &str) -> Result<serde_json::Value> {
    let s = raw.trim();

    // ── Pass 1: exact input ───────────────────────────────────────────────────
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        return Ok(v);
    }

    // ── Pass 2: wrap bare comma-list in [...] ─────────────────────────────────
    // Handles: `"general", "alice"` (user forgot outer brackets)
    let wrapped = format!("[{}]", s);
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&wrapped) {
        return Ok(v);
    }

    // ── Pass 3: re-quote bare words stripped by PowerShell ────────────────────
    // Input looks like `[general, alice]` or `general, alice`.
    // Strip outer brackets, split on commas, quote each bare-word token.
    let inner = s.trim_start_matches('[').trim_end_matches(']').trim();
    if !inner.is_empty() {
        let tokens: Vec<String> = inner
            .split(',')
            .map(|tok| {
                let t = tok.trim();
                // Leave already-quoted strings, numbers, booleans, and null as-is.
                if t.starts_with('"')
                    || t.starts_with('\'')
                    || t == "true"
                    || t == "false"
                    || t == "null"
                    || t.parse::<f64>().is_ok()
                {
                    t.to_string()
                } else {
                    // Bare word — wrap as a JSON string.
                    format!("\"{}\"", t.replace('\\', "\\\\").replace('"', "\\\""))
                }
            })
            .collect();
        let candidate = format!("[{}]", tokens.join(", "));
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&candidate) {
            return Ok(v);
        }
    }

    // ── Give up: actionable error ─────────────────────────────────────────────
    eprintln!();
    eprintln!("  Error: could not parse args as JSON.");
    eprintln!("  Raw input received: {}", s);
    eprintln!();
    eprintln!("  PowerShell sometimes strips quotes.  Working alternatives:");
    eprintln!("    neondb call reducer '[\"arg1\", \"arg2\"]'");
    eprintln!("    neondb call reducer \"['arg1', 'arg2']\"");
    eprintln!();

    Err(NeonDBError::invalid_argument(format!(
        "Invalid args JSON: could not parse '{}' as a JSON array",
        s
    )))
}

// ── status ─────────────────────────────────────────────────────────────────────

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
            let found = rows
                .iter()
                .find(|row| row.get("row_key").and_then(|v| v.as_str()) == Some(k));
            match found {
                Some(row) => {
                    let data = row.get("data").cloned().unwrap_or(serde_json::Value::Null);
                    println!("{}", serde_json::to_string_pretty(&data).unwrap_or_default());
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
                println!("  [{}] {}", rk, serde_json::to_string(&data).unwrap_or_default());
            }
        }
    }
    Ok(())
}

// ── call ───────────────────────────────────────────────────────────────────────

pub async fn cmd_call(
    ws_url: &str,
    reducer: &str,
    args_json: Option<&str>,
    api_key: Option<&str>,
) -> Result<()> {
    let args_value: serde_json::Value = match args_json {
        Some(s) => parse_args_json(s)?,
        None => serde_json::json!([]),
    };
    let args_bytes = rmp_serde::to_vec(&args_value)
        .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    let msg = ClientMessage::ReducerCall(ReducerCall {
        call_id: 1,
        reducer_name: reducer.to_string(),
        args: args_bytes,
    });
    let frame = rmp_serde::to_vec(&msg)
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

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| NeonDBError::network_error(e.to_string()))?;

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| NeonDBError::network_error("Timed out waiting for response".to_string()))?;

    match resp {
        Some(Ok(Message::Binary(data))) => {
            match rmp_serde::from_slice::<(u64, bool, Option<Vec<u8>>, Option<String>)>(&data) {
                Ok((_cid, success, result, error)) => {
                    if success {
                        println!("✓ Reducer '{}' succeeded.", reducer);
                        if let Some(bytes) = result {
                            match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
                                Ok(v) => println!(
                                    "Result: {}",
                                    serde_json::to_string_pretty(&v).unwrap_or_default()
                                ),
                                Err(_) => println!("Result: {} bytes (binary)", bytes.len()),
                            }
                        }
                    } else {
                        println!("✗ Reducer '{}' failed: {}", reducer, error.unwrap_or_default());
                    }
                }
                Err(e) => println!("Could not decode response: {}", e),
            }
        }
        Some(Ok(_)) => println!("Unexpected non-binary response"),
        Some(Err(e)) => return Err(NeonDBError::network_error(e.to_string())),
        None => return Err(NeonDBError::network_error(
            "Connection closed without a response".to_string(),
        )),
    }

    let _ = ws.close(None).await;
    Ok(())
}

// ── watch ──────────────────────────────────────────────────────────────────────

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

    let msg = ClientMessage::Subscribe {
        subscription_id: "cli_watch".to_string(),
        query: query.to_string(),
    };
    let frame = rmp_serde::to_vec(&msg)
        .map_err(|e| NeonDBError::SerializationError(e.to_string()))?;

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| NeonDBError::network_error(e.to_string()))?;

    println!("Watching '{}' (Ctrl-C to stop)…\n", query);

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

fn handle_watch_frame(data: &[u8], pending_route: &mut Option<Vec<String>>) {
    if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(data) {
        if let Some(obj) = val.as_object() {
            if let Some((variant, content)) = obj.iter().next() {
                let fields = content.as_array().cloned().unwrap_or_default();
                match variant.as_str() {
                    "SubscriptionAck" => {
                        let ok = content.get("success").and_then(|v| v.as_bool())
                            .unwrap_or_else(|| fields.get(1).and_then(|v| v.as_bool()).unwrap_or(false));
                        if ok {
                            println!("[{}] subscribed ✓", ts());
                        } else {
                            let msg = content.get("message").and_then(|v| v.as_str())
                                .or_else(|| fields.get(2).and_then(|v| v.as_str()))
                                .unwrap_or("");
                            println!("[{}] subscription failed: {}", ts(), msg);
                        }
                    }
                    "SubscriptionDiff" => {
                        let table = content.get("table_name").and_then(|v| v.as_str())
                            .or_else(|| fields.get(1).and_then(|v| v.as_str())).unwrap_or("?");
                        let key = content.get("row_key").and_then(|v| v.as_str())
                            .or_else(|| fields.get(2).and_then(|v| v.as_str())).unwrap_or("?");
                        let op = content.get("operation").and_then(|v| v.as_str())
                            .or_else(|| fields.get(3).and_then(|v| v.as_str())).unwrap_or("?");
                        let row = content.get("row_data").cloned()
                            .or_else(|| fields.get(4).cloned()).unwrap_or(serde_json::Value::Null);
                        println!("[{}] {:<16} {}.{} = {}", ts(), op, table, key, row);
                    }
                    "SubscriptionRoute" => {
                        let ids = content.get("subscription_ids").and_then(|v| v.as_array())
                            .or_else(|| fields.get(0).and_then(|v| v.as_array()))
                            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        *pending_route = Some(ids);
                    }
                    "SubscriptionBody" => {
                        let table = content.get("table_name").and_then(|v| v.as_str())
                            .or_else(|| fields.get(0).and_then(|v| v.as_str())).unwrap_or("?");
                        let key = content.get("row_key").and_then(|v| v.as_str())
                            .or_else(|| fields.get(1).and_then(|v| v.as_str())).unwrap_or("?");
                        let op = content.get("operation").and_then(|v| v.as_str())
                            .or_else(|| fields.get(2).and_then(|v| v.as_str())).unwrap_or("?");
                        let row = content.get("row_data").cloned()
                            .or_else(|| fields.get(3).cloned()).unwrap_or(serde_json::Value::Null);
                        let n = pending_route.take().map(|r| r.len()).unwrap_or(1);
                        println!("[{}] {:<16} {}.{} = {} (×{} sub)", ts(), op, table, key, row, n);
                    }
                    _ => {}
                }
            }
        }
    }
}
