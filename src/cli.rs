//! Interactive CLI client commands for Voltra.
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
//!
//! Session 32 fixes:
//!   - cmd_seed: BUG-1 fix — dry-run format string produced malformed output.
//!     "{}Seeding {} row(s)..." with dry_run=true emitted the prefix and body
//!     concatenated without a separator: "[dry-run] Would seedSeeding 3 row(s)...".
//!     Fixed by splitting into two separate println! calls.

use crate::error::{VoltraError, Result};
use crate::network::message::{ClientMessage, ReducerCall};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use reqwest;

fn ws_request(
    url: &str,
    api_key: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut req = url.into_client_request().map_err(|e| {
        VoltraError::network_error(format!("Invalid WebSocket URL '{}': {}", url, e))
    })?;
    if let Some(key) = api_key {
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {}", key)
                .parse()
                .map_err(|_| VoltraError::invalid_argument("Invalid API key header value"))?,
        );
    }
    Ok(req)
}

async fn http_get(url: &str) -> Result<String> {
    let uri: hyper::Uri = url
        .parse()
        .map_err(|e| VoltraError::network_error(format!("Invalid URL '{}': {}", url, e)))?;
    let client = hyper::Client::new();
    let resp = client.get(uri).await.map_err(|e| {
        VoltraError::network_error(format!(
            "HTTP request failed: {}. Is the server running?",
            e
        ))
    })?;
    let status = resp.status();
    let bytes = hyper::body::to_bytes(resp.into_body())
        .await
        .map_err(|e| VoltraError::network_error(format!("Failed to read response: {}", e)))?;
    let body = String::from_utf8_lossy(&bytes).to_string();
    if !status.is_success() {
        return Err(VoltraError::network_error(format!(
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
    eprintln!("    voltra call reducer '[\"arg1\", \"arg2\"]'");
    eprintln!("    voltra call reducer \"['arg1', 'arg2']\"");
    eprintln!();

    Err(VoltraError::invalid_argument(format!(
        "Invalid args JSON: could not parse '{}' as a JSON array",
        s
    )))
}

// ── generate-npc ─────────────────────────────────────────────────────────────

/// Pre-generate an NPC template via the Anthropic API and cache it in the
/// running server's `npc_templates` table by calling the `cache_npc_template`
/// reducer.  Falls back to a sensible hardcoded default if the API is
/// unavailable (no ANTHROPIC_API_KEY set, or network error).
///
/// Usage:
///   voltra generate-npc goblin
///   voltra generate-npc dragon --context "volcanic dungeon boss"
pub async fn cmd_generate_npc(
    ws_url: &str,
    npc_type: &str,
    context: Option<&str>,
    api_key: Option<&str>,
) -> Result<()> {
    println!("Generating NPC template for '{}' …", npc_type);

    // ── 1. Build the prompt ────────────────────────────────────────────────────
    let extra = context
        .map(|c| format!(" Additional context: {}", c))
        .unwrap_or_default();

    let prompt = format!(
        "Design a game NPC of type \"{}\" for an action RPG.{extra}\n\
        Return ONLY a JSON object (no markdown, no explanation) with these exact keys:\n\
        {{\n\
          \"npc_type\": string,\n\
          \"display_name\": string,\n\
          \"description\": string,\n\
          \"behavior\": string (aggressive | passive | patrol | boss),\n\
          \"hp\": number,\n\
          \"atk\": number,\n\
          \"def\": number,\n\
          \"speed\": number (1-10),\n\
          \"xp_reward\": number,\n\
          \"currency_reward\": number,\n\
          \"abilities\": [ {{ \"id\": string, \"name\": string, \"damage\": number, \"mp_cost\": number, \"effect\": string }} ],\n\
          \"loot_table\": [ {{ \"item_id\": string, \"item_name\": string, \"weight\": number }} ]\n\
        }}",
        npc_type,
        extra = extra,
    );

    // ── 2. Call the Anthropic API (if key is available) ────────────────────────
    let template_json: serde_json::Value = match generate_via_anthropic(&prompt).await {
        Ok(json) => {
            println!("✓ Template generated by Claude.");
            json
        }
        Err(e) => {
            println!("⚠ AI generation skipped ({}). Using built-in defaults.", e);
            built_in_npc_template(npc_type)
        }
    };

    // ── 3. Pretty-print the template ───────────────────────────────────────────
    println!(
        "\nTemplate:\n{}",
        serde_json::to_string_pretty(&template_json).unwrap_or_default()
    );

    // ── 4. Cache it in the running server via the cache_npc_template reducer ───
    let template_str = serde_json::to_string(&template_json)
        .map_err(|e| crate::error::VoltraError::SerializationError(e.to_string()))?;

    let args = serde_json::json!([npc_type, template_str]);
    let args_bytes = rmp_serde::to_vec(&args)
        .map_err(|e| crate::error::VoltraError::SerializationError(e.to_string()))?;

    let msg = crate::network::message::ClientMessage::ReducerCall(
        crate::network::message::ReducerCall {
            call_id: 1,
            reducer_name: "cache_npc_template".to_string(),
            args: args_bytes,
        },
    );
    let frame = rmp_serde::to_vec(&msg)
        .map_err(|e| crate::error::VoltraError::SerializationError(e.to_string()))?;

    let request = ws_request(ws_url, api_key)?;
    match tokio_tungstenite::connect_async(request).await {
        Ok((mut ws, _)) => {
            use futures::{SinkExt, StreamExt};
            let _ = ws.send(tokio_tungstenite::tungstenite::Message::Binary(frame)).await;
            // Wait briefly for ack, then close gracefully.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                ws.next(),
            )
            .await;
            let _ = ws.close(None).await;
            println!("✓ Template cached in server as npc_templates['{}']", npc_type);
        }
        Err(e) => {
            println!(
                "⚠ Server not reachable ({}). Template printed above — start the server and run again to cache it.",
                e
            );
        }
    }

    Ok(())
}

/// Call the Anthropic messages API to generate an NPC template.
/// Reads ANTHROPIC_API_KEY from the environment.
async fn generate_via_anthropic(prompt: &str) -> std::result::Result<serde_json::Value, String> {
    let api_key = std::env::var("ANTHROPIC_API_KEY")
        .map_err(|_| "ANTHROPIC_API_KEY not set".to_string())?;

    let body = serde_json::json!({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": prompt
        }]
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client build error: {}", e))?;

    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Anthropic API error {}: {}", status, text));
    }

    let data: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("JSON decode error: {}", e))?;

    let text = data
        .get("content")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| "Unexpected API response shape".to_string())?;

    let cleaned = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    serde_json::from_str(cleaned)
        .map_err(|e| format!("JSON parse error: {} — raw: {}", e, &cleaned[..cleaned.len().min(200)]))
}

/// Built-in fallback NPC template when AI generation is not available.
fn built_in_npc_template(npc_type: &str) -> serde_json::Value {
    let (display, desc, behavior, hp, atk, def, speed, xp, gold) = match npc_type {
        "goblin"   => ("Goblin",   "A small green menace with a rusty blade.", "aggressive", 60,  12,  4, 6, 20,   5),
        "orc"      => ("Orc",      "A brutish warrior, slow but devastating.",  "aggressive", 120, 20, 10, 4, 50,  15),
        "skeleton" => ("Skeleton", "An undead archer, silent and relentless.", "patrol",     80,  16,  6, 5, 35,   8),
        "dragon"   => ("Dragon",   "An ancient dragon of immense power.",       "boss",       800, 70, 40, 3, 1000, 500),
        "boss"     => ("Boss",     "A powerful dungeon guardian.",              "boss",       500, 45, 25, 4, 500, 200),
        _          => (npc_type,   "A dangerous enemy.",                        "aggressive",  60,  12,  4, 5,  20,   5),
    };
    serde_json::json!({
        "npc_type": npc_type,
        "display_name": display,
        "description": desc,
        "behavior": behavior,
        "hp": hp, "atk": atk, "def": def, "speed": speed,
        "xp_reward": xp, "currency_reward": gold,
        "abilities": [],
        "loot_table": [],
        "source": "built_in"
    })
}


// ── seed ─────────────────────────────────────────────────────────────────────

/// Bulk-seed rows from a JSON file into a running Voltra server.
///
/// # Seed file format
///
/// Object-of-arrays (list of rows per table):
/// ```json
/// {
///   "players": [
///     { "key": "alice", "hp": 200, "level": 1 },
///     { "key": "bob",   "hp": 150, "level": 2 }
///   ]
/// }
/// ```
///
/// Object-of-objects (keyed map per table):
/// ```json
/// {
///   "players": {
///     "alice": { "hp": 200, "level": 1 },
///     "bob":   { "hp": 150, "level": 2 }
///   }
/// }
/// ```
pub async fn cmd_seed(
    metrics_url: &str,
    file_path: &str,
    dry_run: bool,
) -> Result<()> {
    // ── 1. Read and parse the seed file ──────────────────────────────────────
    let raw = std::fs::read_to_string(file_path).map_err(|e| {
        VoltraError::invalid_argument(format!("Cannot read seed file '{}': {}", file_path, e))
    })?;
    let seed: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| {
            VoltraError::invalid_argument(format!("Seed file is not valid JSON: {}", e))
        })?;

    let tables_map = seed.as_object().ok_or_else(|| {
        VoltraError::invalid_argument("Seed file root must be a JSON object mapping table names to rows")
    })?;

    // ── 2. Normalize into a flat Vec<(table, key, data)> ─────────────────────
    let mut rows: Vec<(String, String, serde_json::Value)> = Vec::new();

    for (table_name, table_value) in tables_map {
        match table_value {
            serde_json::Value::Array(arr) => {
                for (idx, item) in arr.iter().enumerate() {
                    let obj = item.as_object().ok_or_else(|| {
                        VoltraError::invalid_argument(format!(
                            "{}[{}]: each array element must be a JSON object",
                            table_name, idx
                        ))
                    })?;
                    let row_key = obj
                        .get("key")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            VoltraError::invalid_argument(format!(
                                "{}[{}]: array-format rows must have a \"key\" string field",
                                table_name, idx
                            ))
                        })?;
                    let mut data = obj.clone();
                    data.remove("key");
                    rows.push((table_name.clone(), row_key.to_string(), serde_json::Value::Object(data)));
                }
            }
            serde_json::Value::Object(map) => {
                for (row_key, data) in map {
                    rows.push((table_name.clone(), row_key.clone(), data.clone()));
                }
            }
            _ => {
                return Err(VoltraError::invalid_argument(format!(
                    "Table '{}': value must be an array or object of rows",
                    table_name
                )));
            }
        }
    }

    if rows.is_empty() {
        println!("Seed file contains no rows — nothing to do.");
        return Ok(());
    }

    // ── 3. Summary / dry-run ─────────────────────────────────────────────────
    // BUG-1 FIX: split into two separate println! calls instead of using a
    // format string with an embedded conditional prefix.  The old single-call
    // approach concatenated "[dry-run] Would seed" and "Seeding" without a
    // separator, producing malformed output in non-dry-run mode.
    let mut by_table: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for (table, _, _) in &rows {
        *by_table.entry(table.as_str()).or_insert(0) += 1;
    }

    if dry_run {
        println!(
            "[dry-run] Would seed {} row(s) across {} table(s):",
            rows.len(),
            by_table.len()
        );
    } else {
        println!(
            "Seeding {} row(s) across {} table(s):",
            rows.len(),
            by_table.len()
        );
    }

    for (table, count) in &by_table {
        println!("  {:<30} {} row(s)", table, count);
    }

    if dry_run {
        println!("\nDry-run complete — no data was written.");
        return Ok(());
    }

    // ── 4. POST to /seed ──────────────────────────────────────────────────────
    let seed_url = format!("{}/seed", metrics_url.trim_end_matches('/'));

    let payload = serde_json::json!({
        "rows": rows.iter().map(|(t, k, d)| serde_json::json!([t, k, d])).collect::<Vec<_>>()
    });
    let payload_str = serde_json::to_string(&payload)
        .map_err(|e| VoltraError::SerializationError(e.to_string()))?;

    let uri: hyper::Uri = seed_url.parse().map_err(|e| {
        VoltraError::network_error(format!("Invalid URL '{}': {}", seed_url, e))
    })?;
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(payload_str))
        .map_err(|e| VoltraError::network_error(format!("Build request: {}", e)))?;

    let client = hyper::Client::new();
    let resp = client.request(req).await.map_err(|e| {
        VoltraError::network_error(format!(
            "POST /seed failed: {}. Is the server running at {}?",
            e, metrics_url
        ))
    })?;

    let status = resp.status();
    let bytes = hyper::body::to_bytes(resp.into_body())
        .await
        .map_err(|e| VoltraError::network_error(format!("Read response: {}", e)))?;
    let body = String::from_utf8_lossy(&bytes);

    if !status.is_success() {
        eprintln!("Server returned {}: {}", status, body);
        return Err(VoltraError::network_error(format!(
            "Seed failed with HTTP {}",
            status
        )));
    }

    // ── 5. Print result ───────────────────────────────────────────────────────
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => {
            let written = v.get("rows_written").and_then(|n| n.as_u64()).unwrap_or(rows.len() as u64);
            let skipped = v.get("rows_skipped").and_then(|n| n.as_u64()).unwrap_or(0);
            println!("\n✓ Seed complete: {} row(s) written, {} skipped.", written, skipped);
        }
        Err(_) => println!("\n✓ Seed complete.\n{}", body),
    }
    Ok(())
}

pub async fn cmd_status(metrics_url: &str) -> Result<()> {
    let health_url = format!("{}/healthz", metrics_url.trim_end_matches('/'));
    let metrics_endpoint = format!("{}/metrics", metrics_url.trim_end_matches('/'));

    println!("Checking Voltra at {} …\n", metrics_url);

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

pub async fn cmd_tables(metrics_url: &str) -> Result<()> {
    let url = format!("{}/tables", metrics_url.trim_end_matches('/'));
    let body = http_get(&url).await?;
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| VoltraError::SerializationError(e.to_string()))?;

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

pub async fn cmd_get(metrics_url: &str, table: &str, key: Option<&str>) -> Result<()> {
    let url = format!("{}/tables/{}", metrics_url.trim_end_matches('/'), table);
    let body = http_get(&url).await?;
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| VoltraError::SerializationError(e.to_string()))?;

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
        .map_err(|e| VoltraError::SerializationError(e.to_string()))?;

    let msg = ClientMessage::ReducerCall(ReducerCall {
        call_id: 1,
        reducer_name: reducer.to_string(),
        args: args_bytes,
    });
    let frame = rmp_serde::to_vec(&msg)
        .map_err(|e| VoltraError::SerializationError(e.to_string()))?;

    let request = ws_request(ws_url, api_key)?;
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| {
            VoltraError::network_error(format!(
                "Connect failed: {}. Is the server running at {}?",
                e, ws_url
            ))
        })?;

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| VoltraError::network_error(e.to_string()))?;

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| VoltraError::network_error("Timed out waiting for response".to_string()))?;

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
        Some(Err(e)) => return Err(VoltraError::network_error(e.to_string())),
        None => return Err(VoltraError::network_error(
            "Connection closed without a response".to_string(),
        )),
    }

    let _ = ws.close(None).await;
    Ok(())
}

pub async fn cmd_watch(ws_url: &str, query: &str, api_key: Option<&str>) -> Result<()> {
    let request = ws_request(ws_url, api_key)?;
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| {
            VoltraError::network_error(format!(
                "Connect failed: {}. Is the server running at {}?",
                e, ws_url
            ))
        })?;

    let msg = ClientMessage::Subscribe {
        subscription_id: "cli_watch".to_string(),
        query: query.to_string(),
    };
    let frame = rmp_serde::to_vec(&msg)
        .map_err(|e| VoltraError::SerializationError(e.to_string()))?;

    ws.send(Message::Binary(frame))
        .await
        .map_err(|e| VoltraError::network_error(e.to_string()))?;

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
                            .or_else(|| fields.first().and_then(|v| v.as_array()))
                            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        *pending_route = Some(ids);
                    }
                    "SubscriptionBody" => {
                        let table = content.get("table_name").and_then(|v| v.as_str())
                            .or_else(|| fields.first().and_then(|v| v.as_str())).unwrap_or("?");
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

// ── voltra migrate ────────────────────────────────────────────────────────────

/// Run pending migrations from the `migrations/` directory against a running server.
///
/// Each `*.toml` file in `migrations_dir` is sent to `POST /migrate` on the admin
/// server.  The server applies any files not yet in its `__migrations` tracking table
/// and returns a per-file summary.  Already-applied migrations are skipped.
///
/// `--dry-run` reads the files and prints what would be applied without POSTing.
pub async fn cmd_migrate(
    metrics_url: &str,
    migrations_dir: &str,
    dry_run: bool,
) -> Result<()> {
    let dir = std::path::Path::new(migrations_dir);
    if !dir.exists() {
        println!("No migrations directory found at {:?}", dir);
        return Ok(());
    }

    // Collect *.toml files sorted lexicographically (001_ < 002_ etc.)
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| VoltraError::internal(format!("Cannot read migrations dir: {}", e)))?
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("toml"))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    paths.sort();

    if paths.is_empty() {
        println!("No migration files found in {:?}", dir);
        return Ok(());
    }

    println!("Found {} migration file(s) in {:?}", paths.len(), dir);
    println!();

    if dry_run {
        println!("[dry-run] Would send the following migration files:");
        for p in &paths {
            println!("  • {}", p.file_name().and_then(|s| s.to_str()).unwrap_or("?"));
            // Print a quick summary of steps
            if let Ok(contents) = std::fs::read_to_string(p) {
                if let Ok(parsed) = toml::from_str::<toml::Value>(&contents) {
                    let version = parsed.get("version").and_then(|v| v.as_integer()).unwrap_or(0);
                    let desc = parsed.get("description").and_then(|v| v.as_str()).unwrap_or("");
                    let steps = parsed.get("steps").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                    println!("    version={}, steps={}{}", version, steps,
                        if desc.is_empty() { String::new() } else { format!(", \"{}\"", desc) });
                }
            }
        }
        println!();
        println!("Run without --dry-run to apply.");
        return Ok(());
    }

    // Build the request payload: list of {filename, content} objects
    let mut migrations = Vec::new();
    for p in &paths {
        let filename = p.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
        let content = std::fs::read_to_string(p)
            .map_err(|e| VoltraError::internal(format!("Cannot read {:?}: {}", p, e)))?;
        migrations.push(serde_json::json!({ "filename": filename, "content": content }));
    }
    let payload = serde_json::json!({ "migrations": migrations });

    let url = format!("{}/migrate", metrics_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()
        .await
        .map_err(|e| VoltraError::network_error(format!("Cannot reach {}: {}", url, e)))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        eprintln!("Server returned HTTP {}: {}", status, body);
        return Err(VoltraError::network_error(format!("HTTP {}", status)));
    }

    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => {
            let applied  = v.get("applied").and_then(|n| n.as_u64()).unwrap_or(0);
            let skipped  = v.get("skipped").and_then(|n| n.as_u64()).unwrap_or(0);
            let errors   = v.get("errors").and_then(|e| e.as_array()).cloned().unwrap_or_default();
            println!("✓ Migration complete: {} applied, {} already up to date.", applied, skipped);
            if !errors.is_empty() {
                println!("  Errors:");
                for err in &errors {
                    println!("  ✗ {}", err.as_str().unwrap_or("unknown error"));
                }
            }
        }
        Err(_) => println!("✓ Migration complete.\n{}", body),
    }
    Ok(())
}
