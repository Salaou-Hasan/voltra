# Rust SDK

The Rust SDK is located at `voltra-client-rust/`. It uses Tokio for async I/O and tokio-tungstenite for the WebSocket connection.

---

## Adding as a dependency

```toml
# Cargo.toml
[dependencies]
voltra-client = { path = "../voltra-client-rust" }
tokio = { version = "1", features = ["full"] }
```

---

## Connecting

```rust
use voltra_client::{VoltraClient, ClientOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = VoltraClient::connect(ClientOptions {
        url: "ws://localhost:3000".to_string(),
        api_key: Some("your-api-key".to_string()),
        ..Default::default()
    })
    .await?;

    // ...
    Ok(())
}
```

`ClientOptions` fields:

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | `String` | required | WebSocket URL |
| `api_key` | `Option<String>` | `None` | Bearer token |
| `reconnect_interval` | `Option<Duration>` | `Some(3s)` | Delay between reconnect attempts; `None` disables |
| `call_timeout` | `Duration` | `5s` | Timeout per reducer call |

---

## Calling Reducers

```rust
use serde_json::json;

// Call with JSON args
let result_bytes = client.call("increment", json!(["score", 1])).await?;

// Decode MessagePack result
let result: serde_json::Value = rmp_serde::from_slice(&result_bytes)?;
println!("new value: {}", result["value"]);
```

`call()` returns `Result<Vec<u8>, VoltraError>`. It fails if the reducer returned an error, the call timed out, or the connection dropped.

---

## Optimistic Updates

```rust
use std::collections::HashMap;
use serde_json::json;

let result = client.call_optimistic(
    "move_player",
    json!({"x": 5, "y": 3}),
    |mut cache| {
        // cache: CacheSnapshot = HashMap<table, HashMap<row_key, Value>>
        let players = cache.entry("players".to_string()).or_default();
        let alice = players.entry("alice".to_string()).or_insert_with(|| json!({}));
        alice["x"] = json!(5);
        alice["y"] = json!(3);
        cache
    },
)
.await?;
```

`call_optimistic` applies the closure immediately to the local cache before sending the call. On server error, only the rows the closure actually modified are rolled back. Rows that received subscription diffs mid-flight are preserved.

---

## Subscriptions

```rust
let subscription = client.subscribe("players WHERE level >= 5").await?;

// Block on incoming diffs
while let Some(diff) = subscription.recv().await {
    println!("{}: {} / {}", diff.operation, diff.table_name, diff.row_key);
    if let Some(data) = &diff.row_data {
        println!("  {}", data);
    }
}
```

`RowDiff` fields:

| Field | Type | Description |
|---|---|---|
| `subscription_id` | `String` | The subscription this diff belongs to |
| `table_name` | `String` | Table that changed |
| `row_key` | `String` | Row key |
| `operation` | `String` | `"insert"`, `"update"`, `"delete"`, `"initial_snapshot"` |
| `row_data` | `Option<serde_json::Value>` | Row data (absent on delete) |

Cancel a subscription:

```rust
subscription.unsubscribe().await;
```

---

## Reading the Local Cache

```rust
// All rows in a table (DashMap<row_key, Value>)
let rows = client.get_rows("players");
for entry in rows.iter() {
    println!("{}: {}", entry.key(), entry.value());
}

// Single row
let alice = client.get_row("players", "alice");
```

---

## Error Types

```rust
use voltra_client::VoltraError;

match result {
    Err(VoltraError::ReducerError(msg)) => eprintln!("reducer: {}", msg),
    Err(VoltraError::Timeout) => eprintln!("timed out"),
    Err(VoltraError::NotConnected) => eprintln!("not connected"),
    Err(e) => eprintln!("other: {}", e),
    Ok(bytes) => { /* ... */ }
}
```

---

## Auto-Reconnect

The background connection task reconnects automatically when `reconnect_interval` is set. On reconnect, all active subscriptions are re-sent to the server. The local row cache is preserved across reconnects.

To disable:

```rust
let client = VoltraClient::connect(ClientOptions {
    url: "ws://localhost:3000".to_string(),
    reconnect_interval: None,
    ..Default::default()
})
.await?;
```
