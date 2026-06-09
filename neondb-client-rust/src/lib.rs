//! # neondb-client
//!
//! Async Rust client SDK for [NeonDB](https://github.com/your-repo/NeonDB) —
//! the self-hosted, zero-cost real-time game backend.
//!
//! ## Quick start
//!
//! ```no_run
//! use neondb_client::{NeonDBClient, ClientOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = NeonDBClient::connect(ClientOptions {
//!         url: "ws://localhost:3000".to_string(),
//!         api_key: Some("my-secret-key".to_string()),
//!         ..Default::default()
//!     }).await?;
//!
//!     // Call the built-in increment reducer (positional args: [name, delta])
//!     let bytes = client.call("increment", &("score", 1_i32)).await?;
//!     let result: serde_json::Value = client.decode_result(&bytes)?;
//!     println!("new_value: {}", result["new_value"]);
//!
//!     // Subscribe to changes
//!     let (_sub, mut rx) = client.subscribe("counters WHERE value > 100").await?;
//!     while let Some(diff) = rx.recv().await {
//!         println!("[{}] {} = {:?}", diff.operation, diff.row_key, diff.row_data);
//!     }
//!
//!     client.disconnect().await;
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod error;
pub mod protocol;
pub mod types;

pub use client::{ClientEvent, NeonDBClient, ReconnectConfig, Subscription};
pub use error::{NeonDBError, Result};
pub use types::{ClientOptions, RowDiff};
