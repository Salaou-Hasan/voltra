use crate::error::{VoltraError, Result};
use crate::types::{ClientMessage, ReducerResponse, ServerMessage};

/// Encode a `ClientMessage` to MessagePack bytes.
pub fn encode_client_message(msg: &ClientMessage) -> Result<Vec<u8>> {
    rmp_serde::to_vec(msg).map_err(VoltraError::from)
}

/// Decode a server frame.
///
/// The server sends two different formats:
/// - Bare `ReducerResponse`: MessagePack array `[call_id, success, result|nil, error|nil]`
/// - `ServerMessage` enum variant: MessagePack map `{"VariantName": [fields…]}`
pub fn decode_server_frame(bytes: &[u8]) -> Option<ServerMessage> {
    // Try enum first (most common for subscription frames)
    if let Ok(msg) = rmp_serde::from_slice::<ServerMessage>(bytes) {
        return Some(msg);
    }
    // Fall back to bare ReducerResponse array
    if let Ok(resp) = rmp_serde::from_slice::<ReducerResponse>(bytes) {
        return Some(ServerMessage::ReducerResponse(resp));
    }
    None
}

/// Encode args for a reducer call.
///
/// For the built-in `increment` reducer, the server expects a positional array
/// matching `IncrementArgs { name: String, delta: i32 }`:
/// ```rust
/// encode_args(&("my_counter", 1_i32))
/// ```
pub fn encode_args<T: serde::Serialize>(args: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(args).map_err(VoltraError::from)
}

/// Decode reducer result bytes into a typed value.
pub fn decode_result<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(VoltraError::from)
}
