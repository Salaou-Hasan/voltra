use crate::error::Result;
use crate::network::message::{ClientMessage, ReducerCall, ReducerResponse, ServerMessage};

/// Encode any serializable message to bytes.
pub fn encode_message<T: serde::Serialize>(msg: &T) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(msg)?)
}

/// Decode a reducer call from bytes, preserving the old wire format.
pub fn decode_reducer_call(data: &[u8]) -> Result<ReducerCall> {
    Ok(rmp_serde::from_slice(data)?)
}

/// Decode a generic client message.
pub fn decode_client_message(data: &[u8]) -> Result<ClientMessage> {
    Ok(rmp_serde::from_slice(data)?)
}

/// Encode a server message.
pub fn encode_server_message(message: &ServerMessage) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(message)?)
}

/// Encode a plain reducer response for legacy compatibility.
pub fn encode_response(response: &ReducerResponse) -> Result<Vec<u8>> {
    encode_message(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_call() {
        let call = ReducerCall {
            call_id: 1,
            reducer_name: "increment".to_string(),
            args: vec![1, 2, 3],
            sequence: None,
        };

        let encoded = encode_message(&call).unwrap();
        let decoded = decode_reducer_call(&encoded).unwrap();

        assert_eq!(decoded.call_id, 1);
        assert_eq!(decoded.reducer_name, "increment");
    }

    #[test]
    fn test_encode_response() {
        let response = ReducerResponse::success(1, vec![5, 6, 7]);
        let encoded = encode_response(&response).unwrap();
        let decoded: ReducerResponse = rmp_serde::from_slice(&encoded).unwrap();

        assert_eq!(decoded.call_id, 1);
        assert_eq!(decoded.success, true);
    }
}
