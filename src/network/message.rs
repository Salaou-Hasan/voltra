use serde::{Deserialize, Serialize};

/// Wire protocol message: Client requests reducer execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReducerCall {
    pub call_id: u64,
    pub reducer_name: String,
    pub args: Vec<u8>, // Serialized args (MessagePack)
}

/// Wire protocol message: Server responds to reducer call
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReducerResponse {
    pub call_id: u64,
    pub success: bool,
    pub result: Option<Vec<u8>>, // Serialized result (MessagePack)
    pub error: Option<String>,
}

impl ReducerResponse {
    pub fn success(call_id: u64, result: Vec<u8>) -> Self {
        ReducerResponse {
            call_id,
            success: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(call_id: u64, error: String) -> Self {
        ReducerResponse {
            call_id,
            success: false,
            result: None,
            error: Some(error),
        }
    }
}

/// A client command for managing subscriptions or running reducer calls.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    ReducerCall(ReducerCall),
    Subscribe {
        subscription_id: String,
        query: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
}

/// A diff for subscribed clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionDiff {
    pub subscription_id: String,
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

/// Two-frame subscription protocol: the routing header (per client).
///
/// When enabled, the server sends:
/// 1) `SubscriptionRoute { subscription_ids }`
/// 2) `SubscriptionBody { ... }`
///
/// The client must associate the immediately following `SubscriptionBody`
/// with all `subscription_ids` in the route message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionRoute {
    pub subscription_ids: Vec<String>,
}

/// Two-frame subscription protocol: the shared body (encoded once per delta).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionBody {
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

/// Server messages sent back to clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    ReducerResponse(ReducerResponse),
    SubscriptionAck {
        subscription_id: String,
        success: bool,
        message: Option<String>,
    },
    SubscriptionDiff(SubscriptionDiff),
    SubscriptionRoute(SubscriptionRoute),
    SubscriptionBody(SubscriptionBody),
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reducer_call_serialization() {
        let call = ReducerCall {
            call_id: 1,
            reducer_name: "increment".to_string(),
            args: vec![1, 2, 3],
        };

        let serialized = rmp_serde::to_vec(&call).unwrap();
        let deserialized: ReducerCall = rmp_serde::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.call_id, 1);
        assert_eq!(deserialized.reducer_name, "increment");
    }

    #[test]
    fn test_response_success() {
        let response = ReducerResponse::success(1, vec![1, 2, 3]);
        assert_eq!(response.success, true);
        assert_eq!(response.result, Some(vec![1, 2, 3]));
        assert_eq!(response.error, None);
    }

    #[test]
    fn test_response_error() {
        let response = ReducerResponse::error(1, "test error".to_string());
        assert_eq!(response.success, false);
        assert_eq!(response.error, Some("test error".to_string()));
    }
}
