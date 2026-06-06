use thiserror::Error;

#[derive(Error, Debug)]
pub enum NeonDBError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Not connected")]
    NotConnected,

    #[error("Call timed out after {0}ms")]
    Timeout(u64),

    #[error("Reducer error: {0}")]
    ReducerError(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),
}

impl From<rmp_serde::encode::Error> for NeonDBError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        NeonDBError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for NeonDBError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        NeonDBError::Serialization(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, NeonDBError>;
