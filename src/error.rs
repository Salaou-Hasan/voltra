use thiserror::Error;

/// Result type for NeonDB operations
pub type Result<T> = std::result::Result<T, NeonDBError>;

/// All error types that can occur in NeonDB
#[derive(Error, Debug)]
pub enum NeonDBError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Table error: {0}")]
    TableError(String),

    #[error("Reducer error: {0}")]
    ReducerError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Row not found: {0}")]
    RowNotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    /// Optimistic-concurrency conflict: a row this transaction read was
    /// modified before it committed. The caller should retry the reducer.
    #[error("Transaction conflict: {0}")]
    TxnConflict(String),
}

impl NeonDBError {
    pub fn wal_error(msg: impl Into<String>) -> Self {
        NeonDBError::WalError(msg.into())
    }

    pub fn table_error(msg: impl Into<String>) -> Self {
        NeonDBError::TableError(msg.into())
    }

    pub fn reducer_error(msg: impl Into<String>) -> Self {
        NeonDBError::ReducerError(msg.into())
    }

    pub fn network_error(msg: impl Into<String>) -> Self {
        NeonDBError::NetworkError(msg.into())
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        NeonDBError::InvalidArgument(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        NeonDBError::Internal(msg.into())
    }
}

impl From<serde_json::Error> for NeonDBError {
    fn from(err: serde_json::Error) -> Self {
        NeonDBError::SerializationError(format!("JSON serialization error: {}", err))
    }
}

impl From<rmp_serde::decode::Error> for NeonDBError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        NeonDBError::SerializationError(format!("MessagePack decode error: {}", err))
    }
}

impl From<rmp_serde::encode::Error> for NeonDBError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        NeonDBError::SerializationError(format!("MessagePack encode error: {}", err))
    }
}

impl From<std::str::Utf8Error> for NeonDBError {
    fn from(err: std::str::Utf8Error) -> Self {
        NeonDBError::SerializationError(format!("UTF-8 decode error: {}", err))
    }
}

impl From<rquickjs::Error> for NeonDBError {
    fn from(err: rquickjs::Error) -> Self {
        NeonDBError::ReducerError(format!("JS error: {}", err))
    }
}

