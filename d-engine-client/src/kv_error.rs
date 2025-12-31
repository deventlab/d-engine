//! Error types for KV client operations.
//!
//! Provides unified error handling for both remote (gRPC) and embedded (local)
//! KV client implementations.

use crate::ClientApiError;

/// Result type for KV client operations
pub type KvResult<T> = std::result::Result<T, KvClientError>;

/// Error types for KV client operations
#[derive(Debug, Clone)]
pub enum KvClientError {
    /// Channel closed (node shutdown or connection lost)
    ChannelClosed,

    /// Operation timeout
    Timeout,

    /// Server returned error (e.g., not leader, storage error)
    ServerError(String),

    /// Network error (only for remote clients)
    NetworkError(String),

    /// Invalid argument
    InvalidArgument(String),
}

impl std::fmt::Display for KvClientError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            KvClientError::ChannelClosed => write!(f, "Channel closed"),
            KvClientError::Timeout => write!(f, "Operation timeout"),
            KvClientError::ServerError(msg) => write!(f, "Server error: {msg}"),
            KvClientError::NetworkError(msg) => write!(f, "Network error: {msg}"),
            KvClientError::InvalidArgument(msg) => write!(f, "Invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for KvClientError {}

// ==================== Error Conversions ====================

/// Convert ClientApiError to KvClientError
impl From<ClientApiError> for KvClientError {
    fn from(err: ClientApiError) -> Self {
        match err {
            ClientApiError::Network { message, .. } => KvClientError::NetworkError(message),
            ClientApiError::Protocol { message, .. } => KvClientError::ServerError(message),
            ClientApiError::Storage { message, .. } => KvClientError::ServerError(message),
            ClientApiError::Business { code, message, .. } => {
                // Check if it's a timeout or not-leader error
                use d_engine_proto::error::ErrorCode;
                match code {
                    ErrorCode::ConnectionTimeout => KvClientError::Timeout,
                    ErrorCode::NotLeader => KvClientError::ServerError(message),
                    _ => KvClientError::ServerError(message),
                }
            }
            ClientApiError::General { message, .. } => KvClientError::ServerError(message),
        }
    }
}
