use crate::proto::ErrorCode;
use serde::{Deserialize, Serialize};
use tonic::{Code, Status};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientApiError {
    Network(NetworkError),   // Network related
    Protocol(ProtocolError), // Protocol parsing/compatibility
    Storage(StorageError),   // Storage layer
    Business(BusinessError), // Business logic
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkError {
    Timeout,
    ConnectionLost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolError {
    InvalidResponse,
    VersionMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageError {
    DiskFull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusinessError {
    /// Temporary error that requires the client to retry
    #[serde(rename = "retryable")]
    Retryable {
        code: u32,
        message: String,
        retry_after: u64, // milliseconds
    },

    /// Protocol error requiring client intervention
    #[serde(rename = "action_required")]
    ActionRequired {
        code: u32,
        message: String,
        suggested_action: String,
    },

    /// Unrecoverable client error
    #[serde(rename = "fatal")]
    Fatal { code: u32, message: String },
}
impl From<ErrorCode> for ClientApiError {
    fn from(code: ErrorCode) -> Self {
        match code {
            // Network/transport layer error
            ErrorCode::NetworkTimeout => ClientApiError::Network(NetworkError::Timeout),
            ErrorCode::NetworkUnreachable => ClientApiError::Network(NetworkError::ConnectionLost),

            // Protocol/parsing/compatibility error
            ErrorCode::ProtocolInvalidResponse => ClientApiError::Protocol(ProtocolError::InvalidResponse),
            ErrorCode::ProtocolVersionMismatch => ClientApiError::Protocol(ProtocolError::VersionMismatch),

            // Storage layer
            ErrorCode::ServerDiskFull => ClientApiError::Storage(StorageError::DiskFull),

            // Business logic error - Retryable
            ErrorCode::BusinessRetryableTimeout => ClientApiError::Business(BusinessError::Retryable {
                code: ErrorCode::BusinessRetryableTimeout as u32,
                message: "Business timeout, please retry".to_string(),
                retry_after: 1000,
            }),
            ErrorCode::BusinessRetryableNotLeader => ClientApiError::Business(BusinessError::Retryable {
                code: ErrorCode::BusinessRetryableNotLeader as u32,
                message: "Not leader, please retry".to_string(),
                retry_after: 1000,
            }),

            // Business logic error - Action required
            ErrorCode::BusinessActionRequiredLeaderChange => ClientApiError::Business(BusinessError::ActionRequired {
                code: ErrorCode::BusinessActionRequiredLeaderChange as u32,
                message: "Leader changed, please refresh leader info".to_string(),
                suggested_action: "Refresh leader and retry".to_string(),
            }),

            // Business logic error - Fatal
            ErrorCode::BusinessFatalConsistencyBroken => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::BusinessFatalConsistencyBroken as u32,
                message: "Consistency broken, unrecoverable error".to_string(),
            }),

            // Client request error (could be mapped to BusinessError::Fatal or a new variant)
            ErrorCode::ClientInvalidRequest => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ClientInvalidRequest as u32,
                message: "Invalid client request".to_string(),
            }),
            ErrorCode::ClientUnauthorized => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ClientUnauthorized as u32,
                message: "Unauthorized request".to_string(),
            }),

            // Server internal error
            ErrorCode::ServerInternalError => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ServerInternalError as u32,
                message: "Internal server error".to_string(),
            }),

            // 0: Success (not an error, but for completeness)
            ErrorCode::Success => unreachable!(),
        }
    }
}

impl From<Status> for ClientApiError {
    fn from(status: Status) -> Self {
        match status.code() {
            // Service unavailable, not ready, or deadline exceeded: treat as network timeout
            Code::Unavailable | Code::DeadlineExceeded => ClientApiError::Network(NetworkError::Timeout),

            // Internal server error or aborted: treat as fatal business error
            Code::Internal | Code::Aborted => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ServerInternalError as u32,
                message: status.message().to_string(),
            }),

            // Invalid argument: treat as client request error (fatal)
            Code::InvalidArgument => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ClientInvalidRequest as u32,
                message: status.message().to_string(),
            }),

            // Canceled: treat as client request error (fatal)
            Code::Cancelled => ClientApiError::Business(BusinessError::Retryable {
                code: ErrorCode::ClientInvalidRequest as u32,
                message: status.message().to_string(),
                retry_after: 1000,
            }),

            // Permission denied: treat as unauthorized
            Code::PermissionDenied => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ClientUnauthorized as u32,
                message: status.message().to_string(),
            }),

            // Failed precondition: treat as action required
            Code::FailedPrecondition => ClientApiError::Business(BusinessError::ActionRequired {
                code: ErrorCode::BusinessActionRequiredLeaderChange as u32,
                message: status.message().to_string(),
                suggested_action: "Check cluster state or leadership".to_string(),
            }),

            // Unknown: treat as fatal
            Code::Unknown => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ServerInternalError as u32,
                message: status.message().to_string(),
            }),

            // Default
            _ => ClientApiError::Business(BusinessError::Fatal {
                code: ErrorCode::ServerInternalError as u32,
                message: format!("Unhandled tonic status: {:?}", status),
            }),
        }
    }
}
