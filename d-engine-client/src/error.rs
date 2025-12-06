use std::error::Error;

use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinError;
use tonic::Code;
use tonic::Status;

use d_engine_proto::error::ErrorCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientApiError {
    /// Network layer error (retryable)
    #[serde(rename = "network")]
    Network {
        code: ErrorCode,
        message: String,
        retry_after_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        leader_hint: Option<LeaderInfo>,
    },

    /// Protocol layer error (client needs to check protocol compatibility)
    #[serde(rename = "protocol")]
    Protocol {
        code: ErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        supported_versions: Option<Vec<String>>,
    },

    /// Storage layer error (internal problem on the server)
    #[serde(rename = "storage")]
    Storage { code: ErrorCode, message: String },

    /// Business logic error (client needs to adjust behavior)
    #[serde(rename = "business")]
    Business {
        code: ErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        required_action: Option<String>,
    },

    /// General Client API error
    #[serde(rename = "general")]
    General {
        code: ErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        required_action: Option<String>,
    },
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum NetworkErrorType {
//     Timeout,
//     ConnectionLost,
//     InvalidAddress,
//     TlsFailure,
//     ProtocolViolation,
//     JoinError,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum ProtocolErrorType {
//     InvalidResponseFormat,
//     VersionMismatch,
//     ChecksumFailure,
//     SerializationError,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum StorageErrorType {
//     DiskFull,
//     CorruptionDetected,
//     IoFailure,
//     PermissionDenied,
//     KeyNotExist,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum BusinessErrorType {
//     NotLeader,
//     StaleRead,
//     InvalidRequest,
//     RateLimited,
//     ClusterUnavailable,
//     ProposeFailed,
//     RetryRequired,
//     StaleTerm,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum GeneralErrorType {
//     General,
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub id: String,
    pub address: String,
    pub last_contact: u64, // Unix timestamp in ms
}
impl From<tonic::transport::Error> for ClientApiError {
    /// Converts a tonic transport error into a ClientApiError
    ///
    /// This implementation handles different transport error scenarios:
    /// - Connection timeouts
    /// - Invalid URI/address formats
    /// - Unexpected connection loss
    ///
    /// # Parameters
    /// - `err`: The tonic transport error to convert
    ///
    /// # Returns
    /// A ClientApiError with appropriate error code and retry information
    fn from(err: tonic::transport::Error) -> Self {
        // Determine the error details based on the underlying error
        if let Some(io_err) = err.source().and_then(|e| e.downcast_ref::<std::io::Error>()) {
            if io_err.kind() == std::io::ErrorKind::TimedOut {
                return Self::Network {
                    code: ErrorCode::ConnectionTimeout,
                    message: format!("Connection timeout: {err}"),
                    retry_after_ms: Some(3000), // Retry after 3 seconds
                    leader_hint: None,
                };
            }
        }

        // Check for invalid address errors
        if err.to_string().contains("invalid uri") {
            return Self::Network {
                code: ErrorCode::InvalidAddress,
                message: format!("Invalid address: {err}"),
                retry_after_ms: None, // Not retryable - needs address correction
                leader_hint: None,
            };
        }

        // Default case: unexpected connection loss
        Self::Network {
            code: ErrorCode::ConnectionTimeout,
            message: "Connection unexpectedly closed".into(),
            retry_after_ms: Some(5000), // Retry after 5 seconds
            leader_hint: None,
        }
    }
}

impl From<Status> for ClientApiError {
    fn from(status: Status) -> Self {
        let code = status.code();
        let message = status.message().to_string();

        match code {
            Code::Unavailable => Self::Business {
                code: ErrorCode::ClusterUnavailable,
                message,
                required_action: Some("Retry after cluster recovery".into()),
            },

            Code::Cancelled => Self::Network {
                code: ErrorCode::ConnectionTimeout,
                message,
                leader_hint: None,
                retry_after_ms: Some(1000),
            },

            Code::FailedPrecondition => {
                if let Some(leader) = parse_leader_from_metadata(&status) {
                    Self::Network {
                        code: ErrorCode::LeaderChanged,
                        message: "Leadership changed".into(),
                        retry_after_ms: Some(1000),
                        leader_hint: Some(leader),
                    }
                } else {
                    Self::Business {
                        code: ErrorCode::StaleOperation,
                        message,
                        required_action: Some("Refresh cluster state".into()),
                    }
                }
            }

            Code::InvalidArgument => Self::Business {
                code: ErrorCode::InvalidRequest,
                message,
                required_action: None,
            },

            Code::PermissionDenied => Self::Business {
                code: ErrorCode::NotLeader,
                message,
                required_action: Some("Refresh cluster state".into()),
            },

            _ => Self::Business {
                code: ErrorCode::Uncategorized,
                message: format!("Unhandled status code: {code:?}"),
                required_action: None,
            },
        }
    }
}

// impl ClientApiError {
//     pub fn error_category(&self) -> ErrorCategory {
//         match self.code() {
//             ErrorCode::ConnectionTimeout
//             | ErrorCode::InvalidAddress
//             | ErrorCode::LeaderChanged
//             | ErrorCode::JoinError => ErrorCategory::Network,

//             ErrorCode::InvalidResponse | ErrorCode::VersionMismatch => ErrorCategory::Protocol,

//             ErrorCode::DiskFull
//             | ErrorCode::DataCorruption
//             | ErrorCode::StorageIoError
//             | ErrorCode::StoragePermissionDenied
//             | ErrorCode::KeyNotExist => ErrorCategory::Storage,

//             ErrorCode::NotLeader
//             | ErrorCode::StaleOperation
//             | ErrorCode::InvalidRequest
//             | ErrorCode::RateLimited
//             | ErrorCode::ClusterUnavailable
//             | ErrorCode::ProposeFailed
//             | ErrorCode::TermOutdated
//             | ErrorCode::RetryRequired => ErrorCategory::Business,

//             ErrorCode::General | ErrorCode::Uncategorized => ErrorCategory::General,

//             _ => ErrorCategory::General,
//         }
//     }
// }

fn parse_leader_from_metadata(status: &Status) -> Option<LeaderInfo> {
    status
        .metadata()
        .get("x-raft-leader")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            // Manually parse JSON-like string
            let mut id = None;
            let mut address = None;
            let mut last_contact = None;

            // Remove whitespace and outer braces
            let s = s.trim().trim_start_matches('{').trim_end_matches('}');

            // Split into key-value pairs
            for pair in s.split(',') {
                let pair = pair.trim();
                if let Some((key, value)) = pair.split_once(':') {
                    let key = key.trim().trim_matches('"');
                    let value = value.trim().trim_matches('"');

                    match key {
                        "id" => id = Some(value.to_string()),
                        "address" => address = Some(value.to_string()),
                        "last_contact" => last_contact = value.parse().ok(),
                        _ => continue,
                    }
                }
            }

            Some(LeaderInfo {
                id: id?,
                address: address?,
                last_contact: last_contact?,
            })
        })
}

impl From<ErrorCode> for ClientApiError {
    fn from(code: ErrorCode) -> Self {
        match code {
            // Network layer errors
            ErrorCode::ConnectionTimeout => ClientApiError::Network {
                code,
                message: "Connection timeout".to_string(),
                retry_after_ms: None,
                leader_hint: None,
            },
            ErrorCode::InvalidAddress => ClientApiError::Network {
                code,
                message: "Invalid address".to_string(),
                retry_after_ms: None,
                leader_hint: None,
            },
            ErrorCode::LeaderChanged => ClientApiError::Network {
                code,
                message: "Leader changed".to_string(),
                retry_after_ms: Some(100), // suggest immediate retry
                leader_hint: None,         // Note: This would ideally be populated from context
            },
            ErrorCode::JoinError => ClientApiError::Network {
                code,
                message: "Task Join Error".to_string(),
                retry_after_ms: Some(100), // suggest immediate retry
                leader_hint: None,         // Note: This would ideally be populated from context
            },

            // Protocol layer errors
            ErrorCode::InvalidResponse => ClientApiError::Protocol {
                code,
                message: "Invalid response format".to_string(),
                supported_versions: None,
            },
            ErrorCode::VersionMismatch => ClientApiError::Protocol {
                code,
                message: "Version mismatch".to_string(),
                supported_versions: None, // Note: This would ideally be populated from context
            },

            // Storage layer errors
            ErrorCode::DiskFull => ClientApiError::Storage {
                code,
                message: "Disk full".to_string(),
            },
            ErrorCode::DataCorruption => ClientApiError::Storage {
                code,
                message: "Data corruption detected".to_string(),
            },
            ErrorCode::StorageIoError => ClientApiError::Storage {
                code,
                message: "Storage I/O error".to_string(),
            },
            ErrorCode::StoragePermissionDenied => ClientApiError::Storage {
                code,
                message: "Storage permission denied".to_string(),
            },
            ErrorCode::KeyNotExist => ClientApiError::Storage {
                code,
                message: "Key not exist in storage".to_string(),
            },

            // Business logic errors
            ErrorCode::NotLeader => ClientApiError::Business {
                code,
                message: "Not leader".to_string(),
                required_action: Some("redirect to leader".to_string()),
            },
            ErrorCode::StaleOperation => ClientApiError::Business {
                code,
                message: "Stale operation".to_string(),
                required_action: Some("refresh state and retry".to_string()),
            },
            ErrorCode::InvalidRequest => ClientApiError::Business {
                code,
                message: "Invalid request".to_string(),
                required_action: Some("check request parameters".to_string()),
            },
            ErrorCode::RateLimited => ClientApiError::Business {
                code,
                message: "Rate limited".to_string(),
                required_action: Some("wait and retry".to_string()),
            },
            ErrorCode::ClusterUnavailable => ClientApiError::Business {
                code,
                message: "Cluster unavailable".to_string(),
                required_action: Some("try again later".to_string()),
            },
            ErrorCode::ProposeFailed => ClientApiError::Business {
                code,
                message: "Propose failed".to_string(),
                required_action: Some("try again later".to_string()),
            },
            ErrorCode::Uncategorized => ClientApiError::Business {
                code,
                message: "Uncategorized error".to_string(),
                required_action: None,
            },
            ErrorCode::TermOutdated => ClientApiError::Business {
                code,
                message: "Stale term error".to_string(),
                required_action: None,
            },
            ErrorCode::RetryRequired => ClientApiError::Business {
                code,
                message: "Retry required. Please try again.".to_string(),
                required_action: None,
            },

            // Unclassified error
            ErrorCode::General => ClientApiError::General {
                code,
                message: "General Client Api error".to_string(),
                required_action: None,
            },
            // Success case - should normally not be converted to error
            ErrorCode::Success => unreachable!(),
        }
    }
}

impl ClientApiError {
    /// Returns the error code associated with this error
    pub fn code(&self) -> ErrorCode {
        match self {
            ClientApiError::Network { code, .. } => *code,
            ClientApiError::Protocol { code, .. } => *code,
            ClientApiError::Storage { code, .. } => *code,
            ClientApiError::Business { code, .. } => *code,
            ClientApiError::General { code, .. } => *code,
        }
    }

    /// Returns the error message
    pub fn message(&self) -> &str {
        match self {
            ClientApiError::Network { message, .. } => message,
            ClientApiError::Protocol { message, .. } => message,
            ClientApiError::Storage { message, .. } => message,
            ClientApiError::Business { message, .. } => message,
            ClientApiError::General { message, .. } => message,
        }
    }
}

impl From<JoinError> for ClientApiError {
    fn from(_err: JoinError) -> Self {
        ErrorCode::JoinError.into()
    }
}
impl From<std::io::Error> for ClientApiError {
    fn from(_err: std::io::Error) -> Self {
        ErrorCode::StorageIoError.into()
    }
}

impl ClientApiError {
    pub fn general_client_error(message: String) -> Self {
        ClientApiError::General {
            code: ErrorCode::General,
            message,
            required_action: None,
        }
    }
}

impl std::fmt::Display for ClientApiError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code(), self.message())
    }
}

impl std::error::Error for ClientApiError {}
