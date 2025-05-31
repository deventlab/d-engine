use std::error::Error;

use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinError;
use tonic::Code;
use tonic::Status;

use crate::proto::error::ErrorCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientApiError {
    /// Network layer error (retryable)
    #[serde(rename = "network")]
    Network {
        code: u32,
        kind: NetworkErrorType,
        message: String,
        retry_after_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        leader_hint: Option<LeaderInfo>,
    },

    /// Protocol layer error (client needs to check protocol compatibility)
    #[serde(rename = "protocol")]
    Protocol {
        code: u32,
        kind: ProtocolErrorType,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        supported_versions: Option<Vec<String>>,
    },

    /// Storage layer error (internal problem on the server)
    #[serde(rename = "storage")]
    Storage {
        code: u32,
        kind: StorageErrorType,
        message: String,
    },

    /// Business logic error (client needs to adjust behavior)
    #[serde(rename = "business")]
    Business {
        code: u32,
        kind: BusinessErrorType,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        required_action: Option<String>,
    },

    /// General Client API error
    #[serde(rename = "general")]
    General {
        code: u32,
        kind: GeneralErrorType,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        required_action: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkErrorType {
    Timeout,
    ConnectionLost,
    InvalidAddress,
    TlsFailure,
    ProtocolViolation,
    JoinError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolErrorType {
    InvalidResponseFormat,
    VersionMismatch,
    ChecksumFailure,
    SerializationError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageErrorType {
    DiskFull,
    CorruptionDetected,
    IoFailure,
    PermissionDenied,
    KeyNotExist,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusinessErrorType {
    NotLeader,
    StaleRead,
    InvalidRequest,
    RateLimited,
    ClusterUnavailable,
    ProposeFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeneralErrorType {
    General,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub id: String,
    pub address: String,
    pub last_contact: u64, // Unix timestamp in ms
}
impl From<tonic::transport::Error> for ClientApiError {
    fn from(err: tonic::transport::Error) -> Self {
        let (kind, message, retry) = match err {
            e if e
                .source()
                .and_then(|e| e.downcast_ref::<std::io::Error>())
                .map(|e| e.kind() == std::io::ErrorKind::TimedOut)
                .unwrap_or(false) =>
            {
                (
                    NetworkErrorType::Timeout,
                    format!("Connection timeout: {e}"),
                    Some(3000), // Retry after 3 seconds
                )
            }
            e if e.to_string().contains("invalid uri") => {
                (NetworkErrorType::InvalidAddress, format!("Invalid address: {e}"), None)
            }
            _ => (
                NetworkErrorType::ConnectionLost,
                "Connection unexpectedly closed".into(),
                Some(5000),
            ),
        };

        // Convert NetworkErrorType to an appropriate ErrorCode
        let code = match kind {
            NetworkErrorType::Timeout => ErrorCode::ConnectionTimeout,
            NetworkErrorType::InvalidAddress => ErrorCode::InvalidAddress,
            NetworkErrorType::ConnectionLost => ErrorCode::ConnectionTimeout,
            _ => ErrorCode::Uncategorized,
        };

        ClientApiError::Network {
            code: code as u32,
            kind,
            message,
            retry_after_ms: retry,
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
                code: ErrorCode::ClusterUnavailable as u32,
                kind: BusinessErrorType::ClusterUnavailable,
                message,
                required_action: Some("Retry after cluster recovery".into()),
            },

            Code::Cancelled => Self::Network {
                code: ErrorCode::ConnectionTimeout as u32,
                kind: NetworkErrorType::Timeout,
                message,
                leader_hint: None,
                retry_after_ms: Some(1000),
            },

            Code::FailedPrecondition => {
                if let Some(leader) = parse_leader_from_metadata(&status) {
                    Self::Network {
                        code: ErrorCode::LeaderChanged as u32,
                        kind: NetworkErrorType::ProtocolViolation,
                        message: "Leadership changed".into(),
                        retry_after_ms: Some(1000),
                        leader_hint: Some(leader),
                    }
                } else {
                    Self::Business {
                        code: ErrorCode::StaleOperation as u32,
                        kind: BusinessErrorType::StaleRead,
                        message,
                        required_action: Some("Refresh cluster state".into()),
                    }
                }
            }

            Code::InvalidArgument => Self::Business {
                code: ErrorCode::InvalidRequest as u32,
                kind: BusinessErrorType::InvalidRequest,
                message,
                required_action: None,
            },

            Code::PermissionDenied => Self::Business {
                code: ErrorCode::NotLeader as u32,
                kind: BusinessErrorType::NotLeader,
                message,
                required_action: Some("Refresh cluster state".into()),
            },

            _ => Self::Business {
                code: ErrorCode::Uncategorized as u32,
                kind: BusinessErrorType::InvalidRequest,
                message: format!("Unhandled status code: {code:?}"),
                required_action: None,
            },
        }
    }
}

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
                code: code as u32,
                kind: NetworkErrorType::Timeout,
                message: "Connection timeout".to_string(),
                retry_after_ms: None,
                leader_hint: None,
            },
            ErrorCode::InvalidAddress => ClientApiError::Network {
                code: code as u32,
                kind: NetworkErrorType::InvalidAddress,
                message: "Invalid address".to_string(),
                retry_after_ms: None,
                leader_hint: None,
            },
            ErrorCode::LeaderChanged => ClientApiError::Network {
                code: code as u32,
                kind: NetworkErrorType::ConnectionLost,
                message: "Leader changed".to_string(),
                retry_after_ms: Some(100), // suggest immediate retry
                leader_hint: None,         // Note: This would ideally be populated from context
            },
            ErrorCode::JoinError => ClientApiError::Network {
                code: code as u32,
                kind: NetworkErrorType::JoinError,
                message: "Task Join Error".to_string(),
                retry_after_ms: Some(100), // suggest immediate retry
                leader_hint: None,         // Note: This would ideally be populated from context
            },

            // Protocol layer errors
            ErrorCode::InvalidResponse => ClientApiError::Protocol {
                code: code as u32,
                kind: ProtocolErrorType::InvalidResponseFormat,
                message: "Invalid response format".to_string(),
                supported_versions: None,
            },
            ErrorCode::VersionMismatch => ClientApiError::Protocol {
                code: code as u32,
                kind: ProtocolErrorType::VersionMismatch,
                message: "Version mismatch".to_string(),
                supported_versions: None, // Note: This would ideally be populated from context
            },

            // Storage layer errors
            ErrorCode::DiskFull => ClientApiError::Storage {
                code: code as u32,
                kind: StorageErrorType::DiskFull,
                message: "Disk full".to_string(),
            },
            ErrorCode::DataCorruption => ClientApiError::Storage {
                code: code as u32,
                kind: StorageErrorType::CorruptionDetected,
                message: "Data corruption detected".to_string(),
            },
            ErrorCode::StorageIoError => ClientApiError::Storage {
                code: code as u32,
                kind: StorageErrorType::IoFailure,
                message: "Storage I/O error".to_string(),
            },
            ErrorCode::StoragePermissionDenied => ClientApiError::Storage {
                code: code as u32,
                kind: StorageErrorType::PermissionDenied,
                message: "Storage permission denied".to_string(),
            },
            ErrorCode::KeyNotExist => ClientApiError::Storage {
                code: code as u32,
                kind: StorageErrorType::KeyNotExist,
                message: "Key not exist in storage".to_string(),
            },

            // Business logic errors
            ErrorCode::NotLeader => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::NotLeader,
                message: "Not leader".to_string(),
                required_action: Some("redirect to leader".to_string()),
            },
            ErrorCode::StaleOperation => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::StaleRead,
                message: "Stale operation".to_string(),
                required_action: Some("refresh state and retry".to_string()),
            },
            ErrorCode::InvalidRequest => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::InvalidRequest,
                message: "Invalid request".to_string(),
                required_action: Some("check request parameters".to_string()),
            },
            ErrorCode::RateLimited => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::RateLimited,
                message: "Rate limited".to_string(),
                required_action: Some("wait and retry".to_string()),
            },
            ErrorCode::ClusterUnavailable => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::ClusterUnavailable,
                message: "Cluster unavailable".to_string(),
                required_action: Some("try again later".to_string()),
            },
            ErrorCode::ProposeFailed => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::ProposeFailed,
                message: "Propose failed".to_string(),
                required_action: Some("try again later".to_string()),
            },

            // Unclassified error
            ErrorCode::Uncategorized => ClientApiError::Business {
                code: code as u32,
                kind: BusinessErrorType::InvalidRequest,
                message: "Uncategorized error".to_string(),
                required_action: None,
            },

            ErrorCode::General => ClientApiError::General {
                code: code as u32,
                kind: GeneralErrorType::General,
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
    pub fn code(&self) -> u32 {
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
            code: ErrorCode::General as u32,
            kind: GeneralErrorType::General,
            message,
            required_action: None,
        }
    }
}
