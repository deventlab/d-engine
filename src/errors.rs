//! Raft Consensus Protocol Error Hierarchy
//!
//! Defines comprehensive error types for a Raft-based distributed system,
//! categorized by protocol layer and operational concerns.

use std::sync::Arc;
use std::time::Duration;

use config::ConfigError;
use tokio::task::JoinError;

#[doc(hidden)]
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Infrastructure-level failures (network, storage, serialization)
    #[error(transparent)]
    System(#[from] SystemError),

    /// Cluster configuration validation failures
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Raft consensus protocol violations and failures
    #[error(transparent)]
    Consensus(#[from] ConsensusError),

    /// Unrecoverable failures requiring process termination
    #[error("Fatal error: {0}")]
    Fatal(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// Illegal Raft node state transitions
    #[error(transparent)]
    StateTransition(#[from] StateTransitionError),

    /// Leader election failures (Section 5.2 Raft paper)
    #[error(transparent)]
    Election(#[from] ElectionError),

    /// Log replication failures (Section 5.3 Raft paper)
    #[error(transparent)]
    Replication(#[from] ReplicationError),

    /// Cluster membership change failures (Section 6 Raft paper)
    #[error(transparent)]
    Membership(#[from] MembershipError),
}

#[derive(Debug, thiserror::Error)]
#[doc(hidden)]
pub enum StateTransitionError {
    #[error("Not enough votes to transition to leader.")]
    NotEnoughVotes,

    #[error("Invalid state transition.")]
    InvalidTransition,

    #[error("Lock error.")]
    LockError,
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    /// Endpoint unavailable (HTTP 503 equivalent)
    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    /// Peer communication timeout
    #[error("Connection timeout to {node_id} after {duration:?}")]
    Timeout { node_id: u64, duration: Duration },

    /// Unreachable node with source context
    #[error("Network unreachable: {source}")]
    Unreachable {
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    /// Persistent connection failures
    #[error("Socket connect failed error")]
    ConnectError,

    /// Retry policy exhaustion
    #[error("Retry timeout after {0:?}")]
    RetryTimeoutError(Duration),

    /// TLS negotiation failures
    #[error("TLS handshake failed")]
    TlsHandshakeFailure,

    /// Missing peer list for RPC
    #[error("Request list for {request_type} contains no peers")]
    EmptyPeerList { request_type: &'static str },

    /// Malformed node addresses
    #[error("Invalid URI format: {0}")]
    InvalidURI(String),

    /// RPC transmission failures with context
    #[error("Failed to send {request_type} request")]
    RequestSendFailure {
        request_type: &'static str,
        #[source]
        source: tonic::transport::Error,
    },

    /// Low-level TCP configuration errors
    #[error("TCP keepalive configuration error")]
    TcpKeepaliveError,

    /// HTTP/2 protocol configuration errors
    #[error("HTTP/2 keepalive configuration error")]
    Http2KeepaliveError,

    /// gRPC transport layer errors
    #[error(transparent)]
    TonicError(#[from] tonic::transport::Error),

    /// gRPC status code errors
    #[error(transparent)]
    TonicStatusError(#[from] tonic::Status),

    #[error("Failed to send read request: {0}")]
    ReadSend(#[from] ReadSendError),

    #[error("Failed to send write request: {0}")]
    WriteSend(#[from] WriteSendError),

    #[error("Background task failed: {0}")]
    TaskFailed(#[from] JoinError),

    #[error("{0}")]
    TaskBackoffFailed(String),

    #[error("{0}")]
    SingalSendFailed(String),

    #[error("{0}")]
    SingalReceiveFailed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Disk I/O failures during log/snapshot operations
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// Serialization failures for persisted data
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),

    /// State machine application errors
    #[error("State Machine error: {0}")]
    StateMachineError(String),

    /// Log storage subsystem failures
    #[error("Log storage failure: {0}")]
    LogStorage(String),

    /// Snapshot creation/restoration failures
    #[error("Snapshot operation failed: {0}")]
    Snapshot(String),

    /// Checksum validation failures
    #[error("Data corruption detected at {location}")]
    DataCorruption { location: String },

    /// Configuration storage failures
    #[error("Configuration storage error: {0}")]
    ConfigStorage(String),

    /// Embedded database errors
    #[error(transparent)]
    SledError(#[from] sled::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ReadSendError {
    #[error("Network timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("Connection failed")]
    Connection(#[from] tonic::transport::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteSendError {
    #[error("Not cluster leader")]
    NotLeader,

    #[error("Network unreachable")]
    Unreachable,

    #[error("Payload too large")]
    PayloadExceeded,
}

#[derive(Debug, thiserror::Error)]
pub enum SystemError {
    // Network layer
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    // Storage layer
    #[error("Storage operation failed")]
    Storage(#[from] StorageError),

    //Serialization
    #[error("Serialization error")]
    Serialization(#[from] SerializationError),

    // Basic node operations
    #[error("Node failed to start: {0}")]
    NodeStartFailed(String),

    #[error("General server error: {0}")]
    GeneralServer(String),

    #[error("Internal server error")]
    ServerUnavailable,
}

// Serialization is classified separately (across protocol layers and system layers)
#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("Bincode serialization failed: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ElectionError {
    /// General election process failure
    #[error("Election failed: {0}")]
    Failed(String),

    /// Stale term detection (Section 5.1 Raft paper)
    #[error("Found higher term(={0}) during election process")]
    HigherTerm(u64),

    /// Term number inconsistency
    #[error("Term conflict (current: {current}, received: {received})")]
    TermConflict { current: u64, received: u64 },

    /// Log inconsistency during vote requests (Section 5.4.1 Raft paper)
    #[error("Log conflict at index {index} (expected: {expected_term}, actual: {actual_term})")]
    LogConflict {
        index: u64,
        expected_term: u64,
        actual_term: u64,
    },

    /// Quorum not achieved (Section 5.2 Raft paper)
    #[error("Quorum not reached (required: {required}, succeed: {succeed})")]
    QuorumFailure { required: usize, succeed: usize },

    /// Leadership handoff failures
    #[error("Leadership consensus error: {0}")]
    LeadershipConsensus(String),

    /// Isolated node scenario
    #[error("No voting member found for candidate {candidate_id}")]
    NoVotingMemberFound { candidate_id: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    /// Stale leader detected during AppendEntries RPC
    #[error("Found higher term(={0}) during replication process")]
    HigherTerm(u64),

    /// Failed to achieve majority acknowledgment
    #[error("Quorum not reached for log replication")]
    QuorumNotReached,

    /// Target follower node unreachable
    #[error("Node {node_id} unreachable for replication")]
    NodeUnreachable { node_id: u32 },

    /// Network timeout during replication RPC
    #[error("RPC timeout after {duration}ms")]
    RpcTimeout { duration: u64 },

    /// Missing peer configuration in leader state
    #[error("No peer mapping for leader {leader_id}")]
    NoPeerFound { leader_id: u32 },

    /// Log inconsistency detected during replication (§5.3)
    #[error("Log conflict at index {index} (expected term {expected_term}, actual {actual_term})")]
    LogConflict {
        index: u64,
        expected_term: u64,
        actual_term: u64,
    },

    /// Node not in leader state for replication requests
    #[error("Replication requires leader role (known leader: {leader_id:?})")]
    NotLeader { leader_id: Option<u32> },
}

#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    /// Failed to reach consensus on configuration change
    #[error("Membership update consensus failure: {0}")]
    UpdateFailed(String),

    /// Non-leader node attempted membership change
    #[error("Membership changes require leader role")]
    NotLeader,

    /// Cluster not in operational state
    #[error("Cluster bootstrap not completed")]
    ClusterIsNotReady,

    /// Connection establishment failure during join
    #[error("Cluster connection setup failed: {0}")]
    SetupClusterConnectionFailed(String),

    /// Missing node metadata in configuration
    #[error("Metadata missing for node {node_id} in cluster config")]
    NoMetadataFoundForNode { node_id: u32 },
}

// ============== Conversion Implementations ============== //
impl From<NetworkError> for Error {
    fn from(e: NetworkError) -> Self {
        Error::System(SystemError::Network(e))
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::System(SystemError::Storage(e))
    }
}

impl From<SerializationError> for Error {
    fn from(e: SerializationError) -> Self {
        Error::System(SystemError::Serialization(e))
    }
}

// ===== Consensus Error conversions =====

impl From<StateTransitionError> for Error {
    fn from(e: StateTransitionError) -> Self {
        Error::Consensus(ConsensusError::StateTransition(e))
    }
}

impl From<ElectionError> for Error {
    fn from(e: ElectionError) -> Self {
        Error::Consensus(ConsensusError::Election(e))
    }
}

impl From<ReplicationError> for Error {
    fn from(e: ReplicationError) -> Self {
        Error::Consensus(ConsensusError::Replication(e))
    }
}

impl From<MembershipError> for Error {
    fn from(e: MembershipError) -> Self {
        Error::Consensus(ConsensusError::Membership(e))
    }
}

// ===== Network sub-error conversions =====
impl From<ReadSendError> for Error {
    fn from(e: ReadSendError) -> Self {
        Error::System(SystemError::Network(NetworkError::ReadSend(e)))
    }
}

impl From<WriteSendError> for Error {
    fn from(e: WriteSendError) -> Self {
        Error::System(SystemError::Network(NetworkError::WriteSend(e)))
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        NetworkError::TonicError(err).into()
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        StorageError::SledError(err).into()
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        StorageError::IoError(err).into()
    }
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        NetworkError::TaskFailed(err).into()
    }
}
