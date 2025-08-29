//! Raft Consensus Protocol Error Hierarchy
//!
//! Defines comprehensive error types for a Raft-based distributed system,
//! categorized by protocol layer and operational concerns.

use std::path::PathBuf;
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

    /// Snapshot-related errors during installation or restoration
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),

    /// Role permission conflict error
    #[error("Operation requires {required_role} role but current role is {current_role}")]
    RoleViolation {
        current_role: &'static str,
        required_role: &'static str,
        context: String,
    },
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
    Timeout { node_id: u32, duration: Duration },

    /// Unreachable node with source context
    #[error("Network unreachable: {source}")]
    Unreachable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Persistent connection failures
    #[error("Socket connect failed error: {0}")]
    ConnectError(String),

    /// Retry policy exhaustion
    #[error("Retry timeout after {0:?}")]
    RetryTimeoutError(Duration),

    /// TLS negotiation failures
    #[error("TLS handshake failed")]
    TlsHandshakeFailure,

    /// Missing peer list for RPC
    #[error("Request list for {request_type} contains no peers")]
    EmptyPeerList { request_type: &'static str },

    /// Peer connection not found
    #[error("Peer({0}) connection not found")]
    PeerConnectionNotFound(u32),

    /// Peer address not found
    #[error("Peer({0}) address not found")]
    PeerAddressNotFound(u32),

    /// Malformed node addresses
    #[error("Invalid URI format: {0}")]
    InvalidURI(String),

    /// RPC transmission failures with context
    #[error("Failed to send {request_type} request")]
    RequestSendFailure {
        request_type: &'static str,
        #[source]
        source: Box<tonic::transport::Error>,
    },

    /// Low-level TCP configuration errors
    #[error("TCP keepalive configuration error")]
    TcpKeepaliveError,

    /// HTTP/2 protocol configuration errors
    #[error("HTTP/2 keepalive configuration error")]
    Http2KeepaliveError,

    /// gRPC transport layer errors
    #[error(transparent)]
    TonicError(#[from] Box<tonic::transport::Error>),

    /// gRPC status code errors
    #[error(transparent)]
    TonicStatusError(#[from] Box<tonic::Status>),

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

    // #[error("Install snapshot RPC request been rejected, last_chunk={last_chunk}")]
    // SnapshotRejected { last_chunk: u32 },

    // #[error("Install snapshot RPC request failed")]
    // SnapshotTransferFailed,
    #[error("New node join cluster failed: {0}")]
    JoinFailed(String),

    #[error("Network timeout: {0}")]
    GlobalTimeout(String),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Disk I/O failures during log/snapshot operations
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// Custom error with a path as a string slice (`&str`)
    #[error("Error occurred at path: {path}")]
    PathError {
        path: PathBuf, // Use &str for lightweight references
        source: std::io::Error,
    },

    /// Serialization failures for persisted data
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),

    /// State machine application errors
    #[error("State Machine error: {0}")]
    StateMachineError(String),

    /// Log storage subsystem failures
    #[error("Log storage failure: {0}")]
    LogStorage(String),

    // /// Snapshot creation/restoration failures
    // #[error("Snapshot operation failed: {0}")]
    // Snapshot(String),
    /// Checksum validation failures
    #[error("Data corruption detected at {location}")]
    DataCorruption { location: String },

    /// Configuration storage failures
    #[error("Configuration storage error: {0}")]
    ConfigStorage(String),

    /// Embedded database errors
    #[error("Embedded database error: {0}")]
    DbError(String),

    /// Error type for value conversion operations
    #[error("Value convert failed")]
    Convert(#[from] ConvertError),

    /// File errors
    #[error("File errors")]
    File(#[from] FileError),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// ID allocation errors
    #[error(transparent)]
    IdAllocation(#[from] IdAllocationError),
}

#[derive(Debug, thiserror::Error)]
pub enum IdAllocationError {
    /// ID allocation overflow
    #[error("ID allocation overflow: {start} > {end}")]
    Overflow { start: u64, end: u64 },

    /// Invalid ID range
    #[error("Invalid ID range: {start}..={end}")]
    InvalidRange { start: u64, end: u64 },

    /// No available IDs
    #[error("No available IDs")]
    NoIdsAvailable,
}

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("Path does not exist: {0}")]
    NotFound(String),
    #[error("Path is a directory: {0}")]
    IsDirectory(String),
    #[error("File is busy: {0}")]
    Busy(String),
    #[error("Insufficient permissions: {0}")]
    PermissionDenied(String),
    #[error("File is occupied: {0}")]
    FileBusy(String),
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Too small: {0}")]
    TooSmall(u64),
    #[error("Invalid extension: {0}")]
    InvalidExt(String),
    #[error("Invalid GZIP header: {0}")]
    InvalidGzipHeader(String),
    #[error("Unknown IO error: {0}")]
    UnknownIo(String),
}

/// Error type for value conversion operations
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// Invalid input length error
    ///
    /// This occurs when the input byte slice length doesn't match the required 8 bytes.
    #[error("invalid byte length: expected 8 bytes, received {0} bytes")]
    InvalidLength(usize),

    /// Generic conversion failure with detailed message
    ///
    /// Wraps underlying parsing/conversion errors with context information
    #[error("conversion failure: {0}")]
    ConversionFailure(String),
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

    /// Protocol buffer encoding/decoding specific errors
    #[error("Protobuf operation failed: {0}")]
    Prost(#[from] ProstError),

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

/// Wrapper for prost encoding/decoding errors
#[derive(Debug, thiserror::Error)]
pub enum ProstError {
    #[error("Encoding failed: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("Decoding failed: {0}")]
    Decode(#[from] prost::DecodeError),
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

    /// Timeout to receive majority response
    #[error("Timeout to receive majority response")]
    QuorumTimeout,

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
    ConfigChangeUpdateFailed(String),

    /// Non-leader node attempted membership change
    #[error("Membership changes require leader role")]
    NotLeader,

    /// No leader information available
    #[error("No leader information available")]
    NoLeaderFound,

    /// Non-learner node attempted join cluster
    #[error("Only Learner can join cluster")]
    NotLearner,

    /// Cluster not in operational state
    #[error("Cluster bootstrap not completed")]
    ClusterIsNotReady,

    /// Connection establishment failure during join
    #[error("Cluster connection setup failed: {0}")]
    SetupClusterConnectionFailed(String),

    /// Missing node metadata in configuration
    #[error("Metadata missing for node {node_id} in cluster config")]
    NoMetadataFoundForNode { node_id: u32 },

    /// No available peers found for request
    #[error("No reachable peers found in cluster membership")]
    NoPeersAvailable,

    /// Node already been added into cluster config
    #[error("Node({0}) already been added into cluster config.")]
    NodeAlreadyExists(u32),

    /// To be removed node is leader.
    #[error("To be removed node({0}) is leader.")]
    RemoveNodeIsLeader(u32),

    #[error("Cannot promote node {node_id}: current role is {role} (expected LEARNER)")]
    InvalidPromotion { node_id: u32, role: i32 },

    #[error("Invalid membership change request")]
    InvalidChangeRequest,

    #[error("Commit Timeout")]
    CommitTimeout,

    #[error("Learner({0}) join cluster failed.")]
    JoinClusterFailed(u32),

    #[error("Join cluster error: {0}")]
    JoinClusterError(String),

    #[error("Not leader")]
    NoLeader,

    #[error("Mark leader id failed: {0}")]
    MarkLeaderIdFailed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("Snapshot receiver lagging, dropping chunk")]
    Backpressure,

    /// Snapshot chunk rejected during installation
    #[error("Install snapshot RPC request been rejected, last_chunk={last_chunk}")]
    Rejected { last_chunk: u32 },

    #[error("Install snapshot RPC request been rejected")]
    RemoteRejection,

    /// Snapshot transfer failed due to stream/network issues
    #[error("Install snapshot RPC request failed")]
    TransferFailed,

    /// Snapshot transfer timeout due to network issues
    #[error("Install snapshot RPC request timeout")]
    TransferTimeout,

    /// Snapshot operation failed with context
    #[error("Snapshot operation failed: {0}")]
    OperationFailed(String),

    /// Snapshot is outdated and cannot be used
    #[error("Snapshot is outdated")]
    Outdated,

    /// Snapshot file checksum mismatch
    #[error("Snapshot file checksum mismatch")]
    ChecksumMismatch,

    /// Invalid snapshot
    #[error("Invalid snapshot")]
    InvalidSnapshot,

    /// Invalid chunk sequence
    #[error("Invalid chunk sequence")]
    InvalidChunkSequence,

    /// Stream receiver disconnected
    #[error("Stream receiver disconnected")]
    ReceiverDisconnected,

    #[error("Invalid first snapshot stream chunk")]
    InvalidFirstChunk,

    #[error("Empty snapshot stream chunk")]
    EmptySnapshot,

    #[error("Incomplete snapshot error")]
    IncompleteSnapshot,

    #[error("Requested chunk {0} out of range (max: {1})")]
    ChunkOutOfRange(u32, u32),

    #[error("Chunk in stream is out of order")]
    OutOfOrderChunk,

    #[error("No metadata in chunk")]
    MissingMetadata,

    #[error("Chunk not cached: {0}")]
    ChunkNotCached(u32),

    #[error("Background stream push task died")]
    BackgroundTaskDied,
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

impl From<ConvertError> for Error {
    fn from(e: ConvertError) -> Self {
        Error::System(SystemError::Storage(StorageError::Convert(e)))
    }
}

impl From<FileError> for Error {
    fn from(e: FileError) -> Self {
        Error::System(SystemError::Storage(StorageError::File(e)))
    }
}

impl From<SerializationError> for Error {
    fn from(e: SerializationError) -> Self {
        Error::System(SystemError::Serialization(e))
    }
}

// // These allow direct conversion from prost errors to SystemError
// impl From<prost::EncodeError> for SystemError {
//     fn from(error: prost::EncodeError) -> Self {
//         SystemError::Prost(ProstError::Encode(error))
//     }
// }

// impl From<prost::DecodeError> for SystemError {
//     fn from(error: prost::DecodeError) -> Self {
//         SystemError::Prost(ProstError::Decode(error))
//     }
// }

impl From<ProstError> for Error {
    fn from(error: ProstError) -> Self {
        Error::System(SystemError::Prost(error))
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
        NetworkError::TonicError(Box::new(err)).into()
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        StorageError::DbError(err.to_string()).into()
    }
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        NetworkError::TaskFailed(err).into()
    }
}

impl From<SnapshotError> for Error {
    fn from(e: SnapshotError) -> Self {
        Error::Consensus(ConsensusError::Snapshot(e))
    }
}

impl From<IdAllocationError> for Error {
    fn from(e: IdAllocationError) -> Self {
        StorageError::IdAllocation(e).into()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        StorageError::IoError(e).into()
    }
}
