use std::net::AddrParseError;

use config::ConfigError;
use thiserror::Error;
use tokio::task::JoinError;

use crate::proto::ClientRequestError;

#[doc(hidden)]
pub type Result<T> = std::result::Result<T, Error>;

#[doc(hidden)]
#[macro_export]
macro_rules! define_error_mapping {
    ($proto:ident, $app:ident; $($app_pattern:path => $proto_variant:path),+) => {
        impl From<$app> for $proto {
            fn from(e: $app) -> Self {
                match e {
                    $(
                        $app_pattern => $proto_variant,
                    )+
                    _ => Self::Unknown,
                }
            }
        }

        impl From<$proto> for $app {
            fn from(e: $proto) -> Self {
                match e {
                    $(
                        $proto_variant => $app_pattern,
                    )+
                    _ => Self::ServerError,
                }
            }
        }
    };
}

/// Custom errors definition
#[derive(Debug, Error)]
pub enum Error {
    /// General file IO error
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// Sled DB related error
    #[error(transparent)]
    SledError(#[from] sled::Error),

    /// RPC framework tonic related error
    #[error(transparent)]
    TonicError(#[from] tonic::transport::Error),

    /// General server error
    #[error("General server error: {0}")]
    GeneralServerError(String),

    /// General server error without message
    #[error("Server error")]
    ServerError,

    /// Bincode related error
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),

    /// Node start failed error
    #[error("Node failed to start error")]
    NodeFailedToStartError,

    /// RPC connection error
    #[error("Socket connect failed error")]
    ConnectError,

    /// Async task retry timeout error
    #[error("Retry Timeout error")]
    RetryTimeoutError,

    /// State machine related error
    #[error("State Machine error: {0}")]
    StateMachinneError(String),

    /// The request was sent to a follower or non-leader node; the client should retry with the
    /// leader.
    #[error("Dispatch client request leader error")]
    NodeIsNotLeaderError,

    /// The request was canceled by client
    #[error("Client canceled the request error")]
    ClientRequestCanceledError,

    /// The cluster has no known membership configuration; it may be unavailable or uninitialized.
    #[error("Cluster is unavailable")]
    ClusterMembershipNotFound,

    /// Returned when the cluster has no elected leader or the leader is unknown.
    /// This typically occurs during elections or network partitions.
    #[error("No leader found")]
    NoLeaderFound,

    /// Returned when client `get_multi` is called without any keys.
    #[error("No keys provided")]
    EmptyKeys,

    /// The response from the client response is invalid or missing expected data.
    #[error("Invalid response")]
    InvalidResponse,

    /// Returned when the server response type does not match the expected variant for the
    /// operation. Typically indicates a protocol mismatch or internal bug.
    #[error("Invalid response type")]
    InvalidResponseType,

    /// Indicates failure to fetch or apply the latest cluster membership from the leader.
    /// Additional context is included in the error message.
    #[error("Failed to update cluster membership from leader, {0}")]
    ClusterMembershipUpdateFailed(String),

    /// Indicates that the Raft metadata for the given node ID was not found.
    /// This usually occurs when the client or leader lacks an up-to-date cluster view,
    /// or the node has not been properly registered in cluster membership.
    ///
    /// The `u32` parameter represents the node ID being queried.
    #[error("Raft Metadata not found: {0}")]
    ClusterMetadataNodeMetaNotFound(u32),

    /// The follower could not confirm commitment of the log entry.
    /// This may happen if the follower's log is not sufficiently up-to-date or quorum was not
    /// reached.
    #[error("AppendEntriesCommitNotConfirmed")]
    AppendEntriesCommitNotConfirmed,

    /// The current node rejected the AppendEntries request because it is not the leader.
    /// Clients should redirect requests to the current cluster leader.
    #[error("NotLeader")]
    AppendEntriesNotLeader,

    /// The target peer specified in the AppendEntries request was not found in the current cluster
    /// view. This can occur if the membership view is outdated or the peer was removed.
    #[error("No peer found")]
    AppendEntriesNoPeerFound,

    // The target peer failed to respond to the AppendEntries request within the expected timeout period.
    /// Typically caused by network partitions, slow nodes, or overloaded peers.
    #[error("ServerTimeoutResponding")]
    AppendEntriesServerTimeoutResponding,

    /// The node failed to win the election.
    /// This could happen due to split vote, quorum not reached, or other election logic failure.
    /// The error message may contain additional context such as vote count or term mismatch.
    #[error("ElectionFailed: {0}")]
    ElectionFailed(String),

    /// Error occurred during a Raft state transition (e.g., follower → candidate, candidate →
    /// leader). Propagates the underlying StateTransitionError.
    #[error(transparent)]
    StateTransitionError(#[from] StateTransitionError),

    /// A message was received with a higher term than the local node's current term.
    /// The node should step down and update its term accordingly.
    #[error("Higher term found error: higher term = {0}")]
    HigherTermFoundError(u64),

    /// Receive Exist Signals
    #[error("Exit")]
    Exit,

    /// Indicated RPC service stops serving
    #[error("RPC server dies")]
    RPCServerDies,

    /// Indicate node is not ready to server request
    #[error("Node is not ready error")]
    ServerIsNotReadyError,

    /// Receive status error from RPC service
    #[error("Node status error: {0}")]
    RPCServerStatusError(String),

    /// The error is returned when the provided URI is invalid.
    /// The error message will include the invalid URI for context.
    #[error("invalid uri {0}")]
    InvalidURI(String),

    /// Indicates a failure to send a read request.
    #[error("Failed to send read request error")]
    FailedToSendReadRequestError,

    /// Indicates a failure to send a write request.
    #[error("Failed to send write request error")]
    FailedToSendWriteRequestError,

    /// A task retry failed, often due to reaching the maximum retry limit or encountering
    /// persistent errors.
    #[error("Task retry failed: {0}")]
    RetryTaskFailed(String),

    /// Address parsing error when invalid network address is encountered.
    #[error(transparent)]
    AddrParseError(#[from] AddrParseError),

    /// Failed to set up a peer connection, possibly due to network or configuration issues.
    /// The error message may contain more details about the failure.
    #[error("Set peer connection failed: {0}")]
    FailedSetPeerConnection(String),

    /// General error indicating a configuration issue.
    /// The error message will provide details about the specific configuration problem.
    #[error(transparent)]
    ConfigError(#[from] ConfigError),

    /// Error encountered when sending a Tonic status via tokio.
    /// The error message will include details about the status failure.
    #[error("tokio send Tonic Status error: {0}")]
    TokioSendStatusError(String),

    //===== Role responbilitiies errors ====
    /// The operation failed because the current node is not a leader.
    #[error("Not Leader")]
    NotLeader,

    /// The operation failed because the node is a learner and cannot perform this action.
    #[error("Learner can not handle")]
    LearnerCanNot,

    /// A generic error indicating an illegal state or operation.
    #[error("Illegal")]
    Illegal,

    /// The error indicates that a state transition has failed.
    #[error("Transition failed")]
    TransitionFailed,

    /// Error encountered when a tokio task join fails.
    #[error(transparent)]
    JoinError(#[from] JoinError),

    //===== Client errors =====
    //
    /// General client request error
    #[error("Client error: {0}")]
    GeneralClientError(String),

    //===== Config errors =====
    //
    /// Configuration is invalid. The error message will contain details about what is wrong with
    /// the config.
    #[error("Invalid config: {0}")]
    InvalidConfig(String),

    //===== RAFT::election =====
    /// Indicates a failure to reach consensus on leadership, typically due to
    /// disagreements or lack of quorum during leader election or leadership verification.
    #[error("Leadership Consensus Error: {0}")]
    LeadershipConsensusError(String),
}

#[doc(hidden)]
define_error_mapping!(
    ClientRequestError, Error;
    Error::AppendEntriesCommitNotConfirmed => ClientRequestError::CommitNotConfirmed,
    Error::AppendEntriesNotLeader => ClientRequestError::NotLeader,
    Error::AppendEntriesServerTimeoutResponding => ClientRequestError::ServerTimeout
);

#[derive(Debug, Error)]
#[doc(hidden)]
pub enum StateTransitionError {
    #[error("Not enough votes to transition to leader.")]
    NotEnoughVotes,

    #[error("Invalid state transition.")]
    InvalidTransition,

    #[error("Lock error.")]
    LockError,
}
