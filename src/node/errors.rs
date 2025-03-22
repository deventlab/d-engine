use crate::{grpc::rpc_service::ClientRequestError, NewLeaderInfo, RoleEvent};
use config::ConfigError;
use prost::{DecodeError, EncodeError};
use std::net::AddrParseError;
use thiserror::Error;
use tokio::{sync::mpsc::error::SendError, task::JoinError};

pub type Result<T> = std::result::Result<T, Error>;

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

#[derive(Debug, Error)]
pub enum Error {
    #[error("Vote stage error: {0}")]
    VoteStageError(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ProstEncodeError(#[from] EncodeError),

    #[error(transparent)]
    ProstDecodeError(#[from] DecodeError),

    #[error(transparent)]
    SledError(#[from] sled::Error),

    #[error(transparent)]
    TonicError(#[from] tonic::transport::Error),

    #[error(transparent)]
    BincodeError(#[from] bincode::Error),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Rpc timeout error")]
    RpcTimeout,

    #[error("Not Found")]
    NotFound,

    #[error("Unexpected EOF")]
    UnexpectedEOF,

    #[error("Found new leader({0:?}) error")]
    FoundNewLeaderError(NewLeaderInfo),

    #[error("Append Entries: no result return")]
    AppendEntriesNoResultReturn,

    #[error("Append Entries: Learner({0}) become Follower")]
    AppendEntriesLearnerBecomeFollower(u64),

    #[error("Append Entries: Follower({0}) become Learner")]
    AppendEntriesFollowerBecomeLearner(u64),

    #[error("General server error: {0}")]
    GeneralServerError(String),

    #[error(" server error")]
    ServerError,

    #[error("Request not committed yet error")]
    RequestNotCommittedYet,

    #[error("Node failed to start error")]
    ServerFailedToStartError,

    #[error("Socket connect failed error")]
    ConnectError,

    #[error("Message IO error")]
    MessageIOError,

    #[error("Retry Timeout error")]
    RetryTimeoutError,

    #[error("Duplicated entry in memory log error")]
    DupEntryInMemoryLogError,

    /// TODO: if we need to take any action if commit notify action failed.
    /// Now, we did nothing.
    #[error("Failed to notify commit success handler")]
    FailedToNotifyCommitSuccessHandler,

    #[error("Failed to sync messages from leader to follower")]
    FailedToAppendEntries,

    #[error("General local log IO error")]
    GeneralLocalLogIOError,

    #[error("Raft state error: {0}")]
    NodeStateError(String),

    #[error("State Machine error")]
    StateMachinneError,

    // received term smaller than mine
    #[error("Illegal term")]
    IllegalTermError,

    //===== Client errors =====
    //
    #[error("Client request error: {0}")]
    ClientRequestTimeoutError(String),

    #[error("Dispatch client request leader error")]
    NodeIsNotLeaderError,

    #[error("No connected leader channel error")]
    DispatchClientNoLeaderChannelError,
    //===== Cluster membership sync errors =====
    //
    #[error("Failed to sync cluster membership to some peers")]
    ClusterMembershipSyncFailed,

    //===== Cluster Metadata errors =====
    //
    #[error("Raft Metadata not found: {0}")]
    ClusterMetadataNodeMetaNotFound(u32),

    //===== General signal errors =====
    //
    #[error("Singal sender closed: {0}")]
    SignalSenderClosed(String),

    //===== Append Entries  =====
    //
    #[error("AppendEntriesCommitNotConfirmed")]
    AppendEntriesCommitNotConfirmed,
    #[error("NotLeader")]
    AppendEntriesNotLeader,
    #[error("No peer found")]
    AppendEntriesNoPeerFound,
    #[error("ServerTimeoutResponding")]
    AppendEntriesServerTimeoutResponding,
    #[error("ServerGeneralError")]
    AppendEntriesServerGeneralError,

    //===== ElectionController errors =====
    //
    #[error("ElectionFailed: {0}")]
    ElectionFailed(String),

    #[error("ElectionFailed, Learner can not vote.")]
    LearnerCanNotVote,

    #[error("ElectionFailed, Leader can not vote.")]
    LeaderCanNotVote,

    #[error(transparent)]
    StateTransitionError(#[from] StateTransitionError),
    //===== Receive Exist Signals =====
    //
    #[error("Exit")]
    Exit,
    //===== TONIC RPC server =====
    //
    #[error("Failed to start")]
    RPCServerFailedToStart,

    #[error("RPC server dies")]
    RPCServerDies,

    #[error("Node is not ready error")]
    ServerIsNotReadyError,

    #[error("Node status error: {0}")]
    RPCServerStatusError(String),

    //===== TONIC RPC client =====
    //
    // InvalidURI is the error when the uri is invalid.
    #[error("invalid uri {0}")]
    InvalidURI(String),

    // InvalidPeer is the error when the peer is invalid.
    #[error("invalid peer {0}")]
    InvalidPeer(String),

    #[error("Failed to retrieve leader address error")]
    FailedToRetrieveLeaderAddrError,

    #[error("Failed to send read request error")]
    FailedToSendReadRequestError,

    #[error("Failed to send write request error")]
    FailedToSendWriteRequestError,

    //===== Async Task Retry errors =====
    //
    #[error("Task retry failed: {0}")]
    RetryTaskFailed(String),

    //===== EventListern errors =====
    //
    #[cfg(test)]
    #[error("EventListernerError")]
    EventListernerError,

    //===== Network errors ====
    #[error(transparent)]
    AddrParseError(#[from] AddrParseError),

    #[error("Set peer connection failed: {0}")]
    FailedSetPeerConnection(String),

    #[error("Peer channles does not exist")]
    PeerChannelsNotExist,
    //===== Config errors ====
    #[error(transparent)]
    ConfigError(#[from] ConfigError),

    //===== mspc send errors ====
    #[error("tokio send RoleEvent error: {0}")]
    TokioSendRoleEventError(#[from] SendError<RoleEvent>),
    #[error("tokio send Tonic Status error: {0}")]
    TokioSendStatusError(String),

    //===== Role responbilitiies errors ====
    #[error("Not Leader")]
    NotLeader,
    #[error("Learner can not handle")]
    LearnerCanNot,
    #[error("Illegal")]
    Illegal,
    #[error("Transition failed")]
    TransitionFailed,
    //===== tokio errors ====
    #[error(transparent)]
    JoinError(#[from] JoinError),

    //===== Handle Raft Event Errors =====
    //
    #[error("ClientReadRequestFailed failed: {0}")]
    ClientReadRequestFailed(String),

    //===== Client errors =====
    //
    #[error("Client error: {0}")]
    ClientError(String),
}

define_error_mapping!(
    ClientRequestError, Error;
    Error::AppendEntriesCommitNotConfirmed => ClientRequestError::CommitNotConfirmed,
    Error::AppendEntriesNotLeader => ClientRequestError::NotLeader,
    Error::AppendEntriesServerTimeoutResponding => ClientRequestError::ServerTimeout
);

#[derive(Debug, Error)]
pub enum StateTransitionError {
    #[error("Not enough votes to transition to leader.")]
    NotEnoughVotes,

    #[error("Invalid state transition.")]
    InvalidTransition,

    #[error("Lock error.")]
    LockError,
}

// impl<T> From<SendError<T>> for Error {
//     fn from(e: SendError<T>) -> Self {
//         Error::RPCServerStatusError(format!("Channel send error: {:?}", e))
//     }
// }
