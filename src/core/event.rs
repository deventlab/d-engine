use tonic::Status;

use crate::grpc::rpc_service::AppendEntriesRequest;
use crate::grpc::rpc_service::AppendEntriesResponse;
use crate::grpc::rpc_service::ClientProposeRequest;
use crate::grpc::rpc_service::ClientReadRequest;
use crate::grpc::rpc_service::ClientResponse;
use crate::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::grpc::rpc_service::ClusterConfUpdateResponse;
use crate::grpc::rpc_service::ClusterMembership;
use crate::grpc::rpc_service::MetadataRequest;
use crate::grpc::rpc_service::VoteRequest;
use crate::grpc::rpc_service::VoteResponse;
use crate::MaybeCloneOneshotSender;

#[derive(Debug)]
pub enum RoleEvent {
    BecomeFollower(Option<u32>), // BecomeFollower(Option<leader_id>)
    BecomeCandidate,
    BecomeLeader,
    BecomeLearner,

    NotifyNewCommitIndex { new_commit_index: u64 },
    ReprocessEvent(RaftEvent), //Replay the raft event when step down as another role
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub enum RaftEvent {
    ReceiveVoteRequest(
        VoteRequest,
        MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    ),

    ClusterConf(
        MetadataRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterMembership, Status>>,
    ),

    ClusterConfUpdate(
        ClusteMembershipChangeRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterConfUpdateResponse, Status>>,
    ),

    AppendEntries(
        AppendEntriesRequest,
        MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>,
    ),

    ClientPropose(
        ClientProposeRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),

    ClientReadRequest(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
}
