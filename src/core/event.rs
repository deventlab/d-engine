use crate::{
    grpc::rpc_service::{
        AppendEntriesRequest, AppendEntriesResponse, ClientProposeRequest, ClientReadRequest,
        ClientResponse, ClusteMembershipChangeRequest, ClusterConfUpdateResponse,
        ClusterMembership, MetadataRequest, VoteRequest, VoteResponse, VotedFor,
    },
    MaybeCloneOneshotSender,
};
use tonic::Status;

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
