use tonic::Status;

use crate::proto::client::ClientProposeRequest;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientResponse;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::ClusterMembershipChangeRequest;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::MetadataRequest;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotResponse;
use crate::MaybeCloneOneshotSender;

#[derive(Debug, Clone)]
pub(crate) struct NewCommitData {
    pub(crate) new_commit_index: u64,
    pub(crate) role: i32,
    pub(crate) current_term: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum RoleEvent {
    BecomeFollower(Option<u32>), // BecomeFollower(Option<leader_id>)
    BecomeCandidate,
    BecomeLeader,
    BecomeLearner,

    NotifyNewCommitIndex(NewCommitData),

    ReprocessEvent(Box<RaftEvent>), //Replay the raft event when step down as another role
}

#[derive(Debug)]
pub(crate) enum RaftEvent {
    ReceiveVoteRequest(
        VoteRequest,
        MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    ),

    ClusterConf(
        MetadataRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterMembership, Status>>,
    ),

    ClusterConfUpdate(
        ClusterMembershipChangeRequest,
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

    InstallSnapshotChunk(
        Box<tonic::Streaming<SnapshotChunk>>,
        MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
    ),

    RaftLogCleanUp(
        PurgeLogRequest,
        MaybeCloneOneshotSender<std::result::Result<PurgeLogResponse, Status>>,
    ),

    JoinCluster(
        JoinRequest,
        MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
    ),

    // None RPC event
    #[allow(dead_code)]
    CreateSnapshotEvent,
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, Clone))]
#[allow(unused)]
pub(crate) enum TestEvent {
    ReceiveVoteRequest(VoteRequest),

    ClusterConf(MetadataRequest),

    ClusterConfUpdate(ClusterMembershipChangeRequest),

    AppendEntries(AppendEntriesRequest),

    ClientPropose(ClientProposeRequest),

    ClientReadRequest(ClientReadRequest),

    InstallSnapshotChunk,

    RaftLogCleanUp(PurgeLogRequest),

    JoinCluster(JoinRequest),

    // None RPC event
    CreateSnapshotEvent,
}

#[cfg(test)]
pub(crate) fn raft_event_to_test_event(event: &RaftEvent) -> TestEvent {
    match event {
        RaftEvent::ReceiveVoteRequest(req, _) => TestEvent::ReceiveVoteRequest(*req),
        RaftEvent::ClusterConf(req, _) => TestEvent::ClusterConf(*req),
        RaftEvent::ClusterConfUpdate(req, _) => TestEvent::ClusterConfUpdate(req.clone()),
        RaftEvent::AppendEntries(req, _) => TestEvent::AppendEntries(req.clone()),
        RaftEvent::ClientPropose(req, _) => TestEvent::ClientPropose(req.clone()),
        RaftEvent::ClientReadRequest(req, _) => TestEvent::ClientReadRequest(req.clone()),
        RaftEvent::InstallSnapshotChunk(_, _) => TestEvent::InstallSnapshotChunk,
        RaftEvent::RaftLogCleanUp(purge_log_request, _) => TestEvent::RaftLogCleanUp(purge_log_request.clone()),
        RaftEvent::JoinCluster(join_cluster_request, _) => TestEvent::JoinCluster(join_cluster_request.clone()),
        RaftEvent::CreateSnapshotEvent => TestEvent::CreateSnapshotEvent,
    }
}
