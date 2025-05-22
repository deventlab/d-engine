use tonic::Status;

use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientProposeRequest;
use crate::proto::ClientReadRequest;
use crate::proto::ClientResponse;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::ClusterConfUpdateResponse;
use crate::proto::ClusterMembership;
use crate::proto::LogId;
use crate::proto::MetadataRequest;
use crate::proto::PurgeLogRequest;
use crate::proto::PurgeLogResponse;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotResponse;
use crate::proto::VoteRequest;
use crate::proto::VoteResponse;
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

    InstallSnapshotChunk(
        Box<tonic::Streaming<SnapshotChunk>>,
        MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
    ),

    RaftLogCleanUp(
        PurgeLogRequest,
        MaybeCloneOneshotSender<std::result::Result<PurgeLogResponse, Status>>,
    ),

    // None RPC event
    #[allow(dead_code)]
    CreateSnapshotEvent,

    // Event after log been purged successfully
    LogPurgedEvent(LogId),
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, Clone))]
#[allow(unused)]
pub(crate) enum TestEvent {
    ReceiveVoteRequest(VoteRequest),

    ClusterConf(MetadataRequest),

    ClusterConfUpdate(ClusteMembershipChangeRequest),

    AppendEntries(AppendEntriesRequest),

    ClientPropose(ClientProposeRequest),

    ClientReadRequest(ClientReadRequest),

    InstallSnapshotChunk,

    RaftLogCleanUp(PurgeLogRequest),

    // None RPC event
    CreateSnapshotEvent,

    // Event after log been purged successfully
    LogPurgedEvent(LogId),
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
        RaftEvent::CreateSnapshotEvent => TestEvent::CreateSnapshotEvent,
        RaftEvent::LogPurgedEvent(log_id) => TestEvent::LogPurgedEvent(*log_id),
    }
}
