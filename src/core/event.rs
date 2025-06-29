use tonic::Status;

use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientResponse;
use crate::proto::client::ClientWriteRequest;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::JoinRequest;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::LeaderDiscoveryRequest;
use crate::proto::cluster::LeaderDiscoveryResponse;
use crate::proto::cluster::MetadataRequest;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotResponse;
use crate::MaybeCloneOneshotSender;
use crate::StreamResponseSender;

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
        ClusterConfChangeRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterConfUpdateResponse, Status>>,
    ),

    AppendEntries(
        AppendEntriesRequest,
        MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>,
    ),

    ClientPropose(
        ClientWriteRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),

    ClientReadRequest(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),

    // Response snapshot stream from Leader
    InstallSnapshotChunk(
        Box<tonic::Streaming<SnapshotChunk>>,
        MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
    ),

    // Request snapshot stream from Leader
    StreamSnapshot(Box<tonic::Streaming<SnapshotAck>>, StreamResponseSender),

    RaftLogCleanUp(
        PurgeLogRequest,
        MaybeCloneOneshotSender<std::result::Result<PurgeLogResponse, Status>>,
    ),

    JoinCluster(
        JoinRequest,
        MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
    ),

    DiscoverLeader(
        LeaderDiscoveryRequest,
        MaybeCloneOneshotSender<std::result::Result<LeaderDiscoveryResponse, Status>>,
    ),

    // Leader connect with peers and push snapshot stream
    #[allow(unused)]
    TriggerSnapshotPush {
        peer_id: u32,
    },

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

    ClusterConfUpdate(ClusterConfChangeRequest),

    AppendEntries(AppendEntriesRequest),

    ClientPropose(ClientWriteRequest),

    ClientReadRequest(ClientReadRequest),

    InstallSnapshotChunk,

    StreamSnapshot,

    RaftLogCleanUp(PurgeLogRequest),

    JoinCluster(JoinRequest),

    DiscoverLeader(LeaderDiscoveryRequest),

    TriggerSnapshotPush { peer_id: u32 },

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
        RaftEvent::StreamSnapshot(_, _) => TestEvent::StreamSnapshot,
        RaftEvent::RaftLogCleanUp(req, _) => TestEvent::RaftLogCleanUp(req.clone()),
        RaftEvent::JoinCluster(req, _) => TestEvent::JoinCluster(req.clone()),
        RaftEvent::DiscoverLeader(req, _) => TestEvent::DiscoverLeader(req.clone()),
        RaftEvent::CreateSnapshotEvent => TestEvent::CreateSnapshotEvent,
        RaftEvent::TriggerSnapshotPush { peer_id } => TestEvent::TriggerSnapshotPush { peer_id: *peer_id },
    }
}
