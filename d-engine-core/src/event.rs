use std::path::PathBuf;

use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::SnapshotResponse;
use tonic::Status;

use crate::MaybeCloneOneshotSender;
use crate::Result;
use crate::StreamResponseSender;

#[derive(Debug, Clone)]
pub struct NewCommitData {
    pub new_commit_index: u64,
    pub role: i32,
    pub current_term: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum RoleEvent {
    BecomeFollower(Option<u32>), // BecomeFollower(Option<leader_id>)
    BecomeCandidate,
    BecomeLeader,
    BecomeLearner,

    NotifyNewCommitIndex(NewCommitData),

    /// Notify when follower/learner confirms leader via committed vote
    /// Triggered when committed vote changes from false to true
    /// No state transition - pure notification for watch channel
    LeaderDiscovered(u32, u64), // (leader_id, term)

    ReprocessEvent(Box<RaftEvent>), //Replay the raft event when step down as another role
}

#[derive(Debug)]
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

    CreateSnapshotEvent,

    LogPurgeCompleted(LogId),

    SnapshotCreated(Result<(SnapshotMetadata, PathBuf)>),

    // Lightweight promotion trigger
    PromoteReadyLearners,

    /// Node removed itself from cluster membership
    /// Leader must step down immediately after self-removal per Raft protocol
    StepDownSelfRemoved,

    /// Membership change has been applied to state
    /// Leader should refresh cluster metadata cache
    MembershipApplied,

    /// Signal to flush pending read requests
    /// Sent by timeout task when read buffer reaches time threshold
    FlushReadBuffer,
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, Clone))]
#[allow(unused)]
pub enum TestEvent {
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

    SnapshotCreated,

    LogPurgeCompleted(LogId),

    PromoteReadyLearners,

    FlushReadBuffer,
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
        RaftEvent::SnapshotCreated(_result) => TestEvent::SnapshotCreated,
        RaftEvent::LogPurgeCompleted(id) => TestEvent::LogPurgeCompleted(*id),
        RaftEvent::TriggerSnapshotPush { peer_id } => {
            TestEvent::TriggerSnapshotPush { peer_id: *peer_id }
        }
        RaftEvent::PromoteReadyLearners => TestEvent::PromoteReadyLearners,
        RaftEvent::StepDownSelfRemoved => {
            // StepDownSelfRemoved is handled at Raft level, not converted to TestEvent
            // This is a control flow event, not a user-facing event
            TestEvent::CreateSnapshotEvent // Placeholder - this event won't be emitted to tests
        }
        RaftEvent::MembershipApplied => {
            // MembershipApplied is internal event for cache refresh
            TestEvent::CreateSnapshotEvent // Placeholder
        }
        RaftEvent::FlushReadBuffer => TestEvent::FlushReadBuffer,
    }
}
