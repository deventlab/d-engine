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
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::SnapshotResponse;
use tonic::Status;

use crate::ApplyResult;
use crate::MaybeCloneOneshotSender;
use crate::Result;
use crate::StreamResponseSender;

#[derive(Debug, Clone, PartialEq)]
pub struct NewCommitData {
    pub new_commit_index: u64,
    pub role: i32,
    pub current_term: u64,
}

/// Client commands that require batching for performance
/// Separated from internal RaftEvent for drain-driven processing
#[derive(Debug)]
pub enum ClientCmd {
    Propose(
        ClientWriteRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
    Read(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
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

    /// Notify Raft loop that log entries up to `durable_index` are crash-safe (fsync complete).
    /// Sent by batch_processor after flush completes.
    /// Follower/Learner: triggers pending ACK send. Leader: triggers commit re-calculation.
    LogFlushed {
        durable_index: u64,
    },

    /// AppendEntries result from a per-follower ReplicationWorker back to the Raft loop.
    /// Leader processes this in handle_append_result: updates match_index, re-calculates commit,
    /// and drains pending_client_writes when quorum is achieved.
    AppendResult {
        follower_id: u32,
        result: Result<AppendEntriesResponse>,
    },

    /// Snapshot push result from a per-follower ReplicationWorker back to the Raft loop.
    /// Emitted after `transport.send_snapshot` completes (success or failure).
    /// Leader uses this to clear the per-worker `snapshot_in_progress` flag if needed.
    SnapshotPushCompleted {
        peer_id: u32,
        success: bool,
    },

    /// Noop entry committed — leader has confirmed quorum leadership.
    /// Sent by LeaderState::drain_commit_actions when the noop log index is committed.
    /// Raft loop responds by calling on_noop_committed() + notify_leader_change().
    NoopCommitted {
        term: u64,
    },

    /// State machine apply completed — processed at P2 (unbounded role_tx) to avoid
    /// priority inversion: AppendEntries RPCs at P4 (bounded event_tx) must not starve
    /// internal commit-driven events.
    ApplyCompleted {
        last_index: u64,
        results: Vec<ApplyResult>,
    },

    /// Bidi replication stream to a follower broke (network disconnect or error).
    /// Emitted by the replication worker's recv task when it gets an Err from the stream.
    /// Raft loop resets `next_index[peer] = match_index[peer] + 1` so that the next
    /// heartbeat re-sends any unACKed entries.  Worker handles reconnection internally.
    PeerStreamError {
        peer_id: u32,
    },

    /// Fatal error from SM worker — node must shutdown.
    /// Sent via role_tx (P2) so it is not blocked behind external RPCs on event_tx (P4).
    FatalError {
        source: String,
        error: String,
    },
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

    // Response snapshot stream from Leader
    InstallSnapshotChunk(
        Box<tonic::Streaming<SnapshotChunk>>,
        MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
    ),

    // Request snapshot stream from Leader
    StreamSnapshot(Box<tonic::Streaming<SnapshotAck>>, StreamResponseSender),

    JoinCluster(
        JoinRequest,
        MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
    ),

    DiscoverLeader(
        LeaderDiscoveryRequest,
        MaybeCloneOneshotSender<std::result::Result<LeaderDiscoveryResponse, Status>>,
    ),

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

    /// State machine apply failed - node must shutdown.
    /// Conservative: treat all SM errors as fatal (future: distinguish fatal vs application errors).
    FatalError {
        source: String, // Error source
        error: String,  // Error message
    },
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

    JoinCluster(JoinRequest),

    DiscoverLeader(LeaderDiscoveryRequest),

    // None RPC event
    CreateSnapshotEvent,

    SnapshotCreated,

    LogPurgeCompleted(LogId),

    PromoteReadyLearners,

    FatalError {
        source: String,
        error: String,
    },

    ApplyCompleted {
        last_index: u64,
        results: Vec<ApplyResult>,
    },
}

#[cfg(test)]
pub(crate) fn raft_event_to_test_event(event: &RaftEvent) -> TestEvent {
    match event {
        RaftEvent::ReceiveVoteRequest(req, _) => TestEvent::ReceiveVoteRequest(*req),
        RaftEvent::ClusterConf(req, _) => TestEvent::ClusterConf(*req),
        RaftEvent::ClusterConfUpdate(req, _) => TestEvent::ClusterConfUpdate(req.clone()),
        RaftEvent::AppendEntries(req, _) => TestEvent::AppendEntries(req.clone()),
        RaftEvent::InstallSnapshotChunk(_, _) => TestEvent::InstallSnapshotChunk,
        RaftEvent::StreamSnapshot(_, _) => TestEvent::StreamSnapshot,
        RaftEvent::JoinCluster(req, _) => TestEvent::JoinCluster(req.clone()),
        RaftEvent::DiscoverLeader(req, _) => TestEvent::DiscoverLeader(req.clone()),
        RaftEvent::CreateSnapshotEvent => TestEvent::CreateSnapshotEvent,
        RaftEvent::SnapshotCreated(_result) => TestEvent::SnapshotCreated,
        RaftEvent::LogPurgeCompleted(id) => TestEvent::LogPurgeCompleted(*id),
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
        RaftEvent::FatalError { source, error } => TestEvent::FatalError {
            source: source.clone(),
            error: error.clone(),
        },
    }
}
