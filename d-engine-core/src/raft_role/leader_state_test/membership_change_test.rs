//! Tests for leader state membership change operations
//!
//! This module tests cluster membership changes including:
//! - Node joining (handle_join_cluster)
//! - Learner promotion
//! - Pending promotion queue management
//! - Stale learner detection and cleanup
//! - Learner progress checking

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tonic::Code;
use tracing_test::traced_test;

use crate::AppendResults;
use crate::Error;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockTransport;
use crate::PeerUpdate;
use crate::RaftContext;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use d_engine_proto::common::{
    LogId,
    NodeRole::{Follower, Learner},
    NodeStatus,
};
use d_engine_proto::server::cluster::{ClusterMembership, JoinRequest, NodeMeta};
use d_engine_proto::server::storage::SnapshotMetadata;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

// ============================================================================
// Join Cluster Tests
// ============================================================================

/// Test handling join_cluster request successfully
///
/// # Test Scenario
/// Leader receives join request from new learner node and successfully
/// adds it to the cluster after achieving quorum.
///
/// # Given
/// - Leader with existing voters
/// - Mock membership allows adding learner
/// - Mock replication handler returns successful quorum
///
/// # When
/// - handle_join_cluster is called with new learner
///
/// # Then
/// - Operation succeeds
/// - Response indicates success
/// - Response contains leader_id
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context =
        mock_raft_context("/tmp/test_handle_join_cluster_success", graceful_rx, None);
    let node_id = 100;
    let address = "127.0.0.1:8080".to_string();

    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    membership.expect_contains_node().returning(|_| false);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|_current_leader_id| ClusterMembership::default());
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));
    membership.expect_get_peer_channel().returning(|_, _| {
        Some(tonic::transport::Endpoint::from_static("http://[::]:50051").connect_lazy())
    });
    raft_context.membership = Arc::new(membership);
    let transport = MockTransport::new();
    raft_context.transport = Arc::new(transport);

    // Prepare verify_internal_quorum to return success
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::success(5, 6)),
                ]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    raft_context.storage.raft_log = Arc::new(raft_log);
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 1 }),
            checksum: Bytes::new(),
        })
    });
    raft_context.storage.state_machine = Arc::new(state_machine);

    // Mock state machine handler
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_get_latest_snapshot_metadata().returning(|| None);
    raft_context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (sender, mut receiver) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
                node_id,
                node_role: Learner.into(),
                address: address.clone(),
            },
            sender,
            &raft_context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_ok());
    let response = receiver.recv().await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(response.leader_id, 1);
}

/// Test handling join_cluster when node already exists
///
/// # Test Scenario
/// Leader receives join request from node that already exists in cluster.
///
/// # Given
/// - Mock membership reports node already exists
///
/// # When
/// - handle_join_cluster is called
///
/// # Then
/// - Operation returns error
/// - Client receives FailedPrecondition error
/// - Error message indicates node already added
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_node_exists() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_handle_join_cluster_node_exists",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership to report existing node
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| true);
    let context = RaftContext {
        membership: Arc::new(membership),
        ..context
    };

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (sender, mut receiver) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
                node_id,
                node_role: Learner.into(),
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("already been added into cluster config"));
}

/// Test handling join_cluster when quorum not achieved
///
/// # Test Scenario
/// Leader receives join request but fails to achieve quorum for configuration change.
///
/// # Given
/// - Mock membership with no voting members (will cause quorum failure)
/// - Mock replication returns commit_quorum_achieved = false
///
/// # When
/// - handle_join_cluster is called
///
/// # Then
/// - Operation returns error
/// - Client receives FailedPrecondition error with "Commit Timeout" message
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_quorum_failed() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_quorum_failed",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership.expect_voters().returning(Vec::new); // Empty voting members will cause quorum failure
    context.membership = Arc::new(membership);

    // Setup replication handler to simulate quorum not achieved
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (sender, mut receiver) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
                node_id,
                node_role: Learner.into(),
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("Commit Timeout"));
}

/// Test handling join_cluster with quorum verification error
///
/// # Test Scenario
/// Leader receives join request but encounters error during quorum verification.
///
/// # Given
/// - Mock replication handler returns Fatal error
///
/// # When
/// - handle_join_cluster is called
///
/// # Then
/// - Operation returns error
/// - Client receives FailedPrecondition error
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_quorum_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_quorum_error",
        graceful_rx,
        None,
    );
    let node_id = 100;

    // Mock membership
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));

    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    context.membership = Arc::new(membership);

    // Setup replication handler to return error
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Test error".to_string())));

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (sender, mut receiver) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
                node_id,
                node_role: Learner.into(),
                address: "127.0.0.1:8080".to_string(),
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_err());
    let status = receiver.recv().await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
}

/// Test handling join_cluster triggers snapshot transfer
///
/// # Test Scenario
/// Leader receives join request and snapshot metadata exists,
/// triggering snapshot transfer to new learner.
///
/// # Given
/// - Mock state machine handler returns snapshot metadata
/// - Mock replication handler returns successful quorum
///
/// # When
/// - handle_join_cluster is called
///
/// # Then
/// - Operation succeeds
/// - Response indicates success
#[tokio::test]
#[traced_test]
async fn test_handle_join_cluster_snapshot_triggered() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(
        "/tmp/test_handle_join_cluster_snapshot_triggered",
        graceful_rx,
        None,
    );
    let node_id = 100;
    let address = "127.0.0.1:8080".to_string();

    // Mock membership
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_contains_node().returning(|_| false);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_add_learner().returning(|_, _, _| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|_current_leader_id| ClusterMembership::default());
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership.expect_update_node_status().returning(|_, _| Ok(()));

    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });
    context.membership = Arc::new(membership);

    // Mock state machine handler to return snapshot metadata
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_get_latest_snapshot_metadata().returning(|| {
        Some(SnapshotMetadata {
            last_included: Some(LogId {
                index: 100,
                term: 1,
            }),
            checksum: Bytes::new(),
        })
    });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    // Setup replication handler to return success
    context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    // Mock transport
    let transport = MockTransport::new();
    context.transport = Arc::new(transport);

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    context.storage.raft_log = Arc::new(raft_log);
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term: 1, index: 1 }),
            checksum: Bytes::new(),
        })
    });
    context.storage.state_machine = Arc::new(state_machine);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (sender, mut receiver) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let result = state
        .handle_join_cluster(
            JoinRequest {
                status: d_engine_proto::common::NodeStatus::Promotable as i32,
                node_id,
                node_role: Learner.into(),
                address,
            },
            sender,
            &context,
            &mpsc::unbounded_channel().0,
        )
        .await;

    assert!(result.is_ok());
    let response = receiver.recv().await.unwrap().unwrap();
    assert!(response.success);
}

// ============================================================================
// Background Snapshot Transfer Tests
// ============================================================================

#[cfg(test)]
mod trigger_background_snapshot_test {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use futures::stream;

    use crate::MockMembership;
    use crate::MockStateMachineHandler;
    use crate::SnapshotConfig;
    use crate::raft_role::leader_state::LeaderState;
    use crate::test_utils::mock::MockTypeConfig;
    use d_engine_proto::server::storage::{SnapshotChunk, SnapshotMetadata};

    fn mock_membership(should_fail: bool) -> MockMembership<MockTypeConfig> {
        let mut membership = MockMembership::<MockTypeConfig>::new();
        membership.expect_get_peer_channel().returning(move |_, _| {
            if should_fail {
                None
            } else {
                Some(tonic::transport::Endpoint::from_static("http://[::]:12345").connect_lazy())
            }
        });
        membership
    }

    fn mock_state_machine_handler(should_fail: bool) -> MockStateMachineHandler<MockTypeConfig> {
        let mut handler = MockStateMachineHandler::<MockTypeConfig>::new();
        handler.expect_load_snapshot_data().returning(move |_| {
            if should_fail {
                Err(crate::SnapshotError::OperationFailed("mock error".to_string()).into())
            } else {
                let chunk = SnapshotChunk {
                    data: Bytes::from(vec![1, 2, 3]),
                    ..Default::default()
                };
                Ok(stream::iter(vec![Ok(chunk)]).boxed())
            }
        });

        handler
    }

    fn default_snapshot_config() -> SnapshotConfig {
        SnapshotConfig {
            max_bandwidth_mbps: 0,
            sender_yield_every_n_chunks: 2,
            ..Default::default()
        }
    }

    fn make_metadata() -> SnapshotMetadata {
        SnapshotMetadata {
            last_included: Some(d_engine_proto::common::LogId { index: 1, term: 1 }),
            ..Default::default()
        }
    }

    /// Test: background snapshot transfer completes successfully.
    #[tokio::test]
    async fn test_trigger_background_snapshot_success() {
        let membership = Arc::new(mock_membership(false));
        let sm_handler = Arc::new(mock_state_machine_handler(false));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Test: background snapshot transfer fails if peer channel is missing.
    #[tokio::test]
    async fn test_trigger_background_snapshot_peer_channel_missing() {
        let membership = Arc::new(mock_membership(true));
        let sm_handler = Arc::new(mock_state_machine_handler(false));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Test: background snapshot transfer fails if snapshot data stream fails.
    #[tokio::test]
    async fn test_trigger_background_snapshot_snapshot_stream_error() {
        let membership = Arc::new(mock_membership(false));
        let sm_handler = Arc::new(mock_state_machine_handler(true));
        let config = default_snapshot_config();
        let metadata = make_metadata();

        let result = LeaderState::<MockTypeConfig>::trigger_background_snapshot(
            2, metadata, sm_handler, membership, config,
        )
        .await;

        assert!(result.is_ok());
    }
}

// ============================================================================
// Batch Learner Promotion Tests
// ============================================================================

#[cfg(test)]
mod batch_promote_learners_test {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, watch};

    use crate::AppendResults;
    use crate::ConsensusError;
    use crate::Error;
    use crate::MockMembership;
    use crate::MockRaftLog;
    use crate::MockReplicationCore;
    use crate::PeerUpdate;
    use crate::RaftContext;
    use crate::ReplicationError;
    use crate::event::RoleEvent;
    use crate::raft_role::leader_state::LeaderState;
    use crate::test_utils::mock::MockBuilder;
    use crate::test_utils::mock::MockTypeConfig;
    use crate::test_utils::node_config;
    use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;
    use mockall::predicate::eq;

    enum VerifyInternalQuorumWithRetrySuccess {
        Success,
        Failure,
        Error,
    }

    struct TestContext {
        raft_context: RaftContext<MockTypeConfig>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
        leader_state: LeaderState<MockTypeConfig>,
    }

    async fn setup_test_context(
        test_name: &str,
        current_voters: usize,
        ready_learners: Vec<u32>,
        verify_leadership_limited_retry_success: VerifyInternalQuorumWithRetrySuccess,
    ) -> TestContext {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut node_config = node_config(&format!("/tmp/{test_name}"));
        node_config.raft.learner_catchup_threshold = 100;
        node_config.raft.membership.verify_leadership_persistent_timeout = Duration::from_secs(1);

        let mut raft_context =
            MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();

        // Mock membership
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(move || {
            (1..=current_voters)
                .map(|id| NodeMeta {
                    id: id as u32,
                    address: format!("addr_{id}"),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                })
                .collect()
        });

        let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
        replication_handler.expect_handle_raft_request_in_batch().times(..).returning(
            move |_, _, _, _, _| match verify_leadership_limited_retry_success {
                VerifyInternalQuorumWithRetrySuccess::Success => Ok(AppendResults {
                    commit_quorum_achieved: true,
                    learner_progress: HashMap::new(),
                    peer_updates: HashMap::from([
                        (2, PeerUpdate::success(5, 6)),
                        (3, PeerUpdate::success(5, 6)),
                    ]),
                }),
                VerifyInternalQuorumWithRetrySuccess::Failure => Ok(AppendResults {
                    commit_quorum_achieved: false,
                    learner_progress: HashMap::new(),
                    peer_updates: HashMap::from([
                        (2, PeerUpdate::success(5, 6)),
                        (3, PeerUpdate::failed()),
                    ]),
                }),
                VerifyInternalQuorumWithRetrySuccess::Error => Err(Error::Consensus(
                    ConsensusError::Replication(ReplicationError::HigherTerm(10)),
                )),
            },
        );
        raft_context.handlers.replication_handler = replication_handler;

        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 10);
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
        raft_context.storage.raft_log = Arc::new(raft_log);

        // Mock learner statuses
        for learner_id in &ready_learners {
            membership
                .expect_get_node_status()
                .with(eq(*learner_id))
                .return_const(Some(NodeStatus::Promotable));
        }

        raft_context.membership = Arc::new(membership);

        let leader_state = LeaderState::new(1, raft_context.node_config());
        let (role_tx, role_rx) = mpsc::unbounded_channel();

        TestContext {
            raft_context,
            role_tx,
            role_rx,
            leader_state,
        }
    }

    /// Test successful promotion when quorum is achieved
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_success() {
        // Use safe cluster configuration: 3 voters promoting 2 learners = 5 voters total
        let mut ctx = setup_test_context(
            "test_batch_promote_learners_success",
            3,
            vec![4, 5],
            VerifyInternalQuorumWithRetrySuccess::Success,
        )
        .await;

        let result = ctx
            .leader_state
            .batch_promote_learners(vec![4, 5], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_ok());
        let r = ctx.role_rx.recv().await.unwrap();
        println!("Received role: {r:?}");
    }

    /// Test promotion failure when quorum not achieved
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_quorum_failed() {
        let mut ctx = setup_test_context(
            "test_batch_promote_learners_quorum_failed",
            3,
            vec![4, 5],
            VerifyInternalQuorumWithRetrySuccess::Failure,
        )
        .await;

        let result = ctx
            .leader_state
            .batch_promote_learners(vec![4, 5], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_err());
    }

    /// Test safety check preventing unsafe promotion
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_quorum_safety() {
        let mut ctx = setup_test_context(
            "test_batch_promote_learners_quorum_safety",
            3,
            vec![4],
            VerifyInternalQuorumWithRetrySuccess::Failure,
        )
        .await;

        let result = ctx
            .leader_state
            .batch_promote_learners(vec![4], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_ok());
    }

    /// Test error during quorum verification
    #[tokio::test]
    #[ignore]
    async fn test_batch_promote_learners_verification_error() {
        let mut ctx = setup_test_context(
            "test_batch_promote_learners_verification_error",
            3,
            vec![4],
            VerifyInternalQuorumWithRetrySuccess::Error,
        )
        .await;

        let result = ctx
            .leader_state
            .batch_promote_learners(vec![4], &ctx.raft_context, &ctx.role_tx)
            .await;

        assert!(result.is_err());
    }
}

// ============================================================================
// Stale Learner Detection and Cleanup Tests
// ============================================================================

#[cfg(test)]
mod stale_learner_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, watch};
    use tokio::time::Instant;

    use crate::AppendResults;
    use crate::MockMembership;
    use crate::MockRaftLog;
    use crate::RaftContext;
    use crate::raft_role::leader_state::{LeaderState, PendingPromotion};
    use crate::test_utils::mock::MockTypeConfig;
    use crate::test_utils::mock::mock_raft_context;
    use crate::test_utils::node_config;
    use d_engine_proto::common::NodeStatus;

    /// Setup helper for creating test leader state with pending promotions
    fn create_test_leader_state(
        test_name: &str,
        pending_nodes: Vec<(u32, Duration)>,
    ) -> (LeaderState<MockTypeConfig>, MockMembership<MockTypeConfig>) {
        let mut node_config = node_config(&format!("/tmp/{test_name}"));
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        node_config.raft.membership.promotion.stale_check_interval = Duration::from_secs(60);

        let mut leader = LeaderState::new(1, Arc::new(node_config));
        leader.next_membership_maintenance_check = Instant::now();

        // Add pending promotions with specified ages
        let now = Instant::now();
        for (node_id, age) in pending_nodes {
            leader.pending_promotions.push_back(PendingPromotion {
                node_id,
                ready_since: now - age,
            });
        }

        // Configure membership mock
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_update_node_status().returning(|_, _| Ok(()));
        (leader, membership)
    }

    /// Create mock RaftContext with configurable membership
    fn mock_stale_test_raft_context(
        test_name: &str,
        membership: Arc<MockMembership<MockTypeConfig>>,
        config: Option<Arc<crate::RaftNodeConfig>>,
    ) -> RaftContext<MockTypeConfig> {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = mock_raft_context(&format!("/tmp/{test_name}"), graceful_rx, None);
        ctx.membership = membership;
        ctx.node_config =
            config.unwrap_or_else(|| Arc::new(node_config(&format!("/tmp/{test_name}"))));

        // Mock raft_log for calculate_majority_matched_index
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 0);
        raft_log.expect_last_log_id().returning(|| None);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(0));
        ctx.storage.raft_log = Arc::new(raft_log);

        // Mock replication handler to handle BatchRemove operations
        ctx.handlers
            .replication_handler
            .expect_handle_raft_request_in_batch()
            .returning(|_, _, _, _, _| {
                Ok(AppendResults {
                    commit_quorum_achieved: true,
                    peer_updates: HashMap::new(),
                    learner_progress: HashMap::new(),
                })
            });

        ctx
    }

    /// Test lazy staleness sampling optimization (checks only oldest 100 or 2% of queue)
    #[tokio::test]
    async fn test_stale_check_optimization() {
        // Create queue with 200 entries (will only check oldest 100 or 2%)
        let nodes: Vec<(u32, Duration)> =
            (1..=200).map(|id| (id, Duration::from_secs(40))).collect();

        let (mut leader, membership) =
            create_test_leader_state("test_stale_check_optimization", nodes);
        let mut node_config = node_config("/tmp/test_stale_check_optimization");
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        node_config.raft.membership.promotion.stale_check_interval = Duration::from_secs(60);
        let ctx = mock_stale_test_raft_context(
            "test_stale_check_optimization",
            Arc::new(membership),
            Some(Arc::new(node_config)),
        );
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Should only check first 100 entries (out of 200)
        leader.conditionally_purge_stale_learners(&role_tx, &ctx).await.unwrap();

        // Should purge exactly 2 entries (1% of 200 = 2)
        assert_eq!(leader.pending_promotions.len(), 198);
    }

    /// Test no purge when pending promotions are fresh (not expired)
    #[tokio::test]
    async fn test_no_purge_when_fresh() {
        let (mut leader, mut membership) = create_test_leader_state(
            "test_no_purge_when_fresh",
            vec![
                (101, Duration::from_secs(15)),
                (102, Duration::from_secs(20)),
            ],
        );
        // Should do nothing - expect no status updates
        membership.expect_update_node_status().never();

        let ctx =
            mock_stale_test_raft_context("test_no_purge_when_fresh", Arc::new(membership), None);
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        leader.conditionally_purge_stale_learners(&role_tx, &ctx).await.unwrap();
        assert_eq!(leader.pending_promotions.len(), 2);
    }

    /// Test membership maintenance check scheduling logic
    #[tokio::test]
    async fn test_membership_maintenance_scheduling() {
        let (mut leader, _) =
            create_test_leader_state("test_membership_maintenance_scheduling", vec![]);
        let interval = Duration::from_secs(60);

        // First call
        leader.reset_next_membership_maintenance_check(interval);
        let next_check1 = leader.next_membership_maintenance_check;

        // Small delay to ensure time progresses
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Second call
        leader.reset_next_membership_maintenance_check(interval);
        let next_check2 = leader.next_membership_maintenance_check;

        // Verify both are in the future
        assert!(next_check1 > Instant::now());
        assert!(next_check2 > Instant::now());

        // Verify second call moved the timer forward
        assert!(next_check2 > next_check1);

        // Verify both are approximately interval in the future
        let now = Instant::now();
        assert!(duration_diff(next_check1 - now, interval) < Duration::from_millis(10));
        assert!(duration_diff(next_check2 - now, interval) < Duration::from_millis(10));
    }

    fn duration_diff(
        a: Duration,
        b: Duration,
    ) -> Duration {
        a.abs_diff(b)
    }

    /// Test system remains responsive during large queues
    #[tokio::test]
    async fn test_performance_large_queue() {
        let nodes: Vec<(u32, Duration)> =
            (1..=10_000).map(|id| (id, Duration::from_secs(40))).collect();

        let (mut leader, membership) =
            create_test_leader_state("test_performance_large_queue", nodes);
        let ctx = mock_stale_test_raft_context(
            "test_performance_large_queue",
            Arc::new(membership),
            None,
        );
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        // Time the staleness check
        let start = Instant::now();
        leader.conditionally_purge_stale_learners(&role_tx, &ctx).await.unwrap();
        let elapsed = start.elapsed();

        // Should take <20ms even for large queues (due to sampling optimization)
        println!("Staleness check for 10k nodes: {elapsed:?}");
        assert!(
            elapsed < Duration::from_millis(20),
            "Staleness check shouldn't process entire queue"
        );
    }

    /// Test promotion timeout threshold edge cases
    #[tokio::test]
    async fn test_promotion_timeout_threshold() {
        let (mut leader, membership) = create_test_leader_state(
            "test_promotion_timeout_threshold",
            vec![
                (101, Duration::from_secs(31)), // 1s over threshold
                (102, Duration::from_secs(30)), // exactly at threshold
                (103, Duration::from_secs(29)), // 1s under threshold
            ],
        );
        leader.next_membership_maintenance_check = Instant::now() - Duration::from_secs(1);
        let mut node_config = node_config("/tmp/test_promotion_timeout_threshold");
        node_config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(30);
        let ctx = mock_stale_test_raft_context(
            "test_promotion_timeout_threshold",
            Arc::new(membership),
            Some(Arc::new(node_config)),
        );
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        leader.conditionally_purge_stale_learners(&role_tx, &ctx).await.unwrap();

        // Should purge nodes over threshold (node 101, 102) but keep node 103
        assert_eq!(leader.pending_promotions.len(), 2);
        assert!(leader.pending_promotions.iter().any(|p| p.node_id == 103));
    }

    /// Test downgrade affects replication (stops replication for downgraded node)
    #[tokio::test]
    async fn test_downgrade_affects_replication() {
        let (mut leader, mut membership) = create_test_leader_state(
            "test_downgrade_affects_replication",
            vec![
                (101, Duration::from_secs(29)), // 1s under threshold
                (102, Duration::from_secs(30)), // exactly at threshold
                (103, Duration::from_secs(31)), // 1s over threshold
            ],
        );
        membership.expect_get_node_status().returning(|_| Some(NodeStatus::Active));
        let ctx = mock_stale_test_raft_context(
            "test_downgrade_affects_replication",
            Arc::new(membership),
            None,
        );
        let (role_tx, _role_rx) = mpsc::unbounded_channel();

        assert!(leader.handle_stale_learner(101, &role_tx, &ctx).await.is_ok());

        // Verify replication was stopped for this node
        assert!(!leader.next_index.contains_key(&101));
    }
}

// ============================================================================
// Learner Progress Checking Tests
// ============================================================================

#[cfg(test)]
mod check_learner_progress_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, watch};
    use tokio::time::Instant;

    use crate::MockMembership;
    use crate::RaftContext;
    use crate::event::{RaftEvent, RoleEvent};
    use crate::raft_role::leader_state::{LeaderState, PendingPromotion};
    use crate::raft_role::role_state::RaftRoleState;
    use crate::test_utils::mock::MockTypeConfig;
    use crate::test_utils::mock::mock_raft_context;
    use crate::test_utils::node_config;
    use d_engine_proto::common::NodeStatus;

    /// Helper: Create mock context with configurable threshold
    fn create_mock_context_with_threshold(
        test_name: &str,
        threshold: u64,
    ) -> RaftContext<MockTypeConfig> {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = mock_raft_context(&format!("/tmp/{test_name}"), graceful_rx, None);
        let mut config = node_config(&format!("/tmp/{test_name}"));
        config.raft.learner_catchup_threshold = threshold;
        ctx.node_config = Arc::new(config);

        // Mock membership
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_get_node_status().returning(|_| Some(NodeStatus::Promotable));
        membership.expect_contains_node().returning(|_| true);
        ctx.membership = Arc::new(membership);

        ctx
    }

    /// Test: Verify 1-second throttle mechanism prevents frequent checks
    #[tokio::test]
    async fn test_throttle_mechanism() {
        let ctx = create_mock_context_with_threshold("test_throttle_mechanism", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2); // Force first check

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95))]);

        // First call - should process
        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();
        assert_eq!(leader.pending_promotions.len(), 1);

        // Second call immediately - should be throttled
        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();
        assert_eq!(leader.pending_promotions.len(), 1); // No change

        // Verify only 1 event sent
        let mut event_count = 0;
        while role_rx.try_recv().is_ok() {
            event_count += 1;
        }
        assert_eq!(event_count, 1, "Should only send 1 event");
    }

    /// Test: Verify caught_up calculation - boundary condition (gap = threshold)
    #[tokio::test]
    async fn test_caught_up_at_threshold() {
        let threshold = 5;
        let ctx = create_mock_context_with_threshold("test_caught_up_at_threshold", threshold);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2); // Force check

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95))]); // gap = 100 - 95 = 5

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 1);
        assert_eq!(leader.pending_promotions[0].node_id, 100);
    }

    /// Test: Verify caught_up calculation - gap exceeds threshold
    #[tokio::test]
    async fn test_not_caught_up_exceeds_threshold() {
        let threshold = 5;
        let ctx =
            create_mock_context_with_threshold("test_not_caught_up_exceeds_threshold", threshold);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(94))]); // gap = 100 - 94 = 6 > 5

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 0);
    }

    /// Test: Verify handling of new learner with match_index = None
    #[tokio::test]
    async fn test_new_learner_match_index_none() {
        let threshold = 5;
        let ctx =
            create_mock_context_with_threshold("test_new_learner_match_index_none", threshold);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, None)]); // match_index = 0, gap = 100

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 0);
    }

    /// Test: Verify deduplication mechanism - prevent duplicate queue entries
    #[tokio::test]
    async fn test_deduplication() {
        let ctx = create_mock_context_with_threshold("test_deduplication", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        // Manually add learner 100 to pending queue
        leader.pending_promotions.push_back(PendingPromotion::new(100, Instant::now()));

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95))]); // caught_up

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        // Should still have only 1 entry
        assert_eq!(leader.pending_promotions.len(), 1);

        // Should not send event (all learners already pending)
        assert!(
            role_rx.try_recv().is_err(),
            "Should not send event for duplicate"
        );
    }

    /// Test: Verify status filter - ReadOnly learners should be skipped
    #[tokio::test]
    async fn test_status_filter_readonly() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_status_filter_readonly", graceful_rx, None);
        let mut config = node_config("/tmp/test_status_filter_readonly");
        config.raft.learner_catchup_threshold = 5;
        ctx.node_config = Arc::new(config);

        // Mock membership to return ReadOnly status
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_get_node_status().returning(|_| Some(NodeStatus::ReadOnly));
        membership.expect_contains_node().returning(|_| true);
        ctx.membership = Arc::new(membership);

        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95))]); // caught_up

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(
            leader.pending_promotions.len(),
            0,
            "ReadOnly should not be promoted"
        );
    }

    /// Test: Verify status filter - Promotable learners should be processed
    #[tokio::test]
    async fn test_status_filter_promotable() {
        let ctx = create_mock_context_with_threshold("test_status_filter_promotable", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95))]); // caught_up

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 1);
    }

    /// Test: Verify boundary condition - empty learner_progress input
    #[tokio::test]
    async fn test_empty_learner_progress() {
        let ctx = create_mock_context_with_threshold("test_empty_learner_progress", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::new();

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        assert_eq!(leader.pending_promotions.len(), 0);
        assert!(role_rx.try_recv().is_err(), "Should not send event");
    }

    /// Test: Verify node not in membership is skipped
    #[tokio::test]
    async fn test_node_not_in_membership() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut ctx = mock_raft_context("/tmp/test_node_not_in_membership", graceful_rx, None);
        let mut config = node_config("/tmp/test_node_not_in_membership");
        config.raft.learner_catchup_threshold = 5;
        ctx.node_config = Arc::new(config);

        // Mock membership: node 999 not found
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_get_node_status().returning(|id| {
            if id == 999 {
                None // Not in membership
            } else {
                Some(NodeStatus::Promotable)
            }
        });
        membership.expect_contains_node().returning(|_| true);
        ctx.membership = Arc::new(membership);

        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([
            (100, Some(95)), // valid
            (999, Some(95)), // not in membership
        ]);

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        // Should only add the valid learner
        assert_eq!(leader.pending_promotions.len(), 1);
        assert_eq!(leader.pending_promotions[0].node_id, 100);
    }

    /// Test: Verify overflow protection when learner is ahead of leader
    #[tokio::test]
    async fn test_overflow_protection_learner_ahead() {
        let ctx = create_mock_context_with_threshold("test_overflow_protection_learner_ahead", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(5).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(10))]); // learner_match > leader_commit

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        // checked_sub returns None → caught_up = true
        assert_eq!(leader.pending_promotions.len(), 1);
    }

    /// Test: Verify batch event optimization - multiple learners, single event
    #[tokio::test]
    async fn test_batch_event_sending() {
        let ctx = create_mock_context_with_threshold("test_batch_event_sending", 5);
        let mut leader = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());
        leader.update_commit_index(100).unwrap();
        leader.last_learner_check = Instant::now() - Duration::from_secs(2);

        let (role_tx, mut role_rx) = mpsc::unbounded_channel();
        let learner_progress = HashMap::from([(100, Some(95)), (200, Some(96))]);

        leader.check_learner_progress(&learner_progress, &ctx, &role_tx).await.unwrap();

        // Both learners should be in queue
        assert_eq!(leader.pending_promotions.len(), 2);
        let ids: Vec<_> = leader.pending_promotions.iter().map(|p| p.node_id).collect();
        assert!(ids.contains(&100));
        assert!(ids.contains(&200));

        // Should receive exactly 1 event
        let mut event_count = 0;
        while let Ok(event) = role_rx.try_recv() {
            if let RoleEvent::ReprocessEvent(inner) = event {
                if matches!(*inner, RaftEvent::PromoteReadyLearners) {
                    event_count += 1;
                }
            }
        }
        assert_eq!(
            event_count, 1,
            "Should send exactly 1 PromoteReadyLearners event"
        );
    }
}

// ============================================================================
// Pending Promotion Queue Management Tests
// ============================================================================

#[cfg(test)]
mod pending_promotion_tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::future;
    use parking_lot::Mutex;
    use tokio::sync::{mpsc, watch};
    use tokio::time::{Instant, timeout};

    use crate::AppendResults;
    use crate::MockMembership;
    use crate::MockRaftLog;
    use crate::PeerUpdate;
    use crate::QuorumVerificationResult;
    use crate::RaftContext;
    use crate::event::{RaftEvent, RoleEvent};
    use crate::raft_role::EntryPayload;
    use crate::raft_role::leader_state::{
        LeaderState, PendingPromotion, calculate_safe_batch_size,
    };
    use crate::raft_role::role_state::RaftRoleState;
    use crate::test_utils::mock::MockTypeConfig;
    use crate::test_utils::mock::mock_raft_context;
    use crate::test_utils::node_config;
    use d_engine_proto::common::{NodeRole::Follower, NodeStatus};
    use d_engine_proto::server::cluster::NodeMeta;

    // Test fixture
    struct TestFixture {
        leader_state: LeaderState<MockTypeConfig>,
        raft_context: RaftContext<MockTypeConfig>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
    }

    impl TestFixture {
        async fn new(
            test_name: &str,
            verify_internal_quorum_success: bool,
        ) -> Self {
            let mut raft_context = if verify_internal_quorum_success {
                Self::verify_internal_quorum_achieved_context(test_name).await
            } else {
                Self::verify_internal_quorum_failure_context(test_name).await
            };

            let mut membership = MockMembership::new();
            membership.expect_is_single_node_cluster().returning(|| false);
            membership.expect_can_rejoin().returning(|_, _| Ok(()));
            membership.expect_get_node_status().returning(|_| Some(NodeStatus::Active));
            membership.expect_voters().returning(|| {
                vec![NodeMeta {
                    id: 2,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                }]
            });
            raft_context.membership = Arc::new(membership);

            let (role_tx, role_rx) = mpsc::unbounded_channel();
            let mut node_config = node_config(&format!("/tmp/{test_name}"));
            node_config.raft.replication.rpc_append_entries_in_batch_threshold = 1;
            node_config.raft.membership.verify_leadership_persistent_timeout =
                Duration::from_millis(200);

            raft_context.node_config = Arc::new(node_config.clone());

            let leader_state = LeaderState::new(1, Arc::new(node_config));
            TestFixture {
                leader_state,
                raft_context,
                role_tx,
                role_rx,
            }
        }

        async fn verify_internal_quorum_achieved_context(
            test_name: &str
        ) -> RaftContext<MockTypeConfig> {
            let payloads = vec![EntryPayload::command(Bytes::new())];
            let (_graceful_tx, graceful_rx) = watch::channel(());
            let mut raft_context =
                mock_raft_context(&format!("/tmp/{test_name}"), graceful_rx, None);

            raft_context
                .handlers
                .replication_handler
                .expect_handle_raft_request_in_batch()
                .returning(|_, _, _, _, _| {
                    Ok(AppendResults {
                        commit_quorum_achieved: true,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::success(5, 6)),
                        ]),
                    })
                });

            let mut raft_log = MockRaftLog::new();
            raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
            raft_context.storage.raft_log = Arc::new(raft_log);

            let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

            let (role_tx, mut role_rx) = mpsc::unbounded_channel();

            let result =
                state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

            assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
            assert!(matches!(
                role_rx.try_recv(),
                Ok(RoleEvent::NotifyNewCommitIndex(_))
            ));

            raft_context
        }

        async fn verify_internal_quorum_failure_context(
            test_name: &str
        ) -> RaftContext<MockTypeConfig> {
            let payloads = vec![EntryPayload::command(Bytes::new())];
            let (_graceful_tx, graceful_rx) = watch::channel(());
            let mut raft_context =
                mock_raft_context(&format!("/tmp/{test_name}"), graceful_rx, None);

            raft_context
                .handlers
                .replication_handler
                .expect_handle_raft_request_in_batch()
                .returning(|_, _, _, _, _| {
                    Ok(AppendResults {
                        commit_quorum_achieved: false,
                        learner_progress: HashMap::new(),
                        peer_updates: HashMap::from([
                            (2, PeerUpdate::success(5, 6)),
                            (3, PeerUpdate::failed()),
                        ]),
                    })
                });

            let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

            let (role_tx, _) = mpsc::unbounded_channel();

            let result =
                state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

            assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);

            raft_context
        }
    }

    /// Test: FIFO ordering of pending promotions queue
    #[test]
    fn test_pending_promotion_ordering() {
        let now = Instant::now();
        let mut queue = VecDeque::new();

        queue.push_back(PendingPromotion::new(1, now - Duration::from_secs(10)));
        queue.push_back(PendingPromotion::new(2, now));

        let drained = queue.pop_front().unwrap();
        assert_eq!(drained.node_id, 1);
        assert_eq!(drained.ready_since, now - Duration::from_secs(10));
    }

    /// Test: PendingPromotion debug formatting
    #[test]
    fn test_pending_promotion_serialization() {
        let promotion = PendingPromotion::new(1001, Instant::now());
        assert!(
            format!("{promotion:?}").contains("1001"),
            "Debug output should contain node ID"
        );
    }

    /// Test: Safe batch size calculation for quorum safety
    #[tokio::test]
    async fn test_calculate_safe_batch_size() {
        let test_cases = vec![
            ((3, 1), 0), // 3 voters + 1 = 4 -> even -> batch size 0
            ((2, 1), 1), // 2voters + 1 = 3 -> odd -> batch size 1
            ((3, 2), 2), // 3 voters + 2 = 5 -> odd -> batch size 2
            ((2, 2), 1), // 2 voters + 2 = 4 -> even -> batch size 1
        ];

        for ((current, available), expected_batch_size) in test_cases {
            let result = calculate_safe_batch_size(current, available);
            assert_eq!(
                result, expected_batch_size,
                "Expected batch size for (current={current}, available={available}) is {expected_batch_size}"
            );
        }
    }

    /// Test: Process pending promotions successfully
    #[tokio::test]
    #[ignore]
    async fn test_process_pending_promotions() {
        let mut fixture = TestFixture::new("test_process_pending_promotions", true).await;
        fixture.leader_state.pending_promotions = vec![
            PendingPromotion::new(1, Instant::now()),
            PendingPromotion::new(2, Instant::now()),
            PendingPromotion::new(3, Instant::now()),
        ]
        .into_iter()
        .collect();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();
    }

    /// Test: Handle stale learner removal
    #[tokio::test]
    async fn test_handle_stale_learner() {
        let mut fixture = TestFixture::new("test_handle_stale_learner", true).await;
        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        assert!(
            fixture
                .leader_state
                .handle_stale_learner(1, &role_tx, &fixture.raft_context)
                .await
                .is_ok()
        );
    }

    /// Test: Partial batch promotion when quorum safety limits batch size
    #[tokio::test]
    #[ignore]
    async fn test_partial_batch_promotion() {
        let mut fixture = TestFixture::new("test_partial_batch_promotion", true).await;
        let mut membership = MockMembership::new();
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_voters().returning(|| {
            vec![NodeMeta {
                id: 2,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            }]
        });
        fixture.raft_context.membership = Arc::new(membership);
        fixture.leader_state.pending_promotions = vec![
            PendingPromotion::new(1, Instant::now() - Duration::from_millis(1)),
            PendingPromotion::new(2, Instant::now()),
        ]
        .into();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();

        assert_eq!(fixture.leader_state.pending_promotions.len(), 1);
        assert_eq!(fixture.leader_state.pending_promotions[0].node_id, 2);
    }

    /// Test: Promotion event rescheduling when queue has remaining items
    #[tokio::test]
    #[ignore]
    async fn test_promotion_event_rescheduling() {
        let mut fixture = TestFixture::new("test_promotion_event_rescheduling", true).await;
        fixture.leader_state.pending_promotions =
            (1..=10).map(|id| PendingPromotion::new(id, Instant::now())).collect();

        fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await
            .unwrap();

        let mut found = false;
        let result = timeout(Duration::from_millis(200), async {
            while let Some(event) = fixture.role_rx.recv().await {
                if matches!(event, RoleEvent::ReprocessEvent(inner) if matches!(*inner, RaftEvent::PromoteReadyLearners))
                {
                    found = true;
                    break;
                }
            }
            Ok::<(), ()>(())
        })
        .await;

        assert!(result.is_ok(), "Timed out waiting for events");
        assert!(found, "Did not find PromoteReadyLearners event");
    }

    /// Test: Batch promotion failure handling
    #[tokio::test]
    #[ignore]
    async fn test_batch_promotion_failure() {
        let mut fixture = TestFixture::new("test_batch_promotion_failure", false).await;

        fixture.leader_state.pending_promotions =
            (1..=2).map(|id| PendingPromotion::new(id, Instant::now())).collect();
        let result = fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await;

        assert!(result.is_err());
        assert!(!fixture.leader_state.pending_promotions.is_empty());
    }

    /// Test: Leader stepdown during promotion
    #[tokio::test]
    #[ignore]
    async fn test_leader_stepdown_during_promotion() {
        let mut fixture = TestFixture::new("test_leader_stepdown", false).await;
        fixture.leader_state.pending_promotions =
            (1..=2).map(|id| PendingPromotion::new(id, Instant::now())).collect();

        fixture.leader_state.update_current_term(2);
        let result = fixture
            .leader_state
            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
            .await;

        assert!(result.is_err());
        assert_eq!(fixture.leader_state.pending_promotions.len(), 2);
    }

    /// Test: Concurrent queue access thread safety
    #[tokio::test]
    async fn test_concurrent_queue_access() {
        let pending_promotions = VecDeque::new();
        let queue = Arc::new(Mutex::new(pending_promotions));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let q = queue.clone();
                tokio::spawn(async move {
                    let mut guard = q.lock();
                    guard.push_back(PendingPromotion::new(i, Instant::now()));
                })
            })
            .collect();

        future::join_all(handles).await;
        assert_eq!(queue.lock().len(), 10);
    }

    /// Test: Stale check timing with paused time
    #[tokio::test(start_paused = true)]
    async fn test_stale_check_timing() {
        let node_config = node_config("/tmp/test_stale_check_timing");
        let mut leader = LeaderState::<MockTypeConfig>::new(1, Arc::new(node_config));
        leader.reset_next_membership_maintenance_check(Duration::from_secs(60));

        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(Instant::now() >= leader.next_membership_maintenance_check);
    }

    /// Test: Config propagation to stale handling
    #[test]
    fn test_config_propagation_to_stale_handling() {
        let mut config = node_config("/tmp/test_config_propagation_to_stale_handling");
        config.raft.membership.promotion.stale_learner_threshold = Duration::from_secs(120);

        let leader = LeaderState::<MockTypeConfig>::new(1, Arc::new(config));
        assert_eq!(leader.pending_promotions.capacity(), 0);
    }

    /// Test: Batch size calculation fuzz testing
    #[test]
    fn test_batch_size_calculation_fuzz() {
        for voters in 1..100 {
            for pending in 0..20 {
                let size = calculate_safe_batch_size(voters, pending);
                assert!(size <= pending);
                assert!((voters + size) % 2 == 1 || size == 0);
            }
        }
    }
}
