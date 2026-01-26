//! Tests for leader state replication and quorum verification
//!
//! This module tests the `process_batch` and `verify_internal_quorum` methods
//! which handle log replication, quorum verification, and commit index calculation.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use nanoid::nanoid;
use tokio::sync::{mpsc, watch};
use tonic::Status;
use tracing_test::traced_test;

use crate::AppendResults;
use crate::ConsensusError;
use crate::Error;
use crate::MockMembership;
use crate::MockRaftLog;

use crate::PeerUpdate;
use crate::QuorumVerificationResult;
use crate::RaftRequestWithSignal;
use crate::ReplicationError;

use crate::event::RoleEvent;
use crate::maybe_clone_oneshot::RaftOneshot;
use crate::raft_role::leader_state::LeaderState;
use crate::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::{EntryPayload, NodeRole::Follower, NodeStatus};
use d_engine_proto::server::cluster::{ClusterMembership, NodeMeta};

// ============================================================================
// Test Helper Structures and Functions
// ============================================================================

struct ProcessRaftRequestTestContext {
    state: LeaderState<MockTypeConfig>,
    raft_context: crate::raft_context::RaftContext<MockTypeConfig>,
}

/// Create a mock membership for testing (default: multi-node cluster)
fn create_mock_membership() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership
}

/// Setup helper for process_batch tests with multi-node cluster configuration
async fn setup_process_batch_test_context(
    path: &str,
    graceful_rx: watch::Receiver<()>,
) -> ProcessRaftRequestTestContext {
    let mut context = mock_raft_context(path, graceful_rx, None);

    // Setup default membership mock with voters() and replication_peers() support for
    // init_cluster_metadata. For process_batch tests, we need multi-node cluster (2+ peers) to
    // avoid single_voter logic
    let mut membership = MockMembership::new();
    membership.expect_is_single_node_cluster().returning(|| false);
    membership.expect_voters().returning(|| {
        // Return 2 peer voters (node 2 and 3), so total = 2 + 1 (leader) = 3 (multi-node)
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: Follower.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new); // Empty replication peers by default
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(5).unwrap(); // Initial commit index

    // Initialize cluster metadata with default membership
    state.init_cluster_metadata(&context.membership).await.unwrap();

    ProcessRaftRequestTestContext {
        state,
        raft_context: context,
    }
}

type ClientResponseResult = std::result::Result<ClientResponse, Status>;

fn mock_request(
    sender: crate::MaybeCloneOneshotSender<ClientResponseResult>
) -> RaftRequestWithSignal {
    RaftRequestWithSignal {
        id: nanoid!(),
        payloads: vec![],
        sender,
        wait_for_apply_event: false,
    }
}

// ============================================================================
// Process Batch Tests
// ============================================================================

/// Test process_batch with quorum achieved
///
/// # Test Scenario
/// Leader sends batch to peers and achieves commit quorum.
/// All clients receive success responses.
///
/// # Given
/// - Leader with commit_index = 5
/// - Batch of 2 client requests
/// - Mock replication handler returns successful quorum
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Commit index advances to 6
/// - NotifyNewCommitIndex event is sent
/// - Both clients receive success responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_achieved() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_quorum_achieved", graceful_rx)
            .await;

    // Mock replication to return success with quorum
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                ]),
            })
        });
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    // Prepare batch of 2 requests
    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let receivers = vec![rx1, rx2];

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 6);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    // Verify client responses
    for mut rx in receivers {
        let response = rx.recv().await.unwrap().unwrap();
        assert!(response.is_write_success());
    }
}

/// Test process_batch with quorum NOT achieved (verifiable failure)
///
/// # Test Scenario
/// Majority of peers responded but quorum not achieved.
/// Clients receive RetryRequired responses.
///
/// # Given
/// - Leader with commit_index = 5
/// - Mock replication: peer 2 succeeds, peer 3 fails
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Commit index remains at 5 (not advanced)
/// - Clients receive RetryRequired responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_failed_verifiable() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_quorum_failed_verifiable",
        graceful_rx,
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: None,
                            next_index: 1,
                            success: false,
                        },
                    ), // Failed
                ]),
            })
        });

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, mut rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Should not advance

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_retry_required());
    let response = rx2.recv().await.unwrap().unwrap();
    assert!(response.is_retry_required());
}

/// Test process_batch with quorum NOT achieved (non-verifiable failure)
///
/// # Test Scenario
/// Less than majority of peers responded (timeouts).
/// Clients receive ProposeFailed responses.
///
/// # Given
/// - Leader with commit_index = 5
/// - Mock replication: only peer 2 responds (insufficient for quorum)
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Commit index remains at 5
/// - Clients receive ProposeFailed responses
#[tokio::test]
#[traced_test]
async fn test_process_batch_quorum_non_verifiable_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = setup_process_batch_test_context(
        "/tmp/test_process_batch_quorum_non_verifiable_failure",
        graceful_rx,
    )
    .await;

    let peer2_id = 2;
    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(move |_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::from([(
                    peer2_id,
                    PeerUpdate {
                        match_index: Some(5),
                        next_index: 6,
                        success: true,
                    },
                )]),
                learner_progress: HashMap::new(),
            })
        });

    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                role: Follower.into(),
                address: "127.0.0.1:0".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                role: Follower.into(),
                address: "127.0.0.1:0".to_string(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    context.raft_context.membership = Arc::new(membership);

    // Re-initialize cluster metadata after changing membership
    context
        .state
        .init_cluster_metadata(&context.raft_context.membership)
        .await
        .unwrap();

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, mut rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);

    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Should not advance

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
    let response = rx2.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

/// Test process_batch with higher term detected
///
/// # Test Scenario
/// Follower responds with higher term, triggering leader step-down.
///
/// # Given
/// - Leader with current_term
/// - Mock replication returns HigherTerm(10) error
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Leader steps down (BecomeFollower event sent)
/// - Leader's term is updated to 10
/// - Client receives TermOutdated response
#[tokio::test]
#[traced_test]
async fn test_process_batch_higher_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_higher_term", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10), // Higher term
            )))
        });

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();
    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_err());
    assert_eq!(context.state.current_term(), 10);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_term_outdated());
}

/// Test process_batch with partial timeouts
///
/// # Test Scenario
/// Some peers succeed, some time out (non-verifiable failure).
///
/// # Given
/// - Leader with commit_index = 5
/// - Mock replication: only peer 2 responds (peer 3 timeout)
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Commit index remains at 5
/// - Client receives ProposeFailed response
#[tokio::test]
#[traced_test]
async fn test_process_batch_partial_timeouts() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_partial_timeouts", graceful_rx)
            .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(
                    2,
                    PeerUpdate {
                        match_index: Some(6),
                        next_index: 7,
                        success: true,
                    },
                )]),
            })
        });

    // Prepare AppendResults
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "http://127.0.0.1:55002".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    context.raft_context.membership = Arc::new(membership);

    // Re-initialize cluster metadata after changing membership
    context
        .state
        .init_cluster_metadata(&context.raft_context.membership)
        .await
        .unwrap();

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Initial commit index

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

/// Test process_batch with all peers timeout
///
/// # Test Scenario
/// No successful responses from any peer.
///
/// # Given
/// - Leader with commit_index = 5
/// - Mock replication: empty peer_updates (all timeout)
///
/// # When
/// - process_batch is called
///
/// # Then
/// - Commit index remains at 5 (unchanged)
/// - Client receives failure response
#[tokio::test]
#[traced_test]
async fn test_process_batch_all_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_all_timeout", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([]),
            })
        });
    // Prepare AppendResults
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "http://127.0.0.1:55002".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    context.raft_context.membership = Arc::new(membership);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 5); // Unchanged

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

/// Test process_batch with fatal error during replication
///
/// # Test Scenario
/// Storage failure or unrecoverable error occurs during replication.
///
/// # Given
/// - Mock replication returns Fatal error
///
/// # When
/// - process_batch is called
///
/// # Then
/// - process_batch returns error
/// - Client receives failure response
#[tokio::test]
#[traced_test]
async fn test_process_batch_fatal_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context =
        setup_process_batch_test_context("/tmp/test_process_batch_fatal_error", graceful_rx).await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, mut rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let result = context
        .state
        .process_batch(batch, &mpsc::unbounded_channel().0, &context.raft_context)
        .await;

    assert!(result.is_err());

    let response = rx1.recv().await.unwrap().unwrap();
    assert!(response.is_propose_failure());
}

// ============================================================================
// Commit Index Calculation Tests
// ============================================================================

/// Setup helper for commit index tests with configurable cluster membership
async fn setup_commit_index_test_context(
    path: &str,
    single_voter: bool,
) -> ProcessRaftRequestTestContext {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut context = mock_raft_context(path, graceful_rx, None);

    // Mock membership based on cluster topology
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));

    // For multi-node tests, return peer voters (excluding self/leader)
    // For single-voter tests, return empty (only self is voter)
    if single_voter {
        membership.expect_voters().returning(Vec::new);
    } else {
        // Multi-node: return 2 peer voters (node 2 and 3), so total = 2 + 1 (leader) = 3
        membership.expect_voters().returning(|| {
            vec![
                NodeMeta {
                    id: 2,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
                NodeMeta {
                    id: 3,
                    address: "".to_string(),
                    status: NodeStatus::Active as i32,
                    role: Follower.into(),
                },
            ]
        });
    }

    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership.expect_members().returning(Vec::new);
    membership.expect_check_cluster_is_ready().returning(|| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        });
    membership.expect_get_zombie_candidates().returning(Vec::new);
    membership.expect_pre_warm_connections().returning(|| Ok(()));
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_initial_cluster_size().returning(|| 3);
    context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_commit_index(5).unwrap();

    // Initialize cluster metadata to match the test scenario
    state.init_cluster_metadata(&context.membership).await.unwrap();

    ProcessRaftRequestTestContext {
        state,
        raft_context: context,
    }
}

/// Test commit index calculation for single-node cluster
///
/// # Test Scenario
/// When cluster has only one node, commit_index should advance to last_log_index immediately.
/// This is correct because quorum of 1 = the single node itself.
///
/// # Given
/// - Single-node cluster
/// - Mock: last_entry_id() returns 7
/// - Mock: calculate_majority_matched_index() returns 8
///
/// # When
/// - process_batch is called with empty peer_updates
///
/// # Then
/// - Commit index advances to 7 (from last_entry_id, not 8)
/// - Client receives success response
#[tokio::test]
#[traced_test]
async fn test_single_node_cluster_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_single_node_cluster_commit_index",
        true, // single-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(), // Empty: no peers to replicate to
            })
        });

    let mut raft_log = MockRaftLog::new();
    // Different return values to detect which code path executes:
    // - Fixed code (is_single_node_cluster): calls last_entry_id() -> 7
    // - Buggy code (peer_updates.is_empty): calls calculate_majority_matched_index() -> 8
    raft_log.expect_last_entry_id().returning(|| 7);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(8));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(
        context.state.shared_state().commit_index,
        7,
        "Single-node: commit_index should equal last_log_index"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    let mut rx = rx1;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

/// Test commit index calculation for multi-node cluster with empty peer_updates (Bug #186)
///
/// # Test Scenario
/// This is the critical bug fix: Leader must not use single-node logic just because
/// peer_updates is empty. Empty peer_updates means no responses yet, not single-node cluster.
///
/// # Given
/// - 3-node cluster
/// - Leader has initialized next_index for peers
/// - Mock: last_entry_id() returns 9
/// - Mock: calculate_majority_matched_index() returns 6
///
/// # When
/// - process_batch is called with empty peer_updates
///
/// # Then
/// - Commit index advances to 6 (from calculate_majority_matched_index, not 9)
/// - Client receives success response
#[tokio::test]
#[traced_test]
async fn test_multi_node_cluster_empty_peer_updates_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_multi_node_empty_peer_updates_commit_index",
        false, // multi-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(), /* BUG #186: Empty peer_updates should NOT
                                               * trigger single-node logic */
            })
        });

    let mut raft_log = MockRaftLog::new();
    // Different return values to detect which code path executes:
    // - Buggy code (peer_updates.is_empty): calls last_entry_id() -> 9
    // - Fixed code (is_single_node_cluster): calls calculate_majority_matched_index() -> 6
    raft_log.expect_last_entry_id().returning(|| 9);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(
        context.state.shared_state().commit_index,
        6,
        "Multi-node: commit_index=6 (from calculate_majority_matched_index), not 9 (from last_entry_id)"
    );
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    let mut rx = rx1;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

/// Test commit index calculation for multi-node cluster with peer responses
///
/// # Test Scenario
/// Normal case: Leader receives responses from peers and calculates commit index
/// based on quorum (majority of nodes have replicated the log).
///
/// # Given
/// - 3-node cluster
/// - Mock: peer_updates with match_index=6 for both peers
/// - Mock: last_entry_id() returns 10
/// - Mock: calculate_majority_matched_index() returns 6
///
/// # When
/// - process_batch is called with peer_updates
///
/// # Then
/// - Commit index advances to 6 (from calculate_majority_matched_index)
/// - Clients receive success responses
#[tokio::test]
#[traced_test]
async fn test_multi_node_cluster_with_peer_updates_commit_index() {
    let mut context = setup_commit_index_test_context(
        "/tmp/test_multi_node_with_peer_updates_commit_index",
        false, // multi-node
    )
    .await;

    context
        .raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(6),
                            next_index: 7,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    let mut raft_log = MockRaftLog::new();
    // Different return values to detect which code path executes:
    // - Fixed code (is_single_node_cluster check fails): calls
    //   calculate_majority_matched_index() -> 6
    // - Buggy code (peer_updates.is_empty): calls last_entry_id() -> 10
    raft_log.expect_last_entry_id().returning(|| 10);
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(6));
    context.raft_context.storage.raft_log = Arc::new(raft_log);

    use crate::maybe_clone_oneshot::MaybeCloneOneshot;
    let (tx1, rx1) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let (tx2, rx2) = <MaybeCloneOneshot as RaftOneshot<_>>::new();
    let batch = VecDeque::from(vec![mock_request(tx1), mock_request(tx2)]);
    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = context.state.process_batch(batch, &role_tx, &context.raft_context).await;

    assert!(result.is_ok());
    assert_eq!(context.state.shared_state().commit_index, 6);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));

    let mut rx = rx1;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());

    let mut rx = rx2;
    let response = rx.recv().await.unwrap().unwrap();
    assert!(response.is_write_success());
}

// ============================================================================
// Verify Internal Quorum Tests
// ============================================================================

/// Test verify_internal_quorum with quorum achieved
///
/// # Test Scenario
/// All peers respond successfully, quorum is achieved.
///
/// # Given
/// - Mock replication returns successful quorum
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Ok(QuorumVerificationResult::Success)
/// - NotifyNewCommitIndex event is sent
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_success() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_success",
        graceful_rx,
        None,
    );

    // Setup replication handler to return success
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

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::Success);
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::NotifyNewCommitIndex(_))
    ));
}

/// Test verify_internal_quorum with verifiable failure
///
/// # Test Scenario
/// Majority of peers responded but quorum not achieved.
///
/// # Given
/// - Mock replication: peer 2 succeeds, peer 3 fails
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Ok(QuorumVerificationResult::RetryRequired)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_verifiable_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_verifiable_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return verifiable failure
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
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

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// Test verify_internal_quorum with non-verifiable failure
///
/// # Test Scenario
/// Less than majority of peers responded (leadership likely lost).
///
/// # Given
/// - 4-node cluster (leader + 3 peers)
/// - Mock replication: only peer 2 responds
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Ok(QuorumVerificationResult::LeadershipLost)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_non_verifiable_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_non_verifiable_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return non-verifiable failure
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::success(5, 6))]),
            })
        });

    // Prepare AppendResults
    let mut membership = create_mock_membership();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: 2,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 4,
                address: "".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    membership.expect_replication_peers().returning(Vec::new);
    raft_context.membership = Arc::new(membership);

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    // Initialize cluster metadata after setting up membership
    state.init_cluster_metadata(&raft_context.membership).await.unwrap();

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::LeadershipLost);
}

/// Test verify_internal_quorum with partial timeouts
///
/// # Test Scenario
/// Some peers respond, some time out (verifiable failure).
///
/// # Given
/// - Mock replication: peer 2 succeeds, peer 3 fails (timeout)
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Ok(QuorumVerificationResult::RetryRequired)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_partial_timeouts() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_partial_timeouts",
        graceful_rx,
        None,
    );

    // Setup replication handler to return partial timeouts
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (2, PeerUpdate::success(5, 6)),
                    (3, PeerUpdate::failed()), // Timeout
                ]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// Test verify_internal_quorum with all timeouts
///
/// # Test Scenario
/// No peers respond (all timeout).
///
/// # Given
/// - Mock replication: both peers fail (timeout)
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Ok(QuorumVerificationResult::RetryRequired)
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_all_timeouts() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_all_timeouts",
        graceful_rx,
        None,
    );

    // Setup replication handler to return all timeouts
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([(2, PeerUpdate::failed()), (3, PeerUpdate::failed())]),
            })
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());
    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert_eq!(result.unwrap(), QuorumVerificationResult::RetryRequired);
}

/// Test verify_internal_quorum with higher term detected
///
/// # Test Scenario
/// Follower responds with higher term, triggering step-down.
///
/// # Given
/// - Mock replication returns HigherTerm(10) error
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns Err(HigherTerm(10))
/// - BecomeFollower event is sent
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_higher_term() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_higher_term",
        graceful_rx,
        None,
    );

    // Setup replication handler to return higher term error
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Replication(
                ReplicationError::HigherTerm(10),
            )))
        });

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, mut role_rx) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(
            10
        )))
    ));
    assert!(matches!(
        role_rx.try_recv(),
        Ok(RoleEvent::BecomeFollower(_))
    ));
}

/// Test verify_internal_quorum with critical failure
///
/// # Test Scenario
/// System or logic error occurs during verification.
///
/// # Given
/// - Mock replication returns Fatal error
///
/// # When
/// - verify_internal_quorum is called
///
/// # Then
/// - Returns original error
#[tokio::test]
#[traced_test]
async fn test_verify_internal_quorum_critical_failure() {
    let payloads = vec![EntryPayload::command(Bytes::new())];
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft_context = mock_raft_context(
        "/tmp/test_verify_internal_quorum_critical_failure",
        graceful_rx,
        None,
    );

    // Setup replication handler to return critical error
    raft_context
        .handlers
        .replication_handler
        .expect_handle_raft_request_in_batch()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::Fatal("Storage failure".to_string())));

    let mut state = LeaderState::<MockTypeConfig>::new(1, raft_context.node_config());

    let (role_tx, _) = mpsc::unbounded_channel();

    let result = state.verify_internal_quorum(payloads, true, &raft_context, &role_tx).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::Fatal(msg) if msg == "Storage failure"
    ));
}
