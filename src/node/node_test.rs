use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::watch;
use tracing_test::traced_test;

use crate::{
    learner_state::LearnerState,
    proto::{
        cluster::{JoinResponse, NodeMeta},
        common::NodeStatus,
    },
    test_utils::{mock_membership, mock_node, MockBuilder, MockTypeConfig},
    AppendResults, Error, MockMembership, MockRaftLog, MockReplicationCore, MockTransport, PeerUpdate, RaftNodeConfig,
    RaftRole, FOLLOWER, LEARNER,
};

#[tokio::test]
async fn test_readiness_state_transition() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_readiness_state_transition", shutdown_rx, None);
    assert!(!node.server_is_ready());

    node.set_ready(true);
    assert!(node.server_is_ready());
}

#[tokio::test]
async fn test_run_sequence_with_mock_peers() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_run_sequence_with_mock_peers", shutdown_rx, None);

    shutdown_tx.send(()).expect("Expect send shutdown successfully");
    let result = node.run().await;
    assert!(result.is_ok());
}

fn prepare_succeed_majority_confirmation() -> (MockRaftLog, MockReplicationCore<MockTypeConfig>) {
    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    let mut raft_log = MockRaftLog::new();

    //Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .returning(move |_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
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
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    raft_log
        .expect_calculate_majority_matched_index()
        .returning(|_, _, _| Some(5));
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_flush().return_once(|| Ok(()));

    (raft_log, replication_handler)
}

#[tokio::test(start_paused = true)]
#[traced_test] // Enable log capturing
async fn run_success_without_joining() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create mock membership with expectations
    let membership = mock_membership();
    let (raft_log, replication_handler) = prepare_succeed_majority_confirmation();
    // Build node using MockBuilder
    let node = {
        let builder = MockBuilder::new(shutdown_rx.clone())
            .with_membership(membership) // Inject our mock membership
            .with_raft_log(raft_log)
            .with_replication_handler(replication_handler)
            .wiht_node_config({
                let mut config = RaftNodeConfig::new().expect("Default config");
                // Mark as non-joining node
                config.cluster.initial_cluster = vec![
                    NodeMeta {
                        id: 100,
                        address: "127.0.0.1:8080".to_string(),
                        role: FOLLOWER,
                        status: NodeStatus::Active as i32,
                    },
                    NodeMeta {
                        id: 200,
                        address: "127.0.0.1:8081".to_string(),
                        role: FOLLOWER,
                        status: NodeStatus::Active as i32,
                    },
                ];
                config
            });

        builder.build_node()
    };

    let node = Arc::new(node);
    let node_clone = node.clone();

    // Create shutdown trigger (for terminating raft.run loop)
    let shutdown_tx = _shutdown_tx.clone();

    // Spawn node execution in background
    let handle = tokio::spawn(async move {
        node_clone.run().await.expect("Node run should succeed");
    });

    tokio::time::advance(Duration::from_millis(2)).await;
    // Give some time for execution to start
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Trigger graceful shutdown
    shutdown_tx.send(()).expect("Should send shutdown");

    // Verify node state
    assert!(node.server_is_ready(), "Node should be marked ready");

    // Wait for completion
    handle.await.expect("Node task should complete");

    // Verify logs show correct behavior
    assert!(
        logs_contain("Set node is ready to run Raft protocol"),
        "Readiness should be set"
    );
    assert!(!logs_contain("Node is joining"), "Join cluster should NOT be executed");
    assert!(logs_contain("Node is running"), "Node should be running");
}

#[tokio::test(start_paused = true)]
#[traced_test] // Enable log capturing
async fn run_success_with_joining() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create mock membership with expectations
    let mut membership = mock_membership();
    membership.expect_current_leader_id().returning(|| Some(1));
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    let mut transport = MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    transport
        .expect_request_snapshot_from_leader()
        .returning(|_, _, _, _| Err(Error::Fatal("()".to_string())));
    let (raft_log, replication_handler) = prepare_succeed_majority_confirmation();
    // Build node using MockBuilder
    let node_id = 100;
    let config = {
        let mut config = RaftNodeConfig::new().expect("Default config");
        // Mark as non-joining node
        config.cluster.node_id = node_id;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: node_id,
                address: "127.0.0.1:8080".to_string(),
                role: LEARNER,
                status: NodeStatus::Joining as i32,
            },
            NodeMeta {
                id: 200,
                address: "127.0.0.1:8081".to_string(),
                role: FOLLOWER,
                status: NodeStatus::Active as i32,
            },
        ];
        config
    };
    let config_clone = config.clone();
    let node = {
        let builder = MockBuilder::new(shutdown_rx.clone())
            .id(node_id)
            .with_role(RaftRole::Learner(Box::new(LearnerState::new(
                node_id,
                Arc::new(config),
            ))))
            .with_transport(transport)
            .with_membership(membership)
            .with_raft_log(raft_log)
            .with_replication_handler(replication_handler)
            .wiht_node_config(config_clone);

        builder.build_node()
    };

    let node = Arc::new(node);
    let node_clone = node.clone();

    // Create shutdown trigger (for terminating raft.run loop)
    let shutdown_tx = _shutdown_tx.clone();

    // Spawn node execution in background
    let handle = tokio::spawn(async move {
        node_clone.run().await.expect("Node run should succeed");
    });

    tokio::time::advance(Duration::from_millis(2)).await;
    // Give some time for execution to start
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Trigger graceful shutdown
    shutdown_tx.send(()).expect("Should send shutdown");

    // Wait for completion
    handle.await.expect("Node task should complete");

    // Verify node state
    assert!(node.server_is_ready(), "Node should be marked ready");

    // Verify logs show correct behavior
    assert!(
        logs_contain("Set node is ready to run Raft protocol"),
        "Readiness should be set"
    );
    assert!(logs_contain("Node is joining"), "Join cluster should NOT be executed");
    assert!(logs_contain("Node is running"), "Node should be running");
}

#[tokio::test(start_paused = true)]
#[traced_test] // Enable log capturing
async fn run_fails_on_health_check() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create mock membership with expectations
    let mut membership = MockMembership::new();
    membership
        .expect_check_cluster_is_ready()
        .times(1)
        .returning(|| Err(Error::Fatal("Cluster not ready".to_string())));
    membership.expect_pre_warm_connections().times(0).returning(|| Ok(()));
    // Build node using MockBuilder
    let node = {
        let builder = MockBuilder::new(shutdown_rx.clone())
            .with_membership(membership) // Inject our mock membership
            .wiht_node_config({
                let mut config = RaftNodeConfig::new().expect("Default config");
                // Mark as non-joining node
                config.cluster.initial_cluster = vec![
                    NodeMeta {
                        id: 100,
                        address: "127.0.0.1:8080".to_string(),
                        role: FOLLOWER,
                        status: NodeStatus::Active as i32,
                    },
                    NodeMeta {
                        id: 200,
                        address: "127.0.0.1:8081".to_string(),
                        role: FOLLOWER,
                        status: NodeStatus::Active as i32,
                    },
                ];
                config
            });

        builder.build_node()
    };

    let node = Arc::new(node);
    let node_clone = node.clone();

    // Create shutdown trigger (for terminating raft.run loop)
    let shutdown_tx = _shutdown_tx.clone();

    // Spawn node execution in background
    let handle = tokio::spawn(async move {
        node_clone.run().await.expect("Node run should succeed");
    });

    tokio::time::advance(Duration::from_millis(2)).await;
    // Give some time for execution to start
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Trigger graceful shutdown
    shutdown_tx.send(()).expect("Should send shutdown");

    // Wait for completion
    assert!(handle.await.is_err());

    // Verify node state
    assert!(!node.server_is_ready(), "Node should not be marked ready");

    // Verify logs show correct behavior
    assert!(
        !logs_contain("Set node is ready to run Raft protocol"),
        "Readiness should be set"
    );
    assert!(!logs_contain("Node is joining"), "Join cluster should NOT be executed");
    assert!(!logs_contain("Node is running"), "Node should be running");
}
