use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::debug;
use tracing_test::traced_test;

use crate::test_utils::MockBuilder;
use crate::test_utils::mock_membership;
use crate::test_utils::mock_node;
use d_engine_core::AppendResults;
use d_engine_core::Error;
use d_engine_core::MockMembership;
use d_engine_core::MockRaftLog;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockTransport;
use d_engine_core::MockTypeConfig;
use d_engine_core::PeerUpdate;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RaftRole;
use d_engine_core::learner_state::LearnerState;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::NodeMeta;

#[tokio::test]
#[traced_test]
async fn test_readiness_state_transition() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_readiness_state_transition", shutdown_rx, None);
    assert!(!node.server_is_ready());

    node.set_ready(true);
    assert!(node.server_is_ready());
}

#[tokio::test]
#[traced_test]
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

    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_flush().return_once(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

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
            .with_node_config({
                let mut config = RaftNodeConfig::new().expect("Default config");
                // Mark as non-joining node
                config.cluster.initial_cluster = vec![
                    NodeMeta {
                        id: 100,
                        address: "127.0.0.1:8080".to_string(),
                        role: Follower as i32,
                        status: NodeStatus::Active as i32,
                    },
                    NodeMeta {
                        id: 200,
                        address: "127.0.0.1:8081".to_string(),
                        role: Follower as i32,
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
    assert!(
        !logs_contain("Node is joining"),
        "Join cluster should NOT be executed"
    );
}

#[tokio::test(start_paused = true)]
#[traced_test] // Enable log capturing
async fn run_success_with_joining() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    // Create mock membership with expectations
    let membership = mock_membership();

    let mut transport = MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 3,
                leader_address: "127.0.0.1:8080".to_string(),
                term: 1,
            },
        ])
    });
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
                role: Learner as i32,
                status: NodeStatus::Joining as i32,
            },
            NodeMeta {
                id: 200,
                address: "127.0.0.1:8081".to_string(),
                role: Follower as i32,
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
            .with_node_config(config_clone);

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
    assert!(
        logs_contain("Node is joining"),
        "Join cluster should NOT be executed"
    );
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
            .with_node_config({
                let mut config = RaftNodeConfig::new().expect("Default config");
                // Mark as non-joining node
                config.cluster.initial_cluster = vec![
                    NodeMeta {
                        id: 100,
                        address: "127.0.0.1:8080".to_string(),
                        role: Follower as i32,
                        status: NodeStatus::Active as i32,
                    },
                    NodeMeta {
                        id: 200,
                        address: "127.0.0.1:8081".to_string(),
                        role: Follower as i32,
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
    let handle = tokio::spawn(async move { node_clone.run().await });

    tokio::time::advance(Duration::from_millis(2)).await;
    // Give some time for execution to start
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Trigger graceful shutdown
    shutdown_tx.send(()).expect("Should send shutdown");

    // Wait for completion
    let result = handle.await.unwrap();
    debug!(?result, "Node execution result");
    assert!(result.is_err());

    // Verify node state
    assert!(!node.server_is_ready(), "Node should not be marked ready");

    // Verify logs show correct behavior
    assert!(
        !logs_contain("Set node is ready to run Raft protocol"),
        "Readiness should be set"
    );
    assert!(
        !logs_contain("Node is joining"),
        "Join cluster should NOT be executed"
    );
    assert!(!logs_contain("Node is running"), "Node should be running");
}

/// Unit tests for Node leader election notification
/// TODO: These tests need proper mock implementation
#[cfg(test)]
#[allow(dead_code)]
mod leader_elected_tests {
    /*
    use std::sync::Arc;

    use crate::LeaderInfo;
    use crate::node::Node;
    use d_engine_core::test_utils::mock_raft_context;

    #[tokio::test]
    async fn test_leader_elected_notifier_subscription() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Node::from_raft(raft);

        // Subscribe to leader election notifications
        let mut rx = node.leader_elected_notifier();

        // Initial state should be None (no leader yet)
        assert_eq!(*rx.borrow(), None);
    }

    #[tokio::test]
    async fn test_leader_elected_notifier_receives_updates() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Arc::new(Node::from_raft(raft));

        // Subscribe to leader election notifications
        let mut rx = node.leader_elected_notifier();

        // Simulate leader election by sending to the channel
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 5,
        };
        node.leader_elected_tx
            .send(Some(leader_info.clone()))
            .expect("Should send leader info");

        // Wait for update
        rx.changed().await.expect("Should receive change notification");

        // Verify received data
        let received = rx.borrow().clone();
        assert_eq!(received, Some(leader_info));
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Arc::new(Node::from_raft(raft));

        // Create multiple subscribers
        let mut rx1 = node.leader_elected_notifier();
        let mut rx2 = node.leader_elected_notifier();

        // Send leader info
        let leader_info = LeaderInfo {
            leader_id: 2,
            term: 10,
        };
        node.leader_elected_tx.send(Some(leader_info.clone())).expect("Should send");

        // Both should receive
        rx1.changed().await.expect("Subscriber 1 should receive");
        rx2.changed().await.expect("Subscriber 2 should receive");

        assert_eq!(*rx1.borrow(), Some(leader_info.clone()));
        assert_eq!(*rx2.borrow(), Some(leader_info));
    }

    #[tokio::test]
    async fn test_leader_change_sequence() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Arc::new(Node::from_raft(raft));

        let mut rx = node.leader_elected_notifier();

        // Leader 1 elected
        let leader1 = LeaderInfo {
            leader_id: 1,
            term: 5,
        };
        node.leader_elected_tx.send(Some(leader1.clone())).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(leader1));

        // Leader changes to None (during election)
        node.leader_elected_tx.send(None).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), None);

        // Leader 2 elected
        let leader2 = LeaderInfo {
            leader_id: 2,
            term: 6,
        };
        node.leader_elected_tx.send(Some(leader2.clone())).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(leader2));
    }

    #[tokio::test]
    async fn test_ready_notifier_independent() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Arc::new(Node::from_raft(raft));

        let mut ready_rx = node.ready_notifier();
        let mut leader_rx = node.leader_elected_notifier();

        // Initially both should be false/None
        assert_eq!(*ready_rx.borrow(), false);
        assert_eq!(*leader_rx.borrow(), None);

        // Set ready
        node.set_ready(true);
        ready_rx.changed().await.expect("Should receive ready change");
        assert_eq!(*ready_rx.borrow(), true);

        // Leader should still be None
        assert_eq!(*leader_rx.borrow(), None);

        // Now set leader
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 1,
        };
        node.leader_elected_tx.send(Some(leader_info.clone())).unwrap();
        leader_rx.changed().await.expect("Should receive leader change");
        assert_eq!(*leader_rx.borrow(), Some(leader_info));

        // Ready should still be true
        assert_eq!(*ready_rx.borrow(), true);
    }

    #[tokio::test]
    async fn test_initial_receiver_keeps_channel_alive() {
        // Create a Node instance
        let raft = mock_raft_context(1).await;
        let node = Arc::new(Node::from_raft(raft));

        // The node holds _leader_elected_rx, so new subscribers should work
        let mut rx = node.leader_elected_notifier();

        // Send should succeed
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 1,
        };
        node.leader_elected_tx
            .send(Some(leader_info.clone()))
            .expect("Channel should be alive");

        rx.changed().await.expect("Should receive");
        assert_eq!(*rx.borrow(), Some(leader_info));
    }
    */
}
