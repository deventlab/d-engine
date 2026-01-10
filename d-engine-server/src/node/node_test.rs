use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
use tokio::sync::watch;
use tracing::debug;
use tracing_test::traced_test;

use crate::test_utils::MockBuilder;
use crate::test_utils::mock_membership;
use crate::test_utils::mock_node;

#[tokio::test]
#[traced_test]
async fn test_readiness_state_transition() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_readiness_state_transition", shutdown_rx, None);
    assert!(!node.is_rpc_ready());

    node.set_rpc_ready(true);
    assert!(node.is_rpc_ready());
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
        .returning(move |_, _, _, _, _| {
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
                let mut config = RaftNodeConfig::new()
                    .expect("Default config")
                    .validate()
                    .expect("Validate RaftNodeConfig successfully");
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
    assert!(node.is_rpc_ready(), "Node should be marked ready");

    // Wait for completion
    handle.await.expect("Node task should complete");

    // Verify logs show correct behavior
    assert!(
        logs_contain("Set node RPC server ready: true"),
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
                leader_address: "127.0.0.1:8082".to_string(),
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
        let mut config = RaftNodeConfig::new()
            .expect("Default config")
            .validate()
            .expect("Validate RaftNodeConfig successfully");
        // Mark as non-joining node
        config.cluster.node_id = node_id;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: node_id,
                address: "127.0.0.1:8080".to_string(),
                role: Learner as i32,
                status: NodeStatus::Promotable as i32,
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
    assert!(node.is_rpc_ready(), "Node should be marked ready");

    // Verify logs show correct behavior
    assert!(
        logs_contain("Set node RPC server ready: true"),
        "Readiness should be set"
    );
    assert!(
        logs_contain("Learner joining cluster"),
        "Join cluster should be executed for learner nodes"
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
        .times(0..=1) // May be called 0 or 1 times depending on shutdown timing
        .returning(|| Err(Error::Fatal("Cluster not ready".to_string())));
    membership.expect_pre_warm_connections().times(0).returning(|| Ok(()));
    // Build node using MockBuilder
    let node = {
        let builder = MockBuilder::new(shutdown_rx.clone())
            .with_membership(membership) // Inject our mock membership
            .with_node_config({
                let mut config = RaftNodeConfig::new()
                    .expect("Default config")
                    .validate()
                    .expect("Validate RaftNodeConfig successfully");
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
    // Health check failure should propagate as an error
    assert!(result.is_err(), "Node should fail when health check fails");

    // Verify node state
    assert!(!node.is_rpc_ready(), "Node should not be marked ready");

    // Verify logs show correct behavior
    assert!(
        !logs_contain("Set node RPC server ready: true"),
        "Readiness should be set"
    );
    assert!(
        !logs_contain("Node is joining"),
        "Join cluster should NOT be executed"
    );
    assert!(!logs_contain("Node is running"), "Node should be running");
}

/// Unit tests for Node leader change notification APIs
#[cfg(test)]
mod leader_change_tests {
    use crate::LeaderInfo;
    use crate::node::test_helpers::*;

    #[tokio::test]
    async fn test_leader_change_notifier_subscription() {
        let (node, _shutdown_tx) = create_test_node();

        // Subscribe to leader change notifications
        let rx = node.leader_change_notifier();

        // Initial state should be None (no leader yet)
        assert_eq!(*rx.borrow(), None);
    }

    #[tokio::test]
    async fn test_leader_change_notifier_receives_updates() {
        let (node, _shutdown_tx) = create_test_node_arc();

        // Subscribe to leader change notifications
        let mut rx = node.leader_change_notifier();

        // Simulate leader election by sending to the channel
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 5,
        };
        node.leader_notifier
            .sender()
            .send(Some(leader_info))
            .expect("Should send leader info");

        // Wait for update
        rx.changed().await.expect("Should receive change notification");

        // Verify received data
        let received = *rx.borrow();
        assert_eq!(received, Some(leader_info));
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let (node, _shutdown_tx) = create_test_node_arc();

        // Create multiple subscribers
        let mut rx1 = node.leader_change_notifier();
        let mut rx2 = node.leader_change_notifier();

        // Send leader info
        let leader_info = LeaderInfo {
            leader_id: 2,
            term: 10,
        };
        node.leader_notifier.sender().send(Some(leader_info)).expect("Should send");

        // Both should receive
        rx1.changed().await.expect("Subscriber 1 should receive");
        rx2.changed().await.expect("Subscriber 2 should receive");

        assert_eq!(*rx1.borrow(), Some(leader_info));
        assert_eq!(*rx2.borrow(), Some(leader_info));
    }

    #[tokio::test]
    async fn test_leader_change_sequence() {
        let (node, _shutdown_tx) = create_test_node_arc();

        let mut rx = node.leader_change_notifier();

        // Leader 1 elected
        let leader1 = LeaderInfo {
            leader_id: 1,
            term: 5,
        };
        node.leader_notifier.sender().send(Some(leader1)).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(leader1));

        // Leader changes to None (during election)
        node.leader_notifier.sender().send(None).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), None);

        // Leader 2 elected
        let leader2 = LeaderInfo {
            leader_id: 2,
            term: 6,
        };
        node.leader_notifier.sender().send(Some(leader2)).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(leader2));
    }

    #[tokio::test]
    async fn test_ready_notifier_independent() {
        let (node, _shutdown_tx) = create_test_node_arc();

        let mut ready_rx = node.ready_notifier();
        let mut leader_rx = node.leader_change_notifier();

        // Initially both should be false/None
        assert!(!(*ready_rx.borrow()));
        assert_eq!(*leader_rx.borrow(), None);

        // Set ready
        node.set_rpc_ready(true);
        ready_rx.changed().await.expect("Should receive ready change");
        assert!(*ready_rx.borrow());

        // Leader should still be None
        assert_eq!(*leader_rx.borrow(), None);

        // Now set leader
        let leader_info = LeaderInfo {
            leader_id: 1,
            term: 1,
        };
        node.leader_notifier.sender().send_if_modified(|current| {
            *current = Some(leader_info);
            true
        });
        leader_rx.changed().await.expect("Should receive leader change");
        assert_eq!(*leader_rx.borrow(), Some(leader_info));

        // Ready should still be true
        assert!(*ready_rx.borrow());
    }
}

/// Unit tests for Node readiness state management
#[cfg(test)]
mod readiness_tests {
    use crate::node::test_helpers::*;

    #[tokio::test]
    async fn test_set_ready_and_server_is_ready() {
        let (node, _shutdown_tx) = create_test_node();

        // Initially not ready
        assert!(!node.is_rpc_ready());

        // Set ready
        node.set_rpc_ready(true);
        assert!(node.is_rpc_ready());

        // Set not ready
        node.set_rpc_ready(false);
        assert!(!node.is_rpc_ready());
    }

    #[tokio::test]
    async fn test_ready_notifier_receives_updates() {
        let (node, _shutdown_tx) = create_test_node();

        let mut ready_rx = node.ready_notifier();

        // Initial state
        assert!(!(*ready_rx.borrow()));

        // Set ready
        node.set_rpc_ready(true);
        ready_rx.changed().await.expect("Should receive update");
        assert!(*ready_rx.borrow());
    }

    #[tokio::test]
    async fn test_concurrent_set_ready() {
        let (node, _shutdown_tx) = create_test_node_arc();

        let mut handles = vec![];
        for i in 0..10 {
            let node_clone = node.clone();
            let handle = tokio::spawn(async move {
                node_clone.set_rpc_ready(i % 2 == 0);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task should not panic");
        }

        // Final state should be deterministic (last write wins)
        // Just verify the method is callable without panic
        let _is_ready = node.is_rpc_ready();
    }
}

/// Unit tests for Node local_client API
#[cfg(test)]
mod client_tests {
    use crate::node::test_helpers::*;

    #[tokio::test]
    async fn test_local_client_creation() {
        let (node, _shutdown_tx) = create_test_node();

        let client = node.local_client();
        assert_eq!(client.node_id(), node.node_id());
    }

    #[tokio::test]
    async fn test_local_client_concurrent_creation() {
        let (node, _shutdown_tx) = create_test_node_arc();

        let mut handles = vec![];
        for _ in 0..10 {
            let node_clone = node.clone();
            let handle = tokio::spawn(async move {
                let client = node_clone.local_client();
                client.node_id()
            });
            handles.push(handle);
        }

        for handle in handles {
            let node_id = handle.await.expect("Task should not panic");
            assert_eq!(node_id, node.node_id());
        }
    }
}

/// Unit tests for Node node_id API
#[cfg(test)]
mod node_id_tests {
    use crate::node::test_helpers::*;

    #[tokio::test]
    async fn test_node_id_returns_correct_value() {
        let (node, _shutdown_tx) = create_test_node_with_id(42);
        assert_eq!(node.node_id(), 42);
    }
}

/// Bootstrap strategy tests - core business scenarios
#[cfg(test)]
mod bootstrap_strategy_tests {
    use std::sync::Arc;

    use d_engine_core::MockMembership;
    use d_engine_core::MockRaftLog;
    use d_engine_core::MockReplicationCore;
    use d_engine_core::MockTransport;
    use d_engine_core::MockTypeConfig;
    use d_engine_core::RaftNodeConfig;
    use d_engine_core::RaftRole;
    use d_engine_core::learner_state::LearnerState;
    use d_engine_proto::common::NodeRole::Follower;
    use d_engine_proto::common::NodeRole::Learner;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::JoinResponse;
    use d_engine_proto::server::cluster::NodeMeta;
    use tokio::sync::watch;
    use tracing_test::traced_test;

    use crate::test_utils::MockBuilder;

    fn prepare_mock_raft_log() -> MockRaftLog {
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(1_u64);
        raft_log.expect_flush().return_once(|| Ok(()));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));
        raft_log
    }

    fn prepare_mock_replication() -> MockReplicationCore<MockTypeConfig> {
        let mut replication = MockReplicationCore::<MockTypeConfig>::new();
        replication
            .expect_handle_raft_request_in_batch()
            .returning(move |_, _, _, _, _| {
                Ok(d_engine_core::AppendResults {
                    commit_quorum_achieved: true,
                    learner_progress: std::collections::HashMap::new(),
                    peer_updates: std::collections::HashMap::new(),
                })
            });
        replication
    }

    /// Test 1: Learner bootstrap - skip cluster ready check, join cluster
    ///
    /// Verifies:
    /// - check_cluster_is_ready() is NOT called
    /// - set_rpc_ready(true) is called
    /// - join_cluster() is called
    /// - raft.run() is called
    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_learner_bootstrap_success() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let node_id = 100;

        // Setup mock membership - warmup should be called
        let mut membership = MockMembership::new();
        membership
            .expect_check_cluster_is_ready()
            .times(0) // Learner should NOT call this
            .returning(|| Ok(()));
        membership.expect_pre_warm_connections().times(1).returning(|| Ok(()));

        // Setup mock transport for join_cluster
        let mut transport = MockTransport::new();
        transport.expect_discover_leader().returning(|_, _, _| {
            Ok(vec![
                d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                    leader_id: 200,
                    leader_address: "127.0.0.1:8081".to_string(),
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
                leader_id: 200,
            })
        });
        transport
            .expect_request_snapshot_from_leader()
            .returning(|_, _, _, _| Err(d_engine_core::Error::Fatal("no snapshot".to_string())));

        let config = {
            let mut cfg = RaftNodeConfig::new()
                .expect("Default config")
                .validate()
                .expect("Validate RaftNodeConfig successfully");
            cfg.cluster.node_id = node_id;
            cfg.cluster.initial_cluster = vec![
                NodeMeta {
                    id: node_id,
                    address: "127.0.0.1:8080".to_string(),
                    role: Learner as i32,
                    status: NodeStatus::Promotable as i32,
                },
                NodeMeta {
                    id: 200,
                    address: "127.0.0.1:8081".to_string(),
                    role: Follower as i32,
                    status: NodeStatus::Active as i32,
                },
            ];
            cfg
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
                .with_raft_log(prepare_mock_raft_log())
                .with_replication_handler(prepare_mock_replication())
                .with_node_config(config_clone);

            builder.build_node()
        };

        let node = Arc::new(node);
        let node_clone = node.clone();
        let shutdown_tx = _shutdown_tx.clone();

        let handle = tokio::spawn(async move { node_clone.run().await });

        tokio::time::advance(std::time::Duration::from_millis(2)).await;

        // Trigger shutdown
        shutdown_tx.send(()).expect("Should send shutdown");

        // Verify execution completed
        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Learner bootstrap should succeed");

        // Verify node is ready
        assert!(node.is_rpc_ready(), "Learner should be RPC ready");

        // Verify logs show correct behavior
        assert!(
            logs_contain("Learner node bootstrap initiated"),
            "Should log learner bootstrap start"
        );
        assert!(
            logs_contain("Learner joining cluster"),
            "Should log join_cluster call"
        );
        assert!(
            logs_contain("Set node RPC server ready: true"),
            "Should set RPC ready"
        );
    }

    /// Test 2: Voter bootstrap - wait for cluster ready, then warmup
    ///
    /// Verifies:
    /// - check_cluster_is_ready() IS called
    /// - set_rpc_ready(true) is called AFTER cluster ready
    /// - join_cluster() is NOT called
    /// - raft.run() is called
    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_voter_bootstrap_success() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // Setup mock membership
        let mut membership = MockMembership::new();
        membership.expect_check_cluster_is_ready().times(1).returning(|| Ok(())); // Cluster is ready
        membership.expect_pre_warm_connections().times(1).returning(|| Ok(()));

        let node = {
            let builder = MockBuilder::new(shutdown_rx.clone())
                .with_membership(membership)
                .with_raft_log(prepare_mock_raft_log())
                .with_replication_handler(prepare_mock_replication())
                .with_node_config({
                    let mut cfg = RaftNodeConfig::new()
                        .expect("Default config")
                        .validate()
                        .expect("Validate RaftNodeConfig successfully");
                    cfg.cluster.initial_cluster = vec![
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
                    cfg
                });

            builder.build_node()
        };

        let node = Arc::new(node);
        let node_clone = node.clone();
        let shutdown_tx = _shutdown_tx.clone();

        let handle = tokio::spawn(async move { node_clone.run().await });

        tokio::time::advance(std::time::Duration::from_millis(2)).await;

        // Trigger shutdown
        shutdown_tx.send(()).expect("Should send shutdown");

        // Verify execution completed
        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Voter bootstrap should succeed");

        // Verify node is ready
        assert!(node.is_rpc_ready(), "Voter should be RPC ready");

        // Verify logs show correct behavior
        assert!(
            logs_contain("Voter node bootstrap initiated"),
            "Should log voter bootstrap start"
        );
        assert!(
            !logs_contain("Node is joining"),
            "Voter should NOT join cluster"
        );
        assert!(
            logs_contain("Set node RPC server ready: true"),
            "Should set RPC ready"
        );
    }

    /// Test 3a: Shutdown graceful handling - Voter
    ///
    /// Verifies:
    /// - Shutdown signal interrupts bootstrap gracefully
    /// - Returns Ok(()) without error
    /// - Logs contain shutdown message
    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_shutdown_during_voter_bootstrap() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        let mut membership = MockMembership::new();
        membership.expect_check_cluster_is_ready().times(1).returning(|| Ok(()));
        membership.expect_pre_warm_connections().times(1).returning(|| Ok(()));

        let node = {
            let builder = MockBuilder::new(shutdown_rx.clone())
                .with_membership(membership)
                .with_node_config({
                    let mut cfg = RaftNodeConfig::new()
                        .expect("Default config")
                        .validate()
                        .expect("Validate RaftNodeConfig successfully");
                    cfg.cluster.initial_cluster = vec![NodeMeta {
                        id: 100,
                        address: "127.0.0.1:8080".to_string(),
                        role: Follower as i32,
                        status: NodeStatus::Active as i32,
                    }];
                    cfg
                });

            builder.build_node()
        };

        let node = Arc::new(node);
        let node_clone = node.clone();
        let shutdown_tx = _shutdown_tx.clone();

        let handle = tokio::spawn(async move { node_clone.run().await });

        // Send shutdown immediately - it will be detected before entering raft main loop
        shutdown_tx.send(()).expect("Should send shutdown");

        // Advance time to let the shutdown signal propagate
        tokio::time::advance(std::time::Duration::from_millis(10)).await;

        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Should shutdown gracefully");
        assert!(node.is_rpc_ready(), "Node should reach ready state");
        assert!(
            logs_contain("Voter node bootstrap initiated"),
            "Should log voter bootstrap"
        );
    }

    /// Test 3b: Shutdown graceful handling - Learner
    ///
    /// Verifies:
    /// - Learner shutdown returns Ok(())
    /// - join_cluster is attempted
    /// - Logs show learner bootstrap
    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_shutdown_during_learner_bootstrap() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let node_id = 100;

        let mut membership = MockMembership::new();
        membership
            .expect_check_cluster_is_ready()
            .times(0) // Learner does NOT call this
            .returning(|| Ok(()));
        membership.expect_pre_warm_connections().times(1).returning(|| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_discover_leader().returning(|_, _, _| {
            Ok(vec![
                d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                    leader_id: 200,
                    leader_address: "127.0.0.1:8081".to_string(),
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
                leader_id: 200,
            })
        });
        transport
            .expect_request_snapshot_from_leader()
            .returning(|_, _, _, _| Err(d_engine_core::Error::Fatal("no snapshot".to_string())));

        let config = {
            let mut cfg = RaftNodeConfig::new()
                .expect("Default config")
                .validate()
                .expect("Validate RaftNodeConfig successfully");
            cfg.cluster.node_id = node_id;
            cfg.cluster.initial_cluster = vec![
                NodeMeta {
                    id: node_id,
                    address: "127.0.0.1:8080".to_string(),
                    role: Learner as i32,
                    status: NodeStatus::Promotable as i32,
                },
                NodeMeta {
                    id: 200,
                    address: "127.0.0.1:8081".to_string(),
                    role: Follower as i32,
                    status: NodeStatus::Active as i32,
                },
            ];
            cfg
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
                .with_raft_log(prepare_mock_raft_log())
                .with_replication_handler(prepare_mock_replication())
                .with_node_config(config_clone);

            builder.build_node()
        };

        let node = Arc::new(node);
        let node_clone = node.clone();
        let shutdown_tx = _shutdown_tx.clone();

        let handle = tokio::spawn(async move { node_clone.run().await });

        // Wait for bootstrap to complete (RPC ready is set after cluster ready check)
        while !node.is_rpc_ready() {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
        }

        // Now send shutdown - it will be detected before entering raft main loop
        shutdown_tx.send(()).expect("Should send shutdown");

        // Advance time to let the shutdown signal propagate and task complete
        tokio::time::advance(std::time::Duration::from_millis(10)).await;

        let result = handle.await.expect("Task should complete");
        assert!(result.is_ok(), "Should shutdown gracefully");
        assert!(node.is_rpc_ready(), "Node should reach ready state");
        assert!(
            logs_contain("Learner node bootstrap initiated"),
            "Should log learner bootstrap"
        );
        assert!(
            logs_contain("Learner joining cluster"),
            "Should log join attempt"
        );
    }
}
