use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::vec;

use d_engine_proto::common::NodeStatus;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use crate::ClientConfig;
use crate::ConnectionPool;
use crate::mock_rpc_service::MockNode;
use crate::utils::get_now_as_u32;

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_success() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![
            NodeMeta {
                id: 1,
                role: 0, // Voter
                address: "127.0.0.1:50051".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                role: 0, // Voter
                address: "127.0.0.1:50052".to_string(),
                status: NodeStatus::Active.into(),
            },
        ],
        current_leader_id: Some(1), // Node 1 is leader
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50051");
    assert_eq!(result.1, vec!["http://127.0.0.1:50052"]);
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_no_leader() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![NodeMeta {
            id: 1,
            role: 0, // Voter
            address: "127.0.0.1:50051".to_string(),
            status: NodeStatus::Active.into(),
        }],
        current_leader_id: None, // No leader
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::ClusterUnavailable);
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_leader_not_in_nodes() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![NodeMeta {
            id: 1,
            role: 0,
            address: "127.0.0.1:50051".to_string(),
            status: NodeStatus::Active.into(),
        }],
        current_leader_id: Some(99), // Leader ID not in nodes list
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::ClusterUnavailable);
}

#[tokio::test]
#[traced_test]
async fn test_load_cluster_metadata_success() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    // This test requires actual network connections. For more isolated testing,
    // consider using a mock server or in-memory transport
    let result = ConnectionPool::load_cluster_metadata(&endpoints, &config).await;
    assert!(result.is_ok(), "Should return (membership, leader_conn)");
}

#[tokio::test]
#[traced_test]
async fn test_create_channel_success() {
    let config = ClientConfig {
        id: get_now_as_u32(),
        connect_timeout: Duration::from_millis(1000),
        request_timeout: Duration::from_millis(3000),
        tcp_keepalive: Duration::from_secs(300),
        http2_keepalive_interval: Duration::from_secs(60),
        http2_keepalive_timeout: Duration::from_secs(20),
        max_frame_size: 1 << 20, // 1MB
        enable_compression: true,
        cluster_ready_timeout: Duration::from_secs(5),
    };

    // Test with an invalid address to verify timeout behavior
    let result =
        ConnectionPool::create_channel("http://invalid.address:50051".to_string(), &config).await;
    assert!(result.is_err());
}

#[tokio::test]
#[traced_test]
async fn test_connection_pool_creation() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = ConnectionPool::create(endpoints, config)
        .await
        .expect("Should create connection pool");

    // Verify we have at least the leader connection
    assert!(!pool.get_all_channels().is_empty());
    assert_eq!(pool.follower_conns.len(), 0);
}
#[tokio::test]
#[traced_test]
async fn test_get_all_channels() {
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_tx2, rx2) = oneshot::channel::<()>();
    let (_channel, port1) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx1,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    )
    .await
    .unwrap();
    let (_channel, port2) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx2,
        None::<
            Box<dyn Fn(u16) -> std::result::Result<ClusterMembership, tonic::Status> + Send + Sync>,
        >,
    )
    .await
    .unwrap();
    let addr1 = format!("http://localhost:{port1}",);
    let addr2 = format!("http://localhost:{port2}",);
    let pool = ConnectionPool {
        leader_conn: MockNode::mock_channel_with_port(port1).await,
        follower_conns: vec![MockNode::mock_channel_with_port(port2).await],
        config: ClientConfig::default(),
        members: vec![], // this value will not affect the unit test result
        endpoints: vec![addr1, addr2],
        current_leader_id: Some(1),
    };

    let channels = pool.get_all_channels();
    assert_eq!(channels.len(), 2);
}

#[tokio::test]
#[traced_test]
async fn test_refresh_successful_leader_change() {
    let leader_id = 1;
    let new_leader_id = 2;

    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |port| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: leader_id,
                    role: 0, // Voter
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
                current_leader_id: Some(leader_id),
            })
        })),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{port}")];
    let config = ClientConfig::default();

    let mut pool = match ConnectionPool::create(endpoints, config).await {
        Ok(p) => p,
        Err(e) => {
            panic!("error: {e:?}");
        }
    };
    // Verify we have at least the leader connection
    assert!(pool.members[0].id == leader_id);
    // Check follower count matches test setup (adjust based on your mock data)
    assert_eq!(pool.follower_conns.len(), 0);

    // Now let's refresh the connections
    let (_tx, rx) = oneshot::channel::<()>();

    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |port| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: new_leader_id,
                    role: 0, // Voter
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
                current_leader_id: Some(new_leader_id),
            })
        })),
    )
    .await
    .unwrap();
    let endpoints = vec![format!("http://localhost:{}", port)];
    pool.refresh(Some(endpoints)).await.expect("success");
    assert!(pool.members[0].id == new_leader_id);
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_multiple_nodes_with_leader() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![
            NodeMeta {
                id: 1,
                role: 0, // Voter
                address: "127.0.0.1:50051".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                role: 0, // Voter
                address: "127.0.0.1:50052".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                role: 0, // Voter
                address: "127.0.0.1:50053".to_string(),
                status: NodeStatus::Active.into(),
            },
        ],
        current_leader_id: Some(2), // Node 2 is leader
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50052");
    assert_eq!(result.1.len(), 2);
    assert!(result.1.contains(&"http://127.0.0.1:50051".to_string()));
    assert!(result.1.contains(&"http://127.0.0.1:50053".to_string()));
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_leader_id_zero() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![NodeMeta {
            id: 1,
            role: 0,
            address: "127.0.0.1:50051".to_string(),
            status: NodeStatus::Active.into(),
        }],
        current_leader_id: Some(0), // Invalid leader ID (0 means unknown)
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::ClusterUnavailable);
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_empty_nodes() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![],
        current_leader_id: Some(1),
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::ClusterUnavailable);
}

#[tokio::test]
#[traced_test]
async fn test_load_cluster_metadata_returns_full_membership() {
    let leader_id = 1;
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |port| {
            Ok(ClusterMembership {
                version: 42,
                nodes: vec![
                    NodeMeta {
                        id: leader_id,
                        role: 0,
                        address: format!("127.0.0.1:{port}"),
                        status: NodeStatus::Active.into(),
                    },
                    NodeMeta {
                        id: 2,
                        role: 0,
                        address: "127.0.0.1:50052".to_string(),
                        status: NodeStatus::Active.into(),
                    },
                ],
                current_leader_id: Some(leader_id),
            })
        })),
    )
    .await
    .unwrap();

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let (membership, _conn) = ConnectionPool::load_cluster_metadata(&endpoints, &config)
        .await
        .expect("Should load metadata");

    assert_eq!(membership.version, 42);
    assert_eq!(membership.nodes.len(), 2);
    assert_eq!(membership.current_leader_id, Some(leader_id));
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_with_learner_nodes() {
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![
            NodeMeta {
                id: 1,
                role: 0, // Voter
                address: "127.0.0.1:50051".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                role: 1, // Learner
                address: "127.0.0.1:50052".to_string(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                role: 0, // Voter
                address: "127.0.0.1:50053".to_string(),
                status: NodeStatus::Active.into(),
            },
        ],
        current_leader_id: Some(3), // Voter node 3 is leader
    };

    let result = ConnectionPool::parse_cluster_metadata(&membership).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50053");
    // All non-leader nodes (including learner) go to followers
    assert_eq!(result.1.len(), 2);
}

/// probe_endpoint returns None when node is unreachable
#[tokio::test]
#[traced_test]
async fn test_probe_endpoint_unreachable() {
    let config = ClientConfig::default();
    let budget = Duration::from_secs(5);
    let result = ConnectionPool::probe_endpoint("http://127.0.0.1:1", &config, budget).await;
    assert!(result.is_none());
}

/// probe_endpoint returns Some(Err(())) when node responds but election is in progress
/// (current_leader_id = None)
#[tokio::test]
#[traced_test]
async fn test_probe_endpoint_election_in_progress() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(|port| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: 1,
                    role: 0,
                    address: format!("127.0.0.1:{port}"),
                    status: NodeStatus::Active.into(),
                }],
                current_leader_id: None, // election in progress
            })
        })),
    )
    .await
    .unwrap();

    let config = ClientConfig::default();
    let addr = format!("http://localhost:{port}");
    let budget = Duration::from_secs(5);
    let result = ConnectionPool::probe_endpoint(&addr, &config, budget).await;
    assert!(matches!(result, Some(Err(()))));
}

/// load_cluster_metadata retries until leader becomes ready.
/// First calls return no leader; after N calls the mock returns a valid leader.
#[tokio::test]
#[traced_test]
async fn test_load_cluster_metadata_retries_until_leader_ready() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |port| {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                // First 2 calls: election in progress
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: 0,
                        address: format!("127.0.0.1:{port}"),
                        status: NodeStatus::Active.into(),
                    }],
                    current_leader_id: None,
                })
            } else {
                // Subsequent calls: leader ready
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: 0,
                        address: format!("127.0.0.1:{port}"),
                        status: NodeStatus::Active.into(),
                    }],
                    current_leader_id: Some(1),
                })
            }
        })),
    )
    .await
    .unwrap();

    let config = ClientConfig::default();
    let endpoints = vec![format!("http://localhost:{port}")];
    let (membership, _conn) = ConnectionPool::load_cluster_metadata(&endpoints, &config)
        .await
        .expect("Should eventually return ready leader");

    assert_eq!(membership.current_leader_id, Some(1));
    assert!(call_count.load(Ordering::SeqCst) >= 3);
}

/// load_cluster_metadata returns ClusterUnavailable when cluster_ready_timeout elapses
/// without any node reporting a ready leader.
#[tokio::test]
#[traced_test]
async fn test_load_cluster_metadata_timeout() {
    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(|port| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: 1,
                    role: 0,
                    address: format!("127.0.0.1:{port}"),
                    status: NodeStatus::Active.into(),
                }],
                current_leader_id: None, // always election in progress
            })
        })),
    )
    .await
    .unwrap();

    let config = ClientConfig {
        cluster_ready_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let endpoints = vec![format!("http://localhost:{port}")];

    let err = ConnectionPool::load_cluster_metadata(&endpoints, &config).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::ClusterUnavailable);
}

/// Test defensive error handling for inconsistent cluster metadata.
/// Verifies that load_cluster_metadata retries and recovers when encountering
/// invalid metadata (leader_id present but nodes list empty).
///
/// This tests the defensive `let Ok(...) else { warn!; continue }` pattern
/// added in fix #282. Although probe_endpoint's ready check prevents this
/// scenario (returns Err for empty nodes), this test validates the retry
/// loop correctly handles transient inconsistent states and eventually succeeds.
#[tokio::test]
#[traced_test]
async fn test_load_cluster_metadata_parse_failure_after_probe() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let (_tx, rx) = oneshot::channel::<()>();
    let (_channel, port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |port| {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First call: return invalid state (leader_id but empty nodes)
                // probe_endpoint will return Err(()), triggering retry
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![],
                    current_leader_id: Some(1),
                })
            } else {
                // Second call: return valid cluster state
                Ok(ClusterMembership {
                    version: 1,
                    nodes: vec![NodeMeta {
                        id: 1,
                        role: 0,
                        address: format!("127.0.0.1:{port}"),
                        status: NodeStatus::Active.into(),
                    }],
                    current_leader_id: Some(1),
                })
            }
        })),
    )
    .await
    .unwrap();

    let config = ClientConfig::default();
    let endpoints = vec![format!("http://localhost:{port}")];

    // Should eventually succeed after retrying past the inconsistent state
    let (membership, _conn) = ConnectionPool::load_cluster_metadata(&endpoints, &config)
        .await
        .expect("Should succeed after retrying past invalid state");

    assert_eq!(membership.current_leader_id, Some(1));
    assert_eq!(membership.nodes.len(), 1);
}
