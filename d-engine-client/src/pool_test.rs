use std::time::Duration;
use std::vec;

use d_engine_proto::common::NodeRole;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use crate::ClientConfig;
use crate::ConnectionPool;
use crate::utils::get_now_as_u32;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::error::ErrorCode;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_test_utils::MockNode;

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_success() {
    let nodes = vec![
        NodeMeta {
            id: 1,
            role: NodeRole::Leader.into(),
            address: "127.0.0.1:50051".to_string(),
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            role: NodeRole::Follower.into(),
            address: "127.0.0.1:50052".to_string(),
            status: NodeStatus::Active.into(),
        },
    ];

    let result = ConnectionPool::parse_cluster_metadata(&nodes).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50051");
    assert_eq!(result.1, vec!["http://127.0.0.1:50052"]);
}

#[tokio::test]
#[traced_test]
async fn test_parse_cluster_metadata_no_leader() {
    let nodes = vec![NodeMeta {
        id: 1,
        role: NodeRole::Follower.into(),
        address: "127.0.0.1:50051".to_string(),
        status: NodeStatus::Active.into(),
    }];

    let result = ConnectionPool::parse_cluster_metadata(&nodes);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::NotLeader);
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
    assert!(result.is_ok());
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
                    role: NodeRole::Leader.into(),
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
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
                    role: NodeRole::Leader.into(),
                    address: format!("127.0.0.1:{port}",),
                    status: NodeStatus::Active.into(),
                }],
            })
        })),
    )
    .await
    .unwrap();
    let endpoints = vec![format!("http://localhost:{}", port)];
    pool.refresh(Some(endpoints)).await.expect("success");
    assert!(pool.members[0].id == new_leader_id);
}
