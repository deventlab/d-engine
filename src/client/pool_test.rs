use std::time::Duration;
use std::vec;

use tokio::sync::oneshot;
use tonic::transport::Channel;

use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::NodeStatus;
use crate::proto::error::ErrorCode;
use crate::test_utils::enable_logger;
use crate::test_utils::MockNode;
use crate::test_utils::MockRpcService;
use crate::test_utils::MOCK_CLIENT_PORT_BASE;
use crate::time::get_now_as_u32;
use crate::ClientConfig;
use crate::ConnectionPool;
use crate::FOLLOWER;
use crate::LEADER;

async fn mock_listener(
    port: u64,
    rx: oneshot::Receiver<()>,
    expected_metadata_response: Option<Result<ClusterMembership, tonic::Status>>,
) {
    let mock_service = MockRpcService {
        expected_metadata_response: Some(expected_metadata_response.unwrap_or_else(|| {
            Ok(ClusterMembership {
                version: 1,
                nodes: vec![NodeMeta {
                    id: 1,
                    role: LEADER,
                    address: format!("127.0.0.1:{port}"),
                    status: NodeStatus::Active.into(),
                }],
            })
        })),
        ..Default::default()
    };
    let r = MockNode::mock_listener(mock_service, port, rx, true).await;
    assert!(r.is_ok());
}
#[tokio::test]
async fn test_parse_cluster_metadata_success() {
    let nodes = vec![
        NodeMeta {
            id: 1,
            role: LEADER,
            address: "127.0.0.1:50051".to_string(),
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: 2,
            role: FOLLOWER,
            address: "127.0.0.1:50052".to_string(),
            status: NodeStatus::Active.into(),
        },
    ];

    let result = ConnectionPool::parse_cluster_metadata(&nodes).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50051");
    assert_eq!(result.1, vec!["http://127.0.0.1:50052"]);
}

#[tokio::test]
async fn test_parse_cluster_metadata_no_leader() {
    let nodes = vec![NodeMeta {
        id: 1,
        role: FOLLOWER,
        address: "127.0.0.1:50051".to_string(),
        status: NodeStatus::Active.into(),
    }];

    let result = ConnectionPool::parse_cluster_metadata(&nodes);
    let e = result.unwrap_err();
    assert_eq!(e.code(), ErrorCode::NotLeader as u32);
}

#[tokio::test]
async fn test_load_cluster_metadata_success() {
    let (_tx, rx) = oneshot::channel::<()>();
    let port = MOCK_CLIENT_PORT_BASE + 1;
    mock_listener(port, rx, None).await;

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    // This test requires actual network connections. For more isolated testing,
    // consider using a mock server or in-memory transport
    let result = ConnectionPool::load_cluster_metadata(&endpoints, &config).await;
    assert!(result.is_ok());
}

#[tokio::test]
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
    let result = ConnectionPool::create_channel("http://invalid.address:50051".to_string(), &config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_connection_pool_creation() {
    enable_logger();
    let (_tx, rx) = oneshot::channel::<()>();
    let port = MOCK_CLIENT_PORT_BASE + 3;
    mock_listener(port, rx, None).await;

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = match ConnectionPool::create(endpoints, config).await {
        Ok(p) => p,
        Err(e) => {
            panic!("error: {e:?}",);
        }
    };
    // assert!(pool.is_ok());
    // let pool = pool.unwrap();

    // Verify we have at least the leader connection
    assert!(!pool.get_all_channels().is_empty());
    // Check follower count matches test setup (adjust based on your mock data)
    assert_eq!(pool.follower_conns.len(), 0);
}
#[tokio::test]
async fn test_get_all_channels() {
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_tx2, rx2) = oneshot::channel::<()>();
    let port1 = MOCK_CLIENT_PORT_BASE + 4;
    let port2 = MOCK_CLIENT_PORT_BASE + 5;
    mock_listener(port1, rx1, None).await;
    mock_listener(port2, rx2, None).await;
    let addr1 = format!("http://localhost:{port1}",);
    let addr2 = format!("http://localhost:{port2}",);
    let pool = ConnectionPool {
        leader_conn: Channel::from_shared(addr1.clone()).unwrap().connect().await.unwrap(),
        follower_conns: vec![Channel::from_shared(addr2.clone()).unwrap().connect().await.unwrap()],
        config: ClientConfig::default(),
        members: vec![], // this value will not affect the unit test result
        endpoints: vec![addr1, addr2],
    };

    let channels = pool.get_all_channels();
    assert_eq!(channels.len(), 2);
}

#[tokio::test]
async fn test_refresh_successful_leader_change() {
    enable_logger();
    let leader_id = 1;
    let new_leader_id = 2;

    let (_tx, rx) = oneshot::channel::<()>();
    let port = MOCK_CLIENT_PORT_BASE + 6;
    let metadata = ClusterMembership {
        version: 1,
        nodes: vec![NodeMeta {
            id: leader_id,
            role: LEADER,
            address: format!("127.0.0.1:{port}"),
            status: NodeStatus::Active.into(),
        }],
    };
    mock_listener(port, rx, Some(Ok(metadata))).await;

    let endpoints = vec![format!("http://localhost:{}", port)];
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
    let port = MOCK_CLIENT_PORT_BASE + 7;
    let metadata = ClusterMembership {
        version: 1,
        nodes: vec![NodeMeta {
            id: new_leader_id,
            role: LEADER,
            address: format!("127.0.0.1:{port}"),
            status: NodeStatus::Active.into(),
        }],
    };
    mock_listener(port, rx, Some(Ok(metadata))).await;
    let endpoints = vec![format!("http://localhost:{}", port)];
    pool.refresh(Some(endpoints)).await.expect("success");
    println!("-------- {:?}", pool.members);
    assert!(pool.members[0].id == new_leader_id);
}
