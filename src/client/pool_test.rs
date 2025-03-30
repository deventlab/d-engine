use std::time::Duration;

use crate::{
    grpc::rpc_service::{ClusterMembership, NodeMeta},
    test_utils::{enable_logger, MockNode, MockRpcService, MOCK_CLIENT_PORT_BASE},
    utils::util,
    ClientConfig, ConnectionPool, Error, FOLLOWER, LEADER,
};
use log::error;
use tokio::sync::oneshot;
use tonic::transport::Channel;

async fn mock_listener(port: u64, rx: oneshot::Receiver<()>) {
    let mut mock_service = MockRpcService::default();
    mock_service.expected_metadata_response = Some(Ok(ClusterMembership {
        nodes: vec![NodeMeta {
            id: 1,
            role: LEADER,
            ip: "127.0.0.1".to_string(),
            port: port as u32,
        }],
    }));
    let r = MockNode::mock_listener(mock_service, port, rx, true).await;
    assert!(r.is_ok());
}
#[tokio::test]
async fn test_parse_cluster_metadata_success() {
    let nodes = vec![
        NodeMeta {
            id: 1,
            role: LEADER,
            ip: "127.0.0.1".to_string(),
            port: 50051,
        },
        NodeMeta {
            id: 2,
            role: FOLLOWER,
            ip: "127.0.0.1".to_string(),
            port: 50052,
        },
    ];

    let result = ConnectionPool::parse_cluster_metadata(nodes).unwrap();
    assert_eq!(result.0, "http://127.0.0.1:50051");
    assert_eq!(result.1, vec!["http://127.0.0.1:50052"]);
}

#[tokio::test]
async fn test_parse_cluster_metadata_no_leader() {
    let nodes = vec![NodeMeta {
        id: 1,
        role: FOLLOWER,
        ip: "127.0.0.1".to_string(),
        port: 50051,
    }];

    let result = ConnectionPool::parse_cluster_metadata(nodes);
    assert!(matches!(result.err(), Some(Error::NoLeaderFound)));
}

#[tokio::test]
async fn test_load_cluster_metadata_success() {
    let (_tx, rx) = oneshot::channel::<()>();
    let port = MOCK_CLIENT_PORT_BASE + 1;
    mock_listener(port, rx).await;

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
        id: util::get_now_as_u32(),
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
async fn test_connection_pool_creation() {
    enable_logger();
    let (_tx, rx) = oneshot::channel::<()>();
    let port = MOCK_CLIENT_PORT_BASE + 3;
    mock_listener(port, rx).await;

    let endpoints = vec![format!("http://localhost:{}", port)];
    let config = ClientConfig::default();

    let pool = match ConnectionPool::new(endpoints, config).await {
        Ok(p) => p,
        Err(e) => {
            error!("{:?}", e);
            assert!(false);
            panic!("f");
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
    mock_listener(port1, rx1).await;
    mock_listener(port2, rx2).await;
    let addr1 = format!("http://localhost:{}", port1);
    let addr2 = format!("http://localhost:{}", port2);
    let pool = ConnectionPool {
        leader_conn: Channel::from_shared(addr1)
            .unwrap()
            .connect()
            .await
            .unwrap(),
        follower_conns: vec![Channel::from_shared(addr2)
            .unwrap()
            .connect()
            .await
            .unwrap()],
        config: ClientConfig::default(),
    };

    let channels = pool.get_all_channels();
    assert_eq!(channels.len(), 2);
}
