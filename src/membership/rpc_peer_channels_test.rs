use super::ChannelWithAddress;
use crate::proto::cluster::NodeMeta;
use crate::proto::cluster::NodeStatus;
use crate::test_utils::enable_logger;
use crate::test_utils::node_config;
use crate::test_utils::MockNode;
use crate::test_utils::MOCK_PEER_CHANNEL_PORT_BASE;
use crate::ConnectionType;
use crate::Error;
use crate::NetworkError;
use crate::PeerChannels;
use crate::PeerChannelsFactory;
use crate::RpcPeerChannels;
use crate::SystemError;
use crate::FOLLOWER;
use futures::stream::FuturesUnordered;
use std::sync::Arc;
use std::vec;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;

// Test helper for creating mock peer configurations
async fn mock_peer(
    port: u64,
    rx: oneshot::Receiver<()>,
) -> ChannelWithAddress {
    MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .expect("should succeed")
}

fn crate_peer_channes(
    my_id: u32,
    db_path: &str,
    p2_id: u32,
    p2_port: u32,
    p3_id: u32,
    p3_port: u32,
) -> RpcPeerChannels {
    let mut node_config = node_config(db_path);
    node_config.retry.membership.max_retries = 1;
    node_config.cluster.initial_cluster = vec![
        NodeMeta {
            id: 1,
            address: "127.0.0.1:10000".to_string(),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: p2_id,
            address: format!("127.0.0.1:{p2_port}"),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
        NodeMeta {
            id: p3_id,
            address: format!("127.0.0.1:{p3_port}"),
            role: FOLLOWER,
            status: NodeStatus::Active.into(),
        },
    ];
    RpcPeerChannels::create(my_id, Arc::new(node_config.clone()))
}
/// # Case 1: Tests successful connection establishment with multiple peers
#[tokio::test]
async fn test_connect_with_peers_case1() {
    enable_logger();
    let my_id = 1;
    let (p2, p2_port, p3, p3_port) = (2, MOCK_PEER_CHANNEL_PORT_BASE + 1, 3, MOCK_PEER_CHANNEL_PORT_BASE + 2);
    let mut peer_channels = crate_peer_channes(
        my_id,
        "/tmp/test_connect_with_peers_case1",
        p2,
        p2_port as u32,
        p3,
        p3_port as u32,
    );

    // Create mock peers
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_tx2, rx2) = oneshot::channel::<()>();

    let a1 = mock_peer(p2_port, rx1).await;
    let a2 = mock_peer(p3_port, rx2).await;
    peer_channels.set_peer_channel(p2, a1);
    peer_channels.set_peer_channel(p3, a2);

    // Execute connection flow
    let result = peer_channels.connect_with_peers(my_id).await;

    // Verify results
    assert!(result.is_ok(), "Should connect successfully");
    assert_eq!(peer_channels.channels.len(), 2, "Should have 2 peer connections");
}

/// # Case 2: Tests partial connection failure handling
#[tokio::test]
async fn test_connect_with_peers_case2() {
    enable_logger();
    let my_id = 1;
    let (p2, p2_port, p3, p3_port) = (2, MOCK_PEER_CHANNEL_PORT_BASE + 3, 3, MOCK_PEER_CHANNEL_PORT_BASE + 5);
    let mut peer_channels = crate_peer_channes(
        my_id,
        "/tmp/test_connect_with_peers_case2",
        p2,
        p2_port as u32,
        p3,
        p3_port as u32,
    );

    // Create mock peers
    let (_tx1, rx1) = oneshot::channel::<()>();
    let a1 = mock_peer(p2_port, rx1).await;
    peer_channels.set_peer_channel(p2, a1);

    // Execute connection flow
    let result = peer_channels.connect_with_peers(my_id).await;

    // Verify results
    assert!(result.is_err(), "Should fail with partial connections");
    assert!(peer_channels.channels.len() < 2, "Should have incomplete connections");
}

/// Case 1: test failed to connect with peers
#[tokio::test]
async fn test_check_cluster_is_ready_case1() {
    let mut node_config = node_config("/tmp/test_check_cluster_is_ready_case1");
    node_config.retry.membership.max_retries = 1;

    let peer2_id = 2;
    let peer2_port = MOCK_PEER_CHANNEL_PORT_BASE + 6;

    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, Arc::new(node_config));

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(peer2_port, rx1, true)
        .await
        .expect("should succeed");

    // Setup peer channels
    peer_channels.set_peer_channel(peer2_id, addr1);

    // Test health status
    assert!(peer_channels.check_cluster_is_ready().await.is_ok());
}

/// Case 2: test success by connecting with peers
#[tokio::test]
async fn test_check_cluster_is_ready_case2() {
    enable_logger();
    let peer2_id = 2;
    let peer2_port = MOCK_PEER_CHANNEL_PORT_BASE + 7;

    let mut node_config = node_config("/tmp/test_check_cluster_is_ready_case2");
    node_config.retry.membership.max_retries = 1;

    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, Arc::new(node_config));

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(peer2_port, rx1, true)
        .await
        .expect("should succeed");

    // Setup peer channels
    peer_channels.set_peer_channel(peer2_id, addr1);

    // Test health status
    assert!(peer_channels.check_cluster_is_ready().await.is_ok());
}

/// Tests connection task spawning logic
#[tokio::test]
async fn test_connection_task_spawning() {
    enable_logger();
    let my_id = 1;
    let (p2, p2_port, p3, p3_port) = (2, MOCK_PEER_CHANNEL_PORT_BASE + 8, 3, MOCK_PEER_CHANNEL_PORT_BASE + 9);
    let peer_channels = crate_peer_channes(
        my_id,
        "/tmp/test_connection_task_spawning",
        p2,
        p2_port as u32,
        p3,
        p3_port as u32,
    );

    // Create mock peers
    let (_tx1, rx1) = oneshot::channel::<()>();
    let a1 = mock_peer(p2_port, rx1).await;
    let (_tx2, rx2) = oneshot::channel::<()>();
    let a2 = mock_peer(p3_port, rx2).await;
    peer_channels.set_peer_channel(p2, a1);
    peer_channels.set_peer_channel(p3, a2);

    // Execute connection flow
    let node_config = peer_channels.node_config.clone();
    let initial_cluster = node_config.cluster.initial_cluster.clone();
    let tasks =
        peer_channels.spawn_connection_tasks(1, &initial_cluster, node_config.retry.clone(), &node_config.network);

    assert_eq!(tasks.len(), 2, "Should spawn 2 connection tasks");
}

/// Tests connection collection with mixed results
#[tokio::test]
async fn test_connection_collection() {
    enable_logger();
    let mut node_config = node_config("/tmp/test_connection_collection");
    node_config.retry.membership.max_retries = 1;
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config.clone()));
    let tasks = FuturesUnordered::new();

    // Simulate mixed results
    let (_tx1, rx1) = oneshot::channel::<()>();
    tasks.push(task::spawn(async move {
        Ok((2_u32, mock_peer(MOCK_PEER_CHANNEL_PORT_BASE + 11, rx1).await))
    }));
    tasks.push(task::spawn(async { Err(NetworkError::ConnectError.into()) }));

    let result = peer_channels.collect_connections(tasks, 2).await;
    assert!(result.is_err(), "Should fail with partial success");
}

/// Tests connection retry logic with exponential backoff
#[tokio::test]
async fn test_connection_retry_mechanism() {
    enable_logger();
    let mut node_config = node_config("/tmp/test_connection_retry_mechanism");
    node_config.retry.membership.max_retries = 1;
    let (tx, mut rx) = mpsc::channel(1);

    // Setup mock server that fails first 2 attempts
    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", MOCK_PEER_CHANNEL_PORT_BASE + 12).as_str())
            .await
            .unwrap();
        tx.send(()).await.unwrap();
        let mut connection_count = 0;

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            if connection_count < 2 {
                drop(socket); // Simulate failure
                connection_count += 1;
            } else {
                // Keep connection open
            }
        }
    });

    // Wait for server startup
    rx.recv().await.unwrap();

    let result = RpcPeerChannels::connect_with_retry(
        format!("127.0.0.1:{}", MOCK_PEER_CHANNEL_PORT_BASE + 12),
        &node_config.retry,
        &node_config.network,
    )
    .await;

    assert!(result.is_ok(), "Should succeed after retries");
}

/// # Case 1: Add new peer successfully
#[tokio::test]
async fn test_add_peer_case1_success() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config("/tmp/test_add_peer_case1_success")));
    let port = MOCK_PEER_CHANNEL_PORT_BASE + 13;

    // Start mock server
    let (_tx, rx) = oneshot::channel::<()>();
    let _server = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .unwrap();

    // Add new peer
    let result = peer_channels.add_peer(2, format!("127.0.0.1:{port}")).await;

    assert!(result.is_ok(), "Should add peer successfully");
    assert_eq!(peer_channels.channels.len(), 1, "Should have 1 peer connection");
    // assert!(
    //     peer_channels
    //         .get_peer_channel(2, ConnectionType::Control)
    //         .await
    //         .is_some(),
    //     "Should retrieve added peer"
    // );
}

/// # Case 2: Add duplicate peer (idempotent)
#[tokio::test]
async fn test_add_case2_duplicate_peer() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config("/tmp/test_add_case2_duplicate_peer")));
    let port = MOCK_PEER_CHANNEL_PORT_BASE + 14;

    // Start mock server
    let (_tx, rx) = oneshot::channel::<()>();
    let _server = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .unwrap();

    // Add peer twice
    peer_channels.add_peer(2, format!("127.0.0.1:{port}")).await.unwrap();
    let result = peer_channels.add_peer(2, format!("127.0.0.1:{port}")).await;

    assert!(result.is_ok(), "Should handle duplicate gracefully");
    assert_eq!(peer_channels.channels.len(), 1, "Should not add duplicate peer");
}

/// # Case 3: Add peer with connection failure
#[tokio::test]
async fn test_add_peer_case3_connection_failure() {
    enable_logger();
    let mut node_config = node_config("/tmp/test_add_peer_case3_connection_failure");
    node_config.retry.membership.max_retries = 1;
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config));

    // Use invalid port to force connection failure
    let result = peer_channels
        .add_peer(
            2,
            "127.0.0.1:0".to_string(), // Invalid port
        )
        .await;

    assert!(result.is_err(), "Should fail to connect");
    assert!(matches!(
        result.unwrap_err(),
        Error::System(SystemError::Network(NetworkError::TaskBackoffFailed(_)))
    ));
    assert!(peer_channels.channels.is_empty(), "Should not add failed connection");
}

/// # Case 4: Add multiple peers with different roles
#[tokio::test]
async fn test_add_peer_case4_multiple_peers() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config("/tmp/test_add_peer_case4_multiple_peers")));
    let ports = [17, 15, 16];

    // Add 3 peers with different roles
    for i in 0..3 {
        let port = MOCK_PEER_CHANNEL_PORT_BASE + ports[i];
        let (_tx, rx) = oneshot::channel::<()>();
        let _server = MockNode::simulate_mock_service_without_reps(port, rx, true)
            .await
            .unwrap();

        peer_channels
            .add_peer(2 + i as u32, format!("127.0.0.1:{port}"))
            .await
            .unwrap();
    }

    assert_eq!(peer_channels.channels.len(), 3, "Should add all peers");
}

/// # Case 5: Get existing peer channel
#[tokio::test]
async fn test_get_peer_channel_case1_existing_peer_channel() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(
        1,
        Arc::new(node_config("/tmp/test_get_peer_channel_case1_existing_peer_channel")),
    );
    let port = MOCK_PEER_CHANNEL_PORT_BASE + 18;

    // Add peer
    let (_tx, rx) = oneshot::channel::<()>();
    let server_addr = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .unwrap();
    peer_channels.add_peer(2, format!("127.0.0.1:{port}")).await.unwrap();

    // // Retrieve channel
    // let channel = peer_channels
    //     .get_peer_channel(2, ConnectionType::Control)
    //     .await
    //     .unwrap();

    // assert_eq!(
    //     MockNode::tcp_addr_to_http_addr(channel.address),
    //     server_addr.address,
    //     "Should return correct address"
    // );
}

/// # Case 6: Get non-existent peer channel
#[tokio::test]
async fn test_get_peer_channel_case2_nonexistent_peer_channel() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(
        1,
        Arc::new(node_config("/tmp/test_get_peer_channel_case2_nonexistent_peer_channel")),
    );

    // assert!(
    //     peer_channels
    //         .get_peer_channel(999, ConnectionType::Control)
    //         .await
    //         .is_none(),
    //     "Should return None for non-existent peer"
    // );
}

/// # Case 7: Add peer with inactive status
#[tokio::test]
async fn test_add_peer_case5_inactive_peer() {
    enable_logger();
    let peer_channels = RpcPeerChannels::create(1, Arc::new(node_config("/tmp/test_add_peer_case5_inactive_peer")));
    let port = MOCK_PEER_CHANNEL_PORT_BASE + 19;

    // Start mock server
    let (_tx, rx) = oneshot::channel::<()>();
    let _server = MockNode::simulate_mock_service_without_reps(port, rx, true)
        .await
        .unwrap();

    // Add inactive peer
    peer_channels.add_peer(2, format!("127.0.0.1:{port}")).await.unwrap();

    // assert!(peer_channels
    //     .get_peer_channel(2, ConnectionType::Control)
    //     .await
    //     .is_some());
}

/// # Case 8: Concurrent add_peer calls
#[tokio::test]
async fn test_add_peer_case6_concurrent_add_peer() {
    enable_logger();
    let peer_channels = Arc::new(RpcPeerChannels::create(
        1,
        Arc::new(node_config("/tmp/test_concurrent_add_peer")),
    ));
    let port_base = MOCK_PEER_CHANNEL_PORT_BASE + 20;

    let mut handles = vec![];
    for i in 0..5 {
        let pc = peer_channels.clone();
        let port = port_base + i;
        handles.push(tokio::spawn(async move {
            let (_tx, rx) = oneshot::channel::<()>();
            let _server = MockNode::simulate_mock_service_without_reps(port, rx, true)
                .await
                .unwrap();
            pc.add_peer(i as u32, format!("127.0.0.1:{port}")).await
        }));
    }

    let results = futures::future::join_all(handles).await;
    for result in results {
        assert!(result.unwrap().is_ok(), "All adds should succeed");
    }

    assert_eq!(peer_channels.channels.len(), 5, "Should add all peers concurrently");
}
