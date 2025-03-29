use std::{sync::Arc, vec};

use crate::{
    grpc::rpc_service::NodeMeta,
    test_utils::{enable_logger, settings, MockNode, MOCK_PEER_CHANNEL_PORT_BASE},
    Error, PeerChannels, PeerChannelsFactory, RpcPeerChannels, FOLLOWER,
};
use futures::stream::FuturesUnordered;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task,
};

use super::ChannelWithAddress;

// Test helper for creating mock peer configurations
async fn mock_peer(port: u64, rx: oneshot::Receiver<()>) -> ChannelWithAddress {
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
    let mut settings = settings(db_path);
    settings.retry.membership.max_retries = 1;
    settings.cluster.initial_cluster = vec![
        NodeMeta {
            id: 1,
            ip: "127.0.0.1".to_string(),
            port: 10000,
            role: FOLLOWER,
        },
        NodeMeta {
            id: p2_id,
            ip: "127.0.0.1".to_string(),
            port: p2_port,
            role: FOLLOWER,
        },
        NodeMeta {
            id: p3_id,
            ip: "127.0.0.1".to_string(),
            port: p3_port,
            role: FOLLOWER,
        },
    ];
    RpcPeerChannels::create(my_id, Arc::new(settings.clone()))
}
/// # Case 1: Tests successful connection establishment with multiple peers
#[tokio::test]
async fn test_connect_with_peers_case1() {
    enable_logger();
    let my_id = 1;
    let (p2, p2_port, p3, p3_port) = (
        2,
        MOCK_PEER_CHANNEL_PORT_BASE + 1,
        3,
        MOCK_PEER_CHANNEL_PORT_BASE + 2,
    );
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
    assert_eq!(
        peer_channels.channels.len(),
        2,
        "Should have 2 peer connections"
    );
}

/// # Case 2: Tests partial connection failure handling
#[tokio::test]
async fn test_connect_with_peers_case2() {
    enable_logger();
    let my_id = 1;
    let (p2, p2_port, p3, p3_port) = (
        2,
        MOCK_PEER_CHANNEL_PORT_BASE + 3,
        3,
        MOCK_PEER_CHANNEL_PORT_BASE + 5,
    );
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
    assert!(
        peer_channels.channels.len() < 2,
        "Should have incomplete connections"
    );
}

/// Case 1: test failed to connect with peers
///
#[tokio::test]
async fn test_check_cluster_is_ready_case1() {
    let mut settings = settings("/tmp/test_check_cluster_is_ready_case2");
    settings.retry.membership.max_retries = 1;

    let peer2_id = 2;
    let peer2_port = MOCK_PEER_CHANNEL_PORT_BASE + 6;

    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, Arc::new(settings));

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
///
#[tokio::test]
async fn test_check_cluster_is_ready_case2() {
    enable_logger();
    let peer2_id = 2;
    let peer2_port = MOCK_PEER_CHANNEL_PORT_BASE + 7;

    let mut settings = settings("/tmp/test_check_cluster_is_ready_case2");
    settings.retry.membership.max_retries = 1;

    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, Arc::new(settings));

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(peer2_port, rx1, false)
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
    let (p2, p2_port, p3, p3_port) = (
        2,
        MOCK_PEER_CHANNEL_PORT_BASE + 8,
        3,
        MOCK_PEER_CHANNEL_PORT_BASE + 9,
    );
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
    let settings = peer_channels.settings.clone();
    let initial_cluster = settings.cluster.initial_cluster.clone();
    let tasks = peer_channels.spawn_connection_tasks(
        1,
        &initial_cluster,
        settings.retry.clone(),
        &settings.network,
    );

    assert_eq!(tasks.len(), 2, "Should spawn 2 connection tasks");
}

/// Tests connection collection with mixed results
#[tokio::test]
async fn test_connection_collection() {
    enable_logger();
    let mut settings = settings("/tmp/test_connection_collection");
    settings.retry.membership.max_retries = 1;
    let peer_channels = RpcPeerChannels::create(1, Arc::new(settings.clone()));
    let tasks = FuturesUnordered::new();

    // Simulate mixed results
    let (_tx1, rx1) = oneshot::channel::<()>();
    tasks.push(task::spawn(async move {
        Ok((
            2 as u32,
            mock_peer(MOCK_PEER_CHANNEL_PORT_BASE + 11, rx1).await,
        ))
    }));
    tasks.push(task::spawn(async { Err(Error::ConnectError) }));

    let result = peer_channels.collect_connections(tasks, 2).await;
    assert!(result.is_err(), "Should fail with partial success");
}

/// Tests connection retry logic with exponential backoff
#[tokio::test]
async fn test_connection_retry_mechanism() {
    enable_logger();
    let mut settings = settings("/tmp/test_connection_retry_mechanism");
    settings.retry.membership.max_retries = 1;
    let (tx, mut rx) = mpsc::channel(1);

    // Setup mock server that fails first 2 attempts
    tokio::spawn(async move {
        let listener =
            TcpListener::bind(format!("127.0.0.1:{}", MOCK_PEER_CHANNEL_PORT_BASE + 12).as_str())
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
        &NodeMeta {
            id: 2,
            ip: "127.0.0.1".to_string(),
            port: (MOCK_PEER_CHANNEL_PORT_BASE + 12) as u32,
            role: FOLLOWER,
        },
        &settings.retry,
        &settings.network,
    )
    .await;

    assert!(result.is_ok(), "Should succeed after retries");
}
