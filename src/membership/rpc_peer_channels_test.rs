use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    grpc::rpc_service::NodeMeta,
    test_utils::{MockNode, MOCK_MEMBERSHIP_PORT_BASE},
    PeerChannels, PeerChannelsFactory, RpcPeerChannels, Settings, FOLLOWER,
};

fn prepare_health_check_settings(db_root_dir: &str, peer2_port: u32, peer3_port: u32) -> Settings {
    let mut settings = match Settings::new() {
        Ok(s) => s,
        Err(e) => panic!("failed to init settings: {:?}", e),
    };
    settings.rpc_connection_settings.connect_timeout_in_ms = 100;
    settings.rpc_connection_settings.request_timeout_in_ms = 100;
    settings
        .rpc_connection_settings
        .concurrency_limit_per_connection = 8192;
    settings.rpc_connection_settings.tcp_keepalive_in_secs = 3600;
    settings
        .rpc_connection_settings
        .http2_keep_alive_interval_in_secs = 300;
    settings
        .rpc_connection_settings
        .http2_keep_alive_timeout_in_secs = 20;
    settings.rpc_connection_settings.max_frame_size = 12582912;
    settings
        .rpc_connection_settings
        .initial_connection_window_size = 12582912;
    settings.rpc_connection_settings.initial_stream_window_size = 12582912;
    settings.rpc_connection_settings.buffer_size = 65536;

    settings
        .cluster_settings
        .cluster_membership_sync_max_retries = 3;
    settings
        .cluster_settings
        .cluster_membership_sync_exponential_backoff_duration_in_ms = 100;
    settings
        .cluster_settings
        .cluster_membership_sync_timeout_duration_in_ms = 100;
    settings.cluster_settings.cluster_healtcheck_max_retries = 1;
    settings
        .cluster_settings
        .cluster_healtcheck_timeout_duration_in_ms = 100;
    settings
        .cluster_settings
        .cluster_healtcheck_exponential_backoff_duration_in_ms = 100;
    settings
        .cluster_settings
        .cluster_healthcheck_probe_service_name = "rpc_service.RpcService".to_string();
    settings.server_settings.id = 1;
    settings.server_settings.listen_address = format!("127.0.0.1:{}", peer2_port);
    settings.server_settings.db_root_dir = format!("{}", db_root_dir);
    settings.server_settings.initial_cluster = vec![
        NodeMeta {
            id: 2,
            ip: "127.0.0.1".to_string(),
            port: peer2_port,
            role: FOLLOWER,
        },
        NodeMeta {
            id: 3,
            ip: "127.0.0.1".to_string(),
            port: peer3_port,
            role: FOLLOWER,
        },
    ];
    settings
}

/// Case 1: test failed to connect with peers
///
#[tokio::test]
async fn test_check_cluster_is_ready_case1() {
    let db_root_dir = "/tmp/test_check_cluster_is_ready_case1";
    let peer2_id = 2;
    let peer2_port = MOCK_MEMBERSHIP_PORT_BASE + 1;
    let peer3_port = MOCK_MEMBERSHIP_PORT_BASE + 2;

    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(
        1,
        event_tx,
        Arc::new(prepare_health_check_settings(
            db_root_dir,
            peer2_port as u32,
            peer3_port as u32,
        )),
    );

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
    let db_root_dir = "/tmp/test_check_cluster_is_ready_case2";
    let peer2_id = 2;
    let peer2_port = MOCK_MEMBERSHIP_PORT_BASE + 3;
    let peer3_port = MOCK_MEMBERSHIP_PORT_BASE + 4;

    let (event_tx, _event_rx) = mpsc::channel(1);
    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(
        1,
        event_tx,
        Arc::new(prepare_health_check_settings(
            db_root_dir,
            peer2_port as u32,
            peer3_port as u32,
        )),
    );

    // Simulate ChannelWithAddress: prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_without_reps(peer2_port, rx1, false)
        .await
        .expect("should succeed");

    // Setup peer channels
    peer_channels.set_peer_channel(peer2_id, addr1);

    // Test health status
    assert!(peer_channels.check_cluster_is_ready().await.is_err());
}
