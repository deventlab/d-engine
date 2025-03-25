use std::sync::Arc;

use crate::{
    test_utils::{mock_raft_context, MockNode, MOCK_MEMBERSHIP_PORT_BASE},
    PeerChannels, PeerChannelsFactory, RpcPeerChannels, Settings,
};
use tokio::sync::{oneshot, watch};

/// Case 1: test failed to connect with peers
///
#[tokio::test]
async fn test_check_cluster_is_ready_case1() {
    let mut settings = Settings::new().expect("success");
    settings.cluster_settings.cluster_healtcheck_max_retries = 1;

    let peer2_id = 2;
    let peer2_port = MOCK_MEMBERSHIP_PORT_BASE + 1;

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
    let peer2_id = 2;
    let peer2_port = MOCK_MEMBERSHIP_PORT_BASE + 3;

    let mut settings = Settings::new().expect("success");
    settings.cluster_settings.cluster_healtcheck_max_retries = 1;

    let peer_channels: RpcPeerChannels = PeerChannelsFactory::create(1, Arc::new(settings));

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
