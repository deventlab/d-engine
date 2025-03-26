use crate::{test_utils::mock_node, Error, MockPeerChannels};
use tokio::sync::watch;

#[tokio::test]
async fn test_readiness_state_transition() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_readiness_state_transition", shutdown_rx, None);
    assert!(!node.server_is_ready());

    node.set_ready(true);
    assert!(node.server_is_ready());
}

#[tokio::test]
async fn test_run_sequence_with_mock_peers() {
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_run_sequence_with_mock_peers", shutdown_rx, None);

    shutdown_tx
        .send(())
        .expect("Expect send shutdown successfully");
    let result = node.run().await;
    assert!(result.is_ok());
}
