use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tracing_test::traced_test;

use super::*;
use d_engine_core::ConnectionParams;
use d_engine_core::NetworkConfig;
use d_engine_core::test_utils::MockNode;
use d_engine_core::test_utils::MockRpcService;

/// Case 1: server is not ready
#[tokio::test]
#[traced_test]
async fn test_check_peer_is_ready_case1() {
    let (tx, rx) = oneshot::channel::<()>();
    let is_ready = false;
    let mock_service = MockRpcService::default();
    let (_port, addr) = match d_engine_core::test_utils::MockNode::mock_listener(
        mock_service,
        rx,
        is_ready,
    )
    .await
    {
        Ok(a) => a,
        Err(e) => {
            panic!("error: {e:?}");
        }
    };

    let peer_addr = addr.to_string();
    loop {
        if TcpStream::connect(&peer_addr).await.is_ok() {
            println!("Node is ready!");
            break;
        } else {
            eprintln!("Node not ready, retrying...");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    let settings = NetworkConfig {
        control: ConnectionParams {
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            connection_window_size: 12582912,
            stream_window_size: 12582912,
            ..Default::default()
        },
        ..Default::default()
    };

    let r = HealthChecker::check_peer_is_ready(
        MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
        settings,
        "d_engine.server.cluster.ClusterManagementService".to_string(),
    )
    .await;
    assert!(r.is_err());
    tx.send(()).expect("should succeed");
}

/// Case 2: server is ready
#[tokio::test]
#[traced_test]
async fn test_check_peer_is_ready_case2() {
    let (tx, rx) = oneshot::channel::<()>();
    let is_ready = true;
    let mock_service = MockRpcService::default();
    let (_port, addr) = match d_engine_core::test_utils::MockNode::mock_listener(
        mock_service,
        rx,
        is_ready,
    )
    .await
    {
        Ok(a) => a,
        Err(e) => {
            panic!("error: {e:?}");
        }
    };
    let peer_addr = addr.to_string();
    loop {
        if TcpStream::connect(&peer_addr).await.is_ok() {
            println!("Node is ready!");
            break;
        } else {
            eprintln!("Node not ready, retrying...");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    let settings = NetworkConfig {
        control: ConnectionParams {
            connect_timeout_in_ms: 100,
            request_timeout_in_ms: 100,
            concurrency_limit: 8192,
            tcp_keepalive_in_secs: 3600,
            http2_keep_alive_interval_in_secs: 300,
            http2_keep_alive_timeout_in_secs: 20,
            max_frame_size: 12582912,
            connection_window_size: 12582912,
            stream_window_size: 12582912,
            ..Default::default()
        },
        ..Default::default()
    };

    let r = HealthChecker::check_peer_is_ready(
        MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
        settings,
        "d_engine.server.cluster.ClusterManagementService".to_string(),
    )
    .await;
    assert!(r.is_ok());
    tx.send(()).expect("should succeed");
}
