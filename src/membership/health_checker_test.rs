use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::membership::health_checker::HealthChecker;
use crate::membership::health_checker::HealthCheckerApis;
use crate::test_utils::MockNode;
use crate::test_utils::MockRpcService;
use crate::test_utils::{self};
use crate::NetworkConfig;

/// Case 1: server is not ready
#[tokio::test]
async fn test_check_peer_is_ready_case1() {
    let port = crate::test_utils::MOCK_HEALTHCHECK_PORT_BASE + 1;
    let (tx, rx) = oneshot::channel::<()>();
    let is_ready = false;
    let mock_service = MockRpcService::default();
    let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, is_ready).await {
        Ok(a) => a,
        Err(_) => {
            assert!(false);
            return;
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

    let mut settings = NetworkConfig::default();
    settings.connect_timeout_in_ms = 100;
    settings.request_timeout_in_ms = 100;
    settings.concurrency_limit_per_connection = 8192;
    settings.tcp_keepalive_in_secs = 3600;
    settings.http2_keep_alive_interval_in_secs = 300;
    settings.http2_keep_alive_timeout_in_secs = 20;
    settings.max_frame_size = 12582912;
    settings.initial_connection_window_size = 12582912;
    settings.initial_stream_window_size = 12582912;
    settings.buffer_size = 65536;

    let r = HealthChecker::check_peer_is_ready(
        MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
        settings,
        "rpc_service.RpcService".to_string(),
    )
    .await;
    assert!(r.is_err());
    tx.send(()).expect("should succeed");
}

/// Case 2: server is ready
#[tokio::test]
async fn test_check_peer_is_ready_case2() {
    let port = crate::test_utils::MOCK_HEALTHCHECK_PORT_BASE + 2;
    let (tx, rx) = oneshot::channel::<()>();
    let is_ready = true;
    let mock_service = MockRpcService::default();
    let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, is_ready).await {
        Ok(a) => a,
        Err(_) => {
            assert!(false);
            return;
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
    let mut settings = NetworkConfig::default();
    settings.connect_timeout_in_ms = 100;
    settings.request_timeout_in_ms = 100;
    settings.concurrency_limit_per_connection = 8192;
    settings.tcp_keepalive_in_secs = 3600;
    settings.http2_keep_alive_interval_in_secs = 300;
    settings.http2_keep_alive_timeout_in_secs = 20;
    settings.max_frame_size = 12582912;
    settings.initial_connection_window_size = 12582912;
    settings.initial_stream_window_size = 12582912;
    settings.buffer_size = 65536;

    let r = HealthChecker::check_peer_is_ready(
        MockNode::tcp_addr_to_http_addr(peer_addr.to_string()),
        settings,
        "rpc_service.RpcService".to_string(),
    )
    .await;
    assert!(r.is_ok());
    tx.send(()).expect("should succeed");
}
