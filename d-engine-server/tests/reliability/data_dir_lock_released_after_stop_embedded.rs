use std::time::Duration;

use d_engine_server::DefaultEmbeddedEngine;

use crate::common::get_available_ports;

/// Purpose: after a clean `stop()` (not a crash), the lock must release so a
/// restarted node can reopen the same data_dir. This is the common case —
/// most restarts are graceful. It also proves `stop()` truly drops every
/// `Arc<Node<T>>` clone; a lingering background-task reference would keep
/// the lock held and make this fail.
#[tokio::test]
async fn test_restart_after_clean_stop_reacquires_lock() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut ports = get_available_ports(2).await;
    ports.release_listeners();

    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var(
            "RAFT__CLUSTER__LISTEN_ADDRESS",
            format!("127.0.0.1:{}", ports[0]),
        );
    }
    let engine1 = DefaultEmbeddedEngine::start(temp_dir.path())
        .await
        .expect("first start must succeed");
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }
    engine1.wait_ready(Duration::from_secs(5)).await.expect("first engine ready");
    engine1.stop().await.expect("clean stop must succeed");

    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var(
            "RAFT__CLUSTER__LISTEN_ADDRESS",
            format!("127.0.0.1:{}", ports[1]),
        );
    }
    let engine2 = DefaultEmbeddedEngine::start(temp_dir.path()).await;
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }

    assert!(
        engine2.is_ok(),
        "restart after clean stop must succeed: {:?}",
        engine2.err()
    );
    engine2.unwrap().stop().await.ok();
}
