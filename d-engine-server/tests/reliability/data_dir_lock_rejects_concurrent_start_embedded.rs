use std::time::Duration;

use d_engine_server::DefaultEmbeddedEngine;

use crate::common::get_available_ports;

/// Purpose: a second `start()` on an already-locked data_dir must fail
/// cleanly (no panic, no hang), and the first, already-running engine must
/// stay fully functional — the failed second attempt must not affect it.
/// This is the copy-paste-shared-directory mistake the lock exists to catch.
#[tokio::test]
async fn test_second_start_on_same_data_dir_fails_first_unaffected() {
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
    engine1
        .wait_ready(Duration::from_secs(5))
        .await
        .expect("first engine must become ready");

    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var(
            "RAFT__CLUSTER__LISTEN_ADDRESS",
            format!("127.0.0.1:{}", ports[1]),
        );
    }
    let second = DefaultEmbeddedEngine::start(temp_dir.path()).await;
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }
    // Must fail with the data_dir lock's own error — not a RocksDB-level `LOCK`
    // error. This proves the lock is acquired (and checked) before storage is
    // ever touched, giving a clear rejection instead of a confusing DB error.
    match second {
        Ok(_) => panic!("second start on a locked data_dir must fail"),
        Err(d_engine_core::Error::System(d_engine_core::SystemError::NodeStartFailed(msg))) => {
            assert!(
                msg.contains("already in use"),
                "expected a data_dir-in-use message, got: {msg}"
            );
        }
        Err(other) => panic!(
            "expected Err(SystemError::NodeStartFailed(_)) from the data_dir lock, got: {other:?}"
        ),
    }

    // The failed second attempt must not have touched the first engine's state.
    let client = engine1.client();
    let put = client.put(b"k".to_vec(), b"v".to_vec()).await;
    assert!(
        put.is_ok(),
        "first engine must remain usable: {:?}",
        put.err()
    );

    engine1.stop().await.ok();
}
