use d_engine_server::DefaultEmbeddedEngine;

use crate::common::get_available_ports;

/// The data_dir lock is acquired before storage is opened. If storage init
/// then fails, the lock must still be released — otherwise a single bad
/// storage error would permanently wedge the data_dir, blocking even a
/// corrected retry.
///
/// Trigger: pre-create a regular file at `data_dir/storage` — `DataDir`'s
/// directory validation (which normally creates the subdirectory RocksDB
/// needs) fails on it, since a file can't be written into like a directory.
/// This failure happens after the lock is acquired but before RocksDB is
/// ever touched, exactly the window the expert review flagged.
#[tokio::test]
async fn test_lock_released_after_storage_init_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        temp_dir.path().join("storage"),
        b"blocks storage subdir creation",
    )
    .unwrap();

    let mut ports = get_available_ports(2).await;
    ports.release_listeners();

    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "1");
        std::env::set_var(
            "RAFT__CLUSTER__LISTEN_ADDRESS",
            format!("127.0.0.1:{}", ports[0]),
        );
    }
    let first = DefaultEmbeddedEngine::start(temp_dir.path()).await;
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
        std::env::remove_var("RAFT__CLUSTER__LISTEN_ADDRESS");
    }
    assert!(
        first.is_err(),
        "start must fail: storage subdir creation is blocked by a file"
    );
    // Must NOT be the data_dir-lock-conflict error — that would mean storage
    // init never even ran, defeating the point of this test.
    assert!(
        !matches!(
            first,
            Err(d_engine_core::Error::System(
                d_engine_core::SystemError::NodeStartFailed(_)
            ))
        ),
        "first attempt must fail on storage init itself, not a lock conflict: {:?}",
        first.err()
    );

    // Second attempt, same data_dir, still blocked by the same file. If the
    // first attempt's lock leaked, this fails with the lock-conflict error
    // instead of the same storage error — proving release (or not).
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
    match second {
        Ok(_) => panic!("start should still fail: the blocking file is still in place"),
        Err(d_engine_core::Error::System(d_engine_core::SystemError::NodeStartFailed(msg))) => {
            panic!("lock from the first failed attempt was never released: {msg}");
        }
        Err(_) => {
            // Same storage-init failure as the first attempt — the lock was released.
        }
    }
}
