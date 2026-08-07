//! Integration test for the SM `last_applied` / SM-data atomicity fix (#422 follow-up).
//!
//! `apply_chunk()` now writes `last_applied_index`/`term` into the SAME RocksDB
//! `WriteBatch` as the SM data mutations, so the two can never diverge on disk.
//!
//! Single-node cluster: with no peer to replicate from, whatever survives a
//! crash depends entirely on this node's own storage. In a multi-node cluster
//! this class of bug is masked by Raft replication — a crashed node's local
//! state gets fully overwritten by the surviving majority regardless of what
//! it persisted locally, so it cannot be exercised there.
//!
//! Note: this test cannot prove the fix by failing on the pre-fix code. RocksDB
//! auto-flushes MemTables holding `disableWAL` data whenever `Close()` or
//! `CancelAllBackgroundWork()` runs (`avoid_flush_during_shutdown=false`) —
//! and `close_db_for_crash_simulation()` calls the latter. That safety net
//! papers over the pre-fix bug in any crash simulation available to us, so it
//! passes both before and after the fix. It is kept as end-to-end behavioral
//! coverage of the fixed mechanism, not as a regression guard.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::StateMachine;
use d_engine_server::api::DefaultEmbeddedEngine;
use tracing_test::traced_test;

use crate::common::create_node_config;
use crate::common::get_available_ports;

/// Recursively copies `src` into `dst`, creating `dst` if needed.
fn copy_dir_all(
    src: &std::path::Path,
    dst: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&entry.path(), &dst_path)?;
        } else {
            std::fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// Starts a single-node embedded engine with directly-held `storage`/`sm`
/// handles. Returns the on-disk DB path (derived from the generated config)
/// alongside the engine and handles.
async fn start_single_node(
    temp_dir: &std::path::Path,
    config_path: &str,
    listen_port: u16,
) -> Result<
    (
        DefaultEmbeddedEngine,
        Arc<d_engine_server::RocksDBStorageEngine>,
        Arc<d_engine_server::RocksDBStateMachine>,
        std::path::PathBuf,
    ),
    Box<dyn std::error::Error>,
> {
    let db_root = temp_dir.join("db_root");
    let log_dir = temp_dir.join("logs");
    let config_str = create_node_config(
        1,
        listen_port,
        &[listen_port],
        db_root.to_str().unwrap(),
        log_dir.to_str().unwrap(),
    )
    .await;
    tokio::fs::write(config_path, &config_str).await?;

    let db_path = db_root.join("node1").join("db");
    tokio::fs::create_dir_all(&db_path).await?;

    let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
    let storage_arc = Arc::new(storage);
    let sm_arc = Arc::new(sm);

    let engine = DefaultEmbeddedEngine::start_custom(
        &db_path,
        Arc::clone(&storage_arc),
        Arc::clone(&sm_arc),
        Some(config_path),
    )
    .await?;
    engine.wait_ready(Duration::from_secs(10)).await?;

    Ok((engine, storage_arc, sm_arc, db_path))
}

/// A real crash (no graceful shutdown) immediately after an explicit `flush()`
/// must preserve SM data and `last_applied` exactly together — not one without
/// the other.
///
/// `get_linearizable()` on the last write guarantees the SM has fully applied
/// the whole batch (both the data and the atomically-written last_applied
/// metadata are already in the MemTable) before `flush_async()` runs.
#[tokio::test]
#[traced_test]
async fn test_flush_then_crash_preserves_data_and_last_applied_together()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let config_path = "/tmp/d-engine-test-sm-atomicity-crash.toml";

    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];

    let (engine, storage_arc, sm_arc, db_path) =
        start_single_node(temp_dir.path(), config_path, port).await?;

    const N: u8 = 5;
    for i in 0..N {
        engine
            .client()
            .put(
                format!("key-{i:02}").into_bytes(),
                format!("val-{i:02}").into_bytes(),
            )
            .await?;
    }
    // Guarantees the SM has applied every write above before flush_async() runs.
    let last = engine
        .client()
        .get_linearizable(format!("key-{:02}", N - 1).into_bytes())
        .await?;
    assert_eq!(
        last.as_deref(),
        Some(format!("val-{:02}", N - 1).as_bytes())
    );

    sm_arc.flush_async().await?;
    let last_applied_after_flush = sm_arc.last_applied().index;

    // True crash: no graceful close, no further flush.
    sm_arc.close_db_for_crash_simulation();
    let snapshot_path = temp_dir.path().join("snapshot");
    copy_dir_all(&db_path, &snapshot_path)?;
    drop(storage_arc);
    drop(sm_arc);
    // sm's db slot is already cleared — stop()'s internal close_db() finds
    // nothing to flush and errors out harmlessly (matches the pattern used in
    // sm_wal_disabled_crash_recovery_embedded.rs).
    let _ = engine.stop().await;

    let (_recovered_storage, recovered_sm) = RocksDBUnifiedEngine::open(&snapshot_path)?;
    assert_eq!(
        recovered_sm.last_applied().index,
        last_applied_after_flush,
        "last_applied must survive exactly as flushed"
    );
    for i in 0..N {
        let key = format!("key-{i:02}");
        assert_eq!(
            recovered_sm.get(key.as_bytes())?,
            Some(Bytes::from(format!("val-{i:02}").into_bytes())),
            "key {key} was applied before flush_async() — must survive the crash"
        );
    }

    Ok(())
}
