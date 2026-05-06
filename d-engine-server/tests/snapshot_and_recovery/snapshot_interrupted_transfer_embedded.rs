//! Snapshot interrupted transfer recovery tests (Embedded mode)
//!
//! Verifies that a stale `temp-snapshot.part.tar.gz` left on disk from a
//! previously interrupted transfer is truncated (not appended to) when a
//! new SnapshotAssembler starts, producing a clean snapshot on retry.
//!
//! This is the end-to-end integration counterpart of the unit test
//! `test_assembler_on_stale_temp_file_does_not_append_old_data`.

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_server::EmbeddedEngine;
use d_engine_server::RocksDBUnifiedEngine;
use serial_test::serial;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;
use crate::common::wait_for_snapshot;

/// Test: Learner recovers from stale snapshot temp file left by prior interrupted transfer.
///
/// ## Scenario
///
/// Before the Learner starts, a garbage `temp-snapshot.part.tar.gz` is injected into
/// its snapshot directory — exactly what happens when a transfer is interrupted mid-way
/// and the process is restarted.  Without the fix (append mode), the new transfer would
/// append fresh chunks to the stale data, producing a corrupted tar.gz that fails to
/// decompress.  With the fix (truncate mode), the assembler overwrites the stale file
/// and the Learner catches up cleanly.
///
/// ## Test Flow
///
/// 1. Start 3-node cluster (snapshot_threshold = 100).
/// 2. Write 150 entries — triggers snapshot + log purge.
/// 3. Inject 4 KB of garbage bytes as `temp-snapshot.part.tar.gz` in Node 4's snapshot dir.
/// 4. Start Learner Node 4 (stale file already present, simulates restart after failure).
/// 5. Wait for Learner to catch up to all 150 entries.
/// 6. Verify stale temp file has been replaced by the final snapshot file.
/// 7. Verify snapshot-only entries (purged from log) are readable on the Learner.
///
/// ## Expected Results
///
/// ✅ Learner successfully applies snapshot despite stale temp file
/// ✅ All 150 entries present with correct values
/// ✅ `temp-snapshot.part.tar.gz` replaced — no corruption
#[tokio::test]
#[traced_test]
#[serial]
async fn test_snapshot_receiver_truncates_stale_temp_file() -> Result<(), Box<dyn std::error::Error>>
{
    const SNAPSHOT_THRESHOLD: u64 = 100;
    const RETAINED_LOGS: u64 = 50;
    const BASELINE_ENTRIES: u64 = 150;

    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let snapshots_dir = temp_dir.path().join("snapshots");

    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Start 3-node cluster
    let mut engines = Vec::new();
    for node_id in 1u64..=3 {
        let config = format!(
            r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }}
]
db_root_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
snapshots_dir = '{}'
"#,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
            db_root_dir.join(format!("node{node_id}")).display(),
            snapshots_dir.join(format!("node{node_id}")).display(),
        );

        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, &config).await?;

        let db_path = db_root_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;
        tokio::fs::create_dir_all(snapshots_dir.join(format!("node{node_id}"))).await?;

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let engine = EmbeddedEngine::start_custom(
            Arc::new(storage),
            Arc::new(sm),
            Some(config_path.to_str().unwrap()),
        )
        .await?;
        engines.push(engine);
    }

    let leader_info = engines[0].wait_ready(Duration::from_secs(15)).await?;
    let leader_idx = engines
        .iter()
        .position(|e| e.node_id() == leader_info.leader_id)
        .expect("leader must be one of the 3 engines");
    let leader_client = engines[leader_idx].client().clone();

    info!("Writing {BASELINE_ENTRIES} entries to trigger snapshot and log purge");
    for i in 0..BASELINE_ENTRIES {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    let leader_id = leader_info.leader_id as u64;
    assert!(
        wait_for_snapshot(&snapshots_dir, leader_id, Duration::from_secs(15)).await,
        "Leader snapshot must exist before learner starts"
    );
    info!("Leader snapshot ready");

    // Inject stale temp file — simulates a Learner that was killed mid-transfer
    let node4_snap_dir = snapshots_dir.join("node4");
    tokio::fs::create_dir_all(&node4_snap_dir).await?;
    let stale_path = node4_snap_dir.join("temp-snapshot.part.tar.gz");
    tokio::fs::write(&stale_path, vec![0xAAu8; 4096]).await?;
    info!(
        "Injected stale temp file: {:?} (4096 bytes of garbage)",
        stale_path
    );

    // Start Learner Node 4 — it must truncate the stale file and apply a clean snapshot
    let learner_config = format!(
        r#"
[cluster]
node_id = 4
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 3, status = 3 }},
    {{ id = 4, name = 'n4', address = '127.0.0.1:{}', role = 4, status = 2 }}
]
db_root_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
snapshots_dir = '{}'
"#,
        ports[3],
        ports[0],
        ports[1],
        ports[2],
        ports[3],
        db_root_dir.join("node4").display(),
        node4_snap_dir.display(),
    );

    let learner_config_path = temp_dir.path().join("node4.toml");
    tokio::fs::write(&learner_config_path, &learner_config).await?;

    let learner_db_path = db_root_dir.join("node4/db");
    tokio::fs::create_dir_all(&learner_db_path).await?;

    let (learner_storage, learner_sm) = RocksDBUnifiedEngine::open(&learner_db_path)?;
    let learner_engine = EmbeddedEngine::start_custom(
        Arc::new(learner_storage),
        Arc::new(learner_sm),
        Some(learner_config_path.to_str().unwrap()),
    )
    .await?;
    info!("Learner Node 4 started with stale temp file in place");

    // Wait for Learner to catch up
    let last_key = format!("key_{}", BASELINE_ENTRIES - 1).into_bytes();
    let mut caught_up = false;
    for _ in 0..30 {
        if learner_engine
            .client()
            .get_eventual(last_key.clone())
            .await
            .ok()
            .flatten()
            .is_some()
        {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(caught_up, "Learner failed to catch up within 30 seconds");
    info!("Learner caught up to all {BASELINE_ENTRIES} entries");

    // Stale temp file must be gone — replaced by the final snapshot
    assert!(
        !stale_path.exists(),
        "stale temp-snapshot.part.tar.gz must have been replaced by the final snapshot file"
    );
    assert!(
        wait_for_snapshot(&snapshots_dir, 4, Duration::from_secs(5)).await,
        "final snapshot file must exist on Learner"
    );

    // Verify snapshot-only entries on Learner directly.
    // With retained_log_entries = 50 and snapshot_threshold = 100, entries in
    // 0..(SNAPSHOT_THRESHOLD - RETAINED_LOGS) are purged from the log and exist
    // only in the snapshot. If these are readable on the Learner, the snapshot
    // was correctly applied — AppendEntries alone cannot account for them.
    // Step 5 confirmed the Learner is fully caught up, so get_eventual reads
    // the local state machine which is guaranteed to be current.
    let snapshot_only_boundary = SNAPSHOT_THRESHOLD - RETAINED_LOGS;
    for i in [0, snapshot_only_boundary / 2, snapshot_only_boundary - 1] {
        let actual = learner_engine
            .client()
            .get_eventual(format!("key_{i}").into_bytes())
            .await?
            .unwrap_or_default();
        assert_eq!(
            actual,
            format!("value_{i}").into_bytes(),
            "key_{i} is snapshot-only — readable only if snapshot was correctly applied"
        );
    }
    info!(
        "Snapshot-only entries [0, {}, {}] verified on Learner",
        snapshot_only_boundary / 2,
        snapshot_only_boundary - 1
    );

    Ok(())
}
