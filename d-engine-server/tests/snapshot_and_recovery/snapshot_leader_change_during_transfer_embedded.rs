//! Snapshot transfer resilience under leader change (Embedded mode)
//!
//! Verifies that a Learner behind the log-purge boundary successfully catches up
//! after its snapshot source leader dies: the new leader detects the Learner is
//! still behind and delivers the snapshot, eventually bringing the Learner up to date.

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

/// Test: Learner catches up after its snapshot source leader dies during delivery.
///
/// ## Scenario
///
/// A Learner joins a cluster whose log has already been purged (needs snapshot).
/// Once the Learner is connected and syncing, the current leader is dropped
/// (simulating a crash).  The two surviving voters elect a new leader, which
/// detects the Learner is still behind and delivers the snapshot from scratch.
///
/// ## Test Flow
///
/// 1. Start 3-node cluster (snapshot_threshold = 50, retained = 20).
/// 2. Write 80 entries — triggers snapshot + log purge on all nodes.
/// 3. Start Learner Node 4 (empty DB, needs snapshot because behind purge boundary).
/// 4. Wait for Learner to connect and be ready (`wait_ready`).
/// 5. Drop the current leader engine (simulates crash during snapshot delivery).
/// 6. Poll surviving engines until a new leader different from the old one is found.
/// 7. Wait for Learner to catch up to all 80 entries.
/// 8. Verify snapshot-only entries on the Learner.
///
/// ## Expected Results
///
/// ✅ Learner recovers snapshot from the new leader
/// ✅ Snapshot-only entries (purged from log) readable on the Learner
/// ✅ Cluster maintains quorum throughout (2 of 3 original voters survive)
#[tokio::test]
#[traced_test]
#[serial]
async fn test_snapshot_transfer_resumes_after_leader_change()
-> Result<(), Box<dyn std::error::Error>> {
    const SNAPSHOT_THRESHOLD: u64 = 50;
    const RETAINED_LOGS: u64 = 20;
    const BASELINE_ENTRIES: u64 = 80;

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

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

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
    let old_leader_id = engines[leader_idx].node_id();
    let leader_client = engines[leader_idx].client().clone();

    info!("Writing {BASELINE_ENTRIES} entries to trigger snapshot + log purge");
    for i in 0..BASELINE_ENTRIES {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    assert!(
        wait_for_snapshot(
            &snapshots_dir,
            old_leader_id as u64,
            Duration::from_secs(15)
        )
        .await,
        "Leader snapshot must exist before learner starts"
    );
    info!("Leader (Node {old_leader_id}) snapshot ready, log purge in effect");

    // Start Learner Node 4 — it is behind the purge boundary and needs a snapshot
    let learner_config = format!(
        r#"
[cluster]
node_id = 4
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 4, name = 'n4', address = '127.0.0.1:{}', role = 4, status = 2 }}
]
db_root_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

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
        snapshots_dir.join("node4").display(),
    );

    let learner_config_path = temp_dir.path().join("node4.toml");
    tokio::fs::write(&learner_config_path, &learner_config).await?;

    let learner_db_path = db_root_dir.join("node4/db");
    tokio::fs::create_dir_all(&learner_db_path).await?;
    tokio::fs::create_dir_all(snapshots_dir.join("node4")).await?;

    let (learner_storage, learner_sm) = RocksDBUnifiedEngine::open(&learner_db_path)?;
    let learner_engine = EmbeddedEngine::start_custom(
        Arc::new(learner_storage),
        Arc::new(learner_sm),
        Some(learner_config_path.to_str().unwrap()),
    )
    .await?;
    info!("Learner Node 4 started — waiting for it to connect before killing leader");

    // Wait until the Learner is connected and the cluster has a leader visible to it.
    // This is a reliable observable condition: on any machine speed, we know the Learner
    // is actively receiving data. Whether the snapshot transfer is in-flight or already
    // complete at the moment of the kill, the recovery invariant still holds.
    let _ = learner_engine.wait_ready(Duration::from_secs(15)).await;

    // Stop the current leader — simulates a crash during snapshot delivery.
    // Explicitly awaiting stop() ensures the leader's background tasks are fully
    // shut down before the surviving nodes proceed to elect a new leader.
    info!("Stopping leader Node {old_leader_id} to simulate crash during snapshot delivery");
    let old_leader = engines.remove(leader_idx);
    old_leader.stop().await?;

    // Wait for one of the surviving nodes to become the new leader
    let mut new_leader_id = 0u32;
    for _ in 0..30 {
        for engine in &engines {
            if engine.is_leader() && engine.node_id() != old_leader_id {
                new_leader_id = engine.node_id();
                break;
            }
        }
        if new_leader_id != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_ne!(
        new_leader_id, 0,
        "New leader must be elected within 30 seconds"
    );
    info!("New leader elected: Node {new_leader_id}");

    // Wait for Learner to catch up from the new leader
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
    assert!(
        caught_up,
        "Learner failed to catch up within 30 seconds after leader change"
    );
    info!("Learner caught up to all {BASELINE_ENTRIES} entries after leader change");

    // Verify snapshot-only entries from Learner directly.
    // With retained_log_entries = 20 and snapshot_threshold = 50, entries in
    // 0..(SNAPSHOT_THRESHOLD - RETAINED_LOGS) are purged from the log and exist
    // only in the snapshot. Readable on the Learner only if the snapshot was correctly
    // applied — AppendEntries alone cannot account for them.
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
        "Snapshot-only entries [0, {}, {}] verified on Learner Node 4",
        snapshot_only_boundary / 2,
        snapshot_only_boundary - 1
    );

    let mut stop_err: Option<Box<dyn std::error::Error>> = None;
    for engine in &engines {
        if let Err(e) = engine.stop().await {
            stop_err = Some(e.into());
        }
    }
    if let Err(e) = learner_engine.stop().await {
        stop_err = Some(e.into());
    }
    if let Some(e) = stop_err {
        return Err(e);
    }

    Ok(())
}
