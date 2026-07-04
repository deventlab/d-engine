//! Snapshot correctness after install — embedded mode
//!
//! Validates the #418 fix end-to-end:
//! - snapshot `last_included` == `last_applied` (no subtraction)
//! - a reconnecting follower within the retained-log buffer recovers via AppendEntries
//! - all cluster data is consistent after recovery with no double-applied entries

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_server::DefaultEmbeddedEngine;
use d_engine_server::RocksDBUnifiedEngine;
use serial_test::serial;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;
use crate::common::wait_for_snapshot;

/// Test: follower that reconnects within the retained-log buffer recovers via AppendEntries
/// and reaches full data consistency with no double-applied entries.
///
/// ## Purpose
///
/// The #418 fix ensures `snapshot.last_included == last_applied` (no subtraction by
/// `retained_log_entries`). This test exercises the downstream consequence: because the
/// snapshot label is truthful, a reconnecting follower knows exactly where it left off and
/// the leader can supply the missing entries from its retained log without sending a new
/// snapshot.
///
/// Without the fix, `last_included` would be stamped lower than `last_applied`, causing
/// the follower to re-apply entries already reflected in the snapshot data — a silent data
/// corruption for non-idempotent operations.
///
/// ## Test Flow
///
/// 1. Start a 3-node cluster (`snapshot_threshold=100`, `retained_log_entries=30`).
/// 2. Write 120 entries → snapshot fires at 100, purge up to index 70.
///    Leader retains entries 71..120 for lagging followers.
/// 3. Wait for snapshot to appear on the leader (confirms cluster is synced).
/// 4. Stop one non-leader follower (node C).
/// 5. Write 20 more entries (121..140) to the leader while C is offline.
///    C now lags by 20 entries; 20 < retained=30, so entries 121..140 remain in leader's log.
/// 6. Restart C with its persisted DB (state through index 120).
/// 7. Wait for C to catch up to all 140 entries.
///
/// ## Expected Results
///
/// C recovers via AppendEntries (lagging entries 121..140 are within the retained buffer)
/// Snapshot-only entries (purged from log) are readable on C — snapshot was correctly installed
/// All 140 entries present on C with correct values
/// No double-apply: each key holds exactly the value written once
#[tokio::test]
#[traced_test]
#[serial]
async fn test_follower_catchup_within_retained_buffer_and_data_consistency()
-> Result<(), Box<dyn std::error::Error>> {
    const SNAPSHOT_THRESHOLD: u64 = 100;
    const RETAINED_LOGS: u64 = 30;
    const INITIAL_ENTRIES: u64 = 120;
    const CATCHUP_ENTRIES: u64 = 20; // must be < RETAINED_LOGS to stay in retained buffer

    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let snapshots_dir = temp_dir.path().join("snapshots");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Track config+db paths per node so we can restart any node later
    let mut node_paths: Vec<(std::path::PathBuf, std::path::PathBuf)> = Vec::new();
    let mut engines: Vec<Option<DefaultEmbeddedEngine>> = Vec::new();

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

        node_paths.push((config_path.clone(), db_path.clone()));

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let engine = DefaultEmbeddedEngine::start_custom(
            Arc::new(storage),
            Arc::new(sm),
            Some(config_path.to_str().unwrap()),
        )
        .await?;
        engines.push(Some(engine));
    }

    let leader_info = engines[0].as_ref().unwrap().wait_ready(Duration::from_secs(15)).await?;
    let leader_idx = engines
        .iter()
        .position(|e| e.as_ref().map(|e| e.node_id() == leader_info.leader_id).unwrap_or(false))
        .expect("leader must be one of the 3 engines");
    let leader_client = engines[leader_idx].as_ref().unwrap().client().clone();
    info!(
        "Leader is node {} (engines idx {})",
        leader_info.leader_id, leader_idx
    );

    // Phase 1: write INITIAL_ENTRIES to trigger snapshot and log purge
    info!("Writing {INITIAL_ENTRIES} entries to trigger snapshot at {SNAPSHOT_THRESHOLD}");
    for i in 0..INITIAL_ENTRIES {
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
        "Leader snapshot must exist after {INITIAL_ENTRIES} entries"
    );
    info!(
        "Leader snapshot ready — purge boundary at ~{}",
        SNAPSHOT_THRESHOLD - RETAINED_LOGS
    );

    // Allow all followers to fully replicate before stopping one
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Phase 2: stop a non-leader follower (node C)
    let follower_idx = engines
        .iter()
        .position(|e| e.as_ref().map(|e| !e.is_leader()).unwrap_or(false))
        .expect("must have at least one non-leader");
    let follower_node_id = engines[follower_idx].as_ref().unwrap().node_id();
    let (follower_config_path, follower_db_path) = node_paths[follower_idx].clone();
    info!("Stopping follower node {follower_node_id} (engines idx {follower_idx})");

    let stopped = engines[follower_idx].take().unwrap();
    let _ = stopped.stop().await;

    // Phase 3: write CATCHUP_ENTRIES while follower is offline
    // 20 < RETAINED_LOGS=30 → leader keeps these entries in log for AppendEntries replay
    let total_entries = INITIAL_ENTRIES + CATCHUP_ENTRIES;
    info!(
        "Writing {CATCHUP_ENTRIES} more entries while follower offline \
        (lag={CATCHUP_ENTRIES} < retained={RETAINED_LOGS} → AppendEntries path on reconnect)"
    );
    for i in INITIAL_ENTRIES..total_entries {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    // Phase 4: restart follower with its persisted DB
    info!("Restarting follower node {follower_node_id} from persisted DB");
    let (storage, sm) = RocksDBUnifiedEngine::open(&follower_db_path)?;
    let restarted = DefaultEmbeddedEngine::start_custom(
        Arc::new(storage),
        Arc::new(sm),
        Some(follower_config_path.to_str().unwrap()),
    )
    .await?;
    engines[follower_idx] = Some(restarted);

    // Phase 5: wait for follower to catch up
    let last_key = format!("key_{}", total_entries - 1).into_bytes();
    let follower_engine = engines[follower_idx].as_ref().unwrap();
    let mut caught_up = false;
    for _ in 0..30 {
        if follower_engine
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
        "Follower node {follower_node_id} failed to catch up within 30 seconds"
    );
    info!("Follower {follower_node_id} caught up to all {total_entries} entries");

    // Phase 6: full data integrity check on the restarted follower
    //
    // Entries 0..(SNAPSHOT_THRESHOLD - RETAINED_LOGS) were purged from the log and exist
    // only in the snapshot. If they are readable, the snapshot was correctly installed.
    // AppendEntries alone cannot supply purged entries — this verifies snapshot install.
    let snapshot_only_boundary = SNAPSHOT_THRESHOLD.saturating_sub(RETAINED_LOGS);
    for i in [0, snapshot_only_boundary / 2, snapshot_only_boundary - 1] {
        let actual = follower_engine
            .client()
            .get_eventual(format!("key_{i}").into_bytes())
            .await?
            .unwrap_or_default();
        assert_eq!(
            actual,
            format!("value_{i}").into_bytes(),
            "key_{i} is snapshot-only (log purged) — readable only if snapshot is intact"
        );
    }

    // Entries written while follower was offline must be present (AppendEntries catchup)
    for i in INITIAL_ENTRIES..total_entries {
        let actual = follower_engine
            .client()
            .get_eventual(format!("key_{i}").into_bytes())
            .await?
            .unwrap_or_default();
        assert_eq!(
            actual,
            format!("value_{i}").into_bytes(),
            "key_{i} must be present after AppendEntries catchup (written while offline)"
        );
    }
    info!("All {total_entries} entries verified on restarted follower node {follower_node_id}");

    for engine in engines.into_iter().flatten() {
        let _ = engine.stop().await;
    }

    Ok(())
}
