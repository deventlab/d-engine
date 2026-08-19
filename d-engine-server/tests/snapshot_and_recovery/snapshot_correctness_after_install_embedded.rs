//! Snapshot correctness after install — embedded mode
//!
//! Validates the #418 fix end-to-end:
//! - snapshot `last_included` == `last_applied` (no subtraction)
//! - a reconnecting follower within the retained-log buffer recovers via AppendEntries
//! - all cluster data is consistent after recovery with no double-applied entries

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_core::capture_logs_globally_filtered;
use d_engine_core::logs_contain_globally;
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
    let data_dir = temp_dir.path().join("db");

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

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
"#,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
        );

        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, &config).await?;

        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;

        node_paths.push((config_path.clone(), db_path.clone()));

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let engine = DefaultEmbeddedEngine::start_custom(
            &db_path,
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
    let leader_snapshots_dir = data_dir.join(format!("node{leader_id}/db/snapshots"));
    assert!(
        wait_for_snapshot(&leader_snapshots_dir, Duration::from_secs(15)).await,
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
        &follower_db_path,
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

/// Test: follower that reconnects BEYOND the retained-log buffer must recover via
/// InstallSnapshot, not AppendEntries.
///
/// ## Purpose (ticket #436)
///
/// This is the companion scenario to `test_follower_catchup_within_retained_buffer_and_data_consistency`
/// above — same judgment point (`prev_log_term` lookup deciding AppendEntries vs snapshot),
/// opposite branch. A manual, larger-scale repro (`deventlab-product-design` snapshot-test.sh
/// scenario_4) found that a follower whose lag exceeds the retained-log window can still end
/// up "catching up" through ordinary AppendEntries instead of `InstallSnapshot` — the data
/// happened to come out correct, but the mechanism was wrong, and it stayed wrong through a
/// leader-killed-and-reconnected cycle without any test catching it. See
/// `d-engine/tickets/milestones/v0.2.5/436-plan-prev-log-term-fallback.md`.
///
/// ## Test Flow
///
/// 1. Start a 3-node cluster (`snapshot_threshold=20`, `retained_log_entries=5`).
/// 2. Write 5 entries so all 3 nodes are in sync at index 5.
/// 3. Stop one non-leader follower (node C) — it is now frozen at index 5.
/// 4. Write 40 more entries (index 6..45) to the leader while C is offline. This crosses
///    the snapshot threshold (20), so the leader's retained window ends up starting well
///    above index 5 — C's frozen position is now behind the purge boundary, not just behind
///    the leader.
/// 5. Restart C with its persisted DB and wait for it to catch up to all 45 entries.
///
/// ## Expected Results (this is the assertion that must currently FAIL before #436 is fixed)
///
/// The follower's own tracing output must contain evidence that it actually received and
/// applied a snapshot stream — not just that its data happens to be correct at the end.
#[tokio::test]
#[serial]
async fn test_follower_catchup_beyond_retained_buffer_requires_install_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );
    const SNAPSHOT_THRESHOLD: u64 = 20;
    const RETAINED_LOGS: u64 = 5;
    const INITIAL_ENTRIES: u64 = 5;
    const OFFLINE_ENTRIES: u64 = 40; // pushes last_applied well past SNAPSHOT_THRESHOLD

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

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

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
snapshot_cool_down_since_last_check = {{ secs = 0 }}
"#,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
        );

        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, &config).await?;

        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;

        node_paths.push((config_path.clone(), db_path.clone()));

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let engine = DefaultEmbeddedEngine::start_custom(
            &db_path,
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

    // Phase 1: small baseline, all 3 nodes in sync.
    for i in 0..INITIAL_ENTRIES {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 2: stop a non-leader follower (node C) — frozen at index INITIAL_ENTRIES.
    let follower_idx = engines
        .iter()
        .position(|e| e.as_ref().map(|e| !e.is_leader()).unwrap_or(false))
        .expect("must have at least one non-leader");
    let follower_node_id = engines[follower_idx].as_ref().unwrap().node_id();
    let (follower_config_path, follower_db_path) = node_paths[follower_idx].clone();
    info!("Stopping follower node {follower_node_id}, frozen at index {INITIAL_ENTRIES}");

    let stopped = engines[follower_idx].take().unwrap();
    let _ = stopped.stop().await;

    // Phase 3: write enough entries while offline to cross the snapshot threshold and
    // push the purge boundary past the follower's frozen position.
    let total_entries = INITIAL_ENTRIES + OFFLINE_ENTRIES;
    info!(
        "Writing {OFFLINE_ENTRIES} entries while follower offline \
        (crosses snapshot_threshold={SNAPSHOT_THRESHOLD} > follower's frozen index={INITIAL_ENTRIES})"
    );
    for i in INITIAL_ENTRIES..total_entries {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    let leader_id = leader_info.leader_id as u64;
    let leader_snapshots_dir = data_dir.join(format!("node{leader_id}/db/snapshots"));
    assert!(
        wait_for_snapshot(&leader_snapshots_dir, Duration::from_secs(15)).await,
        "Leader snapshot must exist after {total_entries} entries"
    );

    // Phase 4: restart follower with its persisted DB (still only has entries 0..INITIAL_ENTRIES).
    info!("Restarting follower node {follower_node_id} from persisted DB");
    let (storage, sm) = RocksDBUnifiedEngine::open(&follower_db_path)?;
    let restarted = DefaultEmbeddedEngine::start_custom(
        &follower_db_path,
        Arc::new(storage),
        Arc::new(sm),
        Some(follower_config_path.to_str().unwrap()),
    )
    .await?;
    engines[follower_idx] = Some(restarted);

    // Phase 5: wait for follower to catch up to all entries.
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

    // Phase 6: data correctness (necessary but NOT sufficient — see phase 7).
    for i in 0..total_entries {
        let actual = follower_engine
            .client()
            .get_eventual(format!("key_{i}").into_bytes())
            .await?
            .unwrap_or_default();
        assert_eq!(
            actual,
            format!("value_{i}").into_bytes(),
            "key_{i} must be present and correct after catchup"
        );
    }

    // Phase 7: THE assertion that actually catches ticket #436. Data being correct is not
    // enough — #436 was found precisely because a follower's data came out correct while
    // InstallSnapshot was never invoked (it "caught up" through AppendEntries instead, which
    // should have been impossible once its lag exceeded the retained-log window). This checks
    // the follower's own tracing output for proof that it actually went through the snapshot
    // receive path, not just that the end state happens to look right.
    assert!(
        logs_contain_globally(&logs, "Snapshot stream successfully received and applied"),
        "follower's lag ({OFFLINE_ENTRIES}) exceeded the retained-log window ({RETAINED_LOGS}) \
        after the leader purged past its frozen index — it MUST have caught up via \
        InstallSnapshot, not AppendEntries. If this fails, the data-correctness assertions in \
        phase 6 may still have passed by accident (see ticket #436): the follower can end up \
        with correct data through a wrong mechanism, which does not protect against a follower \
        that falls behind a gap AppendEntries genuinely cannot supply."
    );

    for engine in engines.into_iter().flatten() {
        let _ = engine.stop().await;
    }

    Ok(())
}

/// Test: a new learner (empty log) joining a leader that has already compacted
/// (snapshot + purge) must catch up via InstallSnapshot, not AppendEntries.
///
/// This pins the PULL-removal regression: before #436, a learner fetched its
/// initial snapshot eagerly on startup. After removing that path, the learner
/// relies on the leader's PUSH replication loop (AppendEntries → reject-retreat
/// → NeedSnapshot → InstallSnapshot). A brand-new learner with an empty log joins
/// a leader whose retained log no longer reaches index 1, so AppendEntries cannot
/// supply the gap — it MUST receive a full snapshot.
#[tokio::test]
#[serial]
async fn test_learner_join_after_compaction_requires_install_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );
    const SNAPSHOT_THRESHOLD: u64 = 20;
    const RETAINED_LOGS: u64 = 5;
    const INITIAL_ENTRIES: u64 = 50;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(2).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Single-node leader with a small snapshot threshold so compaction kicks in fast.
    let leader_config = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 3, status = 3 }}
]

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
snapshot_cool_down_since_last_check = {{ secs = 0 }}
"#,
        ports[0], ports[0]
    );
    let leader_config_path = temp_dir.path().join("leader.toml");
    tokio::fs::write(&leader_config_path, &leader_config).await?;
    let leader_db = data_dir.join("leader/db");
    tokio::fs::create_dir_all(&leader_db).await?;
    let (storage, sm) = RocksDBUnifiedEngine::open(&leader_db)?;
    let leader = DefaultEmbeddedEngine::start_custom(
        &leader_db,
        Arc::new(storage),
        Arc::new(sm),
        Some(leader_config_path.to_str().unwrap()),
    )
    .await?;
    leader.wait_ready(Duration::from_secs(10)).await?;

    // Write enough entries to cross the snapshot threshold and compact the log.
    for i in 0..INITIAL_ENTRIES {
        leader
            .client()
            .put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }
    assert!(
        wait_for_snapshot(&leader_db.join("snapshots"), Duration::from_secs(15)).await,
        "leader snapshot must exist after {INITIAL_ENTRIES} entries"
    );

    // New learner joining the already-compacted leader.
    let learner_config = format!(
        r#"
[cluster]
node_id = 2
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 3, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 4, status = 1 }}
]

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
snapshot_cool_down_since_last_check = {{ secs = 0 }}
"#,
        ports[1], ports[0], ports[1]
    );
    let learner_config_path = temp_dir.path().join("learner.toml");
    tokio::fs::write(&learner_config_path, &learner_config).await?;
    let learner_db = data_dir.join("learner/db");
    tokio::fs::create_dir_all(&learner_db).await?;
    let (storage, sm) = RocksDBUnifiedEngine::open(&learner_db)?;
    let learner = DefaultEmbeddedEngine::start_custom(
        &learner_db,
        Arc::new(storage),
        Arc::new(sm),
        Some(learner_config_path.to_str().unwrap()),
    )
    .await?;

    // Wait for the learner to catch up to the last written key.
    let last_key = format!("key_{}", INITIAL_ENTRIES - 1).into_bytes();
    let mut caught_up = false;
    for _ in 0..30 {
        if learner.client().get_eventual(last_key.clone()).await.ok().flatten().is_some() {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(caught_up, "learner failed to catch up within 30s");

    // The learner's empty log is far behind the purge boundary, so AppendEntries
    // cannot supply the gap — it MUST have caught up via InstallSnapshot.
    assert!(
        logs_contain_globally(&logs, "Snapshot stream successfully received and applied"),
        "learner with empty log joining a compacted leader MUST catch up via \
        InstallSnapshot, not AppendEntries"
    );

    let _ = learner.stop().await;
    let _ = leader.stop().await;
    Ok(())
}
