//! Snapshot transfer does not block ApplyEntries (Embedded mode)
//!
//! Guards the architecture invariant that a large, slow InstallSnapshot push to a
//! lagging Learner never stalls the Leader's own commit/apply path. The slow work
//! (streaming chunks + decompression) lives in the role layer and the final install
//! lives in the Worker, so healthy nodes keep applying new writes while one peer is
//! being snapshotted.

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_server::DefaultEmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::StateMachine;
use serial_test::serial;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;
use crate::common::wait_for_snapshot;

/// Deterministic, incompressible payload so the snapshot does not shrink under gzip and
/// the throttled transfer stays meaningfully slow.
fn deterministic_value(
    seed: u64,
    len: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.push((s & 0xFF) as u8);
    }
    out
}

/// Test: a throttled snapshot push to a lagging Learner does not block Leader apply.
///
/// ## Why this is meaningful
///
/// A Learner that joins after the log has been purged must receive a full snapshot.
/// The transfer is intentionally throttled (`max_bandwidth_mbps = 1`, `push_queue_size = 1`,
/// 64 KiB chunks) so it takes seconds. While it is in flight, the Leader writes new
/// entries and must apply them without waiting for the Learner — otherwise every snapshot
/// transfer would freeze the cluster's write path for its full duration.
///
/// ## Flow
///
/// 1. Start a 3-node cluster; write enough large entries to trigger snapshot + purge.
/// 2. Record the Leader's applied index.
/// 3. Start an empty Learner (behind the purge boundary → needs a snapshot).
/// 4. Immediately write `CONCURRENT_ENTRIES` small entries to the Leader.
/// 5. Assert the Leader's applied index advances past the concurrent writes **while the
///    Learner has still applied nothing** (snapshot still in flight).
/// 6. Assert the Learner eventually catches up; then stop all engines.
#[tokio::test]
#[traced_test]
#[serial]
async fn test_snapshot_transfer_does_not_block_apply() -> Result<(), Box<dyn std::error::Error>> {
    const SNAPSHOT_THRESHOLD: u64 = 64;
    const RETAINED_LOGS: u64 = 8;
    const BASELINE_ENTRIES: u64 = 80;
    const VALUE_SIZE: usize = 16 * 1024;
    const CONCURRENT_ENTRIES: u64 = 20;
    const CHUNK_SIZE: usize = 64 * 1024;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines = Vec::new();
    let mut voter_sms = Vec::new();

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

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
chunk_size = {CHUNK_SIZE}
push_queue_size = 1
max_bandwidth_mbps = 1
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

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let sm = Arc::new(sm);
        voter_sms.push(Arc::<RocksDBStateMachine>::clone(&sm));

        let engine = DefaultEmbeddedEngine::start_custom(
            &db_path,
            Arc::new(storage),
            sm,
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
    let leader_sm = &voter_sms[leader_idx];

    info!("Writing {BASELINE_ENTRIES} large entries to trigger snapshot + purge");
    for i in 0..BASELINE_ENTRIES {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                deterministic_value(i, VALUE_SIZE),
            )
            .await?;
    }

    let leader_id = leader_info.leader_id as u64;
    let leader_snapshots_dir = data_dir.join(format!("node{leader_id}/db/snapshots"));
    assert!(
        wait_for_snapshot(&leader_snapshots_dir, Duration::from_secs(15)).await,
        "Leader snapshot must exist before the learner starts"
    );

    let baseline_applied = leader_sm.last_applied().index;
    info!("Leader applied index before learner joins: {baseline_applied}");

    // Start an empty Learner (behind the purge boundary → needs a full snapshot).
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

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
chunk_size = {CHUNK_SIZE}
push_queue_size = 1
max_bandwidth_mbps = 1
"#,
        ports[3], ports[0], ports[1], ports[2], ports[3],
    );

    let learner_config_path = temp_dir.path().join("node4.toml");
    tokio::fs::write(&learner_config_path, &learner_config).await?;

    let learner_db_path = data_dir.join("node4/db");
    tokio::fs::create_dir_all(&learner_db_path).await?;

    let (learner_storage, learner_sm) = RocksDBUnifiedEngine::open(&learner_db_path)?;
    let learner_sm = Arc::new(learner_sm);

    let learner_engine = DefaultEmbeddedEngine::start_custom(
        &learner_db_path,
        Arc::new(learner_storage),
        Arc::<RocksDBStateMachine>::clone(&learner_sm),
        Some(learner_config_path.to_str().unwrap()),
    )
    .await?;
    info!("Learner Node 4 started — snapshot transfer is now in flight");

    info!("Writing {CONCURRENT_ENTRIES} entries while the snapshot transfer runs");
    for i in 0..CONCURRENT_ENTRIES {
        leader_client
            .put(
                format!("post_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    // Core non-blocking assertion: the Leader must have applied the concurrent writes
    // while the Learner has still applied nothing (snapshot install not yet run).
    let target = baseline_applied + CONCURRENT_ENTRIES;
    let mut leader_advanced = false;
    for _ in 0..20 {
        if leader_sm.last_applied().index >= target {
            leader_advanced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        leader_advanced,
        "Leader applied index did not reach {target} (stuck at {}) while snapshot was in flight",
        leader_sm.last_applied().index
    );

    assert_eq!(
        learner_sm.last_applied().index,
        0,
        "Learner must still be mid-snapshot (applied=0) while the Leader already applied \
         {CONCURRENT_ENTRIES} new entries — a snapshot transfer must not block Leader apply"
    );
    info!("Leader applied {CONCURRENT_ENTRIES} entries while Learner was still mid-snapshot");

    // Eventually the Learner must catch up (full snapshot + the concurrent tail).
    let mut caught_up = false;
    for _ in 0..40 {
        if learner_sm.last_applied().index >= target {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        caught_up,
        "Learner failed to catch up (applied {}, expected >= {target})",
        learner_sm.last_applied().index
    );
    info!(
        "Learner caught up to applied index {}",
        learner_sm.last_applied().index
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
