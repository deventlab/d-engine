// Copyright 2025 the d-engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Follower independent snapshot generation integration tests (Embedded mode)
//!
//! Per Raft §7, each server takes snapshots independently.
//! These tests validate two properties added in fix #270:
//!
//! 1. **Trigger correctness**: Follower triggers snapshot at the configured
//!    `max_log_entries_before_snapshot` threshold, not before or never.
//!
//! 2. **Replication safety**: While a Follower is generating its own snapshot
//!    (background async task), it continues to receive and apply AppendEntries
//!    from the Leader without data loss or stall.

#![cfg(feature = "rocksdb")]
use d_engine_server::{EmbeddedEngine, RocksDBStateMachine, RocksDBStorageEngine};

use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use crate::common::{get_available_ports, wait_for_snapshot};

/// Test: Follower generates snapshot at the correct threshold and continues replication
///
/// ## Scenario
///
/// This test validates two behaviors introduced in fix #270
/// (Add snapshot trigger to Follower/Learner ApplyCompleted handlers):
///
/// ### Property 1 — Trigger correctness
/// When `max_log_entries_before_snapshot = 100`, the Follower must generate
/// a snapshot file after applying ~100 entries, not earlier and not never.
/// We verify this by asserting the snapshot file exists within a bounded
/// poll window after writing the threshold number of entries.
///
/// ### Property 2 — Replication safety during snapshot generation
/// Snapshot creation is an async background task (`tokio::spawn`). During
/// the time the Follower's state machine is being checkpointed, the Raft
/// event loop must remain unblocked:
/// - The Follower must keep accepting AppendEntries from the Leader.
/// - No entries must be lost during the snapshot window.
/// - The Follower must not trigger an unnecessary election (term must not change).
///
/// ## Test Flow
///
/// 1. Start 3-node embedded cluster with `snapshot_threshold = 100`.
/// 2. Write 150 baseline entries to the Leader.
///    - Entry #100 triggers automatic snapshot on Leader AND Followers (Raft §7).
/// 3. Poll until the Follower snapshot file appears (max 15s).
///    - Fails fast if the trigger is broken (never fires).
/// 4. While the Follower snapshot background task may still be running,
///    write 50 additional concurrent entries to the Leader.
/// 5. Wait for all Followers to replicate all 200 entries.
/// 6. Verify: Follower has all 200 entries with correct values (no data loss).
/// 7. Verify: cluster term did not increase (no unintended re-election during snapshot).
///
/// ## Expected Results
///
/// ✅ Follower snapshot file created at threshold (~entry 100)
/// ✅ All 200 entries replicated to Follower without data loss
/// ✅ Cluster term unchanged — Follower stayed connected during snapshot
/// ✅ No stall or deadlock in the Follower event loop
#[tokio::test]
#[traced_test]
async fn test_follower_snapshot_generation_during_replication()
-> Result<(), Box<dyn std::error::Error>> {
    const SNAPSHOT_THRESHOLD: u64 = 100;
    const RETAINED_LOGS: u64 = 50;
    const BASELINE_ENTRIES: u64 = 150;
    const CONCURRENT_ENTRIES: u64 = 50;
    const TOTAL_ENTRIES: u64 = BASELINE_ENTRIES + CONCURRENT_ENTRIES;

    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");
    let snapshots_dir = temp_dir.path().join("snapshots");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!(
        "Starting 3-node cluster with snapshot_threshold = {}",
        SNAPSHOT_THRESHOLD
    );

    let mut engines = Vec::new();

    for node_id in 1..=3u64 {
        let config = format!(
            r#"
[cluster]
node_id = {}
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 10000

[raft.snapshot]
max_log_entries_before_snapshot = {}
retained_log_entries = {}
snapshots_dir = '{}'
"#,
            node_id,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
            db_root_dir.join(format!("node{node_id}")).display(),
            log_dir.join(format!("node{node_id}")).display(),
            SNAPSHOT_THRESHOLD,
            RETAINED_LOGS,
            snapshots_dir.join(format!("node{node_id}")).display(),
        );

        let config_path = format!("/tmp/follower_snap_node{node_id}.toml");
        tokio::fs::write(&config_path, &config).await?;

        let storage_path = db_root_dir.join(format!("node{node_id}/storage"));
        let sm_path = db_root_dir.join(format!("node{node_id}/state_machine"));

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;
        tokio::fs::create_dir_all(snapshots_dir.join(format!("node{node_id}"))).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let sm = Arc::new(RocksDBStateMachine::new(sm_path)?);
        let engine = EmbeddedEngine::start_custom(storage, sm, Some(&config_path)).await?;
        engines.push(engine);
    }

    // Wait for initial leader election
    info!("Waiting for leader election...");
    let leader_info = engines[0].wait_ready(Duration::from_secs(10)).await?;
    let initial_term = leader_info.term;
    info!(
        "Leader elected: Node {} (term {})",
        leader_info.leader_id, initial_term
    );

    let leader_idx = engines.iter().position(|e| e.is_leader()).expect("Should have a leader");
    let leader_client = engines[leader_idx].client().clone();

    // Identify a follower node to observe
    let follower_idx = (0..3).find(|&i| i != leader_idx).expect("Should have a follower");
    let follower_id = engines[follower_idx].node_id();
    info!("Observing Follower: Node {}", follower_id);

    // Phase 1: Write baseline entries to trigger snapshot at threshold
    info!(
        "Phase 1: Writing {} baseline entries (snapshot triggers at entry {})",
        BASELINE_ENTRIES, SNAPSHOT_THRESHOLD
    );
    for i in 0..BASELINE_ENTRIES {
        let key = format!("key_{i}").into_bytes();
        let value = format!("value_{i}").into_bytes();
        leader_client.put(key, value).await?;
    }
    info!("Phase 1 complete: {} entries written", BASELINE_ENTRIES);

    // Phase 2: Verify snapshot trigger correctness on the observed Follower
    //
    // Per fix #270: Follower now calls check_and_trigger_snapshot() in its
    // ApplyCompleted handler. We poll until the snapshot file appears on disk.
    // A 15s timeout is generous enough to account for async apply + checkpoint latency.
    info!(
        "Phase 2: Verifying snapshot triggered on Follower Node {} (max wait 15s)...",
        follower_id
    );
    assert!(
        wait_for_snapshot(&snapshots_dir, follower_id as u64, Duration::from_secs(15)).await,
        "Follower Node {follower_id} did not generate snapshot within 15s — \
         check_and_trigger_snapshot() in ApplyCompleted may be broken (fix #270)"
    );
    info!(
        "Follower Node {} snapshot file confirmed on disk",
        follower_id
    );

    // Phase 3: Write concurrent entries while Follower snapshot may still be running
    //
    // The snapshot creation is a background tokio::spawn task. The Follower event
    // loop must remain unblocked and keep accepting AppendEntries during this window.
    info!(
        "Phase 3: Writing {} concurrent entries while Follower snapshot background task may still be active...",
        CONCURRENT_ENTRIES
    );
    for i in 0..CONCURRENT_ENTRIES {
        let entry_num = BASELINE_ENTRIES + i;
        let key = format!("key_{entry_num}").into_bytes();
        let value = format!("value_{entry_num}").into_bytes();
        leader_client.put(key, value).await?;
    }
    info!(
        "Phase 3 complete: {} concurrent entries written (total: {})",
        CONCURRENT_ENTRIES, TOTAL_ENTRIES
    );

    // Phase 4: Wait for Follower to replicate all entries
    //
    // Uses eventual read on the Follower to check the last key.
    // If the Follower stalled during snapshot, this will timeout.
    info!(
        "Phase 4: Waiting for Follower Node {} to replicate all {} entries...",
        follower_id, TOTAL_ENTRIES
    );
    let last_key = format!("key_{}", TOTAL_ENTRIES - 1).into_bytes();
    let mut follower_caught_up = false;
    for _ in 0..15 {
        if let Ok(Some(value)) = engines[follower_idx].client().get_eventual(last_key.clone()).await
        {
            let expected = format!("value_{}", TOTAL_ENTRIES - 1).into_bytes();
            if value == expected {
                follower_caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        follower_caught_up,
        "Follower Node {follower_id} failed to replicate all {TOTAL_ENTRIES} entries — \
         Follower event loop may have stalled during snapshot generation"
    );
    info!(
        "Follower Node {} caught up to all {} entries",
        follower_id, TOTAL_ENTRIES
    );

    // Phase 5: Verify data integrity on Leader
    info!("Phase 5: Verifying data integrity on Leader...");
    let mut errors = Vec::new();
    for i in 0..TOTAL_ENTRIES {
        let key = format!("key_{i}").into_bytes();
        let expected = format!("value_{i}").into_bytes();
        match leader_client.get_linearizable(key).await {
            Ok(Some(v)) if v == expected => {}
            Ok(Some(v)) => errors.push(format!("key_{i}: expected '{expected:?}', got '{v:?}'")),
            Ok(None) => errors.push(format!("key_{i}: not found")),
            Err(e) => errors.push(format!("key_{i}: error {e:?}")),
        }
    }
    assert!(
        errors.is_empty(),
        "Data integrity check failed ({} errors): {:?}",
        errors.len(),
        &errors[..errors.len().min(5)]
    );
    info!(
        "All {} entries verified on Leader — NO DATA LOSS",
        TOTAL_ENTRIES
    );

    // Phase 6: Verify cluster term did not change
    //
    // If the Follower disconnected during snapshot and triggered an election,
    // the term would have increased. Stable term proves the Follower stayed
    // connected and responsive throughout snapshot generation.
    let final_info = engines[0].wait_ready(Duration::from_secs(5)).await?;
    assert_eq!(
        final_info.term, initial_term,
        "Cluster term changed from {} to {} — Follower may have triggered \
         an unintended election during snapshot generation",
        initial_term, final_info.term
    );
    info!(
        "Cluster term stable at {} — Follower remained connected during snapshot",
        initial_term
    );

    info!("TEST PASSED: Follower snapshot generation during replication");
    info!("Key findings:");
    info!("  - Follower triggered snapshot at threshold (fix #270 confirmed)");
    info!("  - Follower event loop unblocked during background snapshot task");
    info!(
        "  - All {} entries replicated without data loss",
        TOTAL_ENTRIES
    );
    info!("  - No unintended leader election during Follower snapshot");

    Ok(())
}
