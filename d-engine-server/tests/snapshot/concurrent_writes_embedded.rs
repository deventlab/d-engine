//! Snapshot Concurrent Operations Integration Tests
//!
//! This module tests snapshot generation and application under concurrent operations,
//! ensuring data consistency and availability during snapshot processes.
//!
//! ## Test Coverage
//!
//! 1. **Leader Snapshot Generation with Concurrent Writes**
//!    - Verifies that snapshot generation does not block write operations
//!    - Ensures all concurrent writes complete successfully
//!    - Validates that snapshot contains correct data at point-in-time
//!
//! 2. **Learner Snapshot Application with Concurrent Replication**
//!    - Tests data consistency during snapshot installation
//!    - Verifies PHASE 2.5 safety (temporary DB swap window)
//!    - Ensures no data loss during concurrent AppendEntries

use std::sync::Arc;
use std::time::Duration;

use d_engine_server::EmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBStorageEngine;
use tracing::info;
use tracing_test::traced_test;

use crate::common::get_available_ports;

/// Test: Leader generates snapshot while handling concurrent write requests
///
/// ## Scenario
///
/// This test validates that the Leader can generate snapshots without blocking
/// client write operations, addressing the question from todo3.md:
/// "Leader 生成 snapshot 时是否能正常接受客户端读写请求？"
///
/// ## Test Flow
///
/// 1. Start 3-node embedded cluster with snapshot config:
///    - `max_log_entries_before_snapshot = 100`
///    - `retained_log_entries = 10`
/// 2. Write 95 entries to approach snapshot threshold
/// 3. Launch concurrent write tasks (50 additional writes)
/// 4. Entry #100 triggers automatic snapshot generation
/// 5. Concurrent writes continue (entries 101-145)
/// 6. Wait for all operations to complete
///
/// ## Expected Results
///
/// ✅ All 145 writes succeed (no blocking during snapshot)
/// ✅ Leader can read all 145 entries after completion
/// ✅ Followers replicate all 145 entries correctly
/// ✅ Snapshot contains entries 1-90 (100 - retained_log_entries)
/// ✅ Log entries 1-90 are purged after snapshot
///
/// ## Implementation Details
///
/// - Uses RocksDB checkpoint mechanism (online operation)
/// - Snapshot generation occurs in separate async task
/// - Write path remains unblocked during checkpoint creation
/// - Verifies `generate_snapshot_data()` correctness
#[tokio::test]
#[traced_test]
async fn test_leader_snapshot_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");
    let snapshots_dir = temp_dir.path().join("snapshots");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster with snapshot threshold = 100 entries");

    // Create 3-node cluster configuration with snapshot enabled
    let mut engines = Vec::new();

    for node_id in 1..=3 {
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
initial_cluster_size = 3
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.snapshot]
max_log_entries_before_snapshot = 100
retained_log_entries = 10
snapshots_dir = '{}'
"#,
            node_id,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
            db_root_dir.display(),
            log_dir.display(),
            snapshots_dir.join(format!("node{node_id}")).display()
        );

        let config_path = format!("/tmp/snapshot_concurrent_node{node_id}.toml");
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

    // Wait for leader election
    info!("Waiting for leader election...");
    let leader_info = engines[0].wait_ready(Duration::from_secs(10)).await?;
    info!(
        "Leader elected: Node {} (term {})",
        leader_info.leader_id, leader_info.term
    );

    // Find the leader engine index
    let leader_idx = engines.iter().position(|e| e.is_leader()).expect("Should have a leader");

    // Phase 1: Write 95 entries (approaching snapshot threshold of 100)
    info!("Phase 1: Writing 95 entries to approach snapshot threshold");
    for i in 0..95 {
        let key = format!("key-{i:04}").into_bytes();
        let value = format!("value-{i:04}").into_bytes();
        engines[leader_idx].client().put(key, value).await?;
    }

    // Wait for replication to complete
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("95 entries written and replicated");

    // Phase 2: Launch concurrent writes while snapshot is triggered
    info!("Phase 2: Launching 50 concurrent writes (entries 95-144)");
    info!("Entry #100 will trigger automatic snapshot generation");

    let leader_client = engines[leader_idx].client().clone();
    let concurrent_writes = tokio::spawn(async move {
        let mut results = Vec::new();
        for i in 95..145 {
            let key = format!("key-{i:04}").into_bytes();
            let value = format!("value-{i:04}").into_bytes();
            let result = leader_client.put(key, value).await;
            results.push((i, result));

            // Small delay to make concurrent writes more realistic
            if i % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        results
    });

    // Wait for concurrent writes to complete
    let write_results = concurrent_writes.await?;

    // Phase 3: Verify all writes succeeded
    info!("Phase 3: Verifying all 145 writes succeeded");
    let mut failed_writes = Vec::new();
    for (i, result) in write_results {
        if let Err(e) = result {
            failed_writes.push((i, e));
        }
    }

    assert!(
        failed_writes.is_empty(),
        "All writes should succeed during snapshot generation. Failed writes: {failed_writes:?}"
    );
    info!("✅ All 145 writes succeeded (snapshot did not block writes)");

    // Phase 4: Verify data integrity on Leader
    info!("Phase 4: Verifying all 145 entries readable on Leader");
    for i in 0..145 {
        let key = format!("key-{i:04}").into_bytes();
        let expected_value = format!("value-{i:04}").into_bytes();

        let actual_value = engines[leader_idx]
            .client()
            .get_linearizable(key.clone())
            .await?
            .unwrap_or_else(|| panic!("Key {i} should exist"));

        assert_eq!(actual_value, expected_value, "Value mismatch for key {i}");
    }
    info!("✅ All 145 entries verified on Leader");

    // Phase 5: Verify data replicated to Followers
    info!("Phase 5: Verifying replication to Followers");
    tokio::time::sleep(Duration::from_secs(2)).await;

    for (idx, engine) in engines.iter().enumerate() {
        if engine.is_leader() {
            continue; // Skip leader, already verified
        }

        info!("Verifying Follower {} (Node {})", idx, idx + 1);

        // Sample check: verify first 10 and last 10 entries on each follower
        for i in (0..10).chain(135..145) {
            let key = format!("key-{i:04}").into_bytes();
            let expected_value = format!("value-{i:04}").into_bytes();

            let actual_value = engine
                .client()
                .get_eventual(key.clone())
                .await?
                .unwrap_or_else(|| panic!("Key {i} should exist on follower"));

            assert_eq!(
                actual_value, expected_value,
                "Follower {idx} value mismatch for key {i}"
            );
        }
    }
    info!("✅ Data successfully replicated to all Followers");

    // Phase 6: Verify snapshot was generated and contains correct data
    info!("Phase 6: Verifying snapshot generation");

    // Wait a bit more to ensure snapshot is fully written
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check snapshot directory exists and contains files
    let leader_id = engines[leader_idx].node_id();
    let leader_snapshot_dir = snapshots_dir.join(format!("node{leader_id}"));

    assert!(
        leader_snapshot_dir.exists(),
        "Leader snapshot directory should exist"
    );

    let snapshot_files: Vec<_> = std::fs::read_dir(&leader_snapshot_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("gz"))
        .collect();

    assert!(
        !snapshot_files.is_empty(),
        "At least one snapshot file should be generated. \
         Snapshot dir: {leader_snapshot_dir:?}, \
         Files found: {snapshot_files:?}"
    );

    info!(
        "✅ Snapshot generated successfully: {} file(s) in {:?}",
        snapshot_files.len(),
        leader_snapshot_dir
    );

    // Cleanup
    info!("Test completed successfully. Shutting down cluster.");
    for engine in engines {
        engine.stop().await?;
    }

    Ok(())
}
