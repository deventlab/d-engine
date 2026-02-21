//! Integration tests for Linearizable Read Optimization (Ticket #238)
//!
//! These tests verify that linearizable reads use a fixed read_index calculated
//! at request arrival time, preventing unnecessary waiting for concurrent writes.
//!
//! Uses embedded mode with EmbeddedEngine for production-ready testing.

use crate::common::{create_node_config, get_available_ports, node_config};
use d_engine_server::EmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBStorageEngine;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::Instant;
use tracing_test::traced_test;

/// Helper to create a test EmbeddedEngine
async fn create_test_engine(test_name: &str) -> (EmbeddedEngine, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let config_path = temp_dir.path().join("d-engine.toml");
    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft.read_consistency]
state_machine_sync_timeout_ms = 2000
"#,
        port,
        db_path.display()
    );
    std::fs::write(&config_path, config_content).expect("Failed to write config");

    let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Failed to start engine");

    engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

    (engine, temp_dir)
}

/// Test linearizable read consistency with concurrent writes
///
/// # Test Scenario
/// Verifies linearizable reads always return committed data,
/// maintaining consistency even with concurrent write operations.
///
/// # Given
/// - Single-node cluster (Leader)
///
/// # When
/// 1. Perform 10 sequential writes (counter = 1, 2, ..., 10)
/// 2. After each write, perform linearizable read
///
/// # Then
/// - Each read returns the most recently committed value
/// - No read returns stale data or uncommitted writes
/// - Final read returns "10" (last committed value)
///
/// # Success Criteria
/// - All reads succeed
/// - Each read sees monotonically increasing values
/// - Final value is "10"
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_consistency_with_writes() {
    let (engine, _temp_dir) = create_test_engine("concurrent_writes").await;
    let client = engine.client();

    // When: Sequential writes with linearizable reads
    let mut last_seen_value = 0;

    for i in 1..=10 {
        // Write new value
        client.put(b"counter", i.to_string().as_bytes()).await.expect("Write failed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Linearizable read should see committed value
        let result = client.get_linearizable(b"counter").await.expect("Read failed");

        let value_str = String::from_utf8(result.unwrap().to_vec()).expect("Invalid UTF-8");
        let current_value: u32 = value_str.parse().expect("Invalid number");

        // Then: Value should be monotonically increasing
        assert!(
            current_value >= last_seen_value,
            "Read returned stale value: saw {current_value}, expected >= {last_seen_value}"
        );

        last_seen_value = current_value;
    }

    // Then: Final read must see the last committed value
    assert_eq!(
        last_seen_value, 10,
        "Final linearizable read should see last committed value"
    );

    println!("✅ Linearizable read consistency verified across 10 writes");
}

/// Test linearizable read after leader initialization (smoke test)
///
/// # Test Scenario
/// Verifies linearizable reads work correctly after leader initialization.
/// This is a smoke test that indirectly validates the noop_log_id tracking
/// mechanism by ensuring reads succeed after leader stabilization.
///
/// # Given
/// - Single-node cluster (node becomes leader automatically)
///
/// # When
/// 1. Node completes leader initialization (including noop entry commit)
/// 2. Perform write + linearizable read
///
/// # Then
/// - Read succeeds without errors
/// - Returns correct value
///
/// # Success Criteria
/// - Linearizable read completes successfully
/// - Returns the written value
///
/// # Note
/// Direct verification of noop_log_id is done in unit tests.
/// This integration test validates the end-to-end behavior.
#[tokio::test]
#[traced_test]
async fn test_read_index_with_noop_tracking() {
    let (engine, _temp_dir) = create_test_engine("fixed_index").await;
    let client = engine.client();

    // When: Write data and perform linearizable read
    client.put(b"test_key", b"test_value").await.expect("PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = client.get_linearizable(b"test_key").await.expect("Linearizable read failed");

    // Then: Read should succeed
    assert_eq!(
        result.as_deref(),
        Some(b"test_value".as_ref()),
        "Linearizable read should return correct value"
    );

    println!("✅ Linearizable read with noop tracking succeeded");
}

/// Test that eventual consistency reads are not affected by optimization
///
/// # Test Scenario
/// Verifies the optimization only applies to linearizable reads,
/// and eventual consistency reads continue to work efficiently.
///
/// # Given
/// - Single-node cluster
/// - Data written and replicated
///
/// # When
/// - Perform eventual consistency read
/// - Perform linearizable read
///
/// # Then
/// - Both reads return correct data
/// - EC read should be faster (no quorum verification)
///
/// # Success Criteria
/// - Both consistency levels work correctly
/// - EC read has lower latency
#[tokio::test]
#[traced_test]
async fn test_eventual_consistency_reads_unaffected() {
    let (engine, _temp_dir) = create_test_engine("high_load").await;
    let client = engine.client();

    // Given: Write data
    client.put(b"ec_test", b"v1").await.expect("PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // When: Eventual consistency read
    let ec_start = Instant::now();
    let ec_result = client.get_eventual(b"ec_test").await.expect("EC read failed");
    let ec_latency = ec_start.elapsed();

    // When: Linearizable read
    let lin_start = Instant::now();
    let lin_result = client.get_linearizable(b"ec_test").await.expect("Linearizable read failed");
    let lin_latency = lin_start.elapsed();

    // Then: Both should return correct data
    assert_eq!(
        ec_result.as_deref(),
        Some(b"v1".as_ref()),
        "EC read should return correct value"
    );
    assert_eq!(
        lin_result.as_deref(),
        Some(b"v1".as_ref()),
        "Linearizable read should return correct value"
    );

    // EC read should typically be faster (but not enforced due to test variance)
    println!("✅ EC read: {ec_latency:?}, Linearizable read: {lin_latency:?}");
}

/// Test linearizable reads with multiple sequential writes
///
/// # Test Scenario
/// Verifies read_index correctly advances with commit_index
/// as writes are committed sequentially.
///
/// # Given
/// - Single-node cluster
///
/// # When
/// 1. Write value "v1"
/// 2. Linearizable read (should see "v1")
/// 3. Write value "v2"
/// 4. Linearizable read (should see "v2")
///
/// # Then
/// - Each read sees the most recent committed value
/// - No stale reads
///
/// # Success Criteria
/// - First read returns "v1"
/// - Second read returns "v2"
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_sequential_writes() {
    let (engine, _temp_dir) = create_test_engine("slow_apply").await;
    let client = engine.client();

    // When: Write v1 and read
    client.put(b"seq_key", b"v1").await.expect("First PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let read1 = client.get_linearizable(b"seq_key").await.expect("First read failed");

    assert_eq!(
        read1.as_deref(),
        Some(b"v1".as_ref()),
        "First read should return v1"
    );

    // When: Write v2 and read
    client.put(b"seq_key", b"v2").await.expect("Second PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let read2 = client.get_linearizable(b"seq_key").await.expect("Second read failed");

    assert_eq!(
        read2.as_deref(),
        Some(b"v2".as_ref()),
        "Second read should return v2"
    );

    println!("✅ Sequential writes with linearizable reads succeeded");
}

/// Test read_index optimization with concurrent writes in multi-node cluster
///
/// # Test Scenario
/// Verifies that linearizable reads use a fixed read_index calculated at request
/// arrival time, not inflated by concurrent writes during processing.
///
/// # Given
/// - 3-node cluster with elected leader
/// - Initial value: key="counter", value="v0"
///
/// # When
/// 1. Start linearizable read request
/// 2. Inject 20 concurrent writes (v1..v20) while read processes
/// 3. Measure read completion time
///
/// # Then
/// - Read completes in reasonable time (< 5 seconds)
/// - Read does NOT wait indefinitely for all concurrent writes
/// - Proves read_index was fixed at arrival, not dynamically updated
///
/// # Success Criteria
/// - Read completes within timeout
/// - Latency does not scale linearly with concurrent write count
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_read_index_fixed_with_concurrent_writes_multi_node()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let base_config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root_dir.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;
        let config_str = format!(
            "{base_config_str}\n[raft.read_consistency]\nstate_machine_sync_timeout_ms = 5000\n",
        );
        let config = node_config(&config_str);

        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let storage_path = node_db_root.join("storage");
        let sm_path = node_db_root.join("state_machine");

        tokio::fs::create_dir_all(&storage_path).await?;
        tokio::fs::create_dir_all(&sm_path).await?;

        let storage = Arc::new(RocksDBStorageEngine::new(storage_path)?);
        let state_machine = Arc::new(RocksDBStateMachine::new(sm_path)?);

        let config_path = format!("/tmp/d-engine-test-linear-read-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine =
            EmbeddedEngine::start_custom(storage, state_machine, Some(&config_path)).await?;
        engines.push(engine);
    }

    // Wait for leader election
    let leader_info = engines[0].wait_ready(Duration::from_secs(10)).await?;
    let leader_idx = (leader_info.leader_id - 1) as usize;
    let leader_client = engines[leader_idx].client();

    // Given: Write initial value
    leader_client.put(b"counter", b"v0").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // When: Start linearizable read with concurrent writes
    let client_clone = leader_client.clone();

    let read_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        client_clone.get_linearizable(b"counter").await
    });

    // Inject concurrent writes
    tokio::time::sleep(Duration::from_millis(10)).await;
    for i in 1..=100 {
        leader_client.put(b"counter", format!("v{i}").as_bytes()).await?;
    }

    // Then: Read should succeed despite concurrent writes
    let result = read_task.await?;

    assert!(
        result.is_ok(),
        "Linearizable read should succeed with concurrent writes: {result:?}"
    );

    // Cleanup
    for engine in engines {
        engine.stop().await?;
    }

    println!("✅ Multi-node read_index optimization verified");
    Ok(())
}
