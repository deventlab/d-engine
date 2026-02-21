//! Integration tests for Lease Read
//!
//! These tests verify lease-based read consistency:
//! - Lease Read with valid lease (local read, no quorum)
//! - Lease Read consistency across sequential writes
//! - Lease Read vs Linearizable Read comparison
//!

use std::time::Duration;

use d_engine_core::ClientApi;
use d_engine_server::EmbeddedEngine;
use tempfile::TempDir;
use tracing_test::traced_test;

use crate::common::get_available_ports;

/// Helper to create a test EmbeddedEngine with lease configuration
async fn create_test_engine_with_lease(test_name: &str) -> (EmbeddedEngine, TempDir) {
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

[raft.state_machine.lease]
enabled = true

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

/// Test T3: Lease Read - Valid Lease (Local Read)
///
/// # Test Objective
/// Verify lease read uses local state machine when lease is valid,
/// avoiding expensive quorum verification.
///
/// # Test Scenario
/// Leader has valid lease. Client performs lease read.
///
/// # Given
/// - Single-node cluster (Leader with valid lease)
/// - Lease mechanism enabled
/// - Data written and committed
///
/// # When
/// - Perform lease read immediately after write
/// - Lease is still valid (no expiration)
///
/// # Then
/// - Read succeeds without quorum verification
/// - Returns correct committed value
/// - Lower latency than linearizable read (local read path)
///
/// # Success Criteria
/// - Lease read returns correct value
/// - No errors during read
#[tokio::test]
#[traced_test]
async fn test_lease_read_with_valid_lease() {
    // Given: Create engine with lease enabled
    let (engine, _temp_dir) = create_test_engine_with_lease("lease_valid").await;

    assert!(engine.is_leader(), "Single-node cluster should be leader");

    let client = engine.client();

    // Write test data
    client.put(b"lease_key_1", b"lease_value_1").await.expect("PUT failed");

    // Wait for commit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // When: Perform lease read (lease should be valid)
    let result = client.get_lease(b"lease_key_1").await.expect("Lease read failed");

    // Then: Read should return correct value
    assert_eq!(
        result.as_deref(),
        Some(b"lease_value_1".as_ref()),
        "Lease read should return committed value"
    );

    println!("✅ Lease read with valid lease succeeded (local read path)");
}

/// Test T4: Lease Read - Multiple Reads Verifying Consistency
///
/// # Test Objective
/// Verify lease reads maintain consistency across multiple operations.
///
/// # Test Scenario
/// Perform sequential writes and lease reads, verifying each read
/// sees the most recent committed value.
///
/// # Given
/// - Single-node cluster with lease enabled
///
/// # When
/// 1. Write v1, lease read (should see v1)
/// 2. Write v2, lease read (should see v2)
/// 3. Write v3, lease read (should see v3)
///
/// # Then
/// - Each lease read sees the latest committed value
/// - No stale reads
/// - Lease mechanism maintains consistency
///
/// # Success Criteria
/// - All reads return correct sequential values
/// - Monotonically increasing values observed
#[tokio::test]
#[traced_test]
async fn test_lease_read_consistency_across_writes() {
    // Given: Create engine with lease enabled
    let (engine, _temp_dir) = create_test_engine_with_lease("lease_consistency").await;

    let client = engine.client();

    // When: Sequential writes with lease reads
    for i in 1..=5 {
        let value = format!("value_{i}");

        client.put(b"seq_key", value.as_bytes()).await.expect("PUT failed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Lease read should see committed value
        let result = client.get_lease(b"seq_key").await.expect("Lease read failed");

        assert_eq!(
            result.as_deref(),
            Some(value.as_bytes()),
            "Lease read should see value_{i}"
        );
    }

    println!("✅ Lease read consistency verified across 5 sequential writes");
}

/// Test: Lease Read vs Linearizable Read - Both Return Correct Data
///
/// # Test Objective
/// Verify both lease read and linearizable read return the same
/// committed value, validating consistency between different read policies.
///
/// # Given
/// - Single-node cluster with lease enabled
/// - Data written and committed
///
/// # When
/// - Perform lease read
/// - Perform linearizable read
///
/// # Then
/// - Both reads return identical value
/// - Both see committed data
///
/// # Success Criteria
/// - lease_result == linearizable_result
/// - Both return correct committed value
#[tokio::test]
#[traced_test]
async fn test_lease_vs_linearizable_read_consistency() {
    let (engine, _temp_dir) = create_test_engine_with_lease("lease_vs_lin").await;
    let client = engine.client();

    // Write test data
    client.put(b"compare_key", b"compare_value").await.expect("PUT failed");

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Lease read
    let lease_result = client.get_lease(b"compare_key").await.expect("Lease read failed");

    // Linearizable read
    let lin_result =
        client.get_linearizable(b"compare_key").await.expect("Linearizable read failed");

    // Then: Both should return same value
    assert_eq!(
        lease_result, lin_result,
        "Lease and linearizable reads should return identical data"
    );

    assert_eq!(
        lease_result.as_deref(),
        Some(b"compare_value".as_ref()),
        "Both reads should see committed value"
    );

    println!("✅ Lease and linearizable reads return consistent data");
}
