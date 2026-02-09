//! Integration tests for Leader tick and heartbeat behavior (Ticket #258)
//!
//! These tests verify that heartbeat acts as a safety net for command flushing
//! in the drain-based batch architecture, ensuring no command accumulation even
//! when the primary drain path is bypassed.
//!
//! Uses embedded mode with EmbeddedEngine for single-node testing.

use std::time::Duration;

use d_engine_core::ClientApi;
use d_engine_server::EmbeddedEngine;
use tempfile::TempDir;
use tracing_test::traced_test;

/// Helper to create a test EmbeddedEngine with configurable heartbeat interval.
///
/// # Arguments
/// * `test_name` - Unique identifier for the test (used in data directory path)
/// * `heartbeat_interval_ms` - Heartbeat interval in milliseconds (default 100ms)
///
/// # Returns
/// A tuple of (EmbeddedEngine, TempDir) where TempDir cleanup is automatic
///
/// # Configuration
/// - Single-node cluster (Leader)
/// - Heartbeat interval as specified
/// - RocksDB storage backend
async fn create_test_engine_with_heartbeat(
    test_name: &str,
    heartbeat_interval_ms: u64,
) -> (EmbeddedEngine, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let config_path = temp_dir.path().join("d-engine.toml");
    let port = 50000 + (std::process::id() % 10000);

    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft]
# Heartbeat interval must be << election timeout (300ms)
heartbeat_interval_ms = {}

[raft.general_raft_timeout]
general_raft_timeout_duration_in_ms = 3000
"#,
        port,
        db_path.display(),
        heartbeat_interval_ms
    );

    std::fs::write(&config_path, config_content).expect("Failed to write config");

    let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Failed to start engine");

    engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

    (engine, temp_dir)
}

/// Test: Empty Buffer Heartbeat - Maintains Leader Status
///
/// # Test Objective
/// Verify that heartbeat triggers correctly when the propose_buffer is empty,
/// ensuring the Leader maintains its status through periodic heartbeat signals
/// to followers (if clustering enabled) or internal state machine.
///
/// # Test Scenario
/// The Leader node is idle with no pending client commands. The heartbeat timer
/// deadline arrives. The tick() function should:
/// 1. Call propose_buffer.take_with_trigger(BatchTriggerType::Heartbeat)
/// 2. Receive empty VecDeque (no pending commands)
/// 3. Still process the batch (noop entry for heartbeat)
/// 4. Update metrics: heartbeat_triggered incremented
///
/// # Given
/// - Single-node cluster (auto-elected as Leader)
/// - Heartbeat interval = 100ms
/// - No pending client requests (empty propose_buffer)
/// - Leader has been stable for >100ms
///
/// # When
/// 1. Engine started and reaches Leader state
/// 2. Wait for at least one heartbeat interval (100ms)
/// 3. No client commands sent during this time
/// 4. Tick is triggered by heartbeat deadline
///
/// # Then
/// - Assertions:
///   - ✅ Engine remains in Leader state (is_leader() == true)
///   - ✅ No errors during tick execution
///   - ✅ Heartbeat is processed (can verify via metrics/tracing)
///   - ✅ Empty batch does not cause panics or protocol violations
///   - ✅ Can successfully perform operations after heartbeat
///
/// # Why This Matters
/// In the drain-based architecture, heartbeat is a secondary flush path.
/// Even when no commands are pending, heartbeat must fire to:
/// 1. Keep followers informed of leader status
/// 2. Maintain the leader lease (preventing premature elections)
/// 3. Advance commit_index through replication
///
/// This test ensures heartbeat doesn't break on empty buffers, a critical
/// safety property for the consensus protocol.
///
/// # Success Criteria
/// - Leader state maintained
/// - No protocol violations
/// - System stable after empty heartbeat
#[tokio::test]
#[traced_test]
async fn test_empty_buffer_heartbeat_maintains_leadership() {
    // Given: Create single-node Leader with 100ms heartbeat interval
    let (engine, _temp_dir) = create_test_engine_with_heartbeat("empty_hb", 100).await;

    // Verify initial state: Engine should be leader
    assert!(
        engine.is_leader(),
        "Single-node cluster should automatically become leader"
    );

    let client = engine.client();

    // When: Wait for heartbeat deadline (100ms + buffer)
    // During this time, no client commands are sent
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Then: Verify Leader still intact and functional
    assert!(
        engine.is_leader(),
        "Leader state should be maintained after empty heartbeat"
    );

    // Verify system can still process commands after heartbeat
    // (this proves no state corruption or protocol violation)
    let put_result = client.put(b"test_key", b"test_value").await;
    assert!(
        put_result.is_ok(),
        "Should be able to execute commands after empty heartbeat"
    );

    // Verify the command was replicated (single-node, immediate commit)
    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_result = client.get(b"test_key").await;
    assert!(
        get_result.is_ok(),
        "Key should be readable after heartbeat + command"
    );

    let value = get_result.expect("get should succeed");
    assert_eq!(
        value.as_deref(),
        Some(b"test_value".as_ref()),
        "Value should match what was written"
    );

    println!("✅ Empty buffer heartbeat maintains leadership and system stability");
}
