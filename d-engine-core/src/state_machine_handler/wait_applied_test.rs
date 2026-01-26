use std::sync::Arc;
use std::time::{Duration, Instant};

use super::DefaultStateMachineHandler;
use super::StateMachineHandler;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::MockTypeConfig;
use crate::test_utils::snapshot_config;

/// Helper function to create a test handler with specific last_applied value
fn create_test_handler_with_applied(
    last_applied: u64
) -> DefaultStateMachineHandler<MockTypeConfig> {
    let state_machine = Arc::new(MockStateMachine::new());
    let snap_config = snapshot_config(std::path::PathBuf::from("/tmp/test_wait_applied"));

    DefaultStateMachineHandler::new_without_watch(
        1,            // node_id
        last_applied, // last_applied
        state_machine,
        snap_config,
        MockSnapshotPolicy::new(),
    )
}

// P0 Unit Test 1: Fast path - target_index already applied
#[tokio::test]
async fn test_wait_applied_fast_path_already_applied() {
    let handler = create_test_handler_with_applied(10);

    let start = Instant::now();
    let result = handler.wait_applied(5, Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    // Should succeed immediately
    assert!(result.is_ok(), "Should succeed when already applied");

    // Fast path should be very fast (< 5ms allows for CI system scheduling delays)
    assert!(
        elapsed < Duration::from_millis(5),
        "Fast path should return quickly, actual: {elapsed:?}"
    );
}

// P0 Unit Test 2: Slow path - wait for notification
#[tokio::test]
async fn test_wait_applied_slow_path_wait_notification() {
    let handler = create_test_handler_with_applied(5);
    let handler_clone = Arc::new(handler);
    let handler_for_task = handler_clone.clone();

    // Spawn task to simulate state machine applying to index 10 after delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        handler_for_task.test_simulate_apply(10);
    });

    let start = Instant::now();
    let result = handler_clone.wait_applied(10, Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    // Should succeed after notification
    assert!(result.is_ok(), "Should succeed after notification");

    // Should take approximately 20ms (waiting for apply)
    assert!(
        elapsed >= Duration::from_millis(15) && elapsed < Duration::from_millis(50),
        "Should wait for notification, actual: {elapsed:?}"
    );
}

// P0 Unit Test 3: Timeout - state machine stuck
#[tokio::test]
async fn test_wait_applied_timeout_state_machine_stuck() {
    let handler = create_test_handler_with_applied(5);

    let start = Instant::now();
    let result = handler.wait_applied(100, Duration::from_millis(50)).await;
    let elapsed = start.elapsed();

    // Should timeout
    assert!(result.is_err(), "Should timeout when state machine stuck");

    // Should take approximately 50ms (timeout duration)
    assert!(
        elapsed >= Duration::from_millis(45) && elapsed < Duration::from_millis(100),
        "Should timeout after specified duration, actual: {elapsed:?}"
    );

    // Verify error message contains useful diagnostics
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("Timeout waiting for state machine"),
        "Error should mention timeout"
    );
    assert!(
        err_msg.contains("current_applied: 5"),
        "Error should include current applied index"
    );
    assert!(
        err_msg.contains("target_index") || err_msg.contains("100"),
        "Error should include target index"
    );
}

// P1 Unit Test 4: Concurrent waiters - multiple readers
#[tokio::test]
async fn test_wait_applied_concurrent_waiters() {
    let handler = Arc::new(create_test_handler_with_applied(5));
    let mut tasks = vec![];

    // Spawn 10 concurrent waiters
    for i in 0..10 {
        let h = handler.clone();
        tasks.push(tokio::spawn(async move {
            let result = h.wait_applied(10, Duration::from_millis(200)).await;
            (i, result)
        }));
    }

    // Simulate state machine applying to index 10 after short delay
    let handler_for_apply = handler.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        handler_for_apply.test_simulate_apply(10);
    });

    // All waiters should succeed
    for task in tasks {
        let (waiter_id, result) = task.await.unwrap();
        assert!(
            result.is_ok(),
            "Waiter {waiter_id} should succeed after notification"
        );
    }
}

// P2 Unit Test 5: Channel closed - cannot test directly (private field)
// Note: In production, sender is never dropped while receivers exist.
// If state machine stops, wait_applied will timeout instead.

// P0 Unit Test 6: Exact match - target_index equals last_applied
#[tokio::test]
async fn test_wait_applied_exact_match() {
    let handler = create_test_handler_with_applied(10);

    let start = Instant::now();
    let result = handler.wait_applied(10, Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    // Should succeed immediately (fast path)
    assert!(result.is_ok(), "Should succeed when exact match");

    // Fast path should be very fast (< 5ms allows for CI system scheduling delays)
    assert!(
        elapsed < Duration::from_millis(5),
        "Should use fast path, actual: {elapsed:?}"
    );
}

// P1 Unit Test 7: Multiple sequential waits - simulate multiple linearizable reads
#[tokio::test]
async fn test_wait_applied_multiple_sequential_waits() {
    let handler = Arc::new(create_test_handler_with_applied(0));

    // Simulate incremental apply progress
    for target_index in 1..=5 {
        let h = handler.clone();

        // Simulate apply happening concurrently
        let apply_handler = handler.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            apply_handler.test_simulate_apply(target_index);
        });

        // Wait for this index
        let result = h.wait_applied(target_index, Duration::from_millis(100)).await;
        assert!(
            result.is_ok(),
            "Should succeed waiting for index {target_index}"
        );
    }

    // Verify final state
    assert_eq!(handler.last_applied(), 5);
}
