//! Unit tests for timer modules (ElectionTimer and ReplicationTimer)
//!
//! These tests verify:
//! - Random timeout generation within specified range
//! - Timer reset behavior
//! - Expiration detection
//! - Deadline calculations

use std::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;

use super::*;

// ============================================================================
// ElectionTimer Tests
// ============================================================================

/// Test: ElectionTimer initializes with random deadline in valid range
///
/// Scenario:
/// - Create timer with range [100ms, 200ms]
/// - Verify deadline is in the future
/// - Run multiple times to verify randomness
#[tokio::test]
async fn test_election_timer_init_within_range() {
    let (min, max) = (100u64, 200u64);
    let timer = ElectionTimer::new((min, max));

    let now = Instant::now();
    let next_deadline = timer.next_deadline();

    // Deadline should be in the future
    assert!(next_deadline > now, "Next deadline should be in the future");

    // Deadline should be within reasonable bounds (min-max millis from now)
    let elapsed = next_deadline - now;
    let min_duration = Duration::from_millis(min);
    let max_duration = Duration::from_millis(max);

    assert!(
        elapsed >= min_duration,
        "Elapsed time {elapsed:?} should be at least {min_duration:?}",
    );
    assert!(
        elapsed <= max_duration,
        "Elapsed time {elapsed:?} should be at most {max_duration:?}",
    );
}

/// Test: ElectionTimer shows randomness across multiple initializations
///
/// Scenario:
/// - Create 5 timers with same range
/// - Verify they have different deadlines
#[tokio::test]
async fn test_election_timer_shows_randomness() {
    let (min, max) = (100u64, 200u64);
    let range = (min, max);

    let timers: Vec<_> = (0..5).map(|_| ElectionTimer::new(range)).collect();

    let deadlines: Vec<_> = timers.iter().map(|t| t.next_deadline()).collect();

    // Check that not all deadlines are the same (randomness)
    let first = deadlines[0];
    let all_same = deadlines.iter().all(|&d| d == first);

    // With random timing, it's extremely unlikely all are identical
    // This is a statistical test, but practically should always pass
    assert!(
        !all_same || deadlines.len() == 1,
        "Timers should have different deadlines due to randomness"
    );
}

/// Test: ElectionTimer.is_expired() returns false initially
///
/// Scenario:
/// - Create timer with timeout range [100ms, 200ms]
/// - Check immediately
/// - Should not be expired
#[tokio::test]
async fn test_election_timer_not_expired_initially() {
    let timer = ElectionTimer::new((100u64, 200u64));

    assert!(
        !timer.is_expired(),
        "Timer should not be expired immediately after creation"
    );
}

/// Test: ElectionTimer.is_expired() returns true after timeout
///
/// Scenario:
/// - Create timer with very short range [10ms, 20ms]
/// - Sleep longer than max timeout
/// - Check expiration
#[tokio::test]
async fn test_election_timer_expired_after_timeout() {
    let timer = ElectionTimer::new((10u64, 20u64));

    // Sleep longer than maximum possible timeout
    sleep(Duration::from_millis(50)).await;

    assert!(
        timer.is_expired(),
        "Timer should be expired after timeout period"
    );
}

/// Test: ElectionTimer.reset() sets new deadline
///
/// Scenario:
/// - Create timer
/// - Get initial deadline
/// - Sleep a bit
/// - Reset timer
/// - New deadline should be later than old deadline
#[tokio::test]
async fn test_election_timer_reset() {
    let mut timer = ElectionTimer::new((100u64, 200u64));

    // Wait until timer is about to expire
    sleep(Duration::from_millis(150)).await;

    // Reset timer - should get a fresh deadline from now
    let before_reset = Instant::now();
    timer.reset();
    let new_deadline = timer.next_deadline();

    // New deadline should be at least 100ms from now (min timeout)
    let min_expected = before_reset + Duration::from_millis(100);
    assert!(
        new_deadline >= min_expected,
        "New deadline {new_deadline:?} should be at least 100ms after reset time {before_reset:?}",
    );
}

/// Test: ElectionTimer.random_duration() respects bounds
///
/// Scenario:
/// - Generate 100 random durations
/// - All should fall within [min, max] range
#[tokio::test]
async fn test_election_timer_random_duration_bounds() {
    let (min, max) = (50u64, 150u64);
    let min_duration = Duration::from_millis(min);
    let max_duration = Duration::from_millis(max);

    for _ in 0..100 {
        let duration = ElectionTimer::random_duration(min, max);

        assert!(
            duration >= min_duration,
            "Duration {duration:?} should be >= {min_duration:?}",
        );
        assert!(
            duration < max_duration,
            "Duration {duration:?} should be < {max_duration:?}",
        );
    }
}

/// Test: ElectionTimer.random_duration() has reasonable distribution
///
/// Scenario:
/// - Generate many random durations
/// - Verify they're not all clustered at start or end of range
#[tokio::test]
async fn test_election_timer_random_duration_distribution() {
    let (min, max) = (100u64, 200u64);
    let mut durations = Vec::new();

    for _ in 0..100 {
        let duration = ElectionTimer::random_duration(min, max);
        durations.push(duration.as_millis() as u64);
    }

    durations.sort();

    // Get quartiles to check distribution
    let q1 = durations[24]; // 25th percentile
    let q3 = durations[74]; // 75th percentile

    // With good randomness, q3 should be significantly > q1
    // This would fail if duration was always near min or always near max
    assert!(
        q3 > q1 + 10,
        "Quartiles suggest good distribution: Q1={q1}, Q3={q3}",
    );
}

// ============================================================================
// ReplicationTimer Tests
// ============================================================================

/// Test: ReplicationTimer initializes with correct timeouts
///
/// Scenario:
/// - Create timer with replication_timeout=100ms, batch_interval=50ms
/// - Verify both deadlines are set correctly
#[tokio::test]
async fn test_replication_timer_init() {
    let replication_timeout = 100u64;
    let batch_interval = 50u64;

    let timer = ReplicationTimer::new(replication_timeout, batch_interval);

    let now = Instant::now();

    // Check replication deadline
    let replication_elapsed = timer.replication_deadline() - now;
    assert!(
        replication_elapsed >= Duration::from_millis(replication_timeout - 5),
        "Replication deadline should be close to timeout value"
    );

    // Check batch deadline
    let batch_elapsed = timer.batch_deadline() - now;
    assert!(
        batch_elapsed >= Duration::from_millis(batch_interval - 5),
        "Batch deadline should be close to interval value"
    );
}

/// Test: ReplicationTimer.next_deadline() returns earlier deadline
///
/// Scenario:
/// - Replication timeout: 100ms
/// - Batch interval: 50ms
/// - next_deadline() should return batch deadline (50ms)
#[tokio::test]
async fn test_replication_timer_next_deadline_earlier() {
    let timer = ReplicationTimer::new(100u64, 50u64);

    let replication = timer.replication_deadline();
    let batch = timer.batch_deadline();
    let next = timer.next_deadline();

    // Batch is shorter, so next_deadline should equal batch
    assert_eq!(
        next, batch,
        "next_deadline should be the minimum (batch deadline)"
    );

    assert!(
        next < replication,
        "Batch deadline should be earlier than replication deadline"
    );
}

/// Test: ReplicationTimer.reset_replication() updates replication deadline
///
/// Scenario:
/// - Create timer
/// - Get initial replication deadline
/// - Sleep and reset replication
/// - New deadline should be later
#[tokio::test]
async fn test_replication_timer_reset_replication() {
    let mut timer = ReplicationTimer::new(100u64, 50u64);
    let old_deadline = timer.replication_deadline();

    sleep(Duration::from_millis(10)).await;

    timer.reset_replication();
    let new_deadline = timer.replication_deadline();

    assert!(
        new_deadline > old_deadline,
        "New replication deadline should be later than old one"
    );
}

/// Test: ReplicationTimer.reset_batch() updates batch deadline
///
/// Scenario:
/// - Create timer
/// - Get initial batch deadline
/// - Sleep and reset batch
/// - New deadline should be later
#[tokio::test]
async fn test_replication_timer_reset_batch() {
    let mut timer = ReplicationTimer::new(100u64, 50u64);
    let old_deadline = timer.batch_deadline();

    sleep(Duration::from_millis(10)).await;

    timer.reset_batch();
    let new_deadline = timer.batch_deadline();

    assert!(
        new_deadline > old_deadline,
        "New batch deadline should be later than old one"
    );
}

/// Test: ReplicationTimer.is_expired() reflects next_deadline() expiration
///
/// Scenario:
/// - Create timer with very short timeouts [5ms, 5ms]
/// - Should not be expired immediately
/// - After sleeping, should be expired
#[tokio::test]
async fn test_replication_timer_is_expired() {
    let timer = ReplicationTimer::new(5u64, 5u64);

    assert!(
        !timer.is_expired(),
        "Timer should not be expired immediately"
    );

    sleep(Duration::from_millis(20)).await;

    assert!(timer.is_expired(), "Timer should be expired after timeout");
}

/// Test: ReplicationTimer with equal timeout and interval
///
/// Scenario:
/// - Both timeouts are 100ms
/// - next_deadline should be either one (they're equal)
#[tokio::test]
async fn test_replication_timer_equal_timeouts() {
    let timer = ReplicationTimer::new(100u64, 100u64);

    let replication = timer.replication_deadline();
    let batch = timer.batch_deadline();
    let next = timer.next_deadline();

    // When equal, min will return one of them (implementation-dependent)
    assert_eq!(next, replication.min(batch));
}

/// Test: ReplicationTimer reset_replication doesn't affect batch deadline
///
/// Scenario:
/// - Create timer
/// - Get initial batch deadline
/// - Reset replication
/// - Batch deadline should be unchanged
#[tokio::test]
async fn test_replication_timer_reset_replication_independent() {
    let mut timer = ReplicationTimer::new(100u64, 50u64);
    let old_batch = timer.batch_deadline();

    timer.reset_replication();

    let new_batch = timer.batch_deadline();

    // Batch deadline should not change when we only reset replication
    assert_eq!(
        old_batch, new_batch,
        "Batch deadline should be unchanged after resetting replication"
    );
}

/// Test: ReplicationTimer reset_batch doesn't affect replication deadline
///
/// Scenario:
/// - Create timer
/// - Get initial replication deadline
/// - Reset batch
/// - Replication deadline should be unchanged
#[tokio::test]
async fn test_replication_timer_reset_batch_independent() {
    let mut timer = ReplicationTimer::new(100u64, 50u64);
    let old_replication = timer.replication_deadline();

    timer.reset_batch();

    let new_replication = timer.replication_deadline();

    // Replication deadline should not change when we only reset batch
    assert_eq!(
        old_replication, new_replication,
        "Replication deadline should be unchanged after resetting batch"
    );
}
