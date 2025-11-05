use std::time::Duration;

use tracing_test::traced_test;

use super::*;
use crate::state_machine_handler::snapshot_policy::SnapshotContext;
use crate::state_machine_handler::snapshot_policy::SnapshotPolicy;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;

/// Creates a leader context for testing
fn leader_ctx(last_applied_index: u64) -> SnapshotContext {
    SnapshotContext {
        role: Leader.into(),
        last_included: LogId { term: 1, index: 0 },
        last_applied: LogId {
            term: 1,
            index: last_applied_index,
        },
        current_term: 1,
    }
}

/// Creates a follower context for testing
fn follower_ctx(last_applied_index: u64) -> SnapshotContext {
    SnapshotContext {
        role: Follower.into(),
        last_included: LogId { term: 1, index: 0 },
        last_applied: LogId {
            term: 1,
            index: last_applied_index,
        },
        current_term: 1,
    }
}

#[tokio::test]
#[traced_test]
async fn test_composite_policy_time_trigger() {
    // Setup policy with short time interval (100ms) and high log threshold
    let mut policy = CompositePolicy::new(Duration::from_millis(100), 1000, Duration::from_secs(1));
    let ctx = leader_ctx(0);

    // Should not trigger immediately
    assert!(!policy.should_trigger(&ctx));

    // Wait for interval to pass
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        policy.should_trigger(&ctx),
        "Should trigger after time interval"
    );

    // Reset after snapshot
    policy.mark_snapshot_created();
    assert!(
        !policy.should_trigger(&ctx),
        "Should not trigger immediately after reset"
    );
}

#[tokio::test]
#[traced_test]
async fn test_composite_policy_log_size_trigger() {
    // Setup policy with long time interval (1 hour) and small log threshold (5)
    // Use zero cooldown to avoid timing issues in test
    let mut policy = CompositePolicy::new(Duration::from_secs(3600), 5, Duration::from_millis(0));

    // Initial state: lag=0 (last_applied=0, last_included=0)
    let mut ctx = leader_ctx(0);
    assert!(!policy.should_trigger(&ctx), "Should not trigger initially");

    // Increase log index to exceed threshold (lag = 6 - 0 = 6)
    ctx.last_applied.index = 6;
    assert!(
        policy.should_trigger(&ctx),
        "Should trigger when log size threshold exceeded"
    );

    // Reset after snapshot
    policy.mark_snapshot_created();
    // Reset context to simulate snapshot inclusion
    ctx.last_included.index = 6;
    ctx.last_applied.index = 6;
    assert!(
        !policy.should_trigger(&ctx),
        "Should not trigger immediately after reset"
    );
}

#[tokio::test]
#[traced_test]
async fn test_composite_policy_follower_never_triggers() {
    // Setup policy that should trigger easily
    let policy = CompositePolicy::new(Duration::from_millis(10), 1, Duration::from_secs(1));
    let mut ctx = follower_ctx(100); // High log index

    // Should never trigger for follower
    assert!(!policy.should_trigger(&ctx));

    // Wait and update logs - still shouldn't trigger
    tokio::time::sleep(Duration::from_millis(20)).await;
    ctx.last_applied.index = 200;
    assert!(
        !policy.should_trigger(&ctx),
        "Should not trigger for follower role"
    );
}

#[tokio::test]
#[traced_test]
async fn test_composite_policy_both_conditions() {
    // Setup policy with short time interval and small log threshold
    // Use zero cooldown for log size policy
    let mut policy = CompositePolicy::new(Duration::from_millis(50), 2, Duration::from_millis(0));

    // Test log size trigger
    let mut ctx = leader_ctx(3); // lag=3 (exceeds threshold=2)
    assert!(policy.should_trigger(&ctx), "Should trigger on log size");
    policy.mark_snapshot_created();

    // Reset log positions after snapshot
    ctx.last_included.index = 3;
    ctx.last_applied.index = 3;

    // Test time trigger
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(policy.should_trigger(&ctx), "Should trigger on time");
    policy.mark_snapshot_created();

    // Reset log positions
    ctx.last_included.index = 3;
    ctx.last_applied.index = 3;

    // Test both conditions met simultaneously
    ctx.last_applied.index = 10; // Exceed log threshold
    tokio::time::sleep(Duration::from_millis(60)).await; // Exceed time threshold
    assert!(
        policy.should_trigger(&ctx),
        "Should trigger when both conditions are met"
    );
}

#[tokio::test]
#[traced_test]
async fn test_log_index_update_mechanism() {
    // Setup policy with large time interval and threshold of 5
    let policy = CompositePolicy::new(Duration::from_secs(3600), 5, Duration::from_millis(0));

    // Threshold value (lag=4) should not trigger
    let ctx_threshold = leader_ctx(4);
    assert!(
        !policy.should_trigger(&ctx_threshold),
        "Should not trigger at threshold"
    );

    // Above threshold (lag=6) should trigger
    let ctx_above = leader_ctx(6);
    assert!(
        policy.should_trigger(&ctx_above),
        "Should trigger above threshold"
    );
}
