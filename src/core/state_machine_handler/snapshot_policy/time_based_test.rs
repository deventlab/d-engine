use std::time::Duration;

use tokio::time;

use super::*;
use crate::core::state_machine_handler::snapshot_policy::SnapshotContext;
use crate::core::state_machine_handler::snapshot_policy::SnapshotPolicy;
use crate::proto::common::LogId;
use crate::LEADER;

#[tokio::test]
async fn test_should_trigger_after_interval() {
    // Create policy with 100ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0)); // 0 seconds for testing
    policy.interval = Duration::from_millis(100);
    policy.reset_timer();

    // Create context (leader)
    let ctx = SnapshotContext {
        role: LEADER, // Assuming LEADER is defined elsewhere
        last_included: LogId { term: 1, index: 1 },
        last_applied: LogId { term: 1, index: 10 },
        current_term: 1,
    };

    // Should not trigger immediately
    assert!(!policy.should_trigger(&ctx));

    // Wait for the interval
    time::sleep(Duration::from_millis(110)).await;

    // Should trigger after interval
    assert!(policy.should_trigger(&ctx));
}

#[tokio::test]
async fn test_should_not_trigger_before_interval() {
    // Create policy with 200ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0));
    policy.interval = Duration::from_millis(200);
    policy.reset_timer();

    // Create context (leader)
    let ctx = SnapshotContext {
        role: LEADER,
        last_included: LogId { term: 1, index: 1 },
        last_applied: LogId { term: 1, index: 10 },
        current_term: 1,
    };

    // Wait less than interval
    time::sleep(Duration::from_millis(100)).await;

    // Should not trigger yet
    assert!(!policy.should_trigger(&ctx));
}

#[tokio::test]
async fn test_reset_timer_works() {
    // Create policy with 100ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0));
    policy.interval = Duration::from_millis(100);
    policy.reset_timer();

    let ctx = SnapshotContext {
        role: LEADER,
        last_included: LogId { term: 1, index: 1 },
        last_applied: LogId { term: 1, index: 10 },
        current_term: 1,
    };

    // Wait less than interval
    time::sleep(Duration::from_millis(90)).await;
    policy.reset_timer();

    // Should not trigger yet
    assert!(!policy.should_trigger(&ctx));
}
