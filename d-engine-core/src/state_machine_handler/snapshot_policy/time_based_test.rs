use std::time::Duration;

use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Leader;
use tokio::time;
use tracing_test::traced_test;

use super::*;
use crate::state_machine_handler::snapshot_policy::SnapshotContext;
use crate::state_machine_handler::snapshot_policy::SnapshotPolicy;

#[tokio::test]
#[traced_test]
async fn test_should_trigger_after_interval() {
    // Create policy with 100ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0)); // 0 seconds for testing
    policy.interval = Duration::from_millis(100);
    policy.reset_timer();

    // Create context (leader)
    let ctx = SnapshotContext {
        role: Leader as i32, // Assuming Leader is defined elsewhere
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
#[traced_test]
async fn test_should_not_trigger_before_interval() {
    // Create policy with 200ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0));
    policy.interval = Duration::from_millis(200);
    policy.reset_timer();

    // Create context (leader)
    let ctx = SnapshotContext {
        role: Leader as i32,
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
#[traced_test]
async fn test_reset_timer_works() {
    // Create policy with 100ms interval
    let mut policy = TimeBasedPolicy::new(Duration::from_secs(0));
    policy.interval = Duration::from_millis(100);
    policy.reset_timer();

    let ctx = SnapshotContext {
        role: Leader as i32,
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
