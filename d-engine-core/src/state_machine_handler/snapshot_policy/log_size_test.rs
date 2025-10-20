use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use super::SnapshotContext;
use crate::Leader;
use crate::LogSizePolicy;
use crate::SnapshotPolicy;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;

// Test context builder for better test readability
fn test_context(
    applied: u64,
    snapshot: u64,
    role: i32,
) -> SnapshotContext {
    SnapshotContext {
        last_applied: LogId {
            index: applied,
            term: 1,
        },
        last_included: LogId {
            index: snapshot,
            term: 1,
        },
        current_term: 1,
        role,
    }
}

#[test]
fn triggers_when_log_size_exceeds_threshold() {
    let policy = LogSizePolicy::new(1000, Duration::from_secs(1));
    let ctx = test_context(1500, 500, Leader);
    assert!(policy.should_trigger(&ctx));
}

#[test]
fn does_not_trigger_below_threshold() {
    let policy = LogSizePolicy::new(1000, Duration::from_secs(1));
    let ctx = test_context(1499, 500, Leader);
    assert!(!policy.should_trigger(&ctx));
}

#[test]
fn triggers_at_exact_threshold() {
    let policy = LogSizePolicy::new(1000, Duration::from_secs(1));
    let ctx = test_context(1500, 500, Leader);
    assert!(policy.should_trigger(&ctx));
}

#[test]
fn respects_cooldown_period() {
    let policy = LogSizePolicy::new(100, Duration::from_secs(1));
    let mut ctx = test_context(200, 100, Leader);

    // Initial check should trigger
    assert!(policy.should_trigger(&ctx));

    // Subsequent check during cooldown should not trigger
    ctx.last_applied.index = 300;
    assert!(!policy.should_trigger(&ctx));
}

#[test]
fn resets_after_cooldown_period() {
    let policy = LogSizePolicy::new(100, Duration::from_millis(100));
    let ctx = test_context(200, 100, Leader);

    // First trigger
    assert!(policy.should_trigger(&ctx));

    // Wait longer than cooldown
    std::thread::sleep(Duration::from_millis(150));
    assert!(policy.should_trigger(&ctx));
}

#[test]
fn handles_concurrent_checks_with_cooldown() {
    let policy = Arc::new(LogSizePolicy::new(100, Duration::from_secs(1)));
    let ctx = test_context(200, 100, Leader);

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let p = policy.clone();
            let c = ctx.clone();
            std::thread::spawn(move || p.should_trigger(&c))
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|&&b| b).count(), 1);
}

#[test]
fn non_leader_never_triggers() {
    let policy = LogSizePolicy::new(100, Duration::ZERO);
    let ctx = test_context(200, 100, Follower);
    assert!(!policy.should_trigger(&ctx));
}

#[test]
fn dynamic_threshold_adjustment() {
    let policy = LogSizePolicy::new(1000, Duration::from_secs(1));
    let ctx = test_context(1200, 500, Leader);

    // Initial threshold not met
    assert!(!policy.should_trigger(&ctx));

    // Update threshold
    policy.update_threshold(700);
    assert!(policy.should_trigger(&ctx));
}

#[test]
fn handles_term_regression() {
    let policy = LogSizePolicy::new(100, Duration::ZERO);
    let ctx = SnapshotContext {
        last_applied: LogId {
            index: 200,
            term: 2,
        },
        last_included: LogId {
            index: 100,
            term: 3,
        }, // Higher than current term
        current_term: 2,
        role: Leader,
    };

    assert!(!policy.should_trigger(&ctx));
}

#[test]
fn high_frequency_performance() {
    let policy = LogSizePolicy::new(1000, Duration::from_millis(100));
    let ctx = test_context(1500, 500, Leader);

    let start = Instant::now();
    let mut trigger_count = 0;

    // Simulate 1 million calls
    for _ in 0..1_000_000 {
        if policy.should_trigger(&ctx) {
            trigger_count += 1;
        }
    }

    let duration = start.elapsed();
    assert!(
        duration.as_millis() < 100,
        "Performance regression detected: {duration:?}",
    );
    assert!(
        1 == trigger_count,
        "Unexpected trigger count: {trigger_count}",
    );
}
