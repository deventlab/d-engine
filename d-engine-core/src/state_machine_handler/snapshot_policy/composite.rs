//! Composite snapshot policy.
//! Combines multiple policies (e.g., log size and time-based) for more flexible snapshot
//! triggering.

use std::time::Duration;

use super::LogSizePolicy;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::TimeBasedPolicy;

/// Composite policy that triggers if either time-based or log-size policies require a snapshot
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct CompositePolicy {
    time_policy: TimeBasedPolicy,
    size_policy: LogSizePolicy,
}

impl CompositePolicy {
    /// Creates a new CompositePolicy with both time and size policies
    #[allow(unused)]
    pub(crate) fn new(
        time_interval: Duration,
        log_size_threshold: u64,
        log_size_cooldown: Duration,
    ) -> Self {
        // Create log size policy with default cooldown (1 second)
        let size_policy = LogSizePolicy::new(log_size_threshold, log_size_cooldown);

        Self {
            // Convert Duration to seconds for TimeBasedPolicy
            time_policy: TimeBasedPolicy::new(time_interval),
            size_policy,
        }
    }
}

impl SnapshotPolicy for CompositePolicy {
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        // Only leaders should trigger snapshots
        if !ctx.is_leader() {
            return false;
        }

        // Trigger if either policy requires it
        self.time_policy.should_trigger(ctx) || self.size_policy.should_trigger(ctx)
    }

    /// Mark snapshot created (reset all policy states)
    fn mark_snapshot_created(&mut self) {
        self.time_policy.mark_snapshot_created();
        // LogSizePolicy doesn't need reset after snapshot creation
    }
}
