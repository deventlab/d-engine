//! Time-based snapshot policy.
//! Triggers a snapshot at regular time intervals.

use super::SnapshotContext;
use super::SnapshotPolicy;

#[derive(Debug, Clone, Default)]
pub(crate) struct TimeBasedPolicy {
    last_trigger: u64,
    threshold: u64, // 24 hours
}

impl TimeBasedPolicy {
    pub(crate) fn check(
        &mut self,
        last_applied: u64,
    ) -> bool {
        if last_applied - self.last_trigger > self.threshold {
            self.last_trigger = last_applied;
            true
        } else {
            false
        }
    }

    pub(crate) fn should_trigger(&self) -> bool {
        false
    }
}

impl SnapshotPolicy for TimeBasedPolicy {
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        false
    }
}
