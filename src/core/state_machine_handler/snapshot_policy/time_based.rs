//! Time-based snapshot policy.
//! Triggers a snapshot at regular time intervals.

use std::time::Duration;

use tokio::time::Instant;

use super::SnapshotContext;
use super::SnapshotPolicy;

/// Time-based snapshot policy that triggers snapshots after a configured time interval
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct TimeBasedPolicy {
    /// Time interval between snapshots
    pub(super) interval: Duration,
    /// Last time a snapshot was taken
    pub(super) last_snapshot_time: Instant,
}

impl TimeBasedPolicy {
    /// Creates a new TimeBasedPolicy with the specified interval in seconds
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_snapshot_time: Instant::now(),
        }
    }

    /// Resets the timer (typically called after a snapshot is taken)
    pub(crate) fn reset_timer(&mut self) {
        self.last_snapshot_time = Instant::now();
    }
}

impl SnapshotPolicy for TimeBasedPolicy {
    /// Determines if a snapshot should be triggered based on elapsed time
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        // Only leaders should trigger snapshots
        if !ctx.is_leader() {
            return false;
        }

        // Check if enough time has passed since last snapshot
        let elapsed = self.last_snapshot_time.elapsed();
        elapsed >= self.interval
    }

    fn mark_snapshot_created(&mut self) {
        self.last_snapshot_time = Instant::now();
    }
}
