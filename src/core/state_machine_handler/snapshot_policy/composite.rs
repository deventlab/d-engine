//! Composite snapshot policy.
//! Combines multiple policies (e.g., log size and time-based) for more flexible snapshot
//! triggering.

use super::SnapshotContext;
use super::SnapshotPolicy;

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub(crate) struct CompositePolicy {
    log_size_threshold: u64, // e.g. 5000 log entries
    time_threshold: u64,     // e.g. 5000 log entries
}

impl SnapshotPolicy for CompositePolicy {
    fn should_trigger(
        &self,
        _ctx: &SnapshotContext,
    ) -> bool {
        false
    }
}
