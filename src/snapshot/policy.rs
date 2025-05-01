pub(crate) struct SnapshotPolicy {
    threshold: u64, // e.g. 5000 log entries
    last_trigger: u64,
}

impl SnapshotPolicy {
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
}
