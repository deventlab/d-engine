use std::time::Duration;

use tokio::time::Instant;

#[derive(Clone)]
pub struct ReplicationTimer {
    replication_timeout: Duration,
    batch_interval: Duration,
    replication_deadline: Instant,
    batch_deadline: Instant,
}

impl ReplicationTimer {
    pub fn new(replication_timeout_ms: u64, batch_interval_ms: u64) -> Self {
        let now = Instant::now();
        Self {
            replication_timeout: Duration::from_millis(replication_timeout_ms),
            batch_interval: Duration::from_millis(batch_interval_ms),
            replication_deadline: now + Duration::from_millis(replication_timeout_ms),
            batch_deadline: now + Duration::from_millis(batch_interval_ms),
        }
    }

    pub fn reset_replication(&mut self) {
        self.replication_deadline = Instant::now() + self.replication_timeout;
    }

    pub fn reset_batch(&mut self) {
        self.batch_deadline = Instant::now() + self.batch_interval;
    }

    pub fn remaining(&self) -> Duration {
        self.next_deadline()
            .saturating_duration_since(Instant::now())
    }

    pub fn next_deadline(&self) -> Instant {
        self.replication_deadline.min(self.batch_deadline)
    }

    pub fn batch_deadline(&self) -> Instant {
        self.batch_deadline
    }

    pub fn replication_deadline(&self) -> Instant {
        self.replication_deadline
    }

    pub fn tick_interval(&self) -> Duration {
        self.batch_interval.min(self.replication_timeout)
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.next_deadline() <= Instant::now()
    }
}
