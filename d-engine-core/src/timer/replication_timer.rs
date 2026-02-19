use std::time::Duration;

use tokio::time::Instant;

#[derive(Clone)]
pub struct ReplicationTimer {
    replication_timeout: Duration,
    replication_deadline: Instant,
}

impl ReplicationTimer {
    pub fn new(replication_timeout_ms: u64) -> Self {
        let now = Instant::now();
        Self {
            replication_timeout: Duration::from_millis(replication_timeout_ms),
            replication_deadline: now + Duration::from_millis(replication_timeout_ms),
        }
    }

    pub fn reset_replication(&mut self) {
        self.replication_deadline = Instant::now() + self.replication_timeout;
    }

    pub fn next_deadline(&self) -> Instant {
        self.replication_deadline
    }

    pub fn replication_deadline(&self) -> Instant {
        self.replication_deadline
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.next_deadline() <= Instant::now()
    }
}
