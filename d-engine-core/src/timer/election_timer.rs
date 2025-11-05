use rand::Rng;
use tokio::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct ElectionTimer {
    pub next_deadline: Instant,
    pub timeout_range: (u64, u64),
}

impl ElectionTimer {
    /// @param: timeout_range: (ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    pub fn new(timeout_range: (u64, u64)) -> Self {
        let (min, max) = timeout_range;
        Self {
            next_deadline: Instant::now() + Self::random_duration(min, max),
            timeout_range,
        }
    }

    pub fn reset(&mut self) {
        let (min, max) = self.timeout_range;
        self.next_deadline = Instant::now() + Self::random_duration(min, max);
    }

    pub fn random_duration(
        min: u64,
        max: u64,
    ) -> Duration {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(min..max);
        Duration::from_millis(timeout)
    }

    pub fn next_deadline(&self) -> Instant {
        self.next_deadline
    }

    pub fn is_expired(&self) -> bool {
        self.next_deadline <= Instant::now()
    }
}
