use dashmap::DashMap;
use tonic::async_trait;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait HealthMonitor: Send + Sync + 'static {
    async fn record_failure(
        &self,
        peer_id: u32,
    );
    async fn record_success(
        &self,
        peer_id: u32,
    );
    async fn get_zombie_candidates(&self) -> Vec<u32>;
}

pub(crate) struct RaftHealthMonitor {
    pub(crate) failure_counts: DashMap<u32, u32>,
    pub(crate) zombie_threshold: u32,
}

impl RaftHealthMonitor {
    pub(crate) fn new(zombie_threshold: u32) -> Self {
        RaftHealthMonitor {
            failure_counts: DashMap::new(),
            zombie_threshold,
        }
    }
}

#[async_trait]
impl HealthMonitor for RaftHealthMonitor {
    async fn record_failure(
        &self,
        node_id: u32,
    ) {
        let mut count = self.failure_counts.entry(node_id).or_insert(0);
        *count += 1;
    }

    async fn record_success(
        &self,
        node_id: u32,
    ) {
        self.failure_counts.remove(&node_id); // Reset failure counter
    }

    async fn get_zombie_candidates(&self) -> Vec<u32> {
        self.failure_counts
            .iter()
            .filter(|entry| *entry.value() >= self.zombie_threshold)
            .map(|entry| *entry.key())
            .collect()
    }
}
