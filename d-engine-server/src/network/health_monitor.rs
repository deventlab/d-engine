use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(test)]
use mockall::automock;
use tokio::sync::mpsc;

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
}

pub(crate) struct RaftHealthMonitor {
    pub(crate) failure_counts: DashMap<u32, u32>,
    pub(crate) zombie_threshold: u32,
    /// Fires node_id when failure_count first reaches zombie_threshold.
    /// Consumed by the Raft event loop (core layer) via select!.
    /// Server layer holds only Sender<u32> — zero dependency on core types.
    zombie_tx: mpsc::Sender<u32>,
}

impl RaftHealthMonitor {
    /// Returns (monitor, zombie_rx). Caller passes zombie_rx to the Raft event loop.
    pub(crate) fn new(zombie_threshold: u32) -> (Self, mpsc::Receiver<u32>) {
        let (zombie_tx, zombie_rx) = mpsc::channel(64);
        (
            RaftHealthMonitor {
                failure_counts: DashMap::new(),
                zombie_threshold,
                zombie_tx,
            },
            zombie_rx,
        )
    }
}

#[async_trait]
impl HealthMonitor for RaftHealthMonitor {
    async fn record_failure(
        &self,
        node_id: u32,
    ) {
        let new_count = {
            let mut count = self.failure_counts.entry(node_id).or_insert(0);
            *count += 1;
            *count
        };
        // Signal exactly once when the threshold is first crossed.
        if new_count == self.zombie_threshold {
            let _ = self.zombie_tx.try_send(node_id);
        }
    }

    async fn record_success(
        &self,
        node_id: u32,
    ) {
        self.failure_counts.remove(&node_id);
    }
}
