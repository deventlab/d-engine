use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(test)]
use mockall::automock;
use tokio::sync::mpsc;
use tracing::debug;

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
    /// Consumed by the inbound event loop (core layer) via select!.
    /// Server layer holds only Sender<u32> — zero dependency on core types.
    zombie_tx: mpsc::Sender<u32>,

    started_at: Instant,
    startup_grace: Duration,
}

impl RaftHealthMonitor {
    /// Convenience constructor with no startup grace (existing behavior,
    /// used by tests that don't care about the grace window).
    #[cfg(test)]
    pub(crate) fn new(zombie_threshold: u32) -> (Self, mpsc::Receiver<u32>) {
        Self::new_with_grace(zombie_threshold, Duration::ZERO)
    }

    /// Returns (monitor, zombie_rx). `startup_grace`: see ZombieConfig::startup_grace.
    pub(crate) fn new_with_grace(
        zombie_threshold: u32,
        startup_grace: Duration,
    ) -> (Self, mpsc::Receiver<u32>) {
        let (zombie_tx, zombie_rx) = mpsc::channel(64);
        (
            RaftHealthMonitor {
                failure_counts: DashMap::new(),
                zombie_threshold,
                zombie_tx,
                started_at: Instant::now(),
                startup_grace,
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
        // Use >= rather than == so a failed send attempt (channel full) is
        // retried on every subsequent failure, not just the one call where the
        // counter happens to equal the threshold exactly.
        if new_count >= self.zombie_threshold {
            if self.started_at.elapsed() < self.startup_grace {
                debug!(
                    node_id,
                    "zombie signal suppressed: still within startup grace"
                );
                return;
            }

            // try_send: never block the caller on channel capacity. This method
            // is invoked from hot RPC-handling paths (see #428) — a receiver
            // that's temporarily backlogged must not stall replication/transport
            // tasks waiting for channel space.
            match self.zombie_tx.try_send(node_id) {
                Ok(()) => {
                    // Reset only after a successful send (guard dropped before
                    // the match to avoid holding it across the try_send call),
                    // so a node that recovers and later fails again can
                    // re-trigger the signal on its next failure streak.
                    if let Some(mut c) = self.failure_counts.get_mut(&node_id) {
                        *c = 0;
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Receiver is backlogged. Do not reset the counter — the
                    // next failure will retry the send instead of the signal
                    // being silently lost.
                    debug!(node_id, "zombie signal deferred: channel full");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Receiver has shut down (node shutting down); nothing left
                    // to notify.
                }
            }
        }
    }

    async fn record_success(
        &self,
        node_id: u32,
    ) {
        self.failure_counts.remove(&node_id);
    }
}

impl RaftHealthMonitor {
    /// Returns true if the zombie signal for `node_id` is still valid.
    ///
    /// A zombie is invalid once `record_success` has been called (peer recovered),
    /// which removes the entry from `failure_counts`. The bridge task uses this
    /// to drop stale zombie signals before forwarding them to the inbound event loop.
    pub(crate) fn is_zombie_valid(
        &self,
        node_id: u32,
    ) -> bool {
        self.failure_counts.contains_key(&node_id)
    }
}
