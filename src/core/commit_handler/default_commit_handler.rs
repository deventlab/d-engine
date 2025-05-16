use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::CommitHandler;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::utils::cluster::error;
use crate::Result;
use crate::StateMachineHandler;
use crate::TypeConfig;

#[derive(Debug)]
pub struct DefaultCommitHandler<T>
where T: TypeConfig
{
    state_machine_handler: Arc<SMHOF<T>>,
    raft_log: Arc<ROF<T>>,
    new_commit_rx: Option<mpsc::UnboundedReceiver<u64>>,
    batch_size_threshold: u64,
    process_interval_ms: u64,

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,
}

#[async_trait]
impl<T> CommitHandler for DefaultCommitHandler<T>
where T: TypeConfig
{
    async fn run(&mut self) -> Result<()> {
        let mut batch_counter = 0;
        // let mut interval = tokio::time::interval(Duration::from_millis(10));
        let mut interval = self.dynamic_interval();
        let mut new_commit_rx = self
            .new_commit_rx
            .take()
            .expect("Expected a commit recv but found None");
        let mut shutdown_signal = self.shutdown_signal.clone();

        loop {
            tokio::select! {
                    // P0: shutdown received;
                    _ = shutdown_signal.changed() => {
                        warn!("[CommitHandler] shutdown signal received.");
                        return Ok(());
                    }

                    // Scheduled batch processing
                    _ = interval.tick() => {
                        trace!("_ = interval.tick()");
                        self.process_batch().await;
                    }

                    // Submit events in real time
                    Some(new_commit) = new_commit_rx.recv() => {
                        self.state_machine_handler.update_pending(new_commit);
                        batch_counter += 1;

                        if batch_counter >= self.batch_size_threshold {
                            debug!("_ = self.check_batch_size");
                            self.process_batch().await;
                            batch_counter = 0;
                        }
                        // snapshot checker
                        if self.state_machine_handler.should_snapshot() {
                            info!("Listened a new commit and should generate snapshot now");
                            if let Err(e) = self.state_machine_handler.create_snapshot().await {
                                error!(%e, "self.state_machine_handler.create_snapshot with error.");
                            }

                        }
                    }
            }
        }
    }
}

impl<T> DefaultCommitHandler<T>
where T: TypeConfig
{
    pub fn new(
        state_machine_handler: Arc<SMHOF<T>>,
        raft_log: Arc<ROF<T>>,
        new_commit_rx: mpsc::UnboundedReceiver<u64>,
        batch_size_threshold: u64,
        process_interval_ms: u64,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            state_machine_handler,
            raft_log,
            new_commit_rx: Some(new_commit_rx),
            batch_size_threshold,
            process_interval_ms,
            shutdown_signal,
        }
    }

    /// Process batch logs
    async fn process_batch(&self) {
        if let Err(e) = self.state_machine_handler.apply_batch(self.raft_log.clone()).await {
            error("process_batch", &e);
        }
    }

    /// Dynamically adjusted timer
    /// Behavior: If multiple ticks are missed, the timer will wait for the next
    /// tick instead of firing immediately.
    pub(crate) fn dynamic_interval(&self) -> tokio::time::Interval {
        let mut interval = tokio::time::interval(Duration::from_millis(self.process_interval_ms));
        debug!("process_interval_ms: {}", self.process_interval_ms);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    }
}
