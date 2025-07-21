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
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::Membership;
use crate::NewCommitData;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateMachineHandler;
use crate::TypeConfig;

// Dependencies container
pub struct CommitHandlerDependencies<T: TypeConfig> {
    pub state_machine_handler: Arc<SMHOF<T>>,
    pub raft_log: Arc<ROF<T>>,
    pub membership: Arc<MOF<T>>,
    pub event_tx: mpsc::Sender<RaftEvent>,
    pub shutdown_signal: watch::Receiver<()>,
}

#[derive(Debug)]
pub struct DefaultCommitHandler<T>
where
    T: TypeConfig,
{
    my_id: u32,
    my_role: i32,
    my_current_term: u64,
    state_machine_handler: Arc<SMHOF<T>>,
    raft_log: Arc<ROF<T>>,
    new_commit_rx: Option<mpsc::UnboundedReceiver<NewCommitData>>,
    // batch_size_threshold: u64,
    // process_interval_ms: u64,
    config: Arc<RaftNodeConfig>,
    membership: Arc<MOF<T>>,

    event_tx: mpsc::Sender<RaftEvent>, // Cloned from Raft

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,
}

#[async_trait]
impl<T> CommitHandler for DefaultCommitHandler<T>
where
    T: TypeConfig,
{
    async fn run(&mut self) -> Result<()> {
        info!("[Node-{}] Commit handler started", self.my_id);

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
                        trace!("[Node-{}] commit handler tick...", self.my_id);
                        if let Err(e) = self.process_batch().await {
                            error!("Failed to process batch: {}", e);
                        }
                    }

                    // Submit events in real time
                    Some(new_commit_data) = new_commit_rx.recv() => {
                        trace!("[Node-{}] new commit index = {:?} committed..", self.my_id, new_commit_data.new_commit_index);
                        self.state_machine_handler.update_pending(new_commit_data.new_commit_index);

                        // Keep sync my current term and role from new commit data
                        self.my_current_term = new_commit_data.current_term;
                        self.my_role = new_commit_data.role;

                        batch_counter += 1;

                        if batch_counter >= self.config.raft.commit_handler.batch_size_threshold {
                            trace!("_ = self.check_batch_size");
                            if let Err(e) = self.process_batch().await {
                                error!("Failed to process batch: {}", e);
                            }
                            batch_counter = 0;
                        }
                    }
            }
        }
    }
}

impl<T> DefaultCommitHandler<T>
where
    T: TypeConfig,
{
    pub(crate) fn new(
        my_id: u32,
        my_role: i32,
        my_current_term: u64,
        deps: CommitHandlerDependencies<T>,
        config: Arc<RaftNodeConfig>,
        new_commit_rx: mpsc::UnboundedReceiver<NewCommitData>,
    ) -> Self {
        Self {
            my_id,
            my_role,
            my_current_term,
            state_machine_handler: deps.state_machine_handler,
            raft_log: deps.raft_log,
            membership: deps.membership,
            new_commit_rx: Some(new_commit_rx),
            // batch_size_threshold: config.batch_size,
            // process_interval_ms: config.process_interval_ms,
            config,
            event_tx: deps.event_tx,
            shutdown_signal: deps.shutdown_signal,
        }
    }

    /// Process batch logs
    /// - Separates config changes from application commands
    /// - Applies config changes first via membership module
    /// - Then applies application commands to state machine
    /// - Error handling for config changes
    ///
    /// # Note:Sequential Integrity
    /// Consider this sequence in a single batch: [ConfigRemove(A), ConfigAdd(B), EntryNormal(X)]
    pub(crate) async fn process_batch(&self) -> Result<()> {
        let pending_range = self.state_machine_handler.pending_range();
        trace!("[Node-{}] Pending range: {:?}", self.my_id, pending_range);

        let Some(range) = pending_range else {
            return Ok(());
        };
        let entries = self.raft_log.get_entries_between(range);

        debug!(
            "[Node-{}] Merge consecutive normal commands length = {}",
            self.my_id,
            entries.len()
        );
        // Merge consecutive normal commands
        let mut command_batch = vec![];
        let flush = |batch: &mut Vec<_>| -> Result<()> {
            if !batch.is_empty() {
                // Use take to transfer ownership while preserving the underlying memory allocation
                // Note: when taking out the batch, the original order will be maintained
                trace!("[Node-{} | Before] Flushing command batch: {:?}", self.my_id, batch);
                let entries = std::mem::take(batch);
                trace!("[Node-{} | After] Flushing command batch: {:?}", self.my_id, batch);
                self.state_machine_handler.apply_chunk(entries)?;
            }
            Ok(())
        };

        let mut last_error = None;
        for entry in entries {
            // In exact log order
            if let Some(ref entry_payload) = entry.payload {
                match entry_payload.payload {
                    Some(Payload::Command(_)) => command_batch.push(entry),
                    Some(Payload::Config(_)) => {
                        command_batch.push(entry.clone());

                        flush(&mut command_batch)?;

                        if last_error.is_none() {
                            if let Err(e) = self.apply_config_change(entry).await {
                                last_error = Some(e);
                            }
                        }
                    }
                    Some(Payload::Noop(_)) => {
                        command_batch.push(entry);

                        flush(&mut command_batch)?;
                    }
                    None => unreachable!(),
                }
            }
        }

        debug!(?last_error, "flush complete");
        // Finally force the remaining commands to be refreshed
        if let Some(e) = last_error {
            return Err(e);
        } else {
            flush(&mut command_batch)?;
        }

        debug!("After processing all entries: validate if generate snapshot");
        // After processing all entries:
        let last_applied = self.state_machine_handler.last_applied();
        debug!(
            "[Node-{}] Commit handler process batch - updated last_applied: {}",
            self.my_id, last_applied
        );

        // Generate snapshot if needed
        if self.config.raft.snapshot.enable
            && self.state_machine_handler.should_snapshot(NewCommitData {
                new_commit_index: last_applied,
                role: self.my_role,
                current_term: self.my_current_term,
            })
        {
            info!("Listened a new commit and should generate snapshot now");

            if let Err(e) = self.event_tx.send(RaftEvent::CreateSnapshotEvent).await {
                error!(?e, "send RaftEvent::CreateSnapshotEvent failed");
            }
        }

        Ok(())
    }

    /// If the first configure been applied failed, then all the following commands will be
    /// rejected. (Consistency)
    async fn apply_config_change(
        &self,
        entry: Entry,
    ) -> Result<()> {
        debug!("Received config change:{:?}", &entry);

        if let Some(payload) = entry.payload {
            if let Some(Payload::Config(change)) = payload.payload {
                // 1. Apply to membership state
                if let Err(e) = self.membership.apply_config_change(change).await {
                    error!(
                        "[{}] Failed to apply config change at index {}: {:?}",
                        self.my_id, entry.index, e
                    );
                    // Critical error - should panic or handle carefully
                    return Err(e);
                }

                // 2. CRITICAL: Barrier point
                self.membership.notify_config_applied(entry.index).await;
            }
        }

        Ok(())
    }

    /// Dynamically adjusted timer
    /// Behavior: If multiple ticks are missed, the timer will wait for the next
    /// tick instead of firing immediately.
    pub(crate) fn dynamic_interval(&self) -> tokio::time::Interval {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.config.raft.commit_handler.process_interval_ms,
        ));
        debug!(
            "process_interval_ms: {}",
            self.config.raft.commit_handler.process_interval_ms
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    }
}
