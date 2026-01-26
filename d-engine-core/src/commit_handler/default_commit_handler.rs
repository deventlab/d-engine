use d_engine_proto::common::Entry;
use d_engine_proto::common::entry_payload::Payload;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::CommitHandler;
use crate::Membership;
use crate::NewCommitData;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateMachineHandler;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::scoped_timer::ScopedTimer;

// Dependencies container
pub struct CommitHandlerDependencies<T: TypeConfig> {
    pub state_machine_handler: Arc<SMHOF<T>>,
    pub raft_log: Arc<ROF<T>>,
    pub membership: Arc<MOF<T>>,
    pub event_tx: mpsc::Sender<RaftEvent>,
    pub sm_apply_tx: mpsc::UnboundedSender<Vec<Entry>>,
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
    config: Arc<RaftNodeConfig>,
    membership: Arc<MOF<T>>,

    event_tx: mpsc::Sender<RaftEvent>, // Cloned from Raft
    sm_apply_tx: mpsc::UnboundedSender<Vec<Entry>>, // Send entries to SM Worker

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

        let mut new_commit_rx =
            self.new_commit_rx.take().expect("Expected a commit recv but found None");
        let mut shutdown_signal = self.shutdown_signal.clone();

        loop {
            tokio::select! {
                    // P0: shutdown received;
                    _ = shutdown_signal.changed() => {
                        info!("[CommitHandler] shutdown signal received.");
                        return Ok(());
                    }

                    // Submit events in real time
                    Some(new_commit_data) = new_commit_rx.recv() => {
                        trace!("[Node-{}] new commit index = {:?} committed..", self.my_id, new_commit_data.new_commit_index);
                        self.state_machine_handler.update_pending(new_commit_data.new_commit_index);

                        // Keep sync my current term and role from new commit data
                        self.my_current_term = new_commit_data.current_term;
                        self.my_role = new_commit_data.role;


                            trace!("_ = self.check_batch_size");
                            if let Err(e) = self.process_batch().await {
                                error!("Failed to process batch: {}", e);
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
    pub fn new(
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
            config,
            event_tx: deps.event_tx,
            sm_apply_tx: deps.sm_apply_tx,
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
        let _timer = ScopedTimer::new("CommitHandler::process_batch");

        let pending_range = self.state_machine_handler.pending_range();
        trace!("[Node-{}] Pending range: {:?}", self.my_id, pending_range);

        let Some(range) = pending_range else {
            return Ok(());
        };
        let entries = self.raft_log.get_entries_range(range)?;

        debug!(
            "[Node-{}] commit handler process batch, length = {}",
            self.my_id,
            entries.len()
        );
        // Merge consecutive normal commands
        let mut command_batch = vec![];

        let mut last_error = None;
        for entry in entries {
            // In exact log order
            if let Some(ref entry_payload) = entry.payload {
                match entry_payload.payload {
                    Some(Payload::Command(_)) => command_batch.push(entry),
                    Some(Payload::Config(_)) => {
                        command_batch.push(entry.clone());

                        self.send_to_sm_worker(&mut command_batch).await?;

                        if last_error.is_none() {
                            if let Err(e) = self.apply_config_change(entry).await {
                                last_error = Some(e);
                            }
                        }
                    }
                    Some(Payload::Noop(_)) => {
                        command_batch.push(entry);

                        self.send_to_sm_worker(&mut command_batch).await?;
                    }
                    None => unreachable!(),
                }
            }
        }

        debug!(?last_error, "flush complete");
        // Finally send remaining commands to SM Worker
        if let Some(e) = last_error {
            return Err(e);
        } else {
            self.send_to_sm_worker(&mut command_batch).await?;
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

    /// Check if configuration change is a self-removal
    ///
    /// Returns true if the change is RemoveNode(my_id), indicating
    /// that this node is removing itself from the cluster.
    pub(crate) fn is_self_removal_config(
        my_id: u32,
        change: &d_engine_proto::common::MembershipChange,
    ) -> bool {
        matches!(
            &change.change,
            Some(d_engine_proto::common::membership_change::Change::RemoveNode(remove))
            if remove.node_id == my_id
        )
    }

    /// Apply configuration change and detect self-removal
    ///
    /// If the first configure been applied failed, then all the following commands will be
    /// rejected. (Consistency)
    ///
    /// Per Raft protocol: Leader can remove itself. After applying the removal,
    /// leader must step down immediately.
    async fn apply_config_change(
        &self,
        entry: Entry,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("apply_config_change");
        debug!("Received config change:{:?}", &entry);

        if let Some(payload) = entry.payload {
            if let Some(Payload::Config(change)) = payload.payload {
                // Check if this is a self-removal (check BEFORE applying)
                let is_self_removal = Self::is_self_removal_config(self.my_id, &change);

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

                // 2.5. Notify leader to refresh cluster metadata cache
                // This must happen AFTER membership is applied
                if let Err(e) = self.event_tx.send(RaftEvent::MembershipApplied).await {
                    warn!("Failed to send MembershipApplied event: {:?}", e);
                }

                // 3. Leader self-removal: Step down immediately per Raft protocol
                if is_self_removal {
                    warn!(
                        "[{}] Node removed from cluster membership, triggering step down (index {})",
                        self.my_id, entry.index
                    );
                    // Signal step down - error is non-fatal as removal is already committed
                    if let Err(e) = self.event_tx.send(RaftEvent::StepDownSelfRemoved).await {
                        error!(
                            "[{}] Failed to send StepDownSelfRemoved event: {:?}",
                            self.my_id, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    // Define flush as an async function
    async fn send_to_sm_worker(
        &self,
        batch: &mut Vec<Entry>,
    ) -> Result<()> {
        if !batch.is_empty() {
            let entries = std::mem::take(batch);
            trace!(
                "[Node-{}] Sending batch to SM Worker: {} entries",
                self.my_id,
                entries.len()
            );

            // Send entries to SM Worker without waiting for apply
            self.sm_apply_tx.send(entries).map_err(|e| {
                error!("[Node-{}] SM Worker channel closed: {:?}", self.my_id, e);
                crate::Error::Fatal(format!("SM Worker channel closed: {e:?}"))
            })?;
        }
        Ok(())
    }
}
