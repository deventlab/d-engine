use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::sleep_until;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::follower_state::FollowerState;
#[cfg(test)]
use super::raft_event_to_test_event;
use super::NewCommitData;
use super::RaftContext;
use super::RaftCoreHandlers;
use super::RaftEvent;
use super::RaftRole;
use super::RaftStorageHandles;
use super::RoleEvent;
use crate::alias::MOF;
use crate::alias::TROF;
use crate::learner_state::LearnerState;
use crate::proto::common::EntryPayload;
use crate::Membership;
use crate::NetworkError;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateMachine;
use crate::StateStorage;
use crate::TypeConfig;

pub struct Raft<T>
where
    T: TypeConfig,
{
    pub(crate) node_id: u32,
    pub(crate) role: RaftRole<T>,
    pub(crate) ctx: RaftContext<T>,

    // Network & Storage events
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    event_rx: mpsc::Receiver<RaftEvent>,

    // Timer
    pub(crate) role_tx: mpsc::UnboundedSender<RoleEvent>,
    role_rx: mpsc::UnboundedReceiver<RoleEvent>,

    // For business logic to apply logs into state machine
    new_commit_listener: Vec<mpsc::UnboundedSender<NewCommitData>>,

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,

    // For unit test
    #[cfg(test)]
    test_role_transition_listener: Vec<mpsc::UnboundedSender<i32>>,

    #[cfg(test)]
    test_raft_event_listener: Vec<mpsc::UnboundedSender<super::TestEvent>>,
}

pub(crate) struct SignalParams {
    pub(crate) role_tx: mpsc::UnboundedSender<RoleEvent>,
    pub(crate) role_rx: mpsc::UnboundedReceiver<RoleEvent>,
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    pub(crate) event_rx: mpsc::Receiver<RaftEvent>,
    pub(crate) shutdown_signal: watch::Receiver<()>,
}

impl<T> Raft<T>
where
    T: TypeConfig,
{
    pub(crate) fn new(
        node_id: u32,
        storage: RaftStorageHandles<T>,
        transport: TROF<T>,
        handlers: RaftCoreHandlers<T>,
        membership: Arc<MOF<T>>,
        signal_params: SignalParams,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        // Load last applied index from state machine
        let last_applied_index = Some(storage.state_machine.last_applied().index);

        let ctx = Self::build_context(node_id, storage, transport, membership, handlers, node_config.clone());

        let role = if node_config.is_joining() {
            RaftRole::Learner(Box::new(LearnerState::new(node_id, node_config.clone())))
        } else {
            RaftRole::Follower(Box::new(FollowerState::new(
                node_id,
                node_config.clone(),
                ctx.storage.state_storage.load_hard_state(),
                last_applied_index,
            )))
        };

        Raft {
            node_id,
            ctx,
            role,

            event_tx: signal_params.event_tx,
            event_rx: signal_params.event_rx,

            role_tx: signal_params.role_tx,
            role_rx: signal_params.role_rx,

            new_commit_listener: Vec::new(),

            shutdown_signal: signal_params.shutdown_signal,

            #[cfg(test)]
            test_role_transition_listener: Vec::new(),

            #[cfg(test)]
            test_raft_event_listener: Vec::new(),
        }
    }

    fn build_context(
        id: u32,
        storage: RaftStorageHandles<T>,
        transport: TROF<T>,
        membership: Arc<MOF<T>>,
        handlers: RaftCoreHandlers<T>,
        node_config: Arc<RaftNodeConfig>,
    ) -> RaftContext<T> {
        RaftContext {
            node_id: id,
            storage,
            transport: Arc::new(transport),
            membership,
            handlers,

            node_config,
        }
    }

    pub async fn join_cluster(&self) -> Result<()> {
        self.role.join_cluster(&self.ctx).await
    }

    pub async fn run(&mut self) -> Result<()> {
        // Add snapshot handler before main loop
        if self.ctx.node_config.is_joining() {
            info!("Node({}) is joining and needs to fetch initial snapshot.", self.node_id);
            if let Err(e) = self.role.fetch_initial_snapshot(&self.ctx).await {
                error!("Initial snapshot failed: {:?}", e);
                return Err(e);
            }
        }

        if self.role.is_timer_expired() {
            self.role.reset_timer();
        }

        loop {
            // Note: next_deadline wil be reset in each role's tick function
            let tick = sleep_until(self.role.next_deadline());

            tokio::select! {
                // Use biased to ensure branch order
                biased;
                // P0: shutdown received;
                _ = self.shutdown_signal.changed() => {
                    warn!("[Raft:{}] shutdown signal received.", self.node_id);
                    return Ok(());
                }
                // P1: Tick: start Heartbeat(replication) or start Election
                _ = tick => {

                    trace!("receive tick");
                    let role_tx = &self.role_tx;
                    let event_tx = &self.event_tx;

                    if let Err(e) = self.role.tick(role_tx, event_tx, &self.ctx).await {
                        error!("tick failed: {:?}", e);
                    } else {
                        trace!("tick success");
                    }
                }

                // P2: Role events
                Some(role_event) = self.role_rx.recv() => {
                    debug!(%self.node_id, ?role_event, "receive role event");

                    if let Err(e) = self.handle_role_event(role_event).await {
                        error!(%self.node_id, ?e, "handle_role_event error");
                    }
                }

                // P3: Other events
                Some(raft_event) = self.event_rx.recv() => {
                    debug!(%self.node_id, ?raft_event, "receive raft event");

                    #[cfg(test)]
                    let event = raft_event_to_test_event(&raft_event);

                    if let Err(e) = self.role.handle_raft_event(raft_event, &self.ctx, self.role_tx.clone()).await {
                        error!(%self.node_id, ?e, "handle_raft_event error");
                    }

                    #[cfg(test)]
                    self.notify_raft_event(event);
                }

            }
        }
    }

    /// `handle_role_event` will be responsbile to process role trasnsition and
    /// role state events.
    pub(crate) async fn handle_role_event(
        &mut self,
        role_event: RoleEvent,
    ) -> Result<()> {
        // All inbound and outbound raft event

        match role_event {
            RoleEvent::BecomeFollower(_leader_id_option) => {
                debug!("BecomeFollower");
                self.role = self.role.become_follower()?;

                #[cfg(test)]
                self.notify_role_transition();

                //TODO: update membership
            }
            RoleEvent::BecomeCandidate => {
                debug!("BecomeCandidate");
                self.role = self.role.become_candidate()?;

                #[cfg(test)]
                self.notify_role_transition();
            }
            RoleEvent::BecomeLeader => {
                debug!("BecomeLeader");
                self.role = self.role.become_leader()?;

                let peer_ids = self.ctx.membership().get_peers_id_with_condition(|_| true);

                self.role
                    .init_peers_next_index_and_match_index(self.ctx.raft_log().last_entry_id(), peer_ids)?;

                //async action
                if !self
                    .role
                    .verify_internal_quorum_with_retry(vec![EntryPayload::noop()], true, &self.ctx, &self.role_tx)
                    .await
                    .unwrap_or(false)
                {
                    info!("Verify leadership in new term failed. Now the node is going to step back to Follower...");
                    self.role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }

                #[cfg(test)]
                self.notify_role_transition();
            }
            RoleEvent::BecomeLearner => {
                debug!("BecomeLearner");
                self.role = self.role.become_learner()?;

                #[cfg(test)]
                self.notify_role_transition();
            }
            RoleEvent::NotifyNewCommitIndex(new_commit_data) => {
                debug!(?new_commit_data, "[{}] RoleEvent::NotifyNewCommitIndex.", self.node_id,);
                self.notify_new_commit(new_commit_data);
            }

            RoleEvent::ReprocessEvent(raft_event) => {
                info!("Replay the RaftEvent: {:?}", &raft_event);
                self.event_tx.send(*raft_event).await.map_err(|e| {
                    let error_str = format!("{e:?}");
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }
        };

        Ok(())
    }

    pub(crate) fn register_new_commit_listener(
        &mut self,
        tx: mpsc::UnboundedSender<NewCommitData>,
    ) {
        self.new_commit_listener.push(tx);
    }

    pub(crate) fn notify_new_commit(
        &self,
        new_commit_data: NewCommitData,
    ) {
        debug!(?new_commit_data, "notify_new_commit",);

        for tx in &self.new_commit_listener {
            if let Err(e) = tx.send(new_commit_data.clone()) {
                error!("notify_new_commit failed: {:?}", e);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn register_role_transition_listener(
        &mut self,
        tx: mpsc::UnboundedSender<i32>,
    ) {
        self.test_role_transition_listener.push(tx);
    }

    #[cfg(test)]
    pub(crate) fn notify_role_transition(&self) {
        let new_role_i32 = self.role.as_i32();
        for tx in &self.test_role_transition_listener {
            tx.send(new_role_i32).expect("should succeed");
        }
    }

    #[cfg(test)]
    pub(crate) fn register_raft_event_listener(
        &mut self,
        tx: mpsc::UnboundedSender<super::TestEvent>,
    ) {
        self.test_raft_event_listener.push(tx);
    }

    #[cfg(test)]
    pub(crate) fn notify_raft_event(
        &self,
        event: super::TestEvent,
    ) {
        debug!("unit test:: notify new raft event: {:?}", &event);

        for tx in &self.test_raft_event_listener {
            assert!(tx.send(event.clone()).is_ok(), "should succeed");
        }
    }

    #[cfg(test)]
    pub(crate) fn set_role(
        &mut self,
        role: RaftRole<T>,
    ) {
        self.role = role
    }
}

impl<T> Drop for Raft<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        info!("Raft been dropped.");

        if let Err(e) = self
            .ctx
            .state_storage()
            .save_hard_state(self.role.state().shared_state().hard_state)
        {
            error!(?e, "State storage persist node hard state failed.");
        }

        info!("Graceful shutdown node state ...");
    }
}
