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
use super::RaftContext;
use super::RaftCoreHandlers;
use super::RaftEvent;
use super::RaftRole;
use super::RaftStorageHandles;
use super::RoleEvent;
use crate::alias::MOF;
use crate::alias::POF;
use crate::alias::TROF;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateMachine;
use crate::StateStorage;
use crate::TypeConfig;

pub struct Raft<T>
where T: TypeConfig
{
    pub node_id: u32,
    pub role: RaftRole<T>,
    pub(crate) ctx: RaftContext<T>,
    pub node_config: Arc<RaftNodeConfig>,

    // Channels with peers
    // PeersChannel will be used inside Transport::spawn when sending peers messages
    pub peer_channels: Option<Arc<POF<T>>>,

    // Network & Storage events
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    event_rx: mpsc::Receiver<RaftEvent>,

    // Timer
    pub(crate) role_tx: mpsc::UnboundedSender<RoleEvent>,
    role_rx: mpsc::UnboundedReceiver<RoleEvent>,

    // For business logic to apply logs into state machine
    new_commit_listener: Vec<mpsc::UnboundedSender<u64>>,

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
where T: TypeConfig
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
        let last_applied_index = Some(storage.state_machine.last_applied().0);

        let ctx = Self::build_context(node_id, storage, transport, membership, handlers, node_config.clone());

        // let ctx = Box::new(ctx);
        let role = RaftRole::Follower(Box::new(FollowerState::new(
            node_id,
            node_config.clone(),
            ctx.storage.state_storage.load_hard_state(),
            last_applied_index,
        )));
        Raft {
            node_id,
            ctx,
            role,
            node_config: node_config.clone(),

            peer_channels: None,

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

    pub fn join_cluster(
        &mut self,
        peer_chanels: Arc<POF<T>>,
    ) -> Result<()> {
        self.peer_channels = Some(peer_chanels);
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
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
                    let (peer_channels, ctx) = {
                        let pc = self.peer_channels()?;
                        let ctx = &self.ctx;
                        (pc, ctx)
                    };

                    if let Err(e) = self.role.tick(role_tx, event_tx, peer_channels, ctx).await {
                        error!("tick failed: {:?}", e);
                    } else {
                        trace!("tick success");
                    }
                }

                // P2: Role events
                Some(role_event) = self.role_rx.recv() => {
                    debug!("receive role event: {:?}", role_event);
                    if let Err(e) = self.handle_role_event(role_event).await {
                        error!("handle_role_event: {:?}", e);
                    }
                }

                // P3: Other events
                Some(raft_event) = self.event_rx.recv() => {
                    debug!("receive raft event: {:?}", raft_event);

                    #[cfg(test)]
                    let event = raft_event_to_test_event(&raft_event);

                    if let Err(e) = self.role.handle_raft_event(raft_event, self.peer_channels()?, &self.ctx, self.role_tx.clone()).await {
                        error!("handle_raft_event: {:?}", e);
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
                if self
                    .role
                    .verify_leadership_in_new_term(self.peer_channels()?, &self.ctx, self.role_tx.clone())
                    .await
                    .is_err()
                {
                    info!("Verify leadership in new term failed. Now the node is going to step back to Follower...");
                    self.role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                        let error_str = format!("{:?}", e);
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
            RoleEvent::NotifyNewCommitIndex { new_commit_index } => {
                debug!(
                    "[{}] RoleEvent::NotifyNewCommitIndex: {:?}",
                    self.node_id, new_commit_index
                );
                self.notify_new_commit(new_commit_index);
            }

            RoleEvent::ReprocessEvent(raft_event) => {
                info!("Replay the RaftEvent: {:?}", &raft_event);
                self.event_tx.send(*raft_event).await.map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    NetworkError::SingalSendFailed(error_str)
                })?;
            }
        };

        Ok(())
    }

    pub fn peer_channels(&self) -> Result<Arc<POF<T>>> {
        self.peer_channels
            .clone()
            .ok_or_else(|| MembershipError::SetupClusterConnectionFailed("handle_raft_event".to_string()).into())
    }

    pub fn register_new_commit_listener(
        &mut self,
        tx: mpsc::UnboundedSender<u64>,
    ) {
        self.new_commit_listener.push(tx);
    }

    pub fn notify_new_commit(
        &self,
        new_commit_index: u64,
    ) {
        debug!("notify_new_commit: {}", new_commit_index);

        for tx in &self.new_commit_listener {
            if let Err(e) = tx.send(new_commit_index) {
                error!("notify_new_commit failed: {:?}", e);
            }
        }
    }

    #[cfg(test)]
    pub fn register_role_transition_listener(
        &mut self,
        tx: mpsc::UnboundedSender<i32>,
    ) {
        self.test_role_transition_listener.push(tx);
    }

    #[cfg(test)]
    pub fn notify_role_transition(&self) {
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
    pub fn set_role(
        &mut self,
        role: RaftRole<T>,
    ) {
        self.role = role
    }
}

impl<T> Drop for Raft<T>
where T: TypeConfig
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

        if let Err(e) = self.ctx.state_machine().save_hard_state() {
            error!(?e, "State storage persist node hard state failed.");
        }

        info!("raft_log:: flush...");
        if let Err(e) = self.ctx.storage.raft_log.flush() {
            error!(?e, "Flush raft log failed.");
        }

        info!("state_machine:: before_shutdown...");
        if let Err(e) = self.ctx.storage.state_machine.flush() {
            error!(?e, "Flush raft log failed.");
        }

        info!("Graceful shutdown node state ...");
    }
}
