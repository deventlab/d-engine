use super::{follower_state::FollowerState, RaftContext, RaftEvent, RaftRole, RoleEvent};
use crate::{
    alias::{EOF, MOF, POF, REPOF, ROF, SMHOF, SMOF, SSOF, TROF},
    Error, RaftLog, Result, Settings, StateMachine, StateStorage, TypeConfig,
};
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use tokio::{
    sync::{mpsc, watch},
    time::sleep_until,
};

pub struct Raft<T>
where
    T: TypeConfig,
{
    pub id: u32,
    pub role: RaftRole<T>,
    pub ctx: RaftContext<T>,
    pub settings: Arc<Settings>,

    // Channels with peers
    // PeersChannel will be used inside Transport::spawn when sending peers messages
    pub peer_channels: Option<Arc<POF<T>>>,

    // Network & Storage events
    pub event_tx: mpsc::Sender<RaftEvent>,
    event_rx: mpsc::Receiver<RaftEvent>,

    // Timer
    pub role_tx: mpsc::UnboundedSender<RoleEvent>,
    role_rx: mpsc::UnboundedReceiver<RoleEvent>,

    // For business logic to apply logs into state machine
    new_commit_listener: Vec<mpsc::UnboundedSender<u64>>,

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,

    // For unit test
    #[cfg(test)]
    test_role_transition_listener: Vec<mpsc::UnboundedSender<i32>>,

    #[cfg(test)]
    test_raft_event_listener: Vec<mpsc::UnboundedSender<RaftEvent>>,
}

impl<T> Raft<T>
where
    T: TypeConfig,
{
    pub fn new(
        id: u32,
        raft_log: ROF<T>,
        state_machine: Arc<SMOF<T>>,
        state_storage: SSOF<T>,
        transport: TROF<T>,
        election_handler: EOF<T>,
        replication_handler: REPOF<T>,
        state_machine_handler: Arc<SMHOF<T>>,
        membership: Arc<MOF<T>>,
        settings: Arc<Settings>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
        event_tx: mpsc::Sender<RaftEvent>,
        event_rx: mpsc::Receiver<RaftEvent>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        // Load last applied index from state machine
        let last_applied_index = state_machine.last_entry_index();

        let ctx = Self::build_context(
            id,
            raft_log,
            state_machine,
            state_storage,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            settings.clone(),
        );

        // let ctx = Box::new(ctx);
        let role = RaftRole::Follower(FollowerState::new(
            id,
            settings.clone(),
            ctx.state_storage.load_hard_state(),
            last_applied_index,
        ));
        Raft {
            id,
            ctx,
            role,
            settings: settings.clone(),

            peer_channels: None,

            event_tx,
            event_rx,

            role_tx,
            role_rx,

            new_commit_listener: Vec::new(),

            shutdown_signal,

            #[cfg(test)]
            test_role_transition_listener: Vec::new(),

            #[cfg(test)]
            test_raft_event_listener: Vec::new(),
        }
    }

    fn build_context(
        id: u32,
        raft_log: ROF<T>,
        state_machine: Arc<SMOF<T>>,
        state_storage: SSOF<T>,
        transport: TROF<T>,

        election_handler: EOF<T>,

        replication_handler: REPOF<T>,

        state_machine_handler: Arc<SMHOF<T>>,

        membership: Arc<MOF<T>>,

        settings: Arc<Settings>,
    ) -> RaftContext<T> {
        RaftContext {
            node_id: id,

            raft_log: Arc::new(raft_log),
            state_machine,
            state_storage: Box::new(state_storage),

            transport: Arc::new(transport),

            membership,

            election_handler,

            replication_handler,

            state_machine_handler,

            settings,
        }
    }

    pub fn join_cluster(&mut self, peer_chanels: Arc<POF<T>>) -> Result<()> {
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
                    warn!("[Raft:{}] shutdown signal received.", self.id);
                    return Err(Error::Exit);
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
                    if let Err(e) = self.handle_role_event(role_event) {
                        error!("handle_role_event: {:?}", e);
                    }
                }

                // P3: Other events
                Some(raft_event) = self.event_rx.recv() => {
                    debug!("receive raft event: {:?}", raft_event);

                    #[cfg(test)]
                    let event = raft_event.clone();

                    self.role.handle_raft_event(raft_event, self.peer_channels()?, &self.ctx, self.role_tx.clone()).await?;

                    #[cfg(test)]
                    self.notify_raft_event(event);
                }

            }
        }
    }

    /// `handle_role_event` will be responsbile to process role trasnsition and
    /// role state events.
    pub fn handle_role_event(&mut self, role_event: RoleEvent) -> Result<()> {
        // All inbound and outbound raft event

        match role_event {
            // Election
            // New Join / Leader->Follower,
            RoleEvent::BecomeFollower(leader_id_option) => {
                debug!("BecomeFollower");
                if let Ok(role) = self.role.become_follower() {
                    self.role = role
                }

                #[cfg(test)]
                self.notify_role_transition();

                //TODO: update membership
            }
            // Follower->Candidate
            RoleEvent::BecomeCandidate => {
                debug!("BecomeCandidate");
                if let Ok(role) = self.role.become_candidate() {
                    self.role = role
                }

                #[cfg(test)]
                self.notify_role_transition();
            }
            // Candidate->Leader
            RoleEvent::BecomeLeader => {
                debug!("BecomeLeader");
                if let Ok(role) = self.role.become_leader() {
                    self.role = role
                }

                #[cfg(test)]
                self.notify_role_transition();
            }
            //  Follower->Learner, Candidate->Learner
            RoleEvent::BecomeLearner => {
                debug!("BecomeLearner");
                if let Ok(role) = self.role.become_learner() {
                    self.role = role
                }

                #[cfg(test)]
                self.notify_role_transition();
            }
            RoleEvent::UpdateTerm { new_term } => {
                debug!("RoleEvent::UpdateTerm: {}", new_term);
                self.role.update_term(new_term);
            }
            RoleEvent::UpdateVote { voted_for } => {
                debug!("RoleEvent::UpdateVote: {:?}", &voted_for);
                if let Err(e) = self.role.update_voted_for(voted_for) {
                    error!("self.role.update_voted_for: {:?}", e);
                }
            }

            RoleEvent::NotifyNewCommitIndex { new_commit_index } => {
                debug!("RoleEvent::NotifyNewCommitIndex: {:?}", new_commit_index);
                self.notify_new_commit(new_commit_index);
            }
            RoleEvent::UpdateMatchIndex {
                node_id,
                new_match_index,
            } => {
                debug!(
                    "RoleEvent::UpdateMatchIndex: node_id:{}, new_match_index:{}",
                    node_id, new_match_index,
                );

                if let Err(e) = self.role.update_match_index(node_id, new_match_index) {
                    error!("self.role.update_match_index: {:?}", e);
                }
            }
            RoleEvent::UpdateNextIndex {
                node_id,
                new_next_index,
            } => {
                debug!(
                    "RoleEvent::UpdateNextIndex: node_id:{}, new_next_index:{}",
                    node_id, new_next_index,
                );

                if let Err(e) = self.role.update_next_index(node_id, new_next_index) {
                    error!("self.role.update_next_index: {:?}", e);
                }
            }
            RoleEvent::UpdateMatchIndexAndNextIndex {
                node_id,
                new_match_index,
                new_next_index,
            } => {
                debug!(
                    "RoleEvent::UpdateMatchIndexAndNextIndex: node_id:{}, new_match_index:{}, new_next_index:{}",
                    node_id, new_match_index, new_next_index
                );
                if let Err(e) = self.role.update_match_index(node_id, new_match_index) {
                    error!("self.role.update_match_index: {:?}", e);
                }
                if let Err(e) = self.role.update_next_index(node_id, new_next_index) {
                    error!("self.role.update_next_index: {:?}", e);
                }
            }
        };

        Ok(())
    }

    pub fn peer_channels(&self) -> Result<Arc<POF<T>>> {
        self.peer_channels
            .clone()
            .ok_or_else(|| Error::FailedSetPeerConnection(format!("handle_raft_event")))
    }

    pub fn register_new_commit_listener(&mut self, tx: mpsc::UnboundedSender<u64>) {
        self.new_commit_listener.push(tx);
    }

    pub fn notify_new_commit(&self, new_commit_index: u64) {
        debug!("notify_new_commit: {}", new_commit_index);

        for tx in &self.new_commit_listener {
            if let Err(e) = tx.send(new_commit_index) {
                error!("notify_new_commit failed: {:?}", e);
            }
        }
    }

    #[cfg(test)]
    pub fn register_role_transition_listener(&mut self, tx: mpsc::UnboundedSender<i32>) {
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
    pub fn register_raft_event_listener(&mut self, tx: mpsc::UnboundedSender<RaftEvent>) {
        self.test_raft_event_listener.push(tx);
    }

    #[cfg(test)]
    pub fn notify_raft_event(&self, event: RaftEvent) {
        debug!("unit test:: notify new raft event: {:?}", &event);

        for tx in &self.test_raft_event_listener {
            tx.send(event.clone()).expect("should succeed");
        }
    }

    #[cfg(test)]
    pub fn set_role(&mut self, role: RaftRole<T>) {
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
            error!("State storage persist node hard state failed: {:?}", e);
        }

        info!("raft_log:: flush...");
        if let Err(e) = self.ctx.raft_log.flush() {
            error!("Flush raft log failed: {:?}", e);
        }

        info!("state_machine:: before_shutdown...");
        if let Err(e) = self.ctx.state_machine.flush() {
            error!("Flush raft log failed: {:?}", e);
        }

        info!("Graceful shutdown node state ...");
    }
}
