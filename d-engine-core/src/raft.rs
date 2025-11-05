use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::sleep_until;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::NewCommitData;
use super::RaftContext;
use super::RaftCoreHandlers;
use super::RaftEvent;
use super::RaftRole;
use super::RaftStorageHandles;
use super::RoleEvent;
#[cfg(any(test, feature = "test-utils"))]
use super::raft_event_to_test_event;
use crate::Membership;
use crate::NetworkError;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::TROF;
use d_engine_proto::common::EntryPayload;

pub struct Raft<T>
where
    T: TypeConfig,
{
    pub node_id: u32,
    pub role: RaftRole<T>,
    pub ctx: RaftContext<T>,

    // Network & Storage events
    event_tx: mpsc::Sender<RaftEvent>,
    event_rx: mpsc::Receiver<RaftEvent>,

    // Timer
    role_tx: mpsc::UnboundedSender<RoleEvent>,
    role_rx: mpsc::UnboundedReceiver<RoleEvent>,

    // For business logic to apply logs into state machine
    new_commit_listener: Vec<mpsc::UnboundedSender<NewCommitData>>,

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,

    // For unit test
    #[cfg(any(test, feature = "test-utils"))]
    test_role_transition_listener: Vec<mpsc::UnboundedSender<i32>>,

    #[cfg(any(test, feature = "test-utils"))]
    test_raft_event_listener: Vec<mpsc::UnboundedSender<super::TestEvent>>,
}

pub struct SignalParams {
    pub(crate) role_tx: mpsc::UnboundedSender<RoleEvent>,
    pub(crate) role_rx: mpsc::UnboundedReceiver<RoleEvent>,
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    pub(crate) event_rx: mpsc::Receiver<RaftEvent>,
    pub(crate) shutdown_signal: watch::Receiver<()>,
}

impl SignalParams {
    /// Creates a new SignalParams with the provided channels.
    ///
    /// This is the only way to construct SignalParams from outside d-engine-core,
    /// ensuring controlled initialization of the internal communication channels.
    pub fn new(
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        role_rx: mpsc::UnboundedReceiver<RoleEvent>,
        event_tx: mpsc::Sender<RaftEvent>,
        event_rx: mpsc::Receiver<RaftEvent>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            role_tx,
            role_rx,
            event_tx,
            event_rx,
            shutdown_signal,
        }
    }
}

impl<T> Raft<T>
where
    T: TypeConfig,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u32,
        role: RaftRole<T>,
        storage: RaftStorageHandles<T>,
        transport: TROF<T>,
        handlers: RaftCoreHandlers<T>,
        membership: Arc<MOF<T>>,
        signal_params: SignalParams,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        let ctx = Self::build_context(
            node_id,
            storage,
            transport,
            membership,
            handlers,
            node_config.clone(),
        );

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

            #[cfg(any(test, feature = "test-utils"))]
            test_role_transition_listener: Vec::new(),

            #[cfg(any(test, feature = "test-utils"))]
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
            info!(
                "Node({}) is joining and needs to fetch initial snapshot.",
                self.node_id
            );
            if let Err(e) = self.role.fetch_initial_snapshot(&self.ctx).await {
                warn!(
                    "Initial snapshot failed: {:?}.
            ================================================
            Leader has not generate snapshot yet. New node
            will sync with Leader via append entries requests.
            ================================================
            ",
                    e
                );
                println!(
                    "
            ================================================
            Leader has not generate snapshot yet. New node
            will sync with Leader via append entries requests
            ================================================
                    "
                );
            }
        }

        info!("Node is running");

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
                    trace!(%self.node_id, ?raft_event, "receive raft event");

                    #[cfg(any(test, feature = "test-utils"))]
                    let event = raft_event_to_test_event(&raft_event);

                    if let Err(e) = self.role.handle_raft_event(raft_event, &self.ctx, self.role_tx.clone()).await {
                        error!(%self.node_id, ?e, "handle_raft_event error");
                    }

                    #[cfg(any(test, feature = "test-utils"))]
                    self.notify_raft_event(event);
                }

            }
        }
    }

    /// `handle_role_event` will be responsbile to process role trasnsition and
    /// role state events.
    pub async fn handle_role_event(
        &mut self,
        role_event: RoleEvent,
    ) -> Result<()> {
        // All inbound and outbound raft event

        match role_event {
            RoleEvent::BecomeFollower(_leader_id_option) => {
                debug!("BecomeFollower");
                self.role = self.role.become_follower()?;

                #[cfg(any(test, feature = "test-utils"))]
                self.notify_role_transition();

                //TODO: update membership
            }
            RoleEvent::BecomeCandidate => {
                debug!("BecomeCandidate");
                self.role = self.role.become_candidate()?;

                #[cfg(any(test, feature = "test-utils"))]
                self.notify_role_transition();
            }
            RoleEvent::BecomeLeader => {
                debug!("BecomeLeader");
                self.role = self.role.become_leader()?;

                let peer_ids = self.ctx.membership().get_peers_id_with_condition(|_| true).await;

                self.role.init_peers_next_index_and_match_index(
                    self.ctx.raft_log().last_entry_id(),
                    peer_ids,
                )?;

                //async action
                if !self
                    .role
                    .verify_leadership_persistent(
                        vec![EntryPayload::noop()],
                        true,
                        &self.ctx,
                        &self.role_tx,
                    )
                    .await
                    .unwrap_or(false)
                {
                    warn!(
                        "Verify leadership in new term failed. Now the node is going to step back to Follower..."
                    );
                    self.role_tx.send(RoleEvent::BecomeFollower(None)).map_err(|e| {
                        let error_str = format!("{e:?}");
                        error!("Failed to send: {}", error_str);
                        NetworkError::SingalSendFailed(error_str)
                    })?;
                }

                #[cfg(any(test, feature = "test-utils"))]
                self.notify_role_transition();
            }
            RoleEvent::BecomeLearner => {
                debug!("BecomeLearner");
                self.role = self.role.become_learner()?;

                #[cfg(any(test, feature = "test-utils"))]
                self.notify_role_transition();
            }
            RoleEvent::NotifyNewCommitIndex(new_commit_data) => {
                debug!(
                    ?new_commit_data,
                    "[{}] RoleEvent::NotifyNewCommitIndex.", self.node_id,
                );
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

    pub fn register_new_commit_listener(
        &mut self,
        tx: mpsc::UnboundedSender<NewCommitData>,
    ) {
        self.new_commit_listener.push(tx);
    }

    pub fn notify_new_commit(
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn register_role_transition_listener(
        &mut self,
        tx: mpsc::UnboundedSender<i32>,
    ) {
        self.test_role_transition_listener.push(tx);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn notify_role_transition(&self) {
        let new_role_i32 = self.role.as_i32();
        for tx in &self.test_role_transition_listener {
            tx.send(new_role_i32).expect("should succeed");
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn register_raft_event_listener(
        &mut self,
        tx: mpsc::UnboundedSender<super::TestEvent>,
    ) {
        self.test_raft_event_listener.push(tx);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn notify_raft_event(
        &self,
        event: super::TestEvent,
    ) {
        debug!("unit test:: notify new raft event: {:?}", &event);

        for tx in &self.test_raft_event_listener {
            assert!(tx.send(event.clone()).is_ok(), "should succeed");
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_role(
        &mut self,
        role: RaftRole<T>,
    ) {
        self.role = role
    }

    /// Returns a cloned event sender for external use.
    ///
    /// This provides controlled access to send validated RaftEvents to the Raft core.
    /// Events sent through this sender are still processed through the normal validation
    /// pipeline in the main event loop.
    ///
    /// # Security Note
    /// While this provides access to the event channel, all events are still validated
    /// by the Raft state machine before being applied. The event handler in `handle_raft_event`
    /// performs necessary checks based on current term, role, and state.
    pub fn event_sender(&self) -> mpsc::Sender<RaftEvent> {
        self.event_tx.clone()
    }

    /// Returns a cloned role event sender for internal use.
    ///
    /// # Warning
    /// This is primarily for internal components that need to trigger role transitions.
    /// External callers should not use this unless they understand the Raft protocol deeply.
    #[doc(hidden)]
    pub fn role_event_sender(&self) -> mpsc::UnboundedSender<RoleEvent> {
        self.role_tx.clone()
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
            .raft_log()
            .save_hard_state(&self.role.state().shared_state().hard_state)
        {
            error!(?e, "State storage persist node hard state failed.");
        }

        info!("Graceful shutdown node state ...");
    }
}
