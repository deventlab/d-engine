use crate::{
    grpc::rpc_service::ClusterMembership, MockElectionCore, MockMembership, MockPeerChannels,
    MockRaftLog, MockReplicationCore, MockStateMachine, MockStateMachineHandler, MockStateStorage,
    MockTransport, Node, Raft, RaftContext, RaftEvent, RaftSettings, RoleEvent, Settings,
};
use dashmap::DashMap;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{mpsc, watch, Mutex};

use super::MockTypeConfig;

pub struct MockBuilder {
    pub id: Option<u32>,
    pub raft_log: Option<MockRaftLog>,
    pub state_machine: Option<Arc<MockStateMachine>>,
    pub state_storage: Option<MockStateStorage>,
    pub transport: Option<MockTransport>,
    pub membership: Option<Arc<MockMembership<MockTypeConfig>>>,
    pub election_handler: Option<MockElectionCore<MockTypeConfig>>,
    pub replication_handler: Option<MockReplicationCore<MockTypeConfig>>,
    pub state_machine_handler: Option<Arc<MockStateMachineHandler<MockTypeConfig>>>,
    pub peer_channels: Option<MockPeerChannels>,
    pub settings: Option<Settings>,
    shutdown_signal: watch::Receiver<()>,

    pub event_tx: Option<mpsc::Sender<RaftEvent>>,
    pub event_rx: Option<mpsc::Receiver<RaftEvent>>,

    pub role_tx: Option<mpsc::UnboundedSender<RoleEvent>>,
    pub role_rx: Option<mpsc::UnboundedReceiver<RoleEvent>>,
}

impl MockBuilder {
    pub fn new(shutdown_signal: watch::Receiver<()>) -> Self {
        Self {
            id: None,
            raft_log: None,
            state_machine: None,
            state_storage: None,
            transport: None,
            membership: None,
            election_handler: None,
            replication_handler: None,
            state_machine_handler: None,
            peer_channels: None,
            settings: None,
            shutdown_signal,

            role_tx: None,
            role_rx: None,
            event_tx: None,
            event_rx: None,
        }
    }

    pub fn build_context(self) -> RaftContext<MockTypeConfig> {
        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(10);

        let (
            id,
            raft_log,
            state_machine,
            state_storage,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            peer_channels,
            settings,
            role_tx,
            role_rx,
            event_tx,
            event_rx,
        ) = (
            self.id.unwrap_or_else(|| 1),
            Arc::new(self.raft_log.unwrap_or_else(|| mock_raft_log())),
            self.state_machine
                .unwrap_or_else(|| Arc::new(mock_state_machine())),
            Box::new(self.state_storage.unwrap_or_else(|| mock_state_storage())),
            Arc::new(self.transport.unwrap_or_else(|| mock_transport())),
            self.election_handler.unwrap_or_else(mock_election_core),
            self.replication_handler
                .unwrap_or_else(mock_replication_handler),
            self.state_machine_handler
                .unwrap_or_else(|| Arc::new(mock_state_machine_handler())),
            self.membership
                .unwrap_or_else(|| Arc::new(mock_membership())),
            self.peer_channels.unwrap_or_else(|| mock_peer_channels()),
            self.settings
                .unwrap_or_else(|| Settings::new().expect("Should succeed to init Settings")),
            self.role_tx.unwrap_or_else(|| role_tx),
            self.role_rx.unwrap_or_else(|| role_rx),
            self.event_tx.unwrap_or_else(|| event_tx),
            self.event_rx.unwrap_or_else(|| event_rx),
        );

        mock_raft_context_internal(
            1,
            raft_log,
            state_machine,
            state_storage,
            transport,
            membership,
            election_handler,
            replication_handler,
            state_machine_handler,
            settings,
        )
    }

    pub fn build_raft(self) -> Raft<MockTypeConfig> {
        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(10);
        let (
            id,
            raft_log,
            state_machine,
            state_storage,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            peer_channels,
            settings,
            role_tx,
            role_rx,
            event_tx,
            event_rx,
        ) = (
            self.id.unwrap_or_else(|| 1),
            self.raft_log.unwrap_or_else(|| mock_raft_log()),
            self.state_machine
                .unwrap_or_else(|| Arc::new(mock_state_machine())),
            self.state_storage.unwrap_or_else(|| mock_state_storage()),
            self.transport.unwrap_or_else(|| mock_transport()),
            self.election_handler.unwrap_or_else(mock_election_core),
            self.replication_handler
                .unwrap_or_else(mock_replication_handler),
            self.state_machine_handler
                .unwrap_or_else(|| Arc::new(mock_state_machine_handler())),
            self.membership
                .unwrap_or_else(|| Arc::new(mock_membership())),
            self.peer_channels.unwrap_or_else(|| mock_peer_channels()),
            self.settings
                .unwrap_or_else(|| Settings::new().expect("Should succeed to init Settings")),
            self.role_tx.unwrap_or_else(|| role_tx),
            self.role_rx.unwrap_or_else(|| role_rx),
            self.event_tx.unwrap_or_else(|| event_tx),
            self.event_rx.unwrap_or_else(|| event_rx),
        );

        let mut raft = Raft::new(
            id,
            raft_log,
            state_machine,
            state_storage,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            Arc::new(Settings {
                raft_settings: RaftSettings {
                    election_timeout_min: 1,
                    election_timeout_max: 2,
                    ..settings.raft_settings
                },
                ..settings
            }),
            role_tx,
            role_rx,
            event_tx,
            event_rx,
            self.shutdown_signal,
        );

        raft.join_cluster(Arc::new(peer_channels))
            .expect("join failed");

        raft
    }

    pub fn build_node(self) -> Node<MockTypeConfig> {
        let raft = self.build_raft();
        let event_tx = raft.event_tx.clone();
        let settings = raft.settings.clone();
        Node::<MockTypeConfig> {
            id: raft.id,
            raft_core: Arc::new(Mutex::new(raft)),
            event_tx,
            ready: AtomicBool::new(false),
            settings,
        }
    }

    pub fn with_raft_log(mut self, raft_log: MockRaftLog) -> Self {
        self.raft_log = Some(raft_log);
        self
    }
    pub fn with_state_machine(mut self, sm: MockStateMachine) -> Self {
        self.state_machine = Some(Arc::new(sm));
        self
    }

    pub fn with_state_storage(mut self, state_storage: MockStateStorage) -> Self {
        self.state_storage = Some(state_storage);
        self
    }

    pub fn with_transport(mut self, transport: MockTransport) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn with_membership(mut self, membership: MockMembership<MockTypeConfig>) -> Self {
        self.membership = Some(Arc::new(membership));
        self
    }

    pub fn with_election_handler(
        mut self,
        election_handler: MockElectionCore<MockTypeConfig>,
    ) -> Self {
        self.election_handler = Some(election_handler);
        self
    }

    pub fn with_replication_handler(
        mut self,
        replication_handler: MockReplicationCore<MockTypeConfig>,
    ) -> Self {
        self.replication_handler = Some(replication_handler);
        self
    }

    pub fn with_state_machine_handler(
        mut self,
        state_machine_handler: MockStateMachineHandler<MockTypeConfig>,
    ) -> Self {
        self.state_machine_handler = Some(Arc::new(state_machine_handler));
        self
    }

    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = Some(settings);
        self
    }

    pub fn with_db_path(mut self, db_root_dir: &str) -> Self {
        let mut settings = Settings::new().expect("Should succeed to init Settings.");
        settings.server_settings.db_root_dir = format!("{}", db_root_dir);
        self.settings = Some(settings);
        self
    }
}

pub fn mock_raft_log() -> MockRaftLog {
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log
}
pub fn mock_state_machine() -> MockStateMachine {
    let mut state_machine = MockStateMachine::new();
    state_machine.expect_last_applied().returning(|| 0);
    state_machine.expect_last_entry_index().returning(|| None);
    state_machine.expect_flush().returning(|| Ok(()));
    state_machine
}
pub fn mock_state_storage() -> MockStateStorage {
    let mut state_storage = MockStateStorage::new();
    state_storage.expect_load_hard_state().returning(|| None);
    state_storage.expect_save_hard_state().returning(|_| Ok(()));
    state_storage
}

pub fn mock_transport() -> MockTransport {
    let mut transport = MockTransport::new();
    transport
}

pub fn mock_election_core() -> MockElectionCore<MockTypeConfig> {
    let mut election_handler = MockElectionCore::new();
    election_handler
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    election_handler
}

pub fn mock_replication_handler() -> MockReplicationCore<MockTypeConfig> {
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
}

pub fn mock_state_machine_handler() -> MockStateMachineHandler<MockTypeConfig> {
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_update_pending()
        .returning(|_| {});
    state_machine_handler
        .expect_read_from_state_machine()
        .returning(|_| None);
    state_machine_handler
}

pub fn mock_membership() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_voting_members().returning(|_| vec![]);
    membership
        .expect_get_followers_candidates_channel_and_role()
        .returning(|_| vec![]);
    membership.expect_reset_leader().returning(|| Ok(()));
    membership
        .expect_update_node_role()
        .returning(|_, _| Ok(()));
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership { nodes: vec![] });
    membership
}

pub fn mock_peer_channels() -> MockPeerChannels {
    let mut peer_channels = MockPeerChannels::new();
    peer_channels
        .expect_voting_members()
        .returning(|| DashMap::new());
    peer_channels
}

fn mock_raft_context_internal(
    id: u32,
    raft_log: Arc<MockRaftLog>,
    state_machine: Arc<MockStateMachine>,
    state_storage: Box<MockStateStorage>,
    transport: Arc<MockTransport>,
    membership: Arc<MockMembership<MockTypeConfig>>,
    election_handler: MockElectionCore<MockTypeConfig>,
    replication_handler: MockReplicationCore<MockTypeConfig>,
    state_machine_handler: Arc<MockStateMachineHandler<MockTypeConfig>>,
    settings: Settings,
) -> RaftContext<MockTypeConfig> {
    RaftContext {
        node_id: id,
        raft_log: raft_log,
        state_machine: state_machine,
        state_storage: state_storage,
        transport: transport,
        membership,
        election_handler,
        replication_handler,
        state_machine_handler,
        settings: Arc::new(settings),
    }
}
