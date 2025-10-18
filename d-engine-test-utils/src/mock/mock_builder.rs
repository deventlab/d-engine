use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::error;
use tracing::trace;

use super::MockTypeConfig;
use crate::follower_state::FollowerState;
use crate::grpc;
use crate::ElectionConfig;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockTransport;
use crate::Node;
use crate::Raft;
use crate::RaftConfig;
use crate::RaftContext;
use crate::RaftCoreHandlers;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::RaftRole;
use crate::RaftStorageHandles;
use crate::RoleEvent;
use crate::SignalParams;
use crate::StateMachine;
use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterMembership;

pub struct MockBuilder {
    pub id: Option<u32>,
    pub role: Option<RaftRole<MockTypeConfig>>,
    pub raft_log: Option<MockRaftLog>,
    pub state_machine: Option<Arc<MockStateMachine>>,
    pub transport: Option<MockTransport<MockTypeConfig>>,
    pub membership: Option<Arc<MockMembership<MockTypeConfig>>>,
    pub purge_executor: Option<MockPurgeExecutor>,
    pub election_handler: Option<MockElectionCore<MockTypeConfig>>,
    pub replication_handler: Option<MockReplicationCore<MockTypeConfig>>,
    pub state_machine_handler: Option<Arc<MockStateMachineHandler<MockTypeConfig>>>,
    pub node_config: Option<RaftNodeConfig>,
    pub turn_on_election: Option<bool>,
    shutdown_signal: watch::Receiver<()>,

    pub(crate) event_tx: Option<mpsc::Sender<RaftEvent>>,
    pub(crate) event_rx: Option<mpsc::Receiver<RaftEvent>>,

    pub(crate) role_tx: Option<mpsc::UnboundedSender<RoleEvent>>,
    pub(crate) role_rx: Option<mpsc::UnboundedReceiver<RoleEvent>>,
}

impl MockBuilder {
    pub fn new(shutdown_signal: watch::Receiver<()>) -> Self {
        Self {
            id: None,
            role: None,
            raft_log: None,
            state_machine: None,
            transport: None,
            membership: None,
            purge_executor: None,
            election_handler: None,
            replication_handler: None,
            state_machine_handler: None,
            node_config: None,
            turn_on_election: None,
            shutdown_signal,

            role_tx: None,
            role_rx: None,
            event_tx: None,
            event_rx: None,
        }
    }

    pub(crate) fn build_context(self) -> RaftContext<MockTypeConfig> {
        let (
            raft_log,
            state_machine,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            purge_executor,
            node_config,
        ) = (
            Arc::new(self.raft_log.unwrap_or_else(mock_raft_log)),
            self.state_machine.unwrap_or_else(|| Arc::new(mock_state_machine())),
            Arc::new(self.transport.unwrap_or_else(mock_transport)),
            self.election_handler.unwrap_or_else(mock_election_core),
            self.replication_handler.unwrap_or_else(mock_replication_handler),
            self.state_machine_handler
                .unwrap_or_else(|| Arc::new(mock_state_machine_handler())),
            self.membership.unwrap_or_else(|| Arc::new(mock_membership())),
            self.purge_executor.unwrap_or_else(mock_purge_exewcutor),
            self.node_config.unwrap_or_else(|| {
                RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig")
            }),
        );

        let storage = RaftStorageHandles {
            raft_log,
            state_machine,
        };
        let handlers = RaftCoreHandlers {
            election_handler,
            replication_handler,
            state_machine_handler,
            purge_executor: Arc::new(purge_executor),
        };
        mock_raft_context_internal(1, storage, transport, membership, handlers, node_config)
    }

    pub fn build_raft(self) -> Raft<MockTypeConfig> {
        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(10);
        let (
            id,
            raft_log,
            state_machine,
            transport,
            election_handler,
            replication_handler,
            state_machine_handler,
            membership,
            purge_executor,
            node_config,
            role_tx,
            role_rx,
            event_tx,
            event_rx,
        ) = (
            self.id.unwrap_or(1),
            self.raft_log.unwrap_or_else(mock_raft_log),
            self.state_machine.unwrap_or_else(|| Arc::new(mock_state_machine())),
            self.transport.unwrap_or_else(mock_transport),
            self.election_handler.unwrap_or_else(mock_election_core),
            self.replication_handler.unwrap_or_else(mock_replication_handler),
            self.state_machine_handler
                .unwrap_or_else(|| Arc::new(mock_state_machine_handler())),
            self.membership.unwrap_or_else(|| Arc::new(mock_membership())),
            self.purge_executor.unwrap_or_else(mock_purge_exewcutor),
            self.node_config.unwrap_or_else(|| {
                RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig")
            }),
            self.role_tx.unwrap_or(role_tx),
            self.role_rx.unwrap_or(role_rx),
            self.event_tx.unwrap_or(event_tx),
            self.event_rx.unwrap_or(event_rx),
        );
        trace!(node_config.raft.election.election_timeout_min, "build_raft");

        let election_config = {
            if self.turn_on_election.unwrap_or(true) {
                ElectionConfig {
                    election_timeout_min: 1,
                    election_timeout_max: 2,
                    ..node_config.raft.election
                }
            } else {
                node_config.raft.election
            }
        };
        let arc_node_config = Arc::new(RaftNodeConfig {
            raft: RaftConfig {
                election: election_config,
                ..node_config.raft
            },
            ..node_config
        });

        let role = self.role.unwrap_or(RaftRole::Follower(Box::new(FollowerState::new(
            id,
            arc_node_config.clone(),
            raft_log.load_hard_state().expect("Failed to load hard state"),
            Some(state_machine.last_applied().index),
        ))));

        Raft::new(
            id,
            role,
            RaftStorageHandles::<MockTypeConfig> {
                raft_log: Arc::new(raft_log),
                state_machine,
            },
            transport,
            RaftCoreHandlers::<MockTypeConfig> {
                election_handler,
                replication_handler,
                state_machine_handler,
                purge_executor: Arc::new(purge_executor),
            },
            membership,
            SignalParams {
                role_tx,
                role_rx,
                event_tx,
                event_rx,
                shutdown_signal: self.shutdown_signal,
            },
            arc_node_config.clone(),
        )
    }

    pub fn build_node(self) -> Node<MockTypeConfig> {
        let raft = self.build_raft();
        let event_tx = raft.event_tx.clone();
        let node_config = raft.ctx.node_config.clone();
        let membership = raft.ctx.membership.clone();
        Node::<MockTypeConfig> {
            node_id: raft.node_id,
            raft_core: Arc::new(Mutex::new(raft)),
            membership,
            event_tx,
            ready: AtomicBool::new(false),
            node_config,
        }
    }

    pub fn build_node_with_rpc_server(self) -> Arc<Node<MockTypeConfig>> {
        let shutdown = self.shutdown_signal.clone();
        let node_config_option = self.node_config.clone();

        let raft = self.build_raft();
        let event_tx = raft.event_tx.clone();
        let node_config = node_config_option.unwrap_or_else(|| {
            RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig")
        });
        let membership = raft.ctx.membership.clone();
        trace!(
            node_config.raft.election.election_timeout_min,
            "build_node_with_rpc_server"
        );
        let node = Arc::new(Node::<MockTypeConfig> {
            node_id: raft.node_id,
            raft_core: Arc::new(Mutex::new(raft)),
            membership,
            event_tx,
            ready: AtomicBool::new(false),
            node_config: Arc::new(node_config.clone()),
        });
        let node_clone = node.clone();
        let listen_address = node_config.cluster.listen_address;
        let node_config = node_config.clone();
        tokio::spawn(async move {
            if let Err(e) =
                grpc::start_rpc_server(node_clone, listen_address, node_config, shutdown).await
            {
                eprintln!("RPC server stops. {e:?}");
                error!("RPC server stops. {e:?}");
            }
        });

        node
    }
    pub fn id(
        mut self,
        id: u32,
    ) -> Self {
        self.id = Some(id);
        self
    }
    pub fn with_role(
        mut self,
        role: RaftRole<MockTypeConfig>,
    ) -> Self {
        self.role = Some(role);
        self
    }
    pub fn with_raft_log(
        mut self,
        raft_log: MockRaftLog,
    ) -> Self {
        self.raft_log = Some(raft_log);
        self
    }
    pub fn with_state_machine(
        mut self,
        sm: MockStateMachine,
    ) -> Self {
        self.state_machine = Some(Arc::new(sm));
        self
    }

    pub fn with_transport(
        mut self,
        transport: MockTransport<MockTypeConfig>,
    ) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn with_membership(
        mut self,
        membership: MockMembership<MockTypeConfig>,
    ) -> Self {
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

    pub fn with_node_config(
        mut self,
        node_config: RaftNodeConfig,
    ) -> Self {
        self.node_config = Some(node_config);
        self
    }

    pub fn with_db_path(
        mut self,
        db_root_dir: impl AsRef<Path>,
    ) -> Self {
        let mut node_config =
            RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");
        node_config.cluster.db_root_dir = db_root_dir.as_ref().to_path_buf();
        self.node_config = Some(node_config);
        self
    }

    pub fn turn_on_election(
        mut self,
        is_on: bool,
    ) -> Self {
        self.turn_on_election = Some(is_on);
        self
    }
}

pub fn mock_raft_log() -> MockRaftLog {
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_last_log_id().returning(|| None);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log
}

pub fn mock_transport() -> MockTransport<MockTypeConfig> {
    MockTransport::new()
}

pub fn mock_election_core() -> MockElectionCore<MockTypeConfig> {
    let mut election_handler = MockElectionCore::new();
    election_handler
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    election_handler
}

pub fn mock_replication_handler() -> MockReplicationCore<MockTypeConfig> {
    MockReplicationCore::new()
}

pub fn mock_state_machine() -> MockStateMachine {
    let mut mock = MockStateMachine::new();

    mock.expect_start().returning(|| Ok(()));
    mock.expect_stop().returning(|| Ok(()));
    mock.expect_is_running().returning(|| true);

    mock.expect_get().returning(|_| Ok(None));
    mock.expect_entry_term().returning(|_| None);
    mock.expect_apply_chunk().returning(|_| Ok(()));
    mock.expect_len().returning(|| 0);

    mock.expect_update_last_applied().returning(|_| ());
    mock.expect_last_applied().return_const(LogId::default());
    mock.expect_persist_last_applied().returning(|_| Ok(()));

    mock.expect_update_last_snapshot_metadata().returning(|_| Ok(()));
    mock.expect_snapshot_metadata().returning(|| None);
    mock.expect_persist_last_snapshot_metadata().returning(|_| Ok(()));

    mock.expect_apply_snapshot_from_file().returning(|_, _| Ok(()));
    mock.expect_generate_snapshot_data()
        .returning(|_, _| Ok(Bytes::copy_from_slice(&[0u8; 32])));

    mock.expect_save_hard_state().returning(|| Ok(()));
    mock.expect_flush().returning(|| Ok(()));

    mock
}

pub fn mock_state_machine_handler() -> MockStateMachineHandler<MockTypeConfig> {
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().returning(|_| {});
    state_machine_handler.expect_read_from_state_machine().returning(|_| None);
    state_machine_handler
}

pub fn mock_purge_exewcutor() -> MockPurgeExecutor {
    let mut purge_exewcutor = MockPurgeExecutor::new();
    purge_exewcutor.expect_execute_purge().returning(|_| Ok(()));
    purge_exewcutor
}
pub fn mock_membership() -> MockMembership<MockTypeConfig> {
    let mut membership = MockMembership::new();
    membership.expect_can_rejoin().returning(|_, _| Ok(()));
    membership.expect_pre_warm_connections().returning(|| Ok(()));
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_members().returning(Vec::new);
    membership.expect_reset_leader().returning(|| Ok(()));
    membership.expect_update_node_role().returning(|_, _| Ok(()));
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    membership.expect_check_cluster_is_ready().returning(|| Ok(()));
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership {
            version: 1,
            nodes: vec![],
        });
    membership.expect_get_zombie_candidates().returning(Vec::new);
    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership
}

fn mock_raft_context_internal(
    id: u32,
    storage: RaftStorageHandles<MockTypeConfig>,
    transport: Arc<MockTransport<MockTypeConfig>>,
    membership: Arc<MockMembership<MockTypeConfig>>,
    handlers: RaftCoreHandlers<MockTypeConfig>,
    node_config: RaftNodeConfig,
) -> RaftContext<MockTypeConfig> {
    RaftContext {
        node_id: id,
        storage,

        transport,
        membership,

        handlers,

        node_config: Arc::new(node_config),
    }
}
