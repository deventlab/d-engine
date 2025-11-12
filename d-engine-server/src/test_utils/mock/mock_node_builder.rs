//! Mock node builder for fluent configuration of test Raft instances

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::error;
use tracing::trace;

use super::MockTypeConfig;
use crate::Node;
use crate::network::grpc;
use d_engine_core::ElectionConfig;
use d_engine_core::MockElectionCore;
use d_engine_core::MockMembership;
use d_engine_core::MockPurgeExecutor;
use d_engine_core::MockRaftLog;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockStateMachine;
use d_engine_core::MockStateMachineHandler;
use d_engine_core::MockTransport;
use d_engine_core::Raft;
use d_engine_core::RaftConfig;
use d_engine_core::RaftContext;
use d_engine_core::RaftCoreHandlers;
use d_engine_core::RaftEvent;
use d_engine_core::RaftLog;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RaftRole;
use d_engine_core::RaftStorageHandles;
use d_engine_core::RoleEvent;
use d_engine_core::SignalParams;
use d_engine_core::StateMachine;
use d_engine_core::follower_state::FollowerState;
use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterMembership;

/// Builder for constructing mock Raft components with customizable defaults
///
/// This builder provides a fluent API for creating test fixtures with
/// fine-grained control over individual Raft components.
///
/// # Example: Basic Usage
///
/// ```rust,ignore
/// use d_engine_server::test_utils::MockBuilder;
/// use tokio::sync::watch;
///
/// let (tx, rx) = watch::channel(());
/// let raft = MockBuilder::new(rx)
///     .id(1)
///     .build_raft();
/// ```
///
/// # Example: Custom Configuration
///
/// ```rust,ignore
/// use d_engine_server::test_utils::MockBuilder;
/// use tokio::sync::watch;
///
/// let (tx, rx) = watch::channel(());
/// let node = MockBuilder::new(rx)
///     .id(2)
///     .with_db_path("/tmp/test")
///     .turn_on_election(false)
///     .build_node();
/// ```
///
/// # Builder Methods
///
/// - `id()` - Set node ID
/// - `with_role()` - Set custom Raft role
/// - `with_raft_log()` - Use custom log implementation
/// - `with_state_machine()` - Use custom state machine
/// - `with_transport()` - Use custom transport
/// - `with_membership()` - Use custom membership handler
/// - `turn_on_election()` - Enable/disable election
///
/// # Building Variants
///
/// - `build_context()` - Build RaftContext only
/// - `build_raft()` - Build Raft instance (most common)
/// - `build_node()` - Build Node wrapper
/// - `build_node_with_rpc_server()` - Build Node with gRPC server
pub struct MockBuilder {
    /// Optional node ID (defaults to 1)
    pub id: Option<u32>,
    /// Optional initial Raft role
    pub role: Option<RaftRole<MockTypeConfig>>,
    /// Optional Raft log implementation
    pub raft_log: Option<MockRaftLog>,
    /// Optional state machine implementation
    pub state_machine: Option<Arc<MockStateMachine>>,
    /// Optional network transport
    pub transport: Option<MockTransport<MockTypeConfig>>,
    /// Optional membership handler
    pub membership: Option<Arc<MockMembership<MockTypeConfig>>>,
    /// Optional purge executor
    pub purge_executor: Option<MockPurgeExecutor>,
    /// Optional election handler
    pub election_handler: Option<MockElectionCore<MockTypeConfig>>,
    /// Optional replication handler
    pub replication_handler: Option<MockReplicationCore<MockTypeConfig>>,
    /// Optional state machine handler
    pub state_machine_handler: Option<Arc<MockStateMachineHandler<MockTypeConfig>>>,
    /// Optional node configuration
    pub node_config: Option<RaftNodeConfig>,
    /// Optional election toggle (defaults to true)
    pub turn_on_election: Option<bool>,
    /// Shutdown signal for the Raft instance
    shutdown_signal: watch::Receiver<()>,

    /// Internal event channel sender
    pub(crate) event_tx: Option<mpsc::Sender<RaftEvent>>,
    /// Internal event channel receiver
    pub(crate) event_rx: Option<mpsc::Receiver<RaftEvent>>,

    /// Internal role change channel sender
    pub(crate) role_tx: Option<mpsc::UnboundedSender<RoleEvent>>,
    /// Internal role change channel receiver
    pub(crate) role_rx: Option<mpsc::UnboundedReceiver<RoleEvent>>,
}

impl MockBuilder {
    /// Create a new MockBuilder with default components
    ///
    /// All components are initially `None`, and will use mock defaults
    /// when building unless explicitly set.
    ///
    /// # Arguments
    ///
    /// * `shutdown_signal` - Watch receiver for graceful shutdown
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

    /// Build a RaftContext with current configuration
    ///
    /// Returns the low-level Raft context for testing protocol logic.
    pub fn build_context(self) -> RaftContext<MockTypeConfig> {
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

    /// Build a Raft instance with current configuration
    ///
    /// This is the most commonly used build method. Returns a fully
    /// initialized Raft instance ready for testing.
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
            SignalParams::new(role_tx, role_rx, event_tx, event_rx, self.shutdown_signal),
            arc_node_config.clone(),
        )
    }

    /// Build a Node instance (without RPC server)
    ///
    /// Wraps the Raft instance in a Node container for integration testing.
    pub fn build_node(self) -> Node<MockTypeConfig> {
        let raft = self.build_raft();
        let event_tx = raft.event_sender();
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

    /// Build a Node instance with integrated gRPC server
    ///
    /// This variant starts the RPC server in a background task,
    /// useful for end-to-end testing with network communication.
    pub fn build_node_with_rpc_server(self) -> Arc<Node<MockTypeConfig>> {
        let shutdown = self.shutdown_signal.clone();
        let node_config_option = self.node_config.clone();

        let raft = self.build_raft();
        let event_tx = raft.event_sender();
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

    /// Set the node ID (defaults to 1)
    pub fn id(
        mut self,
        id: u32,
    ) -> Self {
        self.id = Some(id);
        self
    }

    /// Set a custom initial Raft role
    pub fn with_role(
        mut self,
        role: RaftRole<MockTypeConfig>,
    ) -> Self {
        self.role = Some(role);
        self
    }

    /// Set a custom Raft log implementation
    pub fn with_raft_log(
        mut self,
        raft_log: MockRaftLog,
    ) -> Self {
        self.raft_log = Some(raft_log);
        self
    }

    /// Set a custom state machine implementation
    pub fn with_state_machine(
        mut self,
        sm: MockStateMachine,
    ) -> Self {
        self.state_machine = Some(Arc::new(sm));
        self
    }

    /// Set a custom network transport
    pub fn with_transport(
        mut self,
        transport: MockTransport<MockTypeConfig>,
    ) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set a custom membership handler
    pub fn with_membership(
        mut self,
        membership: MockMembership<MockTypeConfig>,
    ) -> Self {
        self.membership = Some(Arc::new(membership));
        self
    }

    /// Set a custom election handler
    pub fn with_election_handler(
        mut self,
        election_handler: MockElectionCore<MockTypeConfig>,
    ) -> Self {
        self.election_handler = Some(election_handler);
        self
    }

    /// Set a custom replication handler
    pub fn with_replication_handler(
        mut self,
        replication_handler: MockReplicationCore<MockTypeConfig>,
    ) -> Self {
        self.replication_handler = Some(replication_handler);
        self
    }

    /// Set a custom state machine handler
    pub fn with_state_machine_handler(
        mut self,
        state_machine_handler: MockStateMachineHandler<MockTypeConfig>,
    ) -> Self {
        self.state_machine_handler = Some(Arc::new(state_machine_handler));
        self
    }

    /// Set custom node configuration
    pub fn with_node_config(
        mut self,
        node_config: RaftNodeConfig,
    ) -> Self {
        self.node_config = Some(node_config);
        self
    }

    /// Set the database root directory path
    ///
    /// Automatically updates the node configuration with the provided path.
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

    /// Enable or disable election handling (defaults to enabled)
    ///
    /// Set to `false` to prevent automatic elections during testing.
    pub fn turn_on_election(
        mut self,
        is_on: bool,
    ) -> Self {
        self.turn_on_election = Some(is_on);
        self
    }
}

// ==================== Mock Factory Functions ====================

/// Create a mock Raft log with sensible test defaults
pub(crate) fn mock_raft_log() -> MockRaftLog {
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 0);
    raft_log.expect_last_log_id().returning(|| None);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log
}

/// Create a mock network transport
pub(crate) fn mock_transport() -> MockTransport<MockTypeConfig> {
    MockTransport::new()
}

/// Create a mock election handler
pub(crate) fn mock_election_core() -> MockElectionCore<MockTypeConfig> {
    let mut election_handler = MockElectionCore::new();
    election_handler
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    election_handler
}

/// Create a mock replication handler
pub(crate) fn mock_replication_handler() -> MockReplicationCore<MockTypeConfig> {
    MockReplicationCore::new()
}

/// Create a mock state machine with all methods stubbed
pub(crate) fn mock_state_machine() -> MockStateMachine {
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
    mock.expect_post_start_init().returning(|| Ok(()));

    mock
}

/// Create a mock state machine handler
pub(crate) fn mock_state_machine_handler() -> MockStateMachineHandler<MockTypeConfig> {
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler.expect_update_pending().returning(|_| {});
    state_machine_handler.expect_read_from_state_machine().returning(|_| None);
    state_machine_handler
}

/// Create a mock purge executor
pub(crate) fn mock_purge_exewcutor() -> MockPurgeExecutor {
    let mut purge_exewcutor = MockPurgeExecutor::new();
    purge_exewcutor.expect_execute_purge().returning(|_| Ok(()));
    purge_exewcutor
}

/// Create a mock membership handler
pub(crate) fn mock_membership() -> MockMembership<MockTypeConfig> {
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

/// Create a RaftContext with mock components
pub(crate) fn mock_raft_context_internal(
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
