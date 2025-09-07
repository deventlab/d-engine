//! A builder pattern implementation for constructing a [`Node`] instance in a
//! Raft cluster.
//!
//! The [`NodeBuilder`] provides a fluent interface to configure and assemble
//! components required by the Raft node, including storage engines, state machines,
//! transport layers, membership management, and asynchronous handlers.
//!
//! ## Key Design Points
//! - **Explicit Component Initialization**: Requires explicit configuration of storage engines and
//!   state machines (no implicit defaults).
//! - **Customization**: Allows overriding components via setter methods (e.g., `storage_engine()`,
//!   `state_machine()`, `transport()`).
//! - **Lifecycle Management**:
//!   - `build()`: Assembles the [`Node`], initializes background tasks (e.g., [`CommitHandler`],
//!     replication, election).
//!   - `ready()`: Finalizes construction and returns the initialized [`Node`].
//!   - `start_rpc_server()`: Spawns the gRPC server for cluster communication.
//!
//! ## Example
//! ```ignore
//! let (shutdown_tx, shutdown_rx) = watch::channel(());
//! let node = NodeBuilder::new(Some("cluster_config.yaml"), shutdown_rx)
//!     .storage_engine(custom_storage_engine)  // Required component
//!     .state_machine(custom_state_machine)    // Required component
//!     .build()
//!     .start_rpc_server().await
//!     .ready()
//!     .unwrap();
//! ```
//!
//! ## Notes
//! - **Thread Safety**: All components wrapped in `Arc`/`Mutex` for shared ownership and thread
//!   safety.
//! - **Resource Cleanup**: Uses `watch::Receiver` for cooperative shutdown signaling.
//! - **Configuration Loading**: Supports loading cluster configuration from file or in-memory
//!   config.

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::RaftTypeConfig;
use crate::alias::MOF;
use crate::alias::SMHOF;
use crate::alias::SNP;
use crate::alias::TROF;
use crate::follower_state::FollowerState;
use crate::grpc;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::learner_state::LearnerState;
use crate::BufferedRaftLog;
use crate::ClusterConfig;
use crate::CommitHandler;
use crate::CommitHandlerDependencies;
use crate::DefaultCommitHandler;
use crate::DefaultPurgeExecutor;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::LogSizePolicy;
use crate::NewCommitData;
use crate::Node;
use crate::Raft;
use crate::RaftConfig;
use crate::RaftCoreHandlers;
use crate::RaftLog;
use crate::RaftMembership;
use crate::RaftNodeConfig;
use crate::RaftRole;
use crate::RaftStorageHandles;
use crate::ReplicationHandler;
use crate::Result;
use crate::SignalParams;
use crate::StateMachine;
use crate::StorageEngine;
use crate::SystemError;

pub enum NodeMode {
    Joiner,
    FullMember,
}

/// Builder pattern implementation for constructing a Raft node with configurable components.
/// Provides a fluent interface to set up node configuration, storage, transport, and other
/// dependencies.
pub struct NodeBuilder<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    node_id: u32,

    pub(super) node_config: RaftNodeConfig,
    pub(super) storage_engine: Option<Arc<SE>>,
    pub(super) membership: Option<MOF<RaftTypeConfig<SE, SM>>>,
    pub(super) state_machine: Option<Arc<SM>>,
    pub(super) transport: Option<TROF<RaftTypeConfig<SE, SM>>>,
    pub(super) state_machine_handler: Option<Arc<SMHOF<RaftTypeConfig<SE, SM>>>>,
    pub(super) snapshot_policy: Option<SNP<RaftTypeConfig<SE, SM>>>,
    pub(super) shutdown_signal: watch::Receiver<()>,

    pub(super) node: Option<Arc<Node<RaftTypeConfig<SE, SM>>>>,
}

impl<SE, SM> NodeBuilder<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    /// Creates a new NodeBuilder with cluster configuration loaded from file
    ///
    /// # Arguments
    /// * `cluster_path` - Optional path to node-specific cluster configuration
    /// * `shutdown_signal` - Watch channel for graceful shutdown signaling
    ///
    /// # Panics
    /// Will panic if configuration loading fails (consider returning Result
    /// instead)
    pub fn new(
        cluster_path: Option<&str>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        let mut node_config = RaftNodeConfig::new().expect("Load node_config successfully");
        if let Some(p) = cluster_path {
            info!("with_override_config from: {}", &p);
            node_config = node_config
                .with_override_config(p)
                .expect("Overwrite node_config successfully.");
        }

        Self::init(node_config, shutdown_signal)
    }

    /// Constructs NodeBuilder from in-memory cluster configuration
    ///
    /// # Arguments
    /// * `cluster_config` - Pre-built cluster configuration
    /// * `shutdown_signal` - Graceful shutdown notification channel
    ///
    /// # Usage
    /// ```ignore
    /// let builder = NodeBuilder::from_cluster_config(my_config, shutdown_rx);
    /// ```
    pub fn from_cluster_config(
        cluster_config: ClusterConfig,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        let mut node_config = RaftNodeConfig::new().expect("Load node_config successfully!");
        node_config.cluster = cluster_config;
        Self::init(node_config, shutdown_signal)
    }

    /// Core initialization logic shared by all construction paths
    pub fn init(
        node_config: RaftNodeConfig,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            node_id: node_config.cluster.node_id,
            storage_engine: None,
            state_machine: None,
            transport: None,
            membership: None,
            node_config,
            shutdown_signal,
            state_machine_handler: None,
            snapshot_policy: None,
            node: None,
        }
    }

    /// Sets a custom storage engine implementation
    pub fn storage_engine(
        mut self,
        storage_engine: Arc<SE>,
    ) -> Self {
        self.storage_engine = Some(storage_engine);
        self
    }

    /// Sets a custom state machine implementation
    pub fn state_machine(
        mut self,
        state_machine: Arc<SM>,
    ) -> Self {
        self.state_machine = Some(state_machine);
        self
    }

    /// Replaces the entire node configuration
    pub fn node_config(
        mut self,
        node_config: RaftNodeConfig,
    ) -> Self {
        self.node_config = node_config;
        self
    }

    /// Replaces the raft  configuration
    pub fn raft_config(
        mut self,
        config: RaftConfig,
    ) -> Self {
        self.node_config.raft = config;
        self
    }

    /// Finalizes the builder and constructs the Raft node instance.
    ///
    /// Initializes default implementations for any unconfigured components:
    /// - Creates file-based databases for state machine and logs
    /// - Sets up default gRPC transport
    /// - Initializes commit handling subsystem
    /// - Configures membership management
    ///
    /// # Panics
    /// Panics if essential components cannot be initialized
    pub fn build(mut self) -> Self {
        let node_id = self.node_id;
        let node_config = self.node_config.clone();
        // let db_root_dir = format!("{}/{}", node_config.cluster.db_root_dir.display(), node_id);

        // Init CommitHandler
        let (new_commit_event_tx, new_commit_event_rx) = mpsc::unbounded_channel::<NewCommitData>();

        // Handle state machine initialization
        let state_machine = self.state_machine.take().expect("State machine must be set");

        // Handle storage engine initialization
        let storage_engine = self.storage_engine.take().expect("Storage engine must be set");

        //Retrieve last applied index from state machine
        let last_applied_index = state_machine.last_applied().index;
        let raft_log = {
            let (log, receiver) = BufferedRaftLog::new(
                node_id,
                node_config.raft.persistence.clone(),
                storage_engine.clone(),
            );

            // Start processor and get Arc-wrapped instance
            log.start(receiver)
        };

        let transport = self.transport.take().unwrap_or(GrpcTransport::new(node_id));

        let snapshot_policy = self.snapshot_policy.take().unwrap_or(LogSizePolicy::new(
            node_config.raft.snapshot.max_log_entries_before_snapshot,
            node_config.raft.snapshot.snapshot_cool_down_since_last_check,
        ));

        let state_machine_handler = self.state_machine_handler.take().unwrap_or_else(|| {
            Arc::new(DefaultStateMachineHandler::new(
                node_id,
                last_applied_index,
                node_config.raft.commit_handler.max_entries_per_chunk,
                state_machine.clone(),
                node_config.raft.snapshot.clone(),
                snapshot_policy,
            ))
        });
        let membership = Arc::new(self.membership.take().unwrap_or_else(|| {
            RaftMembership::new(
                node_id,
                node_config.cluster.initial_cluster.clone(),
                node_config.clone(),
            )
        }));

        let purge_executor = DefaultPurgeExecutor::new(raft_log.clone());

        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(10240);
        let event_tx_clone = event_tx.clone(); // used in commit handler

        let shutdown_signal = self.shutdown_signal.clone();
        let node_config_arc = Arc::new(node_config);

        // Construct my role
        // Role initialization flow:
        // 1. Check joining status from node config
        // 2. Load persisted hard state from storage
        // 3. Determine initial role based on cluster state
        // 4. Inject dependencies to role state
        let last_applied_index = Some(state_machine.last_applied().index);
        let my_role = if node_config_arc.is_joining() {
            RaftRole::Learner(Box::new(LearnerState::new(
                node_id,
                node_config_arc.clone(),
            )))
        } else {
            RaftRole::Follower(Box::new(FollowerState::new(
                node_id,
                node_config_arc.clone(),
                raft_log.load_hard_state().expect("Failed to load hard state"),
                last_applied_index,
            )))
        };
        let my_role_i32 = my_role.as_i32();
        let my_current_term = my_role.current_term();
        info!(
            "Start node with role: {} and term: {}",
            my_role_i32, my_current_term
        );

        // Construct raft core
        let mut raft_core = Raft::<RaftTypeConfig<SE, SM>>::new(
            node_id,
            my_role,
            RaftStorageHandles::<RaftTypeConfig<SE, SM>> {
                raft_log,
                state_machine: state_machine.clone(),
            },
            transport,
            RaftCoreHandlers::<RaftTypeConfig<SE, SM>> {
                election_handler: ElectionHandler::new(node_id),
                replication_handler: ReplicationHandler::new(node_id),
                state_machine_handler: state_machine_handler.clone(),
                purge_executor: Arc::new(purge_executor),
            },
            membership.clone(),
            SignalParams {
                role_tx,
                role_rx,
                event_tx,
                event_rx,
                shutdown_signal: shutdown_signal.clone(),
            },
            node_config_arc.clone(),
        );

        // Register commit event listener
        raft_core.register_new_commit_listener(new_commit_event_tx);

        // Start CommitHandler in a single thread
        let deps = CommitHandlerDependencies {
            state_machine_handler,
            raft_log: raft_core.ctx.storage.raft_log.clone(),
            membership: membership.clone(),
            event_tx: event_tx_clone,
            shutdown_signal,
        };

        let commit_handler = DefaultCommitHandler::<RaftTypeConfig<SE, SM>>::new(
            node_id,
            my_role_i32,
            my_current_term,
            deps,
            node_config_arc.clone(),
            new_commit_event_rx,
        );
        self.enable_state_machine_commit_listener(commit_handler);

        let event_tx = raft_core.event_tx.clone();
        let node = Node::<RaftTypeConfig<SE, SM>> {
            node_id,
            raft_core: Arc::new(Mutex::new(raft_core)),
            membership,
            event_tx: event_tx.clone(),
            ready: AtomicBool::new(false),
            node_config: node_config_arc,
        };

        self.node = Some(Arc::new(node));
        self
    }

    /// When a new commit is detected, convert the log into a state machine log.
    fn enable_state_machine_commit_listener(
        &self,
        mut commit_handler: DefaultCommitHandler<RaftTypeConfig<SE, SM>>,
    ) {
        tokio::spawn(async move {
            match commit_handler.run().await {
                Ok(_) => {
                    info!("commit_handler exit program");
                }
                Err(e) => {
                    error!("commit_handler exit program with unpexected error: {:?}", e);
                    println!("commit_handler exit program");
                }
            }
        });
    }

    /// Sets a custom state machine handler implementation.
    ///
    /// This allows developers to provide their own implementation of the state machine handler
    /// which processes committed log entries and applies them to the state machine.
    ///
    /// # Arguments
    /// * `handler` - custom state machine handler that must implement the `StateMachineHandler`
    ///   trait
    ///
    /// # Notes
    /// - The handler must be thread-safe as it will be shared across multiple threads
    /// - If not set, a default implementation will be used during `build()`
    /// - The handler should properly handle snapshot creation and restoration
    pub fn with_custom_state_machine_handler(
        mut self,
        handler: Arc<SMHOF<RaftTypeConfig<SE, SM>>>,
    ) -> Self {
        self.state_machine_handler = Some(handler);
        self
    }

    pub fn set_snapshot_policy(
        mut self,
        snapshot_policy: SNP<RaftTypeConfig<SE, SM>>,
    ) -> Self {
        self.snapshot_policy = Some(snapshot_policy);
        self
    }

    /// Starts the gRPC server for cluster communication.
    ///
    /// # Panics
    /// Panics if node hasn't been built or address binding fails
    pub async fn start_rpc_server(self) -> Self {
        debug!("1. --- start RPC server --- ");
        if let Some(ref node) = self.node {
            let node_clone = node.clone();
            let shutdown = self.shutdown_signal.clone();
            let listen_address = self.node_config.cluster.listen_address;
            let node_config = self.node_config.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    grpc::start_rpc_server(node_clone, listen_address, node_config, shutdown).await
                {
                    eprintln!("RPC server stops. {e:?}");
                    error!("RPC server stops. {:?}", e);
                }
            });
            self
        } else {
            panic!("failed to start RPC server");
        }
    }

    /// Returns the built node instance after successful construction.
    ///
    /// # Errors
    /// Returns Error::NodeFailedToStartError if build hasn't completed
    pub fn ready(self) -> Result<Arc<Node<RaftTypeConfig<SE, SM>>>> {
        self.node.ok_or_else(|| {
            SystemError::NodeStartFailed("check node ready failed".to_string()).into()
        })
    }

    /// Test constructor with custom database path
    ///
    /// # Safety
    /// Bypasses normal configuration validation - use for testing only
    #[cfg(test)]
    pub fn new_from_db_path(
        db_path: &str,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        use std::path::PathBuf;

        let mut node_config = RaftNodeConfig::new().expect("Load node_config successfully!");
        node_config.cluster.db_root_dir = PathBuf::from(db_path);

        Self::init(node_config, shutdown_signal)
    }
}
