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

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::RaftTypeConfig;
use crate::Node;
use crate::membership::RaftMembership;
use crate::network::grpc;
use crate::network::grpc::grpc_transport::GrpcTransport;
use crate::storage::BufferedRaftLog;
use crate::storage::DefaultLease;
use crate::storage::FileStateMachine;
#[cfg(feature = "rocksdb")]
use crate::storage::RocksDBStateMachine;
use d_engine_core::ClusterConfig;
use d_engine_core::CommitHandler;
use d_engine_core::CommitHandlerDependencies;
use d_engine_core::DefaultCommitHandler;
use d_engine_core::DefaultPurgeExecutor;
use d_engine_core::DefaultStateMachineHandler;
use d_engine_core::ElectionHandler;
use d_engine_core::LogSizePolicy;
use d_engine_core::NewCommitData;
use d_engine_core::Raft;
use d_engine_core::RaftConfig;
use d_engine_core::RaftCoreHandlers;
use d_engine_core::RaftLog;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RaftRole;
use d_engine_core::RaftStorageHandles;
use d_engine_core::ReplicationHandler;
use d_engine_core::Result;
use d_engine_core::SignalParams;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use d_engine_core::SystemError;
use d_engine_core::alias::MOF;
use d_engine_core::alias::SMHOF;
use d_engine_core::alias::SNP;
use d_engine_core::alias::TROF;
use d_engine_core::follower_state::FollowerState;
use d_engine_core::learner_state::LearnerState;

/// Builder for creating a Raft node
///
/// Provides a fluent API for configuring and constructing a [`Node`].
///
/// # Example
///
/// ```rust,ignore
/// use d_engine_server::{NodeBuilder, FileStorageEngine, FileStateMachine};
///
/// let node = NodeBuilder::new(None, shutdown_rx)
///     .storage_engine(Arc::new(FileStorageEngine::new(...)?))
///     .state_machine(Arc::new(FileStateMachine::new(...).await?))
///     .build()
///     .ready()?;
/// ```
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
        let mut state_machine = self.state_machine.take().expect("State machine must be set");

        // Inject lease into state machine
        // Note: Even if cleanup_strategy is "disabled", we inject the lease object
        // The DefaultLease internally handles the disabled case with zero overhead
        {
            let lease_config = &node_config.raft.state_machine.lease;
            let lease = Arc::new(DefaultLease::new(lease_config.clone()));

            // Try to inject lease into concrete state machine types
            // We need mutable access, so try Arc::get_mut first
            if let Some(sm) = Arc::get_mut(&mut state_machine) {
                let sm_any = sm as &mut dyn Any;

                #[cfg(feature = "rocksdb")]
                if let Some(rocksdb_sm) = sm_any.downcast_mut::<RocksDBStateMachine>() {
                    rocksdb_sm.set_lease(lease.clone());
                    // RocksDBStateMachine needs to load persisted lease data after injection
                    if let Err(e) =
                        tokio::runtime::Handle::current().block_on(rocksdb_sm.load_lease_data())
                    {
                        error!("Failed to load lease data for RocksDBStateMachine: {:?}", e);
                    }
                    debug!("Lease injected into RocksDBStateMachine and data loaded");
                    // Continue to check FileStateMachine
                }

                if let Some(file_sm) = sm_any.downcast_mut::<FileStateMachine>() {
                    file_sm.set_lease(lease.clone());
                    // FileStateMachine needs to load persisted lease data after injection
                    if let Err(e) =
                        tokio::runtime::Handle::current().block_on(file_sm.load_lease_data())
                    {
                        error!("Failed to load lease data for FileStateMachine: {:?}", e);
                    }
                    debug!("Lease injected into FileStateMachine and data loaded");
                }
            } else {
                warn!("Cannot inject lease: state machine Arc is already shared");
            }
        }

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
            SignalParams::new(
                role_tx,
                role_rx,
                event_tx,
                event_rx,
                shutdown_signal.clone(),
            ),
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

        let event_tx = raft_core.event_sender();
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
