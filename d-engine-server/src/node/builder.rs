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
//! - **Simple API**: Single `start().await?` method to build and launch the node.
//!
//! ## Example
//! ```ignore
//! let (shutdown_tx, shutdown_rx) = watch::channel(());
//! let node = NodeBuilder::init(config, shutdown_rx)
//!     .storage_engine(custom_storage_engine)  // Required component
//!     .state_machine(custom_state_machine)    // Required component
//!     .start().await?;
//! ```
//!
//! ## Notes
//! - **Thread Safety**: All components wrapped in `Arc`/`Mutex` for shared ownership and thread
//!   safety.
//! - **Resource Cleanup**: Uses `watch::Receiver` for cooperative shutdown signaling.
//! - **Configuration Loading**: Supports loading cluster configuration from file or in-memory
//!   config.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::LeaderNotifier;
use super::RaftTypeConfig;
use crate::Node;
use crate::membership::RaftMembership;
use crate::network::grpc;
use crate::network::grpc::grpc_transport::GrpcTransport;
use crate::storage::BufferedRaftLog;
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
#[cfg(feature = "watch")]
use d_engine_core::watch::{WatchDispatcher, WatchRegistry};

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
///     .start().await?;
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
    pub async fn build(mut self) -> Result<Self> {
        let node_id = self.node_id;
        let node_config = self.node_config.clone();

        // Init CommitHandler
        let (new_commit_event_tx, new_commit_event_rx) = mpsc::unbounded_channel::<NewCommitData>();

        // Handle state machine initialization
        let state_machine = self.state_machine.take().ok_or_else(|| {
            SystemError::NodeStartFailed(
                "State machine must be set before calling build()".to_string(),
            )
        })?;

        // Note: Lease configuration should be injected BEFORE wrapping in Arc.
        // See StandaloneServer::start() or EmbeddedEngine::with_rocksdb() for correct pattern.
        // If state_machine is passed from user code, they are responsible for lease injection.

        // Start state machine: flip flags and load persisted data
        state_machine.start().await?;

        // Spawn lease background cleanup task (if TTL feature is enabled)
        // Framework-level feature: completely transparent to developers
        let lease_cleanup_handle = if node_config.raft.state_machine.lease.enabled {
            info!(
                "Starting lease background cleanup worker (interval: {}ms)",
                node_config.raft.state_machine.lease.interval_ms
            );
            Some(Self::spawn_background_cleanup_worker(
                Arc::clone(&state_machine),
                node_config.raft.state_machine.lease.interval_ms,
                self.shutdown_signal.clone(),
            ))
        } else {
            debug!("Lease feature disabled: no background cleanup worker");
            None
        };

        // Handle storage engine initialization
        let storage_engine = self.storage_engine.take().ok_or_else(|| {
            SystemError::NodeStartFailed(
                "Storage engine must be set before calling build()".to_string(),
            )
        })?;

        //Retrieve last applied index from state machine
        let last_applied_index = state_machine.last_applied().index;
        info!("Node startup, Last applied index: {}", last_applied_index);
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

        let shutdown_signal = self.shutdown_signal.clone();

        // Initialize watch system (controlled by feature flag at compile time)
        // All resource allocation is explicit and visible here (no hidden spawns)
        #[cfg(feature = "watch")]
        let watch_system = {
            let (broadcast_tx, broadcast_rx) =
                tokio::sync::broadcast::channel(node_config.raft.watch.event_queue_size);

            // Create unregister channel
            let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();

            // Create shared registry
            let registry = Arc::new(WatchRegistry::new(
                node_config.raft.watch.watcher_buffer_size,
                unregister_tx,
            ));

            // Create dispatcher
            let dispatcher =
                WatchDispatcher::new(Arc::clone(&registry), broadcast_rx, unregister_rx);

            // Explicitly spawn dispatcher task (resource allocation visible)
            let dispatcher_handle = tokio::spawn(async move {
                dispatcher.run().await;
            });

            Some((broadcast_tx, registry, dispatcher_handle))
        };

        let state_machine_handler = self.state_machine_handler.take().unwrap_or_else(|| {
            #[cfg(feature = "watch")]
            let watch_event_tx = watch_system.as_ref().map(|(tx, _, _)| tx.clone());
            #[cfg(not(feature = "watch"))]
            let watch_event_tx = None;

            Arc::new(DefaultStateMachineHandler::new(
                node_id,
                last_applied_index,
                node_config.raft.commit_handler.max_entries_per_chunk,
                state_machine.clone(),
                node_config.raft.snapshot.clone(),
                snapshot_policy,
                watch_event_tx,
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

        let node_config_arc = Arc::new(node_config);

        // Construct my role
        // Role initialization flow:
        // 1. Check if node is learner from config
        // 2. Load persisted hard state from storage
        // 3. Determine initial role based on cluster state
        // 4. Inject dependencies to role state
        let last_applied_index = Some(state_machine.last_applied().index);
        let my_role = if node_config_arc.is_learner() {
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

        // Create leader notification channel and register with Raft core
        let leader_notifier = LeaderNotifier::new();
        raft_core.register_leader_change_listener(leader_notifier.sender());

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
        // Spawn commit listener via Builder method
        // This ensures all tokio::spawn calls are visible in one place (Builder impl)
        // Following the "one page visible" Builder pattern principle
        let commit_handler_handle = Self::spawn_state_machine_commit_listener(commit_handler);

        let event_tx = raft_core.event_sender();
        let (rpc_ready_tx, _rpc_ready_rx) = watch::channel(false);

        let node = Node::<RaftTypeConfig<SE, SM>> {
            node_id,
            raft_core: Arc::new(Mutex::new(raft_core)),
            membership,
            event_tx: event_tx.clone(),
            ready: AtomicBool::new(false),
            rpc_ready_tx,
            leader_notifier,
            node_config: node_config_arc,
            #[cfg(feature = "watch")]
            watch_registry: watch_system.as_ref().map(|(_, reg, _)| Arc::clone(reg)),
            #[cfg(feature = "watch")]
            _watch_dispatcher_handle: watch_system.map(|(_, _, handle)| handle),
            _commit_handler_handle: Some(commit_handler_handle),
            _lease_cleanup_handle: lease_cleanup_handle,
            shutdown_signal: self.shutdown_signal.clone(),
        };

        self.node = Some(Arc::new(node));
        Ok(self)
    }

    /// Spawn state machine commit listener as background task.
    ///
    /// This method is called during node build() to start the commit handler thread.
    /// All spawn_* methods are centralized in NodeBuilder so developers can see
    /// all resource consumption (threads/tasks) in one place.
    ///
    /// # Returns
    /// * `JoinHandle` - Task handle for lifecycle management
    fn spawn_state_machine_commit_listener(
        mut commit_handler: DefaultCommitHandler<RaftTypeConfig<SE, SM>>
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            match commit_handler.run().await {
                Ok(_) => {
                    info!("commit_handler exit program");
                }
                Err(e) => {
                    error!("commit_handler exit program with unexpected error: {:?}", e);
                    println!("commit_handler exit program");
                }
            }
        })
    }

    /// Spawn lease background cleanup task (if enabled).
    ///
    /// This task runs independently from Raft apply pipeline, avoiding any blocking.
    /// Only spawns when cleanup_strategy = "background" (not "disabled" or "piggyback").
    ///
    /// # Arguments
    /// * `state_machine` - Arc reference to state machine for accessing lease manager
    /// * `lease_config` - Lease configuration determining cleanup behavior
    /// * `shutdown_signal` - Watch channel for graceful shutdown notification
    ///
    /// # Returns
    /// * `Option<JoinHandle>` - Task handle if background cleanup is enabled, None otherwise
    ///
    /// # Design Principles
    /// - **Zero overhead**: If disabled, returns None immediately (no task spawned)
    /// - **One page visible**: All long-running tasks spawned here in builder.rs
    /// - **Industry standard**: Follows etcd/TiKV/Consul background cleanup pattern
    /// - **Graceful shutdown**: Monitors shutdown signal for clean termination
    fn spawn_background_cleanup_worker(
        state_machine: Arc<SM>,
        interval_ms: u64,
        mut shutdown_signal: watch::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Call state machine's lease background cleanup
                        match state_machine.lease_background_cleanup().await {
                            Ok(deleted_keys) => {
                                if !deleted_keys.is_empty() {
                                    debug!(
                                        "Lease background cleanup: deleted {} expired keys",
                                        deleted_keys.len()
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Lease background cleanup failed: {:?}", e);
                            }
                        }
                    }
                    _ = shutdown_signal.changed() => {
                        info!("Lease background cleanup received shutdown signal");
                        break;
                    }
                }
            }

            debug!("Lease background cleanup worker stopped");
        })
    }

    /// Spawn watch dispatcher as background task.
    ///
    /// The dispatcher manages all watch streams for the lifetime of the node.
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
    async fn start_rpc_server(self) -> Self {
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

    /// Builds and starts the Raft node.
    ///
    /// This is the primary method to initialize and start a node. It performs:
    /// 1. State machine initialization (including lease injection if applicable)
    /// 2. Raft core construction
    /// 3. Background task spawning (commit handler, replication, election)
    /// 4. gRPC server startup for cluster communication
    ///
    /// # Returns
    /// An `Arc<Node>` ready for operation
    ///
    /// # Errors
    /// Returns an error if any initialization step fails
    ///
    /// # Example
    /// ```ignore
    /// let node = NodeBuilder::init(config, shutdown_rx)
    ///     .storage_engine(storage)
    ///     .state_machine(state_machine)
    ///     .start().await?;
    /// ```
    pub async fn start(self) -> Result<Arc<Node<RaftTypeConfig<SE, SM>>>> {
        let builder = self.build().await?;
        let builder = builder.start_rpc_server().await;
        builder.node.ok_or_else(|| {
            SystemError::NodeStartFailed("Node build failed unexpectedly".to_string()).into()
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
