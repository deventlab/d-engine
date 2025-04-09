//! A builder pattern implementation for constructing a [`Node`] instance in a
//! Raft cluster.
//!
//! The [`NodeBuilder`] provides a fluent interface to configure and assemble
//! components required by the Raft node, including storage layers (log, state
//! machine, membership), transport, and asynchronous handlers.
//!
//! ## Key Design Points
//! - **Default Components**: Initializes with production-ready defaults (Sled-based storage, gRPC
//!   transport).
//! - **Customization**: Allows overriding defaults via setter methods (e.g., `raft_log()`,
//!   `transport()`).
//! - **Lifecycle Management**:
//!   - `build()`: Assembles the [`Node`] and spawns background tasks (e.g., [`CommitHandler`]).
//!   - `start_metrics_server()`/`start_rpc_server()`: Launches auxiliary services.
//!   - `ready()`: Finalizes construction and returns the initialized [`Node`].
//!
//! ## Example
//! ```ignore
//! 
//! let (shutdown_tx, shutdown_rx) = watch::channel(());
//! let node = NodeBuilder::new(node_config, shutdown_rx)
//!     .raft_log(custom_raft_log)  // Optional override
//!     .build()
//!     .start_metrics_server(shutdown_tx.subscribe())
//!     .start_rpc_server().await
//!     .ready()
//!     .unwrap();
//! ```
//!
//! ## Notes
//! - **Thread Safety**: All components wrapped in `Arc`/`Mutex` for shared ownership.
//! - **Resource Cleanup**: Uses `watch::Receiver` for cooperative shutdown signaling.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use log::debug;
use log::error;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;

use super::RaftTypeConfig;
use crate::alias::COF;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::alias::SMOF;
use crate::alias::SSOF;
use crate::alias::TROF;
use crate::grpc;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::init_sled_raft_log_db;
use crate::init_sled_state_machine_db;
use crate::init_sled_state_storage_db;
use crate::metrics;
use crate::ClusterConfig;
use crate::CommitHandler;
use crate::DefaultCommitHandler;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::Error;
use crate::Node;
use crate::Raft;
use crate::RaftMembership;
use crate::RaftNodeConfig;
use crate::RaftStateMachine;
use crate::ReplicationHandler;
use crate::Result;
use crate::SledRaftLog;
use crate::SledStateStorage;
use crate::StateMachine;

/// Builder pattern implementation for constructing a Raft node with configurable components.
/// Provides a fluent interface to set up node configuration, storage, transport, and other
/// dependencies.
pub struct NodeBuilder {
    node_id: u32,
    pub(super) node_config: RaftNodeConfig,
    pub(super) raft_log: Option<ROF<RaftTypeConfig>>,
    pub(super) membership: Option<MOF<RaftTypeConfig>>,
    pub(super) state_machine: Option<Arc<SMOF<RaftTypeConfig>>>,
    pub(super) state_storage: Option<SSOF<RaftTypeConfig>>,
    pub(super) transport: Option<TROF<RaftTypeConfig>>,
    pub(super) commit_handler: Option<COF<RaftTypeConfig>>,
    pub(super) state_machine_handler: Option<Arc<SMHOF<RaftTypeConfig>>>,
    pub(super) shutdown_signal: watch::Receiver<()>,

    pub(super) node: Option<Arc<Node<RaftTypeConfig>>>,
}

impl NodeBuilder {
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
    /// let builder = NodeBuilder::from_config(my_config, shutdown_rx);
    /// ```
    pub fn from_config(
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
            raft_log: None,
            state_machine: None,
            state_storage: None,
            transport: None,
            membership: None,
            node_config,
            shutdown_signal,
            commit_handler: None,
            state_machine_handler: None,
            node: None,
        }
    }

    /// Sets a custom Raft log storage implementation
    pub fn raft_log(
        mut self,
        raft_log: ROF<RaftTypeConfig>,
    ) -> Self {
        self.raft_log = Some(raft_log);
        self
    }

    /// Sets a custom state machine implementation
    pub fn state_machine(
        mut self,
        state_machine: Arc<SMOF<RaftTypeConfig>>,
    ) -> Self {
        self.state_machine = Some(state_machine);
        self
    }

    /// Sets a custom state storage implementation
    pub fn state_storage(
        mut self,
        state_storage: SSOF<RaftTypeConfig>,
    ) -> Self {
        self.state_storage = Some(state_storage);
        self
    }

    /// Sets a custom network transport implementation
    pub fn transport(
        mut self,
        transport: TROF<RaftTypeConfig>,
    ) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Sets a custom commit handler implementation
    pub fn commit_handler(
        mut self,
        commit_handler: COF<RaftTypeConfig>,
    ) -> Self {
        self.commit_handler = Some(commit_handler);
        self
    }

    /// Sets a custom membership management implementation
    pub fn membership(
        mut self,
        membership: MOF<RaftTypeConfig>,
    ) -> Self {
        self.membership = Some(membership);
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

    /// Finalizes the builder and constructs the Raft node instance.
    ///
    /// Initializes default implementations for any unconfigured components:
    /// - Creates sled-based databases for state machine and logs
    /// - Sets up default gRPC transport
    /// - Initializes commit handling subsystem
    /// - Configures membership management
    ///
    /// # Panics
    /// Panics if essential components cannot be initialized
    pub fn build(mut self) -> Self {
        let node_id = self.node_id;
        let node_config = self.node_config.clone();
        let db_root_dir = format!("{}/{}", node_config.cluster.db_root_dir.display(), node_id);

        // Init CommitHandler
        let (new_commit_event_tx, new_commit_event_rx) = mpsc::unbounded_channel::<u64>();

        let state_machine = self.state_machine.take().unwrap_or_else(|| {
            let state_machine_db =
                init_sled_state_machine_db(&db_root_dir).expect("init_sled_state_machine_db successfully.");
            Arc::new(RaftStateMachine::new(node_id, Arc::new(state_machine_db)))
        });

        //Retrieve last applied index from state machine
        let last_applied_index = state_machine.last_entry_index();

        let raft_log = self.raft_log.take().unwrap_or_else(|| {
            let raft_log_db = init_sled_raft_log_db(&db_root_dir).expect("init_sled_raft_log_db successfully.");
            SledRaftLog::new(Arc::new(raft_log_db), last_applied_index)
        });

        let state_storage = self.state_storage.take().unwrap_or_else(|| {
            let state_storage_db =
                init_sled_state_storage_db(&db_root_dir).expect("init_sled_state_storage_db successfully.");
            SledStateStorage::new(Arc::new(state_storage_db))
        });

        let transport = self.transport.take().unwrap_or(GrpcTransport { my_id: node_id });

        let state_machine_handler = self.state_machine_handler.take().unwrap_or_else(|| {
            Arc::new(DefaultStateMachineHandler::new(
                last_applied_index,
                node_config.raft.commit_handler.max_entries_per_chunk,
                state_machine.clone(),
            ))
        });
        let membership = self
            .membership
            .take()
            .unwrap_or_else(|| RaftMembership::new(node_id, node_config.cluster.initial_cluster.clone()));

        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(1024);

        let settings_arc = Arc::new(node_config);
        let shutdown_signal = self.shutdown_signal.clone();
        let mut raft_core = Raft::<RaftTypeConfig>::new(
            node_id,
            raft_log,
            state_machine.clone(),
            state_storage,
            transport,
            ElectionHandler::new(node_id, event_tx.clone()),
            ReplicationHandler::new(node_id),
            state_machine_handler.clone(),
            Arc::new(membership),
            settings_arc.clone(),
            role_tx,
            role_rx,
            event_tx,
            event_rx,
            shutdown_signal.clone(),
        );

        // Register commit event listener
        raft_core.register_new_commit_listener(new_commit_event_tx);

        // Start CommitHandler in a single thread
        let mut commit_handler = DefaultCommitHandler::<RaftTypeConfig>::new(
            state_machine_handler,
            raft_core.ctx.raft_log.clone(),
            new_commit_event_rx,
            settings_arc.raft.commit_handler.batch_size,
            settings_arc.raft.commit_handler.process_interval_ms,
            shutdown_signal,
        );
        tokio::spawn(async move {
            match commit_handler.run().await {
                Ok(_) => {
                    info!("commit_handler exit program");
                }
                Err(Error::Exit) => {
                    info!("commit_handler exit program");
                    println!("commit_handler exit program");
                }
                Err(e) => {
                    error!("commit_handler exit program with error: {:?}", e);
                    println!("commit_handler exit program");
                }
            }
        });

        let event_tx = raft_core.event_tx.clone();
        let node = Node::<RaftTypeConfig> {
            node_id,
            raft_core: Arc::new(Mutex::new(raft_core)),
            event_tx: event_tx.clone(),
            ready: AtomicBool::new(false),
            settings: settings_arc,
        };

        self.node = Some(Arc::new(node));
        self
    }

    /// Starts the metrics server for monitoring node operations.
    ///
    /// Launches a Prometheus endpoint on the configured port.
    pub fn start_metrics_server(
        self,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        println!("start metric server!");
        let port = self.node_config.monitoring.prometheus_port;
        tokio::spawn(async move {
            metrics::start_server(port, shutdown_signal).await;
        });
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
                if let Err(e) = grpc::start_rpc_server(node_clone, listen_address, node_config, shutdown).await {
                    eprintln!("RPC server stops. {:?}", e);
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
    pub fn ready(self) -> Result<Arc<Node<RaftTypeConfig>>> {
        self.node.ok_or_else(|| Error::NodeFailedToStartError)
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
