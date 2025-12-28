//! Raft node container and lifecycle management.
//!
//! The [`Node`] struct acts as a host for a Raft consensus participant,
//! coordinating between the core protocol implementation (provided by `d-engine-core`)
//! and external subsystems:
//!
//! ## Key Responsibilities
//! - Manages the Raft finite state machine lifecycle
//! - Maintains node readiness state for cluster coordination
//! - Executes the main event processing loop inside Raft
//!
//! ## Example Usage
//! ```ignore
//! let node = NodeBuilder::new(node_config).start().await?;
//! tokio::spawn(async move {
//!     node.run().await.expect("Raft node execution failed");
//! });
//! ```

mod builder;
pub use builder::*;

mod client;
pub use client::*;

mod leader_notifier;
pub(crate) use leader_notifier::*;

#[doc(hidden)]
mod type_config;
use tracing::info;
#[doc(hidden)]
pub use type_config::*;

/// Test Modules
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod node_test;
#[cfg(test)]
mod test_helpers;

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;

use d_engine_core::Membership;
use d_engine_core::Raft;
use d_engine_core::RaftEvent;
use d_engine_core::RaftNodeConfig;
use d_engine_core::Result;
use d_engine_core::TypeConfig;
use d_engine_core::alias::MOF;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatchRegistry;

/// Raft consensus node
///
/// Represents a single node participating in a Raft cluster.
/// Coordinates protocol execution, storage, and networking.
///
/// Created via [`NodeBuilder`].
///
/// # Running the Node
///
/// ```rust,ignore
/// let node = builder.start()?;
/// node.run().await?;  // Blocks until shutdown
/// ```
pub struct Node<T>
where
    T: TypeConfig,
{
    pub(crate) node_id: u32,
    pub(crate) raft_core: Arc<Mutex<Raft<T>>>,

    // Cluster Membership
    pub(crate) membership: Arc<MOF<T>>,

    // Network & Storage events, (copied from Raft)
    // TODO: find a better solution
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    pub(crate) ready: AtomicBool,

    /// Notifies when RPC server is ready to accept requests
    pub(crate) rpc_ready_tx: watch::Sender<bool>,

    /// Notifies when leader is elected (includes leader changes)
    pub(crate) leader_notifier: LeaderNotifier,

    /// Raft node config
    pub node_config: Arc<RaftNodeConfig>,

    /// Optional watch registry for watcher registration
    /// When None, watch functionality is disabled
    #[cfg(feature = "watch")]
    pub(crate) watch_registry: Option<Arc<WatchRegistry>>,

    /// Watch dispatcher task handle (keeps dispatcher alive)
    #[cfg(feature = "watch")]
    pub(crate) _watch_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Commit handler task handle (background log application)
    pub(crate) _commit_handler_handle: Option<tokio::task::JoinHandle<()>>,

    /// Lease cleanup task handle (background TTL cleanup)
    pub(crate) _lease_cleanup_handle: Option<tokio::task::JoinHandle<()>>,

    /// Shutdown signal for graceful termination
    pub(crate) shutdown_signal: watch::Receiver<()>,
}

impl<T> Debug for Node<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("Node").field("node_id", &self.node_id).finish()
    }
}
impl<T> Node<T>
where
    T: TypeConfig,
{
    /// Starts and runs the Raft node's main execution loop.
    ///
    /// # Workflow
    /// Strategy-based bootstrap depending on node type:
    /// - **Learner**: Skip cluster ready check, join cluster after warmup
    /// - **Voter**: Wait for cluster ready, then warmup connections
    ///
    /// Both paths converge to the Raft event processing loop.
    ///
    /// # Errors
    /// Returns `Err` if any bootstrap step or Raft execution fails.
    ///
    /// # Example
    /// ```ignore
    /// let node = Node::new(...);
    /// tokio::spawn(async move {
    ///     node.run().await.expect("Node execution failed");
    /// });
    /// ```
    pub async fn run(&self) -> Result<()> {
        let mut shutdown_signal = self.shutdown_signal.clone();
        shutdown_signal.borrow_and_update();

        // Strategy pattern: bootstrap based on node type
        if self.node_config.is_learner() {
            self.run_as_learner(&mut shutdown_signal).await?;
        } else {
            self.run_as_voter(&mut shutdown_signal).await?;
        }

        // Start Raft main loop
        self.start_raft_loop().await
    }

    /// Learner bootstrap: skip cluster ready check, join after warmup.
    async fn run_as_learner(
        &self,
        shutdown: &mut watch::Receiver<()>,
    ) -> Result<()> {
        info!("Learner node bootstrap initiated");

        // Set RPC ready immediately (no cluster wait needed)
        self.set_rpc_ready(true);

        // Warm up connections
        self.warmup_with_shutdown(shutdown).await?;

        // Join cluster as learner
        let raft = self.raft_core.lock().await;
        info!(%self.node_config.cluster.node_id, "Learner joining cluster");
        raft.join_cluster().await?;
        drop(raft); // Release lock before entering main loop

        Ok(())
    }

    /// Voter bootstrap: wait for cluster ready, then warmup.
    async fn run_as_voter(
        &self,
        shutdown: &mut watch::Receiver<()>,
    ) -> Result<()> {
        info!("Voter node bootstrap initiated");

        // Wait for cluster ready
        tokio::select! {
            result = self.membership.check_cluster_is_ready() => result?,
            _ = shutdown.changed() => {
                info!("Shutdown during cluster ready check");
                return Ok(());
            }
        }

        // Set RPC ready after cluster is healthy
        self.set_rpc_ready(true);

        // Warm up connections
        self.warmup_with_shutdown(shutdown).await
    }

    /// Warm up peer connections with shutdown handling.
    async fn warmup_with_shutdown(
        &self,
        shutdown: &mut watch::Receiver<()>,
    ) -> Result<()> {
        tokio::select! {
            result = self.membership.pre_warm_connections() => result?,
            _ = shutdown.changed() => {
                info!("Shutdown during connection warmup");
                return Ok(());
            }
        }
        Ok(())
    }

    /// Start Raft main loop.
    async fn start_raft_loop(&self) -> Result<()> {
        let mut raft = self.raft_core.lock().await;
        raft.run().await
    }

    /// Marks the node's RPC server as ready to accept requests.
    ///
    /// # Parameters
    /// - `is_ready`: When `true`, marks RPC server as ready. When `false`,
    ///   marks server as temporarily unavailable.
    ///
    /// # Note
    /// This indicates the RPC server is listening, NOT that leader election is complete.
    /// Use `leader_change_notifier()` to wait for leader election.
    ///
    /// # Usage
    /// Called internally after RPC server starts and cluster health check passes.
    pub fn set_rpc_ready(
        &self,
        is_ready: bool,
    ) {
        info!("Set node RPC server ready: {}", is_ready);
        self.ready.store(is_ready, Ordering::SeqCst);
        // Notify waiters that RPC server is ready
        let _ = self.rpc_ready_tx.send(is_ready);
    }

    /// Checks if the node's RPC server is ready to accept requests.
    ///
    /// # Returns
    /// `true` if the RPC server is operational and listening,
    /// `false` otherwise.
    ///
    /// # Note
    /// This does NOT indicate leader election status. Use `leader_change_notifier()` for that.
    pub fn is_rpc_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Returns a receiver for node readiness notifications.
    ///
    /// Subscribe to this channel to be notified when the node becomes ready
    /// to participate in cluster operations (NOT the same as leader election).
    ///
    /// # Example
    /// ```ignore
    /// let ready_rx = node.ready_notifier();
    /// ready_rx.wait_for(|&ready| ready).await?;
    /// // RPC server is now listening
    /// ```
    pub fn ready_notifier(&self) -> watch::Receiver<bool> {
        self.rpc_ready_tx.subscribe()
    }

    /// Returns a receiver for leader change notifications.
    ///
    /// Subscribe to be notified when:
    /// - First leader is elected (initial election)
    /// - Leader changes (re-election)
    /// - No leader exists (during election)
    ///
    /// # Performance
    /// Event-driven notification, <1ms latency
    ///
    /// # Example
    /// ```ignore
    /// let mut leader_rx = node.leader_change_notifier();
    /// while leader_rx.changed().await.is_ok() {
    ///     if let Some(info) = leader_rx.borrow().as_ref() {
    ///         println!("Leader: {} (term {})", info.leader_id, info.term);
    ///     }
    /// }
    /// ```
    pub fn leader_change_notifier(&self) -> watch::Receiver<Option<crate::LeaderInfo>> {
        self.leader_notifier.subscribe()
    }

    /// Create a Node from a pre-built Raft instance
    /// This method is designed to support testing and external builders
    pub fn from_raft(
        raft: Raft<T>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        let event_tx = raft.event_sender();
        let node_config = raft.ctx.node_config();
        let membership = raft.ctx.membership();
        let node_id = raft.node_id;

        let (rpc_ready_tx, _rpc_ready_rx) = watch::channel(false);
        let leader_notifier = LeaderNotifier::new();

        Node {
            node_id,
            raft_core: Arc::new(Mutex::new(raft)),
            membership,
            event_tx,
            ready: AtomicBool::new(false),
            rpc_ready_tx,
            leader_notifier,
            node_config,
            #[cfg(feature = "watch")]
            watch_registry: None,
            #[cfg(feature = "watch")]
            _watch_dispatcher_handle: None,
            _commit_handler_handle: None,
            _lease_cleanup_handle: None,
            shutdown_signal,
        }
    }

    /// Returns this node's unique identifier.
    ///
    /// Useful for logging, metrics, and integrations that need to identify
    /// which Raft node is handling operations.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Creates a zero-overhead local KV client for embedded access.
    ///
    /// Returns a client that directly communicates with Raft core
    /// without gRPC serialization or network traversal.
    ///
    /// # Performance
    /// - 10-20x faster than gRPC client
    /// - <0.1ms latency per operation
    ///
    /// # Example
    /// ```ignore
    /// let node = NodeBuilder::new(config).start().await?;
    /// let client = node.local_client();
    /// client.put(b"key", b"value").await?;
    /// ```
    pub fn local_client(&self) -> LocalKvClient {
        LocalKvClient::new_internal(
            self.event_tx.clone(),
            self.node_id,
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms),
        )
    }
}
