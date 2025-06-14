//! Raft node container and lifecycle management.
//!
//! The [`Node`] struct acts as a host for a Raft consensus participant,
//! coordinating between the core protocol implementation ([`crate::core::raft::Raft`])
//! and external subsystems:
//!
//! ## Key Responsibilities
//! - Manages the Raft finite state machine lifecycle
//! - Coordinates peer networking through [`PeerChannels`]
//! - Maintains node readiness state for cluster coordination
//! - Executes the main event processing loop inside Raft
//!
//! ## Example Usage
//! ```ignore
//! let node = NodeBuilder::new(node_config).build().ready().unwrap();
//! tokio::spawn(async move {
//!     node.run().await.expect("Raft node execution failed");
//! });
//! ```

mod builder;
pub use builder::*;

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

use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::alias::POF;
use crate::membership::PeerChannelsFactory;
use crate::PeerChannels;
use crate::Raft;
use crate::RaftEvent;
use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;

/// Raft node container
pub struct Node<T>
where
    T: TypeConfig,
{
    pub(crate) node_id: u32,
    pub(crate) raft_core: Arc<Mutex<Raft<T>>>,

    // Network & Storage events, (copied from Raft)
    // TODO: find a better solution
    pub(crate) event_tx: mpsc::Sender<RaftEvent>,
    pub(crate) ready: AtomicBool,

    /// Raft node config
    pub node_config: Arc<RaftNodeConfig>,
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
    async fn connect_with_peers(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Result<POF<T>> {
        let mut peer_channels = T::P::create(node_id, node_config.clone());
        peer_channels.connect_with_peers(node_id).await?;

        Ok(peer_channels)
    }

    /// Starts and runs the Raft node's main execution loop.
    ///
    /// # Workflow
    /// 1. Establishes network connections with cluster peers
    /// 2. Performs cluster health check
    /// 3. Marks node as ready for operation
    /// 4. Joins the Raft cluster
    /// 5. Executes the core Raft event processing loop
    ///
    /// # Errors
    /// Returns `Err` if any of these operations fail:
    /// - Peer connection establishment
    /// - Cluster health check
    /// - Raft core initialization
    /// - Event processing failures
    ///
    /// # Example
    /// ```ignore
    /// let node = Node::new(...);
    /// tokio::spawn(async move {
    ///     node.run().await.expect("Node execution failed");
    /// });
    /// ```
    pub async fn run(&self) -> Result<()> {
        // 1. Connect with other peers
        let peer_channels = Self::connect_with_peers(self.node_id, self.node_config.clone()).await?;

        // 2. Healthcheck if all server is start serving
        peer_channels.check_cluster_is_ready().await?;

        // 3. Set node is ready to run Raft protocol
        self.set_ready(true);

        let mut raft = self.raft_core.lock().await;

        // 4. Join the node with cluster
        let peer_channels = Arc::new(peer_channels);
        raft.init_peer_channels(peer_channels.clone())?;

        // 5. if join as a new node
        if self.node_config.is_joining() {
            info!(%self.node_config.cluster.node_id, "Node is joining...");
            raft.join_cluster(peer_channels.clone()).await?;
        }

        // 6. Run the main event processing loop
        raft.run().await?;

        Ok(())
    }

    /// Controls the node's operational readiness state.
    ///
    /// # Parameters
    /// - `is_ready`: When `true`, marks node as ready to participate in cluster. When `false`,
    ///   marks node as temporarily unavailable.
    ///
    /// # Usage
    /// Typically used during cluster bootstrap or maintenance operations.
    /// The readiness state is atomically updated using SeqCst ordering.
    pub fn set_ready(
        &self,
        is_ready: bool,
    ) {
        self.ready.store(is_ready, Ordering::SeqCst);
    }

    /// Checks if the node is in a ready state to participate in cluster operations.
    ///
    /// # Returns
    /// `true` if the node is operational and ready to handle Raft protocol operations,
    /// `false` otherwise.
    pub fn server_is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}
