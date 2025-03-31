//! The core node implementation for Raft consensus protocol execution.
//!
//! ## Key Responsibilities
//! - Manages the Raft finite state machine lifecycle
//! - Coordinates peer networking through [`PeerChannels`]
//! - Maintains node readiness state for cluster coordination
//! - Executes the main event processing loop inside Raft
//!
//! ## Example Usage
//! ```ignore
//! let node = NodeBuilder::new(settings).build().ready().unwrap();
//! tokio::spawn(async move {
//!     node.run().await.expect("Raft node execution failed");
//! });
//! ```

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

pub struct Node<T>
where T: TypeConfig
{
    pub(crate) node_id: u32,
    pub(crate) raft_core: Arc<Mutex<Raft<T>>>,

    // Network & Storage events, (copied from Raft)
    // TODO: find a better solution
    pub event_tx: mpsc::Sender<RaftEvent>,
    pub(crate) ready: AtomicBool,

    pub settings: Arc<RaftNodeConfig>,
}

impl<T> Node<T>
where T: TypeConfig
{
    async fn connect_with_peers(
        node_id: u32,
        settings: Arc<RaftNodeConfig>,
    ) -> Result<POF<T>> {
        let mut peer_channels = T::P::create(node_id, settings.clone());
        peer_channels.connect_with_peers(node_id).await?;

        Ok(peer_channels)
    }

    pub async fn run(&self) -> Result<()> {
        // 1. Connect with other peers
        let peer_channels = Self::connect_with_peers(self.node_id, self.settings.clone()).await?;

        // 2. Healthcheck if all server is start serving
        peer_channels.check_cluster_is_ready().await?;

        // 3. Set node is ready to run Raft protocol
        self.set_ready(true);

        let mut raft = self.raft_core.lock().await;

        // 4. Join the node with cluster
        raft.join_cluster(Arc::new(peer_channels))?;

        // 5. Run the main event processing loop
        raft.run().await?;

        Ok(())
    }

    pub fn set_ready(
        &self,
        is_ready: bool,
    ) {
        self.ready.store(is_ready, Ordering::SeqCst);
    }

    pub fn server_is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}
