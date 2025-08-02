use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tracing::info;

use crate::proto::cluster::NodeMeta;
use crate::MembershipError;
use crate::Result;

pub struct MembershipGuard {
    // Atomic state pointer for lock-free reads
    state: ArcSwap<InnerState>,
    // Mutex for write serialization
    write_mutex: Mutex<()>,
}

#[derive(Clone)]
pub struct InnerState {
    pub nodes: HashMap<u32, NodeMeta>,
    pub cluster_conf_version: u64,
}

impl MembershipGuard {
    pub fn new(
        initial_nodes: Vec<NodeMeta>,
        initial_version: u64,
    ) -> Self {
        info!(
            "Initializing membership: {:?}, version: {}",
            initial_nodes, initial_version
        );
        let state = ArcSwap::new(Arc::new(InnerState {
            nodes: initial_nodes.into_iter().map(|node| (node.id, node)).collect(),
            cluster_conf_version: initial_version,
        }));
        Self {
            state,
            write_mutex: Mutex::new(()),
        }
    }

    /// Provides read access to the state
    pub async fn blocking_read<R>(
        &self,
        f: impl FnOnce(&InnerState) -> R,
    ) -> R {
        // Load current state atomically (no lock)
        let state = self.state.load();
        f(&state)
    }

    /// Provides write access to the state
    pub async fn blocking_write<R>(
        &self,
        f: impl FnOnce(&mut InnerState) -> R,
    ) -> R {
        // Serialize writes with mutex
        let _guard = self.write_mutex.lock().await;

        // Clone current state
        let mut new_state = (**self.state.load()).clone();

        // Apply modifications
        let result = f(&mut new_state);

        // Atomically swap state pointer
        self.state.store(Arc::new(new_state));
        result
    }

    #[allow(unused)]
    pub async fn update_node(
        &self,
        node_id: u32,
        f: impl FnOnce(&mut NodeMeta),
    ) -> Result<()> {
        self.blocking_write(|state| {
            if let Some(node) = state.nodes.get_mut(&node_id) {
                f(node);
                Ok(())
            } else {
                Err(MembershipError::NoMetadataFoundForNode { node_id }.into())
            }
        })
        .await
    }

    #[allow(unused)]
    pub async fn contains_node(
        &self,
        node_id: u32,
    ) -> bool {
        self.blocking_read(|state| state.nodes.contains_key(&node_id)).await
    }
}
