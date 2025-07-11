use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use crate::proto::cluster::NodeMeta;
use crate::MembershipError;
use crate::Result;

pub struct MembershipGuard {
    inner: Arc<RwLock<InnerState>>,
}

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
        let inner = Arc::new(RwLock::new(InnerState {
            nodes: initial_nodes.into_iter().map(|node| (node.id, node)).collect(),
            cluster_conf_version: initial_version,
        }));
        Self { inner }
    }

    /// Provides read access to the state
    pub fn blocking_read<R>(
        &self,
        f: impl FnOnce(&InnerState) -> R,
    ) -> R {
        let guard = self.inner.read();
        f(&guard)
    }

    /// Provides write access to the state
    pub fn blocking_write<R>(
        &self,
        f: impl FnOnce(&mut InnerState) -> R,
    ) -> R {
        let mut guard = self.inner.write();
        f(&mut guard)
    }

    pub fn update_node(
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
    }

    pub fn contains_node(
        &self,
        node_id: u32,
    ) -> bool {
        self.blocking_read(|state| state.nodes.contains_key(&node_id))
    }
}
