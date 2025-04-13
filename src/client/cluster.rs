use std::sync::Arc;

use arc_swap::ArcSwap;

use super::ClientInner;
use crate::proto::NodeMeta;
use crate::Result;

/// Cluster administration interface
///
/// Currently supports member discovery. Node management operations
/// will be added in future releases.
#[derive(Clone)]
pub struct ClusterClient {
    client_inner: Arc<ArcSwap<ClientInner>>,
}

impl ClusterClient {
    pub(crate) fn new(client_inner: Arc<ArcSwap<ClientInner>>) -> Self {
        Self { client_inner }
    }

    /// Lists all cluster members with metadata
    ///
    /// Returns node information including:
    /// - IP address
    /// - Port
    /// - Role (Leader/Follower)
    pub async fn list_members(&self) -> Result<Vec<NodeMeta>> {
        let client_inner = self.client_inner.load();

        Ok(client_inner.pool.get_all_members())
    }

    /// [Unimplemented] Adds new node to cluster
    #[allow(unused)]
    async fn add_member(
        &self,
        _node: NodeMeta,
    ) -> Result<()> {
        unimplemented!("Node management coming in v0.2")
    }
}
