use std::fmt::Debug;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::ClientInner;
use crate::ClientApiError;
use d_engine_proto::server::cluster::NodeMeta;

/// Cluster administration interface
///
/// Currently supports member discovery. Node management operations
/// will be added in future releases.
#[derive(Clone)]
pub struct ClusterClient {
    client_inner: Arc<ArcSwap<ClientInner>>,
}

impl Debug for ClusterClient {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ClusterClient").finish()
    }
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
    pub async fn list_members(&self) -> std::result::Result<Vec<NodeMeta>, ClientApiError> {
        let client_inner = self.client_inner.load();

        Ok(client_inner.pool.get_all_members())
    }

    /// Get the current leader ID
    ///
    /// Returns the leader node ID if known, or None if no leader is currently elected.
    pub async fn get_leader_id(&self) -> std::result::Result<Option<u32>, ClientApiError> {
        let client_inner = self.client_inner.load();

        Ok(client_inner.pool.get_leader_id())
    }
}
