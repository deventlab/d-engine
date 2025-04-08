use super::ConnectionPool;
use crate::proto::NodeMeta;
use crate::Result;

/// Cluster administration interface
///
/// Currently supports member discovery. Node management operations
/// will be added in future releases.
pub struct ClusterClient {
    pool: ConnectionPool,
}

impl ClusterClient {
    pub(crate) fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }

    /// Lists all cluster members with metadata
    ///
    /// Returns node information including:
    /// - IP address
    /// - Port
    /// - Role (Leader/Follower)
    pub async fn list_members(&self) -> Result<Vec<NodeMeta>> {
        Ok(self.pool.get_all_members())
    }

    /// [Unimplemented] Adds new node to cluster
    #[doc(hidden)]
    pub async fn add_member(
        &self,
        _node: NodeMeta,
    ) -> Result<()> {
        //TOOD: in next release
        Ok(())
    }
}
