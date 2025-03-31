use super::ConnectionPool;
use crate::grpc::rpc_service::NodeMeta;
use crate::Result;

pub struct ClusterClient {
    pool: ConnectionPool,
}

impl ClusterClient {
    pub(crate) fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }

    pub async fn list_members(&self) -> Result<Vec<NodeMeta>> {
        //TOOD: in next release
        Ok(Vec::new())
    }

    pub async fn add_member(
        &self,
        node: NodeMeta,
    ) -> Result<()> {
        //TOOD: in next release
        Ok(())
    }
}
