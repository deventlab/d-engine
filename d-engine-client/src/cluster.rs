use std::fmt::Debug;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tonic::codec::CompressionEncoding;
use tracing::debug;
use tracing::error;
use tracing::instrument;

use super::ClientInner;
use crate::ClientApiError;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_management_service_client::ClusterManagementServiceClient;

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

    /// Join a new node to the cluster
    ///
    /// # Parameters
    /// - `node`: NodeMeta
    ///
    /// # Returns
    /// - `JoinResponse` with cluster configuration if successful
    #[instrument(skip(self))]
    pub async fn join_cluster(
        &self,
        node: NodeMeta,
    ) -> std::result::Result<JoinResponse, ClientApiError> {
        let client_inner = self.client_inner.load();
        let channel = client_inner.pool.get_leader();

        let mut client = ClusterManagementServiceClient::new(channel);
        if client_inner.pool.config.enable_compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }

        let request = tonic::Request::new(JoinRequest {
            node_id: node.id,
            node_role: node.role,
            address: node.address,
        });
        let response = match client.join_cluster(request).await {
            Ok(response) => {
                debug!("[:ClusterClient:join_cluster] response: {:?}", response);
                response.into_inner()
            }
            Err(status) => {
                error!("[:ClusterClient:join_cluster] status: {:?}", status);
                return Err(status.into());
            }
        };

        Ok(response)
    }
}
