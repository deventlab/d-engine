use crate::grpc::rpc_service::{
    snapshot_rpc_service_server::SnapshotRpcService, InstallSnapshotRequest,
    InstallSnapshotResponse,
};

#[derive(Debug, Clone, Default)]
pub struct MockSnapshotRpcService {
    // Expected responses for each method
    pub expected_install_snapshot_response: Option<Result<InstallSnapshotResponse, tonic::Status>>,
}

#[tonic::async_trait]
impl SnapshotRpcService for MockSnapshotRpcService {
    async fn install_snapshot(
        &self,
        _request: tonic::Request<InstallSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        match &self.expected_install_snapshot_response {
            Some(Ok(response)) => Ok(tonic::Response::new(response.clone())),
            Some(Err(status)) => Err(status.clone()),
            None => Err(tonic::Status::unknown("No mock response set")),
        }
    }
}
