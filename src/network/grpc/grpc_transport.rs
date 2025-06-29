//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.

use crate::alias::MOF;
use crate::grpc_task_with_timeout_and_exponential_backoff;
use crate::proto::cluster::cluster_management_service_client::ClusterManagementServiceClient;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::JoinResponse;
use crate::proto::cluster::LeaderDiscoveryRequest;
use crate::proto::cluster::LeaderDiscoveryResponse;
use crate::proto::election::raft_election_service_client::RaftElectionServiceClient;
use crate::proto::election::VoteRequest;
use crate::proto::replication::raft_replication_service_client::RaftReplicationServiceClient;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::storage::snapshot_service_client::SnapshotServiceClient;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::AppendResult;
use crate::BackoffPolicy;
use crate::ClusterUpdateResult;
use crate::ConnectionType;
use crate::Error;
use crate::InstallSnapshotBackoffPolicy;
use crate::Membership;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use crate::Transport;
use crate::TypeConfig;
use crate::VoteResult;
use crate::API_SLO;
use autometrics::autometrics;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

#[derive(Debug)]
pub struct GrpcTransport<T>
where
    T: TypeConfig,
{
    pub(crate) my_id: u32,

    // -- Type System Marker --
    /// Phantom data for type parameter anchoring
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T> Transport<T> for GrpcTransport<T>
where
    T: TypeConfig,
{
    #[autometrics(objective = API_SLO)]
    async fn send_cluster_update(
        &self,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<ClusterUpdateResult> {
        debug!("Sending cluster configuration update requests");

        // Get voting members (control plane operation)
        let peers = membership.voters();
        if peers.is_empty() {
            warn!("No voting members available for cluster update");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_cluster_update",
            }
            .into());
        }

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();

        for peer in peers {
            let peer_id = peer.id;
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            // Real-time connection fetch for control operations
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Control).await {
                Some(chan) => chan,
                None => {
                    error!("Failed to get control channel for peer {}", peer_id);
                    continue;
                }
            };

            let req_clone = req.clone();
            let closure = move || {
                let channel = channel.clone();
                let mut client = ClusterManagementServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                let req = req_clone.clone();
                async move { client.update_cluster_conf(tonic::Request::new(req)).await }
            };

            let policy = retry.membership;
            let my_id = self.my_id;
            let task_handle = task::spawn(async move {
                match grpc_task_with_timeout_and_exponential_backoff("update_cluster_conf", closure, policy).await {
                    Ok(response) => {
                        debug!(
                            "[send_cluster_update | {my_id}->{peer_id}] sync_cluster_conf response: {:?}",
                            response
                        );
                        let res = response.into_inner();

                        Ok(res)
                    }
                    Err(e) => {
                        warn!("[send_cluster_update | {my_id}->{peer_id}] Received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        let mut responses = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(r) => responses.push(r),
                Err(e) => {
                    error!("[send_cluster_update] Task failed with error: {:?}", &e);
                    responses.push(Err(Error::from(NetworkError::TaskFailed(e))));
                }
            }
        }

        Ok(ClusterUpdateResult { peer_ids, responses })
    }

    #[tracing::instrument(skip_all,fields(
        total_peers = requests.len(),
        avg_request_size = "N/A", // PLACEHOLDER
    ))]
    async fn send_append_requests(
        &self,
        requests: Vec<(u32, AppendEntriesRequest)>,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<AppendResult> {
        debug!("Sending append entries requests");
        if requests.is_empty() {
            warn!("No append requests to process");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_append_requests",
            }
            .into());
        }
        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();

        // Calculate the real value in advance
        let avg_request_size = requests
            .iter()
            .map(|(_, r)| r.entries.len())
            .sum::<usize>()
            .saturating_div(requests.len().max(1)); // Prevent zero division

        // Update log fields
        tracing::Span::current().record("avg_request_size", avg_request_size);

        for (peer_id, req) in requests {
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            // Real-time connection fetch for data operations
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Data).await {
                Some(chan) => chan,
                None => {
                    error!("Failed to get data channel for peer {}", peer_id);
                    continue;
                }
            };

            let closure = move || {
                let channel = channel.clone();
                let mut client = RaftReplicationServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                let req = req.clone();
                async move { client.append_entries(tonic::Request::new(req)).await }
            };

            let policy = retry.append_entries;
            let my_id = self.my_id;
            let task_handle = task::spawn(async move {
                match grpc_task_with_timeout_and_exponential_backoff("append_entries", closure, policy).await {
                    Ok(response) => {
                        debug!("[send_append_requests| {my_id}->{peer_id}] response: {:?}", response);
                        let res = response.into_inner();

                        Ok(res)
                    }
                    Err(e) => {
                        warn!("[send_append_requests | {my_id}->{peer_id}] Received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        let mut responses = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(r) => {
                    responses.push(r);
                }
                Err(e) => {
                    error!("[send_append_requests] Task failed with error: {:?}", e);
                    responses.push(Err(Error::from(NetworkError::TaskFailed(e))));
                }
            }
        }

        Ok(AppendResult { peer_ids, responses })
    }

    #[autometrics(objective = API_SLO)]
    async fn send_vote_requests(
        &self,
        req: VoteRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<VoteResult> {
        debug!("Sending vote requests");

        // Get voting members (control plane operation)
        let peers = membership.voters();
        if peers.is_empty() {
            warn!("No voting members available for vote requests");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_vote_requests",
            }
            .into());
        }

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();

        for peer in peers {
            let peer_id = peer.id;
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            // Real-time connection fetch for control operations
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Control).await {
                Some(chan) => chan,
                None => {
                    error!("Failed to get control channel for peer {}", peer_id);
                    continue;
                }
            };

            let req_clone = req;
            let closure = move || {
                let channel = channel.clone();
                let mut client = RaftElectionServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                async move { client.request_vote(tonic::Request::new(req_clone)).await }
            };
            let policy = retry.election;
            let my_id = self.my_id;
            let addr = peer.address;
            let task_handle = task::spawn(async move {
                match grpc_task_with_timeout_and_exponential_backoff("request_vote", closure, policy).await {
                    Ok(response) => {
                        debug!(
                            "[send_vote_requests | {my_id}->{peer_id}] resquest [peer({:?})] vote response: {:?}",
                            &addr, response
                        );
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!("[send_vote_requests | {my_id}->{peer_id}] Received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        let mut responses = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(r) => responses.push(r),
                Err(e) => {
                    error!("Task failed with error: {:?}", &e);
                    responses.push(Err(Error::from(NetworkError::TaskFailed(e))));
                }
            }
        }
        Ok(VoteResult { peer_ids, responses })
    }

    async fn send_purge_requests(
        &self,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<Vec<Result<PurgeLogResponse>>> {
        debug!("Sending log purge requests");

        // Get all members (data operation)
        let peers = membership.voters();

        if peers.is_empty() {
            warn!("No peers available for purge requests");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_purge_requests",
            }
            .into());
        }

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();

        for peer in peers {
            let peer_id = peer.id;
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            // Real-time connection fetch for data operations
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Data).await {
                Some(chan) => chan,
                None => {
                    error!("Failed to get data channel for peer {}", peer_id);
                    continue;
                }
            };

            let req_clone = req.clone();
            let closure = move || {
                let channel = channel.clone();
                let mut client = SnapshotServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                let req = req_clone.clone();
                async move { client.purge_log(tonic::Request::new(req)).await }
            };

            let policy = retry.purge_log;
            let addr = peer.address;
            let my_id = self.my_id;
            let task_handle = task::spawn(async move {
                match grpc_task_with_timeout_and_exponential_backoff("purge_log", closure, policy).await {
                    Ok(response) => {
                        debug!(
                            "[send_purge_requests | {my_id}->{peer_id}]resquest [peer({:?})] vote response: {:?}",
                            &addr, response
                        );
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!("[send_purge_requests | {my_id}->{peer_id}]Received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        let mut responses = Vec::new();
        while let Some(result) = tasks.next().await {
            match result {
                Ok(r) => responses.push(r),
                Err(e) => {
                    error!("Task failed with error: {:?}", &e);
                    responses.push(Err(Error::from(NetworkError::TaskFailed(e))));
                }
            }
        }

        Ok(responses)
    }

    #[autometrics(objective = API_SLO)]
    async fn join_cluster(
        &self,
        leader_id: u32,
        request: crate::proto::cluster::JoinRequest,
        retry: BackoffPolicy,
        membership: Arc<MOF<T>>,
    ) -> Result<JoinResponse> {
        debug!("Initiating cluster join via leader {}", leader_id);

        // Real-time connection fetch for control operations
        let channel = membership
            .get_peer_channel(leader_id, ConnectionType::Control)
            .await
            .ok_or(NetworkError::PeerConnectionNotFound(leader_id))?;

        let closure = move || {
            let channel = channel.clone();
            let mut client = ClusterManagementServiceClient::new(channel)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
            let req = request.clone();
            async move { client.join_cluster(tonic::Request::new(req)).await }
        };

        let my_id = self.my_id;
        let response = grpc_task_with_timeout_and_exponential_backoff("join_cluster", closure, retry).await?;
        debug!(
            "[join_cluster | {my_id}->{leader_id}]Join cluster response: {:?}",
            response
        );
        Ok(response.into_inner())
    }

    #[autometrics(objective = API_SLO)]
    async fn discover_leader(
        &self,
        request: LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
        membership: Arc<MOF<T>>,
    ) -> Result<Vec<LeaderDiscoveryResponse>> {
        debug!("Starting leader discovery for node {}", request.node_id);

        let member_ids: Vec<_> = membership.voters().iter().map(|m| m.id).collect();

        let tasks = member_ids.into_iter().map(|member_id| {
            Self::process_member(membership.clone(), member_id, request.clone(), rpc_enable_compression)
        });

        let my_id = self.my_id;
        let results = futures::stream::iter(tasks)
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;
        debug!("[discover_leader | {my_id} ] Discover leader results.");
        Ok(results.into_iter().flatten().collect())
    }

    // Add this to the Transport trait implementation
    #[autometrics(objective = API_SLO)]
    async fn request_snapshot_from_leader(
        &self,
        leader_id: u32,
        ack_rx: mpsc::Receiver<SnapshotAck>,
        _retry: &InstallSnapshotBackoffPolicy,
        membership: Arc<MOF<T>>,
    ) -> Result<Box<tonic::Streaming<SnapshotChunk>>> {
        debug!("Fetching snapshot from leader {}", leader_id);

        // Get bulk connection channel
        let channel = membership
            .get_peer_channel(leader_id, ConnectionType::Bulk)
            .await
            .ok_or(NetworkError::PeerConnectionNotFound(leader_id))?;

        let channel = channel.clone();
        let mut client = SnapshotServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        // ReceiverStream adapts mpsc::Receiver to Stream type
        let request_stream = ReceiverStream::new(ack_rx);
        let response = client
            .stream_snapshot(tonic::Request::new(request_stream))
            .await
            .map_err(|e| NetworkError::TonicStatusError(Box::new(e)))?;

        Ok(Box::new(response.into_inner()))
    }
}

impl<T> GrpcTransport<T>
where
    T: TypeConfig,
{
    pub(crate) fn new(node_id: u32) -> Self {
        Self {
            my_id: node_id,
            _marker: PhantomData,
        }
    }

    #[autometrics(objective = API_SLO)]
    pub(crate) fn if_mark_learner_as_follower(
        leader_commit_index: u64,
        learner_next_id: u64,
    ) -> bool {
        info!(
            "if_mark_learner_as_follower: leader_commit_index: {}, learner_next_id: {}",
            leader_commit_index, learner_next_id
        );
        if leader_commit_index <= learner_next_id {
            return true;
        }

        return false;
    }

    async fn process_member(
        membership: Arc<MOF<T>>,
        member_id: u32,
        request: LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
    ) -> Option<LeaderDiscoveryResponse> {
        match membership.get_peer_channel(member_id, ConnectionType::Control).await {
            Some(channel) => {
                let mut client = ClusterManagementServiceClient::new(channel);
                if rpc_enable_compression {
                    client = client
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);
                }
                client.discover_leader(request).await.ok().map(|res| res.into_inner())
            }
            None => {
                error!(%member_id, "Cannot get channel from membership");
                None
            }
        }
    }
}
