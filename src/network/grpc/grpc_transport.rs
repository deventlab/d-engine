//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

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
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::snapshot_service_client::SnapshotServiceClient;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::scoped_timer::ScopedTimer;
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

pub struct PeerAppender {
    pub(crate) sender: mpsc::Sender<AppendRequest>,
    pub(crate) task_handle: JoinHandle<()>,
}

pub struct AppendRequest {
    pub(crate) request: AppendEntriesRequest,
    pub(crate) response_sender: oneshot::Sender<Result<AppendEntriesResponse>>,
}

pub struct GrpcTransport<T>
where
    T: TypeConfig,
{
    pub(crate) my_id: u32,

    peer_appenders: Arc<DashMap<u32, PeerAppender>>,

    // -- Type System Marker --
    /// Phantom data for type parameter anchoring
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T> Transport<T> for GrpcTransport<T>
where
    T: TypeConfig,
{
    async fn send_cluster_update(
        &self,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<ClusterUpdateResult> {
        debug!("Sending cluster configuration update requests");

        // Get voting members (control plane operation)
        let peers = membership.voters().await;
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
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Control).await
            {
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
                match grpc_task_with_timeout_and_exponential_backoff(
                    "update_cluster_conf",
                    closure,
                    policy,
                )
                .await
                {
                    Ok(response) => {
                        debug!(
                            "[send_cluster_update | {my_id}->{peer_id}] sync_cluster_conf response: {:?}",
                            response
                        );
                        let res = response.into_inner();

                        Ok(res)
                    }
                    Err(e) => {
                        warn!(
                            "[send_cluster_update | {my_id}->{peer_id}] Received RPC error: {}",
                            e
                        );
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

        Ok(ClusterUpdateResult {
            peer_ids,
            responses,
        })
    }

    #[tracing::instrument(skip_all,fields(
        total_peers = requests.len(),
        avg_request_size = "N/A", // PLACEHOLDER
    ))]
    async fn send_append_requests(
        &self,
        requests: Vec<(u32, AppendEntriesRequest)>,
        retry_policies: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<AppendResult> {
        let _timer = ScopedTimer::new("send_append_requests");

        debug!("Sending append entries requests");
        if requests.is_empty() {
            warn!("No append requests to process");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_append_requests",
            }
            .into());
        }

        let mut response_futures = Vec::new();
        let mut peer_ids = HashSet::new();

        // // Calculate the real value in advance
        // let avg_request_size = requests
        //     .iter()
        //     .map(|(_, r)| r.entries.len())
        //     .sum::<usize>()
        //     .saturating_div(requests.len().max(1)); // Prevent zero division

        // // Update log fields
        // tracing::Span::current().record("avg_request_size", avg_request_size);

        for (peer_id, request) in requests {
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            // Get or create appender for this peer
            let appender = match self
                .get_or_create_appender(peer_id, retry_policies.clone(), membership.clone())
                .await
            {
                Ok(appender) => appender,
                Err(e) => {
                    error!("Failed to get appender for peer {}: {}", peer_id, e);
                    continue;
                }
            };

            // Create response channel
            let (response_tx, response_rx) = oneshot::channel();

            // Send request to peer's dedicated task
            if let Err(e) = appender
                .send(AppendRequest {
                    request,
                    response_sender: response_tx,
                })
                .await
            {
                error!("Failed to send request to peer {} appender: {}", peer_id, e);
                continue;
            }

            response_futures.push(async move {
                match response_rx.await {
                    Ok(result) => result,
                    Err(_) => Err(Error::from(NetworkError::ResponseChannelClosed)),
                }
            });
        }

        // Wait for all responses
        let responses = futures::future::join_all(response_futures).await;

        Ok(AppendResult {
            peer_ids,
            responses,
        })
    }

    async fn send_vote_requests(
        &self,
        req: VoteRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<VoteResult> {
        debug!("Sending vote requests");

        // Get voting members (control plane operation)
        let peers = membership.voters().await;
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
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Control).await
            {
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
                match grpc_task_with_timeout_and_exponential_backoff(
                    "request_vote",
                    closure,
                    policy,
                )
                .await
                {
                    Ok(response) => {
                        debug!(
                            "[send_vote_requests | {my_id}->{peer_id}] resquest [peer({:?})] vote response: {:?}",
                            &addr, response
                        );
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!(
                            "[send_vote_requests | {my_id}->{peer_id}] Received RPC error: {}",
                            e
                        );
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
        Ok(VoteResult {
            peer_ids,
            responses,
        })
    }

    async fn send_purge_requests(
        &self,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<Vec<Result<PurgeLogResponse>>> {
        debug!("Sending log purge requests");

        // Get all members (data operation)
        let peers = membership.voters().await;

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
                match grpc_task_with_timeout_and_exponential_backoff("purge_log", closure, policy)
                    .await
                {
                    Ok(response) => {
                        debug!(
                            "[send_purge_requests | {my_id}->{peer_id}]resquest [peer({:?})] vote response: {:?}",
                            &addr, response
                        );
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!(
                            "[send_purge_requests | {my_id}->{peer_id}]Received RPC error: {}",
                            e
                        );
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
        let response =
            grpc_task_with_timeout_and_exponential_backoff("join_cluster", closure, retry).await?;
        debug!(
            "[join_cluster | {my_id}->{leader_id}]Join cluster response: {:?}",
            response
        );
        Ok(response.into_inner())
    }

    async fn discover_leader(
        &self,
        request: LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
        membership: Arc<MOF<T>>,
    ) -> Result<Vec<LeaderDiscoveryResponse>> {
        debug!("Starting leader discovery for node {}", request.node_id);

        let member_ids: Vec<_> = membership.voters().await.iter().map(|m| m.id).collect();

        let tasks = member_ids.into_iter().map(|member_id| {
            Self::process_member(
                membership.clone(),
                member_id,
                request.clone(),
                rpc_enable_compression,
            )
        });

        let my_id = self.my_id;
        let results = futures::stream::iter(tasks).buffer_unordered(10).collect::<Vec<_>>().await;
        debug!("[discover_leader | {my_id} ] Discover leader results.");
        Ok(results.into_iter().flatten().collect())
    }

    // Add this to the Transport trait implementation
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
            peer_appenders: Arc::new(DashMap::new()),
            _marker: PhantomData,
        }
    }

    #[allow(dead_code)]
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

        false
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

    // Get or create appender for a specific peer (lock-free)
    async fn get_or_create_appender(
        &self,
        peer_id: u32,
        retry_policy: RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<mpsc::Sender<AppendRequest>> {
        // Fast path: check if appender already exists
        if let Some(appender) = self.peer_appenders.get(&peer_id) {
            return Ok(appender.sender.clone());
        }

        // Slow path: create new appender (this happens rarely)
        let (tx, rx) = mpsc::channel(100);
        let retry_policy = retry_policy.append_entries;
        let membership = membership.clone();
        let my_id = self.my_id;

        let task_handle = tokio::spawn(Self::peer_appender_task(
            peer_id,
            rx,
            retry_policy,
            membership,
            my_id,
        ));

        let appender = PeerAppender {
            sender: tx.clone(),
            task_handle,
        };

        // Use entry API to handle concurrent creation attempts
        match self.peer_appenders.entry(peer_id) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                // Another thread created an appender first, use theirs
                Ok(entry.get().sender.clone())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(appender);
                Ok(tx)
            }
        }
    }

    // Long-running task for a specific peer
    async fn peer_appender_task(
        peer_id: u32,
        mut receiver: mpsc::Receiver<AppendRequest>,
        retry_policy: BackoffPolicy,
        membership: Arc<MOF<T>>,
        my_id: u32,
    ) {
        while let Some(req) = receiver.recv().await {
            let AppendRequest {
                request,
                response_sender,
            } = req;

            // Get channel for this peer (can be cached for performance)
            let channel = match membership.get_peer_channel(peer_id, ConnectionType::Data).await {
                Some(chan) => chan,
                None => {
                    let _ = response_sender.send(Err(Error::from(
                        NetworkError::PeerConnectionNotFound(peer_id),
                    )));
                    continue;
                }
            };

            let result =
                Self::send_single_append_request(peer_id, request, retry_policy, channel, my_id)
                    .await;

            let _ = response_sender.send(result);
        }

        // Clean up when channel is closed
        debug!("Peer appender task for peer {} is shutting down", peer_id);
    }

    // Helper function to send a single append request
    async fn send_single_append_request(
        peer_id: u32,
        request: AppendEntriesRequest,
        retry_policy: BackoffPolicy,
        channel: Channel,
        my_id: u32,
    ) -> Result<AppendEntriesResponse> {
        let closure = || {
            let channel = channel.clone();
            let mut client = RaftReplicationServiceClient::new(channel)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
            let req = request.clone();
            async move { client.append_entries(tonic::Request::new(req)).await }
        };

        match grpc_task_with_timeout_and_exponential_backoff(
            "append_entries",
            closure,
            retry_policy,
        )
        .await
        {
            Ok(response) => {
                debug!(
                    "[send_append_requests| {my_id}->{peer_id}] response: {:?}",
                    response
                );
                Ok(response.into_inner())
            }
            Err(e) => {
                warn!(
                    "[send_append_requests | {my_id}->{peer_id}] Received RPC error: {}",
                    e
                );
                Err(e)
            }
        }
    }

    // Clean up method to remove appenders for peers that are no longer needed
    pub async fn remove_peer_appender(
        &self,
        peer_id: u32,
    ) {
        if let Some((_, appender)) = self.peer_appenders.remove(&peer_id) {
            appender.task_handle.abort();
            debug!("Removed appender task for peer {}", peer_id);
        }
    }
}
