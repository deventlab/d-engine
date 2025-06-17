//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.

use crate::grpc::RestartableStream;
use crate::proto::cluster::cluster_management_service_client::ClusterManagementServiceClient;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::JoinRequest;
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
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::scoped_timer::ScopedTimer;
use crate::task_with_timeout_and_exponential_backoff;
use crate::AppendResult;
use crate::BackoffPolicy;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::ClusterUpdateResult;
use crate::Error;
use crate::InstallSnapshotBackoffPolicy;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::Transport;
use crate::VoteResult;
use crate::API_SLO;
use autometrics::autometrics;
use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

#[derive(Debug)]
pub struct GrpcTransport {
    pub(crate) my_id: u32,
}

#[async_trait]
impl Transport for GrpcTransport {
    #[autometrics(objective = API_SLO)]
    async fn send_cluster_update(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: ClusterConfChangeRequest,
        retry: &RetryPolicies,
    ) -> Result<ClusterUpdateResult> {
        debug!("-------- send cluster_membership requests --------");
        if peers.is_empty() {
            warn!("peers is empty.");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_cluster_update",
            }
            .into());
        }

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();
        for peer in peers {
            let peer_id = peer.id;

            if peer_id == self.my_id {
                error!(
                    "myself({}) should not be passed into the send_cluster_update",
                    self.my_id
                );
                continue;
            }

            if peer_ids.contains(&peer_id) {
                error!("found duplicated peer which we have send append requests already");
                continue;
            }

            peer_ids.insert(peer_id);

            let channel = peer.channel_with_address.channel;
            let req = req.clone();

            let closure = move || {
                let channel = channel.clone();
                let mut client = ClusterManagementServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                let req = req.clone();
                async move { client.update_cluster_conf(tonic::Request::new(req)).await }
            };

            let membership_backoff_policy = retry.membership;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(closure, membership_backoff_policy).await {
                    Ok(response) => {
                        debug!("sync_cluster_conf response: {:?}", response);
                        let res = response.into_inner();

                        Ok(res)
                    }
                    Err(e) => {
                        warn!("Received RPC error: {}", e);
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

    #[tracing::instrument]
    async fn send_append_requests(
        &self,
        requests_with_peer_address: Vec<(u32, ChannelWithAddress, AppendEntriesRequest)>,
        retry: &RetryPolicies,
    ) -> Result<AppendResult> {
        debug!("-------- send append entries requests --------");
        if requests_with_peer_address.is_empty() {
            warn!("peers is empty.");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_vote_requests",
            }
            .into());
        }

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = HashSet::new();

        for (peer_id, channel_with_address, req) in requests_with_peer_address {
            debug!("start sending append entry request to peer: {}", peer_id);
            if peer_id == self.my_id {
                error!(
                    "myself({}) should not be passed into the send_append_requests",
                    self.my_id
                );
                continue;
            }

            if peer_ids.contains(&peer_id) {
                error!("found duplicated peer which we have send append requests already");
                continue;
            }

            peer_ids.insert(peer_id);

            let channel = channel_with_address.channel;
            // let req = req.clone();
            debug!("[{} -> {}: send_append_requests, req: {:?}", self.my_id, peer_id, &req);

            let closure = move || {
                let channel = channel.clone();
                let mut client = RaftReplicationServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                let req = req.clone();
                async move { client.append_entries(tonic::Request::new(req)).await }
            };

            let append_entries_backoff_policy = retry.append_entries;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(closure, append_entries_backoff_policy).await {
                    Ok(response) => {
                        debug!("append entries response: {:?}", response);
                        let res = response.into_inner();

                        Ok(res)
                    }
                    Err(e) => {
                        warn!("append entries response received RPC error: {}", e);
                        Err(e)
                    }
                }
            });
            tasks.push(task_handle.boxed());
        }

        let mut responses = Vec::new();

        // Note:
        // Even if there are errors, we must not return early unless it's a higher term error.
        // We need to wait for all responses to return before proceeding.
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
        peers: Vec<ChannelWithAddressAndRole>,
        req: VoteRequest,
        retry: &RetryPolicies,
    ) -> Result<VoteResult> {
        debug!("-------- send vote request --------");
        if peers.is_empty() {
            warn!("peers is empty.");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_vote_requests",
            }
            .into());
        }
        let mut tasks = FuturesUnordered::new();

        // make sure the collection items are unique
        let mut peer_ids = HashSet::new();

        debug!("send_vote_requests: {:?}, to: {:?}", &req, &peers);

        for peer in peers {
            let peer_id = peer.id;

            if peer_id == self.my_id {
                error!(
                    "myself({}) should not be passed into the send_vote_requests",
                    self.my_id
                );
                continue;
            }

            if peer_ids.contains(&peer_id) {
                error!("found duplicated peer which we have send append requests already");
                continue;
            }

            peer_ids.insert(peer_id);

            let peer_channel_with_addr = peer.channel_with_address;

            let addr = peer_channel_with_addr.address;

            let closure = move || {
                let channel = peer_channel_with_addr.channel.clone();
                let mut client = RaftElectionServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                async move { client.request_vote(tonic::Request::new(req)).await }
            };

            let election_backoff_policy = retry.election;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(closure, election_backoff_policy).await {
                    Ok(response) => {
                        debug!("resquest [peer({:?})] vote response: {:?}", &addr, response);
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!("Received RPC error: {}", e);
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
        peers: Vec<ChannelWithAddressAndRole>,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
    ) -> Result<Vec<Result<PurgeLogResponse>>> {
        debug!("-------- send purge request --------");
        if peers.is_empty() {
            warn!("peers is empty.");
            return Err(NetworkError::EmptyPeerList {
                request_type: "send_purge_requests",
            }
            .into());
        }
        let mut tasks = FuturesUnordered::new();

        // make sure the collection items are unique
        let mut peer_ids = HashSet::new();

        debug!("send_purge_requests: {:?}, to: {:?}", &req, &peers);

        for peer in peers {
            let peer_id = peer.id;

            if peer_id == self.my_id {
                error!(
                    "myself({}) should not be passed into the send_purge_requests",
                    self.my_id
                );
                continue;
            }

            if peer_ids.contains(&peer_id) {
                error!("found duplicated peer which we have send append requests already");
                continue;
            }

            peer_ids.insert(peer_id);

            let peer_channel_with_addr = peer.channel_with_address;
            let addr = peer_channel_with_addr.address;
            let req_clone = req.clone();

            let closure = move || {
                let req = req_clone.clone();
                let channel = peer_channel_with_addr.channel.clone();
                let mut client = SnapshotServiceClient::new(channel)
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);
                async move { client.purge_log(tonic::Request::new(req)).await }
            };

            let purge_log_backoff_policy = retry.purge_log;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(closure, purge_log_backoff_policy).await {
                    Ok(response) => {
                        debug!("resquest [peer({:?})] vote response: {:?}", &addr, response);
                        let res = response.into_inner();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!("Received RPC error: {}", e);
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

    /// Leader send snapshot
    async fn install_snapshot(
        &self,
        channel: Channel,
        metadata: SnapshotMetadata,
        data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        retry: &InstallSnapshotBackoffPolicy,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("install_snapshot");
        debug!("Starting snapshot installation");
        let mut client = SnapshotServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        // let mut retry_count = 0;
        let mut last_successful_chunk: u32 = 0;

        // Calculate total chunks from metadata
        let total_chunks = metadata.last_included.map(|id| id.index).unwrap_or(0);

        // Convert the data stream to a retryable stream
        let mut restartable_stream = RestartableStream::new(data_stream);
        // loop {
        // Calculate dynamic timeout based on remaining chunks
        let remaining_chunks = total_chunks.saturating_sub(last_successful_chunk.into());
        debug!(?retry, "install_snapshot retry");
        let dynamic_timeout = Duration::from_millis(retry.per_chunk_timeout_ms * remaining_chunks as u64).clamp(
            Duration::from_millis(retry.min_timeout_ms),
            Duration::from_millis(retry.max_timeout_ms),
        );

        // Start from the last successful position
        restartable_stream.seek(last_successful_chunk).await?;

        // Create a new sending channel
        let (tx, rx) = mpsc::channel(32);
        let request = ReceiverStream::new(rx);

        // Send request and monitor for errors
        let grpc_call = client.install_snapshot(request);
        tokio::pin!(grpc_call);

        // Send chunks with backpressure and timeouts
        let send_task = async {
            let mut last_chunk = last_successful_chunk;
            let mut count = 0;

            while let Some(chunk_result) = restartable_stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        last_chunk = chunk.seq;

                        // Add backpressure waiting
                        if tx.capacity() == 0 {
                            debug!("Backpressure: waiting for receiver");
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }

                        // Send with per-chunk timeout
                        match timeout(Duration::from_millis(retry.per_chunk_timeout_ms), tx.send(chunk)).await {
                            Ok(Ok(_)) => (),
                            Ok(Err(_)) => break, // Channel closed
                            Err(_) => {
                                warn!("Chunk {} timeout", last_chunk);
                                return Err(SnapshotError::TransferFailed);
                            }
                        }

                        count += 1;
                        // Yield every 10 chunks to prevent starvation
                        if count % config.sender_yield_every_n_chunks == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(e) => return Err(SnapshotError::OperationFailed(format!("{:?}", e)).into()),
                }
            }
            Ok(last_chunk)
        };

        // Wait for either send task or gRPC response with dynamic timeout
        debug!(?dynamic_timeout, "send_task, grpc_call");
        let result = timeout(dynamic_timeout, async { tokio::join!(send_task, grpc_call) }).await;

        let (last_sent, grpc_result) = match result {
            Ok((send_res, grpc_res)) => (send_res, grpc_res),
            Err(_) => {
                warn!("Snapshot transfer timed out after {:?}", dynamic_timeout);
                (
                    Err(SnapshotError::TransferTimeout),
                    Err(Status::deadline_exceeded("Snapshot transfer timeout")),
                )
            }
        };

        last_successful_chunk = match last_sent {
            Ok(chunk) => chunk,
            Err(e) => {
                warn!("Snapshot stream failed: {:?}", e);
                return Err(e.into());
            }
        };

        // Process gRPC response
        match grpc_result {
            Ok(response) => {
                let response = response.into_inner();
                if response.success {
                    debug!("Snapshot transferred successfully");
                    return Ok(());
                } else {
                    last_successful_chunk = response.next_chunk;
                    warn!(
                        ?last_successful_chunk,
                        "Follower rejected snapshot at chunk {}", response.next_chunk
                    );
                    return Err(SnapshotError::TransferFailed.into());
                }
            }
            Err(status) => {
                warn!("Snapshot transfer failed: {:?}", status);
                return Err(SnapshotError::TransferFailed.into());
            }
        }

        // Handle retries
        // debug!(%retry_count, "install_snapshot");

        // retry_count += 1;
        // if retry_count > retry.max_retries {
        //     return Err(SnapshotError::TransferFailed.into());
        // }

        //     // Exponential backoff
        //     let delay = std::cmp::min(retry.base_delay_ms * 2u64.pow(retry_count as u32), retry.max_delay_ms);
        //     tokio::time::sleep(Duration::from_millis(delay.into())).await;
        // }
    }

    #[autometrics(objective = API_SLO)]
    async fn join_cluster(
        &self,
        leader_channel: Channel,
        request: JoinRequest,
        retry: BackoffPolicy,
    ) -> Result<JoinResponse> {
        debug!("Initiating cluster join for node {}", request.node_id);

        let closure = move || {
            let channel = leader_channel.clone();
            let mut client = ClusterManagementServiceClient::new(channel)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
            let req = request.clone();
            async move { client.join_cluster(tonic::Request::new(req)).await }
        };

        let response = task_with_timeout_and_exponential_backoff(closure, retry).await?;

        debug!("Join cluster response: {:?}", response);
        Ok(response.into_inner())
    }

    #[autometrics(objective = API_SLO)]
    async fn discover_leader(
        &self,
        voting_members: DashMap<u32, ChannelWithAddress>,
        request: LeaderDiscoveryRequest,
        rpc_enable_compression: bool,
    ) -> Result<Vec<LeaderDiscoveryResponse>> {
        debug!("Starting leader discovery for node {}", request.node_id);

        // Build parallel request streams
        let requests = voting_members
            .iter()
            .map(|entry| {
                let channel = entry.value().channel.clone();
                let request = request.clone();

                // Build future directly using asynchronous blocks
                async move {
                    let mut client = ClusterManagementServiceClient::new(channel);
                    if rpc_enable_compression {
                        client = client
                            .send_compressed(CompressionEncoding::Gzip)
                            .accept_compressed(CompressionEncoding::Gzip);
                    }
                    match client.discover_leader(request).await {
                        Ok(res) => Some(res.into_inner()),
                        Err(e) => {
                            // Error logs can be recorded here
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        // Use join_all to execute in parallel (single-thread cooperative concurrency)
        let results = futures::future::join_all(requests).await;

        // Filter results
        Ok(results.into_iter().flatten().collect())
    }
}

impl GrpcTransport {
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
}
