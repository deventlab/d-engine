//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.

use async_trait::async_trait;
use d_engine_core::BackgroundSnapshotTransfer;
use d_engine_core::BackoffPolicy;
use d_engine_core::ClusterUpdateResult;
use d_engine_core::ConnectionType;
use d_engine_core::Error;
use d_engine_core::Membership;
use d_engine_core::NetworkError;
use d_engine_core::ReplicationStream;
use d_engine_core::Result;
use d_engine_core::RetryPolicies;
use d_engine_core::SnapshotConfig;
use d_engine_core::StateMachineHandler;
use d_engine_core::Transport;
use d_engine_core::TypeConfig;
use d_engine_core::alias::MOF;
use d_engine_core::alias::SMHOF;
use d_engine_core::grpc_task_with_timeout_and_exponential_backoff;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::cluster_management_service_client::ClusterManagementServiceClient;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::raft_election_service_client::RaftElectionServiceClient;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::raft_replication_service_client::RaftReplicationServiceClient;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tracing::debug;
use tracing::error;
use tracing::warn;

pub struct GrpcTransport<T>
where
    T: TypeConfig,
{
    pub(crate) my_id: u32,

    /// Fire-and-forget channel to report peer stream failures for zombie detection.
    /// `None` in tests that construct via `GrpcTransport::new(node_id)`.
    peer_failure_tx: Option<mpsc::Sender<u32>>,

    /// Fire-and-forget channel to report peer stream success for health recovery.
    /// Resets the failure counter so transient errors don't accumulate into false zombies.
    /// `None` in tests that construct via `GrpcTransport::new(node_id)`.
    peer_success_tx: Option<mpsc::Sender<u32>>,

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

    async fn send_vote_request(
        &self,
        peer_id: u32,
        request: VoteRequest,
        retry: &RetryPolicies,
        membership: Arc<MOF<T>>,
    ) -> Result<VoteResponse> {
        // Real-time connection fetch for control operations (same pattern as join_cluster).
        let channel = membership
            .get_peer_channel(peer_id, ConnectionType::Control)
            .await
            .ok_or(NetworkError::PeerConnectionNotFound(peer_id))?;

        let req_clone = request;
        let closure = move || {
            let channel = channel.clone();
            let mut client = RaftElectionServiceClient::new(channel)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
            async move { client.request_vote(tonic::Request::new(req_clone)).await }
        };
        let policy = retry.election;
        let my_id = self.my_id;
        match grpc_task_with_timeout_and_exponential_backoff("request_vote", closure, policy).await
        {
            Ok(response) => {
                let res = response.into_inner();
                debug!(
                    "[send_vote_request | {my_id}->{peer_id}] vote response: {:?}",
                    res
                );
                Ok(res)
            }
            Err(e) => {
                // Debug, not error: a single peer RPC failure here is routinely
                // caused by the peer not being ready yet (e.g. cluster bootstrap
                // race) and resolves itself on the next election round (#428).
                debug!(
                    "[send_vote_request | {my_id}->{peer_id}] Received RPC error: {}",
                    e
                );
                Err(e)
            }
        }
    }

    async fn join_cluster(
        &self,
        leader_id: u32,
        request: d_engine_proto::server::cluster::JoinRequest,
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

        let member_ids: Vec<_> = membership.voters().iter().map(|m| m.id).collect();

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
    async fn send_snapshot(
        &self,
        peer_id: u32,
        metadata: d_engine_proto::server::storage::SnapshotMetadata,
        leader_term: u64,
        state_machine_handler: Arc<SMHOF<T>>,
        membership: Arc<MOF<T>>,
        config: SnapshotConfig,
    ) -> Result<()> {
        debug!(%peer_id, "Pushing snapshot to lagging peer");

        let bulk_channel = membership
            .get_peer_channel(peer_id, ConnectionType::Bulk)
            .await
            .ok_or(NetworkError::PeerConnectionNotFound(peer_id))?;

        let data_stream = state_machine_handler
            .load_snapshot_data(metadata, leader_term)
            .await
            .map_err(|e| {
                error!(%peer_id, "Failed to load snapshot data: {:?}", e);
                e
            })?;

        BackgroundSnapshotTransfer::<T>::run_push_transfer(
            peer_id,
            data_stream,
            bulk_channel,
            config,
        )
        .await
    }

    async fn open_replication_stream(
        &self,
        peer_id: u32,
        membership: Arc<MOF<T>>,
        compress: bool,
    ) -> Result<ReplicationStream> {
        debug!(%peer_id, "Opening persistent bidi replication stream");

        let channel = membership
            .get_peer_channel(peer_id, ConnectionType::Data)
            .await
            .ok_or(NetworkError::PeerConnectionNotFound(peer_id))?;

        // Bounded send channel (capacity 128) provides natural backpressure to the Raft loop.
        let (req_tx, req_rx) = mpsc::channel::<AppendEntriesRequest>(128);
        let req_stream = ReceiverStream::new(req_rx);

        let mut client = RaftReplicationServiceClient::new(channel);
        if compress {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }
        let response = match client.stream_append_entries(tonic::Request::new(req_stream)).await {
            Ok(r) => {
                // TCP handshake confirmed: reset failure counter to prevent false zombie signals.
                if let Some(tx) = &self.peer_success_tx {
                    let _ = tx.try_send(peer_id);
                }
                r
            }
            Err(e) => {
                // Actual TCP failure: notify health monitor so zombie detection can fire.
                if let Some(tx) = &self.peer_failure_tx {
                    let _ = tx.try_send(peer_id);
                }
                return Err(NetworkError::TonicStatusError(Box::new(e)).into());
            }
        };

        let receiver = response.into_inner().boxed();

        Ok(ReplicationStream {
            sender: req_tx,
            receiver,
        })
    }
}

impl<T> GrpcTransport<T>
where
    T: TypeConfig,
{
    #[cfg(test)]
    pub(crate) fn new(node_id: u32) -> Self {
        Self {
            my_id: node_id,
            peer_failure_tx: None,
            peer_success_tx: None,
            _marker: PhantomData,
        }
    }

    /// Constructs a transport wired to peer-failure and peer-success channels.
    ///
    /// `peer_failure_tx` is fired when `stream_append_entries` fails (enables zombie detection).
    /// `peer_success_tx` is fired on success (resets failure counter, prevents false zombies).
    /// Use in production builds via `NodeBuilder`; tests use `new()`.
    pub(crate) fn new_with_channels(
        node_id: u32,
        peer_failure_tx: mpsc::Sender<u32>,
        peer_success_tx: mpsc::Sender<u32>,
    ) -> Self {
        Self {
            my_id: node_id,
            peer_failure_tx: Some(peer_failure_tx),
            peer_success_tx: Some(peer_success_tx),
            _marker: PhantomData,
        }
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
