//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.

use std::collections::HashSet;

use autometrics::autometrics;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use tokio::task;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::proto::rpc_service_client::RpcServiceClient;
use crate::proto::AppendEntriesRequest;
use crate::proto::ClusteMembershipChangeRequest;
use crate::proto::PurgeLogRequest;
use crate::proto::PurgeLogResponse;
use crate::proto::VoteRequest;
use crate::task_with_timeout_and_exponential_backoff;
use crate::AppendResult;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::ClusterUpdateResult;
use crate::Error;
use crate::NetworkError;
use crate::Result;
use crate::RetryPolicies;
use crate::Transport;
use crate::VoteResult;
use crate::API_SLO;

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
        req: ClusteMembershipChangeRequest,
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
                let mut client = RpcServiceClient::new(channel)
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
                let mut client = RpcServiceClient::new(channel)
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
                let mut client = RpcServiceClient::new(channel)
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

    async fn send_purge_request(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: PurgeLogRequest,
        retry: &RetryPolicies,
    ) -> Result<PurgeLogResponse> {
        Ok(PurgeLogResponse {
            term: todo!(),
            success: false,
            last_purged: todo!(),
        })
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
