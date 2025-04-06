//! Centerialized all RPC client operations will make unit test eaiser.
//! We also want to refactor all the APIs based its similar parttern.
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use autometrics::autometrics;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use tokio::task;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;

use crate::cluster::is_majority;
use crate::grpc::rpc_service::rpc_service_client::RpcServiceClient;
use crate::grpc::rpc_service::AppendEntriesRequest;
use crate::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::grpc::rpc_service::VoteRequest;
use crate::if_higher_term_found;
use crate::is_learner;
use crate::is_target_log_more_recent;
use crate::task_with_timeout_and_exponential_backoff;
use crate::AppendResults;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::Error;
use crate::NewLeaderInfo;
use crate::PeerUpdate;
use crate::Result;
use crate::RetryPolicies;
use crate::Transport;
use crate::API_SLO;

#[derive(Debug)]
pub struct GrpcTransport {
    pub(crate) my_id: u32,
}

#[async_trait]
impl Transport for GrpcTransport {
    #[autometrics(objective = API_SLO)]
    async fn send_cluster_membership_requests(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: ClusteMembershipChangeRequest,
        retry: &RetryPolicies,
    ) -> Result<bool> {
        debug!("-------- send cluster_membership requests --------");
        if peers.is_empty() {
            warn!("peers is empty.");
            return Ok(false);
        }
        let my_term = req.term;

        let mut tasks = FuturesUnordered::new();
        let mut peer_ids = Vec::new();
        for peer in peers {
            let peer_id = peer.id;
            peer_ids.push(peer_id);
            // let peer_channel_with_addr = ;
            let peer_role = peer.role;

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

            let max_retries = retry.membership.max_retries;
            let base_delay_ms = retry.membership.base_delay_ms;
            let timeout_ms = retry.membership.timeout_ms;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(
                    closure,
                    max_retries,
                    Duration::from_millis(base_delay_ms),
                    Duration::from_millis(timeout_ms),
                )
                .await
                {
                    Ok(response) => {
                        debug!("sync_cluster_conf response: {:?}", response);
                        let res = response.into_inner();

                        //special case for this func
                        if if_higher_term_found(my_term, res.term, is_learner(peer_role)) {
                            return Err(crate::Error::FoundNewLeaderError(NewLeaderInfo {
                                term: res.term,
                                leader_id: res.id,
                            }));
                        }

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

        let mut succeed = 0;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(response)) => {
                    if response.success {
                        debug!("send_cluster_membership_requests success!");
                        succeed += 1;
                    }
                }
                Ok(Err(e)) => {
                    error!("send_cluster_membership_requests error: {:?}", e);
                    return Err(e);
                }
                Err(e) => {
                    error!("[send_cluster_membership_requests] Task failed with error: {:?}", e);
                }
            }
        }

        if peer_ids.len() == succeed {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[tracing::instrument]
    async fn send_append_requests(
        &self,
        leader_current_term: u64,
        requests_with_peer_address: Vec<(u32, ChannelWithAddress, AppendEntriesRequest)>,
        retry: &RetryPolicies,
    ) -> Result<AppendResults> {
        debug!("-------- send append entries requests --------");
        if requests_with_peer_address.is_empty() {
            warn!("requests_with_peer_address is empty.");
            return Err(Error::AppendEntriesNoPeerFound);
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

            let max_retries = retry.append_entries.max_retries;
            let base_delay_ms = retry.append_entries.base_delay_ms;
            let timeout_ms = retry.append_entries.timeout_ms;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(
                    closure,
                    max_retries,
                    Duration::from_millis(base_delay_ms),
                    Duration::from_millis(timeout_ms),
                )
                .await
                {
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

        let mut peer_updates = HashMap::new();
        let mut successes = 1;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(response)) => {
                    let peer_id = response.id;
                    debug!("recv append res from peer(id: {}): {:?}", &peer_id, response);
                    let peer_match_index;
                    let peer_next_index;
                    if !response.success {
                        if if_higher_term_found(leader_current_term, response.term, false) {
                            error!("[send_append_requests] new leader found.");
                            return Err(crate::Error::FoundNewLeaderError(NewLeaderInfo {
                                term: response.term,
                                leader_id: response.id,
                            }));
                        }
                        //bugfix: #112
                        peer_match_index = response.match_index;
                        debug!("follower's log does not match with prev index and prev term. So now we change its next_index to it returned match_index({})", peer_match_index);
                        peer_next_index = peer_match_index;
                    } else {
                        debug!("[send_append_requests] success!");
                        successes += 1;
                        peer_match_index = response.match_index;
                        peer_next_index = peer_match_index + 1;
                    }
                    debug!(
                        "[send_append_requests] update peer(id={}), match_index = {}, next_inde = {}: ",
                        peer_id, peer_match_index, peer_next_index
                    );

                    let update = PeerUpdate {
                        match_index: peer_match_index,
                        next_index: peer_next_index,
                        success: response.success,
                    };
                    peer_updates.insert(peer_id, update);
                }
                Ok(Err(e)) => {
                    error!("[send_append_requests] error: {:?}", e);
                }
                Err(e) => {
                    error!("[send_append_requests] Task failed with error: {:?}", e);
                }
            }
        }

        debug!(
            "send_append_requests to: {:?} with succeed number = {}",
            &peer_ids, successes
        );

        let commit_quorum_achieved = is_majority(successes, peer_ids.len() + 1);

        Ok(AppendResults {
            commit_quorum_achieved,
            peer_updates,
        })
    }

    #[autometrics(objective = API_SLO)]
    async fn send_vote_requests(
        &self,
        peers: Vec<ChannelWithAddressAndRole>,
        req: VoteRequest,
        retry: &RetryPolicies,
    ) -> Result<bool> {
        debug!("-------- send vote request --------");
        if peers.is_empty() {
            warn!("peers is empty.");
            return Ok(false);
        }
        let mut tasks = FuturesUnordered::new();

        // make sure the collection items are unique
        let mut peer_ids = HashSet::new();
        let my_term = req.term;
        let my_last_log_index = req.last_log_index;
        let my_last_log_term = req.last_log_term;

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

            let rpc_election_max_retries = retry.election.max_retries;
            let rpc_election_exponential_backoff_duration_in_ms = retry.election.base_delay_ms;
            let rpc_election_timeout_duration_in_ms = retry.election.timeout_ms;
            let task_handle = task::spawn(async move {
                match task_with_timeout_and_exponential_backoff(
                    closure,
                    rpc_election_max_retries,
                    Duration::from_millis(rpc_election_exponential_backoff_duration_in_ms),
                    Duration::from_millis(rpc_election_timeout_duration_in_ms),
                )
                .await
                {
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

        let mut succeed = 1;

        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(vote_response)) => {
                    if vote_response.vote_granted {
                        debug!("send_vote_requests_to_peers success!");
                        succeed += 1;
                    } else {
                        if if_higher_term_found(my_term, vote_response.term, false) {
                            warn!("Higher term found during election phase.");
                            return Err(crate::Error::HigherTermFoundError(vote_response.term));
                        }

                        if is_target_log_more_recent(
                            my_last_log_index,
                            my_last_log_term,
                            vote_response.last_log_index,
                            vote_response.last_log_term,
                        ) {
                            warn!("More update to date log found in vote response");
                            return Err(crate::Error::HigherTermFoundError(vote_response.term));
                        }

                        warn!("send_vote_requests_to_peers failed!");
                    }
                }
                Ok(Err(e)) => {
                    error!("send_vote_requests_to_peers error: {:?}", e);
                }
                Err(e) => {
                    error!("Task failed with error: {:?}", e);
                }
            }
        }

        debug!(
            "send_vote_requests to: {:?} with succeed number = {}",
            &peer_ids, succeed
        );

        if !peer_ids.is_empty() && is_majority(succeed, peer_ids.len() + 1) {
            debug!("send_vote_requests receives majority.");
            Ok(true)
        } else {
            debug!("send_vote_requests didn't receives majority.");
            Ok(false)
        }
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
