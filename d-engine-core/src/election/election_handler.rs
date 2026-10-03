use async_trait::async_trait;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;

use super::ElectionCore;
use crate::ElectionError;
use crate::Error;
use crate::Membership;
use crate::NetworkError;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateUpdate;
use crate::Transport;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::TROF;
use crate::cluster::is_majority;
use crate::if_higher_term_found;
use crate::is_target_log_more_recent;
use crate::is_target_log_strictly_more_recent;

#[derive(Clone, Copy)]
enum VoteRpc {
    Vote,
    PreVote,
}
impl VoteRpc {
    /// Label for logs, so a PreVote round is never mistaken for a real election.
    fn label(self) -> &'static str {
        match self {
            VoteRpc::Vote => "vote",
            VoteRpc::PreVote => "pre_vote",
        }
    }

    /// True if the peer's term shows this node is behind and must catch up.
    /// `asked_term` is the term this node asked about: for a real vote its current term,
    /// for PreVote the term it wants to move to.
    fn peer_term_shows_behind(
        self,
        asked_term: u64,
        peer_term: u64,
    ) -> bool {
        match self {
            VoteRpc::Vote => if_higher_term_found(asked_term, peer_term, false),
            // A peer already at or beyond the term we want means that term is taken.
            VoteRpc::PreVote => peer_term >= asked_term,
        }
    }

    /// The rule used by `peer_term_shows_behind`, for logs. Keep it next to the code above.
    fn behind_rule(self) -> &'static str {
        match self {
            VoteRpc::Vote => "peer_term > asked_term",
            VoteRpc::PreVote => "peer_term >= asked_term",
        }
    }
}

#[derive(Clone)]
pub struct ElectionHandler<T: TypeConfig> {
    pub(crate) my_id: u32,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> ElectionCore<T> for ElectionHandler<T>
where
    T: TypeConfig,
{
    async fn broadcast_pre_vote_requests(
        &self,
        term: u64,
        membership: Arc<MOF<T>>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()> {
        debug!("broadcast_pre_vote_requests...");
        self.broadcast_election_requests(
            VoteRpc::PreVote,
            term,
            membership,
            raft_log,
            transport,
            settings,
        )
        .await
    }

    async fn broadcast_vote_requests(
        &self,
        term: u64,
        membership: Arc<MOF<T>>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()> {
        debug!("broadcast_vote_requests...");
        self.broadcast_election_requests(
            VoteRpc::Vote,
            term,
            membership,
            raft_log,
            transport,
            settings,
        )
        .await
    }

    fn handle_pre_vote_request(
        &self,
        request: VoteRequest,
        current_term: u64,
        raft_log: &Arc<ROF<T>>,
        leader_active: bool,
    ) -> VoteResponse {
        let last_logid = raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 });

        // First matching reason wins; None means the PreVote would be granted.
        let deny_reason = if leader_active {
            Some("a live leader is still active")
        } else if request.term <= current_term {
            Some("the asked term is already taken")
        } else if !is_target_log_more_recent(
            last_logid.index,
            last_logid.term,
            request.last_log_index,
            request.last_log_term,
        ) {
            Some("the requester's log is behind ours")
        } else {
            None
        };
        debug!(
            "PreVote from node-{} for term {}: {}",
            request.candidate_id,
            request.term,
            deny_reason.map_or("granted".to_string(), |r| format!("denied, {r}")),
        );

        VoteResponse {
            term: current_term,
            vote_granted: deny_reason.is_none(),
            last_log_index: last_logid.index,
            last_log_term: last_logid.term,
        }
    }

    async fn handle_vote_request(
        &self,
        request: VoteRequest,
        current_term: u64,
        voted_for_option: Option<VotedFor>,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<StateUpdate> {
        debug!("VoteRequest::Received: {:?}", request);
        // A vote only counts in the term it was cast in. Ignore one left over from an older
        // term: hard state persisted by earlier versions can still carry it.
        let voted_for_option = voted_for_option.filter(|v| v.voted_for_term >= current_term);

        let mut new_voted_for = None;
        let mut term_update = None;
        let last_logid = raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 });

        // Check if request term is higher than current term
        let new_voted_for_option = if request.term > current_term {
            term_update = Some(request.term);
            // When updating term, reset voted_for to allow voting in new term
            // But we haven't voted yet, so we'll decide below
            None
        } else {
            voted_for_option
        };

        // Check if we should grant the vote
        let grant_vote = if request.term < current_term {
            // Request term is lower, cannot grant vote
            trace!(
                "[node-{} -> node-{}] Request term is lower, cannot grant vote. VoteRequest = {:?}",
                request.candidate_id, self.my_id, &request
            );

            false
        } else {
            // Request term is >= current term
            // Check log completeness
            if !is_target_log_more_recent(
                last_logid.index,
                last_logid.term,
                request.last_log_index,
                request.last_log_term,
            ) {
                trace!(
                    "node-{}: last_log_index({}(t:{})) -> node-{}: last_log_index({}(t:{}))",
                    request.candidate_id,
                    request.last_log_index,
                    request.last_log_term,
                    self.my_id,
                    last_logid.index,
                    last_logid.term
                );

                false
            } else {
                // Check if already voted for someone else in this term
                if let Some(voted_for) = new_voted_for_option {
                    trace!(
                        "[node-{} -> node-{}] node-{} current vote: {:?}",
                        request.candidate_id, self.my_id, self.my_id, &voted_for
                    );
                    // If already voted for someone else, cannot grant vote unless it's the same
                    // candidate
                    voted_for.voted_for_term == request.term
                        && voted_for.voted_for_id == request.candidate_id
                } else {
                    trace!(
                        "node-{} vote for node-{} successfully!",
                        self.my_id, request.candidate_id,
                    );

                    true
                }
            }
        };

        if grant_vote {
            new_voted_for = Some(VotedFor {
                voted_for_id: request.candidate_id,
                voted_for_term: request.term,
                committed: false,
            });
            trace!(
                "node-{} -> node-{} successfully!",
                request.candidate_id, self.my_id,
            );
        } else {
            trace!(
                "node-{} -> node-{} failed!",
                request.candidate_id, self.my_id,
            );
        }

        Ok(StateUpdate {
            new_voted_for,
            term_update,
        })
    }

    /// The function to check RPC request is leagal or not
    ///
    /// Criterias to check:
    /// - votedFor is null or candidateId
    /// - candidate s log is at least as up-to-date as receiver s log
    /// e.g. { my_id: 2 } request=VoteRequest { term: 3, candidate_id: 1, last_log_index: 2,
    /// last_log_term: 10 } current_term=3 last_log_index=3 last_log_term=8 voted_for_option=None
    fn check_vote_request_is_legal(
        &self,
        request: &VoteRequest,
        current_term: u64,
        last_log_index: u64,
        last_log_term: u64,
        voted_for_option: Option<VotedFor>,
    ) -> bool {
        if current_term > request.term {
            debug!(
                "current_term({:?}) > request.term({:?})",
                current_term, request.term
            );
            return false;
        }

        //step 1: check if I have more logs than the requester
        if !is_target_log_more_recent(
            last_log_index,
            last_log_term,
            request.last_log_index,
            request.last_log_term,
        ) {
            debug!(
                "node_log_is_less_than_requester{:?}, last_log_index={:?}, last_log_term={:?}",
                request, last_log_index, last_log_term
            );
            return false;
        }

        //step 2: check if I have voted for this term
        if voted_for_option.is_some()
            && !self.if_node_could_grant_the_vote_request(request, voted_for_option)
        {
            debug!(
                "node_could_not_grant_the_vote_request: {:?}, voted_for_option={:?}",
                request, &voted_for_option
            );
            return false;
        }

        true
    }
}
impl<T> ElectionHandler<T>
where
    T: TypeConfig,
{
    pub fn new(my_id: u32) -> Self {
        Self {
            my_id,
            _phantom: PhantomData,
        }
    }

    async fn broadcast_election_requests(
        &self,
        rpc: VoteRpc,
        term: u64,
        membership: Arc<MOF<T>>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()> {
        // Single-node cluster: no peers to vote, automatically win election
        if membership.is_single_node_cluster() {
            debug!(
                "Single-node cluster detected (node_id={}): automatically winning election",
                self.my_id
            );
            return Ok(());
        }

        let members = membership.voters();
        if members.is_empty() {
            error!("No voting members found for node {}", self.my_id);
            return Err(ElectionError::NoVotingMemberFound {
                candidate_id: self.my_id,
            }
            .into());
        }

        debug!("Sending vote requests to peers: {:?}", &members);

        let LogId {
            index: last_log_index,
            term: last_log_term,
        } = raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 });
        let request = VoteRequest {
            term,
            candidate_id: self.my_id,
            last_log_index,
            last_log_term,
        };

        // One task per peer. Dropping the JoinSet aborts RPCs still in flight, so a decided
        // election never leaves retry loops behind.
        let mut tasks = JoinSet::new();
        let mut peer_ids = HashSet::new();
        for peer in members {
            let peer_id = peer.id;
            if peer_id == self.my_id || peer_ids.contains(&peer_id) {
                continue; // Skip self and duplicates
            }
            peer_ids.insert(peer_id);

            let transport = transport.clone();
            let membership = membership.clone();
            let retry = settings.retry.clone();
            tasks.spawn(async move {
                match rpc {
                    VoteRpc::Vote => {
                        transport.send_vote_request(peer_id, request, &retry, membership).await
                    }
                    VoteRpc::PreVote => {
                        transport.send_pre_vote_request(peer_id, request, &retry, membership).await
                    }
                }
            });
        }

        let required = peer_ids.len() + 1;
        let mut succeed = 1; // own vote
        let mut pending = peer_ids.len(); // answers still outstanding
        // Tally as responses arrive and decide as soon as the outcome is known. Waiting
        // for every peer lets one dead peer's RPC retries (seconds) hold the Raft loop
        // past our own election timeout, discarding a majority we already won.
        while let Some(joined) = tasks.join_next().await {
            pending -= 1;

            let response = match joined {
                Ok(r) => r,
                Err(e) => {
                    error!("Task failed with error: {:?}", &e);
                    Err(Error::from(NetworkError::TaskFailed(e)))
                }
            };
            match response {
                Ok(vote_response) => {
                    if vote_response.vote_granted {
                        debug!("send_vote_requests_to_peers success!");
                        succeed += 1;
                        if is_majority(succeed, required) {
                            debug!("send_vote_requests receives majority.");
                            return Ok(());
                        }
                    } else {
                        let behind = rpc.peer_term_shows_behind(term, vote_response.term);
                        debug!(
                            "[{}] peer denied: asked_term={}, peer_term={}, rule `{}` => behind={}",
                            rpc.label(),
                            term,
                            vote_response.term,
                            rpc.behind_rule(),
                            behind,
                        );
                        if behind {
                            info!(
                                "[{}] higher term found: peer term {}",
                                rpc.label(),
                                vote_response.term
                            );
                            return Err(ElectionError::HigherTerm(vote_response.term).into());
                        }

                        // A denial is just a missing vote: keep counting. A peer with a
                        // strictly newer log is only worth noting (openraft records it to
                        // delay the next election; raft-rs ignores it).
                        if is_target_log_strictly_more_recent(
                            last_log_index,
                            last_log_term,
                            vote_response.last_log_index,
                            vote_response.last_log_term,
                        ) {
                            info!("[{}] a denying peer has a newer log", rpc.label());
                        }

                        info!("send_vote_requests_to_peers failed!");
                    }
                }
                Err(e) => {
                    // Debug, not error: a single peer RPC failure here is routinely
                    // caused by the peer not being ready yet (e.g. cluster bootstrap
                    // race) and resolves itself on the next election round (#428).
                    info!("send_vote_requests_to_peers error: {:?}", e);
                }
            }

            // Even if every peer still outstanding granted, could a majority be reached?
            // If not the round is lost: return now instead of waiting for silent peers
            // (raft-rs reports `Lost` at this point).
            if !is_majority(succeed + pending, required) {
                return Err(ElectionError::QuorumFailure { required, succeed }.into());
            }
        }

        debug!(
            "failed to receive majority votes: {:?} succeed number = {}",
            &peer_ids, succeed
        );
        Err(ElectionError::QuorumFailure { required, succeed }.into())
    }

    fn if_node_could_grant_the_vote_request(
        &self,
        request: &VoteRequest,
        voted_for_option: Option<VotedFor>,
    ) -> bool {
        if let Some(vf) = voted_for_option {
            debug!(
                "voted_id: {:?}, voted_term: {:?}",
                vf.voted_for_id, vf.voted_for_term
            );

            if vf.voted_for_id == 0 {
                return true;
            }

            if vf.voted_for_term < request.term {
                return true;
            }

            false
        } else {
            true
        }
    }
}

impl<T: TypeConfig> Debug for ElectionHandler<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ElectionHandler").field("my_id", &self.my_id).finish()
    }
}

#[cfg(test)]
#[path = "election_handler_test.rs"]
mod election_handler_test;
