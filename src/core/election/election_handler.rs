use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use autometrics::autometrics;
use tokio::sync::mpsc;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::warn;

use super::ElectionCore;
use crate::alias::ROF;
use crate::alias::TROF;
use crate::cluster::is_majority;
use crate::if_higher_term_found;
use crate::is_target_log_more_recent;
use crate::proto::VoteRequest;
use crate::proto::VotedFor;
use crate::ChannelWithAddressAndRole;
use crate::ElectionError;
use crate::RaftEvent;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateUpdate;
use crate::Transport;
use crate::TypeConfig;
use crate::API_SLO;

#[derive(Clone)]
pub struct ElectionHandler<T: TypeConfig> {
    pub(crate) my_id: u32,
    pub(crate) event_tx: mpsc::Sender<RaftEvent>, //cloned from Raft
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> ElectionCore<T> for ElectionHandler<T>
where T: TypeConfig
{
    #[autometrics(objective = API_SLO)]
    async fn broadcast_vote_requests(
        &self,
        term: u64,
        voting_members: Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()> {
        debug!("broadcast_vote_requests...");

        if voting_members.is_empty() {
            error!("my(id={}) peers is empty.", self.my_id);
            return Err(ElectionError::NoVotingMemberFound {
                candidate_id: self.my_id,
            }
            .into());
        } else {
            debug!("going to send_vote_requests to: {:?}", &voting_members);
        }

        let (last_log_index, last_log_term) = raft_log.get_last_entry_metadata();
        let request = VoteRequest {
            term,
            candidate_id: self.my_id,
            last_log_index,
            last_log_term,
        };

        match transport
            .send_vote_requests(voting_members, request, &settings.retry)
            .await
        {
            Ok(vote_result) => {
                let mut succeed = 1;
                for response in vote_result.responses {
                    match response {
                        Ok(vote_response) => {
                            if vote_response.vote_granted {
                                debug!("send_vote_requests_to_peers success!");
                                succeed += 1;
                            } else {
                                debug!("if_higher_term_found({}, {}, false)", term, vote_response.term,);
                                if if_higher_term_found(term, vote_response.term, false) {
                                    warn!("Higher term found during election phase.");
                                    return Err(ElectionError::HigherTerm(vote_response.term).into());
                                }

                                if is_target_log_more_recent(
                                    last_log_index,
                                    last_log_term,
                                    vote_response.last_log_index,
                                    vote_response.last_log_term,
                                ) {
                                    warn!("More update to date log found in vote response");

                                    return Err(ElectionError::LogConflict {
                                        index: last_log_index,
                                        expected_term: last_log_term,
                                        actual_term: vote_response.last_log_term,
                                    }
                                    .into());
                                }

                                warn!("send_vote_requests_to_peers failed!");
                            }
                        }
                        Err(e) => {
                            error!("send_vote_requests_to_peers error: {:?}", e);
                        }
                    }
                }
                debug!(
                    "send_vote_requests to: {:?} with succeed number = {}",
                    &vote_result.peer_ids, succeed
                );

                let required = vote_result.peer_ids.len() + 1;
                if !vote_result.peer_ids.is_empty() && is_majority(succeed, required) {
                    debug!("send_vote_requests receives majority.");
                    return Ok(());
                } else {
                    debug!("failed to receive majority votes.");
                    return Err(ElectionError::QuorumFailure { required, succeed }.into());
                }
            }
            Err(e) => {
                error!("broadcast_vote_requests encountered an error: {:?}", e);
                return Err(e);
            }
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
        let mut new_voted_for = None;
        let mut term_update = None;
        let (last_index, last_term) = raft_log.get_last_entry_metadata();

        if self.check_vote_request_is_legal(&request, current_term, last_index, last_term, voted_for_option) {
            debug!("switch to follower");
            let term = request.term;

            // 1. Update term
            term_update = Some(term);

            // 2. update vote for
            debug!(
                "updated my voted for: target node: {:?} with term:{:?}",
                request.candidate_id, term
            );
            new_voted_for = Some(VotedFor {
                voted_for_id: request.candidate_id,
                voted_for_term: term,
            });
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
    #[tracing::instrument]
    fn check_vote_request_is_legal(
        &self,
        request: &VoteRequest,
        current_term: u64,
        last_log_index: u64,
        last_log_term: u64,
        voted_for_option: Option<VotedFor>,
    ) -> bool {
        if current_term > request.term {
            debug!("current_term({:?}) > request.term({:?})", current_term, request.term);
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
        if voted_for_option.is_some() && !self.if_node_could_grant_the_vote_request(request, voted_for_option) {
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
where T: TypeConfig
{
    pub(crate) fn new(
        my_id: u32,
        event_tx: mpsc::Sender<RaftEvent>,
    ) -> Self {
        Self {
            my_id,
            event_tx,
            _phantom: PhantomData,
        }
    }

    /// logOk == \/ m.mlastLogTerm > LastTerm(log[i])
    ///          \/ /\ m.mlastLogTerm = LastTerm(log[i])
    ///             /\ m.mlastLogIndex >= Len(log[i])
    #[autometrics(objective = API_SLO)]
    fn if_node_log_is_less_than_requester(
        &self,
        request_last_log_index: u64,
        request_last_log_term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> bool {
        (request_last_log_term > last_log_term)
            || (request_last_log_term == last_log_term && request_last_log_index >= last_log_index)
    }
    #[autometrics(objective = API_SLO)]
    fn if_node_could_grant_the_vote_request(
        &self,
        request: &VoteRequest,
        voted_for_option: Option<VotedFor>,
    ) -> bool {
        if let Some(vf) = voted_for_option {
            debug!("voted_id: {:?}, voted_term: {:?}", vf.voted_for_id, vf.voted_for_term);

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
