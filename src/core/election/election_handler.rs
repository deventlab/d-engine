use std::{marker::PhantomData, sync::Arc};

use autometrics::autometrics;
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};
use tonic::{async_trait, Status};

use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{VoteRequest, VoteResponse, VotedFor},
    ChannelWithAddressAndRole, Error, MaybeCloneOneshotSender, RaftEvent, RaftLog, Result,
    RoleEvent, Settings, Transport, TypeConfig, API_SLO,
};

use super::ElectionCore;

#[derive(Clone)]
pub struct ElectionHandler<T: TypeConfig> {
    pub my_id: u32,
    pub role_tx: mpsc::UnboundedSender<RoleEvent>, //cloned from Raft
    pub event_tx: mpsc::Sender<RaftEvent>,         //cloned from Raft
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> ElectionCore<T> for ElectionHandler<T>
where
    T: TypeConfig,
{
    /// 1. Term Increment: A candidate increments its term immediately when it starts an election. This ensures that the candidate s term is higher than any previously known term, reflecting a new election round.
    /// 2.	Voting for Itself: After incrementing its term, the candidate votes for itself before sending RequestVote RPCs to its peers.
    #[autometrics(objective = API_SLO)]
    async fn broadcast_vote_requests(
        &self,
        term: u64,
        voting_members: Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<Settings>,
    ) -> Result<()> {
        debug!("broadcast_vote_requests...");

        // let peers = self
        //     .cluster_membership_controller
        //     .get_followers_candidates_channel_and_role();

        if voting_members.len() < 1 {
            error!("my(id={}) peers is empty.", self.my_id);
            return Err(Error::ElectionFailed(format!(
                "my(id={}) peers is empty.",
                self.my_id
            )));
        } else {
            debug!("going to send_vote_requests to: {:?}", &voting_members);
        }

        let mut last_log_index = 0;
        let mut last_log_term = 0;
        if let Some(last) = raft_log.last() {
            last_log_index = last.index;
            last_log_term = last.term;
        }
        let request = VoteRequest {
            term,
            candidate_id: self.my_id,
            last_log_index,
            last_log_term,
        };

        match transport
            .send_vote_requests(voting_members, request, &settings.raft_settings)
            .await
        {
            Ok(is_won) => {
                debug!("Received peers' vote result: {}", is_won);

                if is_won {
                    return Ok(());
                } else {
                    debug!("failed to receive majority votes.");
                    return Err(Error::ElectionFailed(format!(
                        "failed to receive majority votes."
                    )));
                }
            }
            Err(e) => {
                error!("RPC request encountered an error: {:?}", e);
                return Err(Error::ElectionFailed(format!(
                    "RPC request encountered an error: {:?}",
                    e
                )));
            }
        }
    }

    async fn handle_vote_request(
        &self,
        request: VoteRequest,
        resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
        current_term: u64,
        voted_for_option: Option<VotedFor>,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<()> {
        debug!("VoteRequest::Received: {:?}", request);
        let mut vote_granted = false;

        let mut last_index = 0;
        let mut last_term = 0;
        if let Some(last) = raft_log.last() {
            last_index = last.index;
            last_term = last.term;
            debug!("last_index: {:?}, last_term: {:?}", last_index, last_term);
        }

        if self.check_vote_request_is_legal(
            &request,
            current_term,
            last_index,
            last_term,
            voted_for_option,
        ) {
            debug!("switch to follower");

            //1. switch to follower
            // self.become_follower().await;
            if let Err(e) = self.role_tx.send(RoleEvent::BecomeFollower(None)) {
                error!(
                    "self.my_role_change_event_sender.send(RaftRole::Follower) failed: {:?}",
                    e
                );
            }

            //2. update vote for
            // state.update_vote_for(req);
            let term = request.term;
            debug!(
                "updated my voted for: target node: {:?} with term:{:?}",
                request.candidate_id, term
            );

            if let Err(e) = self.role_tx.send(RoleEvent::UpdateVote {
                voted_for: VotedFor {
                    voted_for_id: request.candidate_id,
                    voted_for_term: term,
                },
            }) {
                error!("send RoleEvent::UpdateVote failed: {:?}", e);
            }

            vote_granted = true;
        }

        let response = VoteResponse {
            term: current_term,
            vote_granted,
        };
        debug!(
            "Response to: {:?}. vote request response: {:?}",
            request.candidate_id, response
        );

        if let Err(e) = resp_tx.send(Ok(response)) {
            error!("resp_tx.send VoteResponse: {:?}", e);
        }
        Ok(())
    }

    /// The function to check RPC request is leagal or not
    ///
    /// Criterias to check:
    /// - votedFor is null or candidateId
    /// - candidate s log is at least as up-to-date as receiver s log
    #[autometrics(objective = API_SLO)]
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
        if !self.if_node_log_is_less_than_requester(request, last_log_index, last_log_term) {
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
    pub fn new(
        my_id: u32,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        event_tx: mpsc::Sender<RaftEvent>,
    ) -> Self {
        Self {
            my_id,
            role_tx,
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
        request: &VoteRequest,
        last_log_index: u64,
        last_log_term: u64,
    ) -> bool {
        if request.last_log_term > last_log_term {
            return true;
        }

        if request.last_log_term == last_log_term && request.last_log_index >= last_log_index {
            return true;
        }

        false
    }
    #[autometrics(objective = API_SLO)]
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
