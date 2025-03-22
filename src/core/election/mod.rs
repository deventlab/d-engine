mod election_handler;
pub use election_handler::*;

#[cfg(test)]
mod election_handler_test;

///--------------------------------------
/// Trait Definition
#[cfg(test)]
use mockall::automock;
use std::sync::Arc;
use tonic::{async_trait, };

use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{VoteRequest, VotedFor},
    ChannelWithAddressAndRole, Result, Settings, TypeConfig,
};

#[derive(Debug)]
pub struct StateUpdate {
    // New votes, if there is
    pub new_voted_for: Option<VotedFor>,
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait ElectionCore<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    /// Sends vote requests to all voting members. Returns Ok() if majority votes are received,
    /// otherwise returns Err. Initiates RPC calls via transport and evaluates collected responses.
    async fn broadcast_vote_requests(
        &self,
        term: u64,
        voting_members: Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<Settings>,
    ) -> Result<()>;

    /// Processes incoming vote requests: validates request legality via check_vote_request_is_legal,
    /// updates node state if valid, triggers role transition to Follower when granting vote.
    ///
    /// If there is a state update, we delegate the update to the parent RoleState
    /// instead of updating it within this function.
    async fn handle_vote_request(
        &self,
        request: VoteRequest,
        current_term: u64,
        voted_for_option: Option<VotedFor>,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<Option<VotedFor>>;

    /// Validates vote request against Raft rules:
    /// 1. Requester's term must be ≥ current term
    /// 2. Requester's log must be at least as recent as local log
    /// 3. Node hasn't voted for another candidate in current term
    fn check_vote_request_is_legal(
        &self,
        request: &VoteRequest,
        current_term: u64,
        last_log_index: u64,
        last_log_term: u64,
        voted_for_option: Option<VotedFor>,
    ) -> bool;
}
