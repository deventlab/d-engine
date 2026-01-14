//! Raft leader election protocol implementation (Section 5.2)
//!
//! Handles leader election mechanics including:
//! - Vote request broadcasting
//! - Candidate vote collection
//! - Voter eligibility validation
mod election_handler;
pub use election_handler::*;

#[cfg(test)]
mod election_handler_test;

use std::sync::Arc;

use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VotedFor;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;
use tonic::async_trait;

use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::ROF;
use crate::alias::TROF;

/// State transition data for election outcomes
#[derive(Debug)]
pub struct StateUpdate {
    /// Updated term if election term changed
    pub term_update: Option<u64>,
    /// New vote assignment if granted
    pub new_voted_for: Option<VotedFor>,
}

#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait ElectionCore<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    /// Sends vote requests to all voting members. Returns Ok() if majority
    /// votes are received, otherwise returns Err. Initiates RPC calls via
    /// transport and evaluates collected responses.
    ///
    /// A vote can be granted only if all the following conditions are met:
    /// - The requests term is greater than the current_term.
    /// - The candidates log is sufficiently up-to-date.
    /// - The current node has not voted in the current term or has already
    /// voted for the candidate.
    async fn broadcast_vote_requests(
        &self,
        term: u64,
        membership: Arc<MOF<T>>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()>;

    /// Processes incoming vote requests: validates request legality via
    /// check_vote_request_is_legal, updates node state if valid, triggers
    /// role transition to Follower when granting vote.
    ///
    /// If there is a state update, we delegate the update to the parent
    /// RoleState instead of updating it within this function.
    async fn handle_vote_request(
        &self,
        request: VoteRequest,
        current_term: u64,
        voted_for_option: Option<VotedFor>,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<StateUpdate>;

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
