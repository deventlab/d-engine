//! Raft leader election protocol implementation (Section 5.2)
//!
//! Handles leader election mechanics including:
//! - Vote request broadcasting
//! - Candidate vote collection
//! - Voter eligibility validation
mod election_handler;
pub use election_handler::*;

use async_trait::async_trait;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VotedFor;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;
use std::sync::Arc;

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
    /// Sends PreVote requests ("would you vote for me at `term`?") to all voting members.
    /// Returns Ok() if a majority would grant. Changes no term/vote state on either side.
    /// `term` is the hypothetical next term (current_term + 1), not yet persisted.
    async fn broadcast_pre_vote_requests(
        &self,
        term: u64,
        membership: Arc<MOF<T>>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<RaftNodeConfig>,
    ) -> Result<()>;

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

    /// Answers a PreVote ("would you vote for me at `request.term`?") without changing any
    /// state. It returns the response itself, not a `StateUpdate`: nothing is recorded, so
    /// no term, vote or timer can be touched through this call.
    ///
    /// Denies when any of these holds:
    /// - `leader_active`: the caller still has evidence of a live leader (Follower: recent
    ///   leader contact; Leader: recent quorum ACK; Candidate: false).
    /// - `request.term <= current_term`: that term is already taken.
    /// - The requester's log is not at least as up-to-date as ours (Raft §5.4.1).
    ///
    /// The response carries our own `current_term` and last log id, so a requester that is
    /// behind can catch up from a denial.
    fn handle_pre_vote_request(
        &self,
        request: VoteRequest,
        current_term: u64,
        raft_log: &Arc<ROF<T>>,
        leader_active: bool,
    ) -> d_engine_proto::server::election::VoteResponse;

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
