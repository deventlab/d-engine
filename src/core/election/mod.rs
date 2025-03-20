mod election_handler;
pub use election_handler::*;

#[cfg(test)]
mod election_handler_test;

///--------------------------------------
/// Trait Definition
#[cfg(test)]
use mockall::automock;
use std::sync::Arc;
use tonic::{async_trait, Status};

use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{VoteRequest, VoteResponse, VotedFor},
    ChannelWithAddressAndRole, MaybeCloneOneshotSender, Result, Settings, TypeConfig,
};

#[cfg_attr(test, automock)]
#[async_trait]
pub trait ElectionCore<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    async fn broadcast_vote_requests(
        &self,
        term: u64,
        voting_members: Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        settings: &Arc<Settings>,
    ) -> Result<()>;

    async fn handle_vote_request(
        &self,
        request: VoteRequest,
        resp_tx: MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
        current_term: u64,
        voted_for_option: Option<VotedFor>,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<()>;

    fn check_vote_request_is_legal(
        &self,
        request: &VoteRequest,
        current_term: u64,
        last_log_index: u64,
        last_log_term: u64,
        voted_for_option: Option<VotedFor>,
    ) -> bool;
}
