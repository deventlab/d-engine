use std::sync::Arc;

use log::debug;
use log::error;
use log::warn;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::Status;

use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use crate::alias::POF;
use crate::cluster::error;
use crate::grpc::rpc_service::AppendEntriesRequest;
use crate::grpc::rpc_service::AppendEntriesResponse;
use crate::grpc::rpc_service::VoteRequest;
use crate::grpc::rpc_service::VoteResponse;
use crate::grpc::rpc_service::VotedFor;
use crate::AppendResponseWithUpdates;
use crate::ElectionCore;
use crate::Error;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::RaftContext;
use crate::RaftEvent;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::Result;
use crate::RoleEvent;
use crate::TypeConfig;

#[async_trait]
pub trait RaftRoleState: Send + Sync + 'static {
    type T: TypeConfig;

    //--- For sharing state behaviors
    fn shared_state(&self) -> &SharedState;
    fn shared_state_mut(&mut self) -> &mut SharedState;
    fn node_id(&self) -> u32 {
        self.shared_state().node_id
    }
    // fn role(&self) -> i32;

    // Leader states
    fn next_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        warn!("next_index NotLeader error");
        None
    }
    fn update_next_index(
        &mut self,
        node_id: u32,
        new_next_id: u64,
    ) -> Result<()> {
        warn!("update_next_index NotLeader error");
        Err(Error::NotLeader)
    }

    fn prev_log_index(
        &self,
        follower_id: u32,
    ) -> Option<u64> {
        warn!("update_next_index NotLeader error");
        None
    }

    fn match_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        warn!("match_index NotLeader error");
        None
    }
    fn update_match_index(
        &mut self,
        node_id: u32,
        new_match_id: u64,
    ) -> Result<()> {
        warn!("update_match_index NotLeader error");
        Err(Error::NotLeader)
    }
    fn init_peers_next_index_and_match_index(
        &mut self,
        _last_entry_id: u64,
        _peer_ids: Vec<u32>,
    ) -> Result<()> {
        warn!("init_peers_next_index_and_match_index NotLeader error");
        Err(Error::NotLeader)
    }
    fn noop_log_id(&self) -> Result<Option<u64>> {
        warn!("noop_log_id NotLeader error");
        Err(Error::NotLeader)
    }

    fn is_follower(&self) -> bool {
        false
    }
    fn is_candidate(&self) -> bool {
        false
    }
    fn is_leader(&self) -> bool {
        false
    }
    fn is_learner(&self) -> bool {
        false
    }

    fn become_leader(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_leader Illegal");
        Err(Error::Illegal)
    }
    fn become_candidate(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_candidate Illegal");
        Err(Error::Illegal)
    }
    fn become_follower(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_follower Illegal");
        Err(Error::Illegal)
    }
    fn become_learner(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_learner Illegal");
        Err(Error::Illegal)
    }

    fn step_down(&self) -> Result<RaftRole<Self::T>> {
        warn!("step_down failed");
        Err(Error::Illegal)
    }

    #[cfg(test)]
    fn decr_next_index(
        &mut self,
        node_id: u32,
    ) -> Result<()> {
        warn!("decr_next_index NotLeader error");
        Err(Error::NotLeader)
    }

    //--- Shared States
    fn current_term(&self) -> u64 {
        self.shared_state().current_term()
    }
    fn update_current_term(
        &mut self,
        term: u64,
    ) {
        self.shared_state_mut().update_current_term(term)
    }
    fn increase_current_term(&mut self) {
        self.shared_state_mut().increase_current_term()
    }
    fn commit_index(&self) -> u64 {
        self.shared_state().commit_index
    }

    fn update_commit_index(
        &mut self,
        new_commit_index: u64,
    ) -> Result<()> {
        if self.commit_index() != new_commit_index {
            debug!("update_commit_index to: {:?}", new_commit_index);
            self.shared_state_mut().commit_index = new_commit_index;
        }
        Ok(())
    }

    fn update_commit_index_with_signal(
        &mut self,
        new_commit_index: u64,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        if let Err(e) = self.update_commit_index(new_commit_index) {
            error!("Follower::update_commit_index: {:?}", e);
            return Err(e);
        }
        debug!("send(RoleEvent::NotifyNewCommitIndex");
        role_tx
            .send(RoleEvent::NotifyNewCommitIndex { new_commit_index })
            .map_err(|e| {
                let error_str = format!("{:?}", e);
                error!("Failed to send: {}", error_str);
                Error::TokioSendStatusError(error_str)
            })?;

        Ok(())
    }

    fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.shared_state().voted_for()
    }
    fn reset_voted_for(&mut self) -> Result<()> {
        self.shared_state_mut().reset_voted_for()
    }
    fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<()> {
        self.shared_state_mut().update_voted_for(voted_for)
    }

    //--- Timer related ---
    fn next_deadline(&self) -> Instant;
    fn is_timer_expired(&self) -> bool;

    fn reset_timer(&mut self);

    async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        event_tx: &mpsc::Sender<RaftEvent>,
        peer_channels: Arc<POF<Self::T>>,
        ctx: &RaftContext<Self::T>,
    ) -> Result<()>;

    async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        peer_channels: Arc<POF<Self::T>>,
        ctx: &RaftContext<Self::T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()>;

    async fn handle_append_entries_request_workflow(
        &mut self,
        append_entries_request: AppendEntriesRequest,
        sender: MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>,
        ctx: &RaftContext<Self::T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        state_snapshot: &StateSnapshot,
        last_applied: u64,
    ) -> Result<()> {
        debug!(
            "handle_raft_event::RaftEvent::AppendEntries: {:?}",
            &append_entries_request
        );
        self.reset_timer();

        // Init response with success is false
        let raft_log_last_index = ctx.raft_log.last_entry_id();
        let mut response = AppendEntriesResponse {
            id: self.node_id(),
            term: self.current_term(),
            success: false,
            match_index: raft_log_last_index,
        };

        let my_term = self.current_term();
        if my_term > append_entries_request.term {
            debug!("AppendEntriesResponse: {:?}", response);

            sender.send(Ok(response)).map_err(|e| {
                let error_str = format!("{:?}", e);
                error!("Failed to send: {}", error_str);
                Error::TokioSendStatusError(error_str)
            })?;
            return Ok(());
        }

        // Important to confirm heartbeat from Leader immediatelly
        // Keep syncing leader_id
        ctx.membership().mark_leader_id(append_entries_request.leader_id)?;

        if my_term < append_entries_request.term {
            self.update_current_term(append_entries_request.term);
        }

        // Handle replication request
        match ctx
            .replication_handler()
            .handle_append_entries(append_entries_request, &state_snapshot, last_applied, ctx.raft_log())
            .await
        {
            Ok(AppendResponseWithUpdates {
                success,
                current_term,
                last_matched_id,
                commit_index_update,
            }) => {
                if let Some(commit) = commit_index_update {
                    if let Err(e) = self.update_commit_index_with_signal(commit, &role_tx) {
                        error!("update_commit_index_with_signal,commit={}, error: {:?}", commit, e);
                        return Err(e);
                    }
                }

                // Create a response
                response = AppendEntriesResponse {
                    id: self.node_id(),
                    term: current_term,
                    success,
                    match_index: last_matched_id,
                };

                debug!("AppendEntriesResponse: {:?}", response);

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    Error::TokioSendStatusError(error_str)
                })?;
            }
            Err(e) => {
                debug!("AppendEntriesResponse: {:?}", response);

                sender.send(Ok(response)).map_err(|e| {
                    let error_str = format!("{:?}", e);
                    error!("Failed to send: {}", error_str);
                    Error::TokioSendStatusError(error_str)
                })?;
                error("handle_raft_event", &e);
                return Err(e);
            }
        }
        return Ok(());
    }
}
