pub mod candidate_state;
pub mod follower_state;
pub mod leader_state;
pub mod learner_state;
pub mod role_state;

#[cfg(test)]
mod candidate_state_test;
#[cfg(test)]
mod follower_state_test;
#[cfg(test)]
mod leader_state_test;
#[cfg(test)]
mod learner_state_test;

//---------------------------------------------------------------------
pub const FOLLOWER: i32 = 0;
pub const CANDIDATE: i32 = 1;
pub const LEADER: i32 = 2;
pub const LEARNER: i32 = 3;

use std::collections::HashMap;
use std::sync::Arc;

use candidate_state::CandidateState;
use follower_state::FollowerState;
use leader_state::LeaderState;
use learner_state::LearnerState;
use role_state::RaftRoleState;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::debug;
use tracing::trace;

use super::RaftContext;
use super::RaftEvent;
use super::RoleEvent;
/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
use crate::alias::POF;
/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
use crate::proto::VotedFor;
/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
use crate::Result;
/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
use crate::TypeConfig;

#[repr(i32)]
pub enum RaftRole<T: TypeConfig> {
    Follower(FollowerState<T>),
    Candidate(CandidateState<T>),
    Leader(LeaderState<T>),
    Learner(LearnerState<T>),
}

#[derive(Clone, Debug, Copy)]
pub struct HardState {
    /// Persistent state on all servers(Updated on stable storage before
    /// responding to RPCs): latest term server has seen (initialized to 0
    /// on first boot, increases monotonically) Terms act as a logical clock
    /// in Raft, and they allow servers to detect obsolete information such as
    /// stale leaders. Each server stores a current term number, which increases
    /// monotonically over time.
    pub current_term: u64,
    /// Persistent state on all servers(Updated on stable storage before
    /// responding to RPCs): candidateId that received vote in current term
    /// (or null if none)
    pub voted_for: Option<VotedFor>,
}

#[derive(Clone, Debug)]
pub struct SharedState {
    pub node_id: u32,

    pub hard_state: HardState,
    /// Volatile state on all servers:
    /// index of highest log entry known to be committed (initialized to 0,
    /// increases monotonically)
    pub commit_index: u64,
}

#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub current_term: u64,
    pub voted_for: Option<VotedFor>,
    pub commit_index: u64,
}
/// This structure will be used to retrieve Leader's current state snapshot
/// e.g. used inside replication handler
#[derive(Clone, Debug)]
pub struct LeaderStateSnapshot {
    pub next_index: HashMap<u32, u64>,
    pub match_index: HashMap<u32, u64>,
    pub noop_log_id: Option<u64>,
}

impl SharedState {
    fn new(
        node_id: u32,
        hard_state_from_db: Option<HardState>,
        last_applied_index_option: Option<u64>,
    ) -> Self {
        let hard_state = if hard_state_from_db.is_none() {
            HardState {
                current_term: 1,
                voted_for: None,
            }
        } else {
            hard_state_from_db.unwrap()
        };
        debug!(
            "New Shared State wtih, hard_state_from_db:{:?}, last_applied_index_option:{:?} ",
            &hard_state_from_db, &last_applied_index_option
        );
        Self {
            node_id,
            hard_state,
            commit_index: last_applied_index_option.unwrap_or(0),
        }
    }
    pub fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    fn update_current_term(
        &mut self,
        term: u64,
    ) {
        self.hard_state.current_term = term;
    }

    fn increase_current_term(&mut self) {
        self.hard_state.current_term += 1;
    }

    pub fn voted_for(&self) -> Result<Option<VotedFor>> {
        Ok(self.hard_state.voted_for)
    }
    pub fn reset_voted_for(&mut self) -> Result<()> {
        self.hard_state.voted_for = None;
        Ok(())
    }
    pub fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<()> {
        self.hard_state.voted_for = Some(voted_for);
        Ok(())
    }
}

impl<T: TypeConfig> RaftRole<T> {
    pub(crate) fn state(&self) -> &dyn RaftRoleState<T = T> {
        match self {
            RaftRole::Follower(state) => state,
            RaftRole::Candidate(state) => state,
            RaftRole::Leader(state) => state,
            RaftRole::Learner(state) => state,
        }
    }

    pub(crate) fn state_mut(&mut self) -> &mut dyn RaftRoleState<T = T> {
        match self {
            RaftRole::Follower(state) => state,
            RaftRole::Candidate(state) => state,
            RaftRole::Leader(state) => state,
            RaftRole::Learner(state) => state,
        }
    }

    pub(crate) fn is_timer_expired(&self) -> bool {
        self.state().is_timer_expired()
    }

    pub(crate) fn reset_timer(&mut self) {
        self.state_mut().reset_timer()
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        self.state().next_deadline()
    }

    #[inline]
    pub(crate) fn as_i32(&self) -> i32 {
        match self {
            RaftRole::Follower(_) => 0,
            RaftRole::Candidate(_) => 1,
            RaftRole::Leader(_) => 2,
            RaftRole::Learner(_) => 3,
        }
    }

    pub(crate) fn become_leader(&self) -> Result<RaftRole<T>> {
        self.state().become_leader()
    }
    pub(crate) fn become_candidate(&mut self) -> Result<RaftRole<T>> {
        self.state_mut().become_candidate()
    }
    pub(crate) fn become_follower(&self) -> Result<RaftRole<T>> {
        self.state().become_follower()
    }
    pub(crate) fn become_learner(&self) -> Result<RaftRole<T>> {
        self.state().become_learner()
    }

    pub(crate) fn is_follower(&self) -> bool {
        self.state().is_follower()
    }
    pub(crate) fn is_candidate(&self) -> bool {
        self.state().is_candidate()
    }
    pub(crate) fn is_leader(&self) -> bool {
        self.state().is_leader()
    }
    pub(crate) fn is_learner(&self) -> bool {
        self.state().is_learner()
    }

    pub(crate) fn current_term(&self) -> u64 {
        self.state().current_term()
    }

    #[cfg(test)]
    pub(crate) fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.state().voted_for()
    }
    #[cfg(test)]
    pub(crate) fn commit_index(&self) -> u64 {
        self.state().commit_index()
    }
    #[cfg(test)]
    pub(crate) fn match_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        self.state().match_index(node_id)
    }
    #[cfg(test)]
    pub(crate) fn next_index(
        &self,
        node_id: u32,
    ) -> Option<u64> {
        self.state().next_index(node_id)
    }

    pub(crate) fn update_term(
        &mut self,
        new_term: u64,
    ) {
        self.state_mut().update_current_term(new_term);
    }

    pub(crate) fn update_voted_for(
        &mut self,
        voted_for: VotedFor,
    ) -> Result<()> {
        self.state_mut().update_voted_for(voted_for)
    }

    pub(crate) fn update_match_index(
        &mut self,
        node_id: u32,
        new_match_id: u64,
    ) -> Result<()> {
        self.state_mut().update_match_index(node_id, new_match_id)
    }

    pub(crate) fn update_next_index(
        &mut self,
        node_id: u32,
        new_next_id: u64,
    ) -> Result<()> {
        self.state_mut().update_next_index(node_id, new_next_id)
    }

    pub(crate) fn init_peers_next_index_and_match_index(
        &mut self,
        last_entry_id: u64,
        peer_ids: Vec<u32>,
    ) -> Result<()> {
        self.state_mut()
            .init_peers_next_index_and_match_index(last_entry_id, peer_ids)
    }

    pub(crate) async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        event_tx: &mpsc::Sender<RaftEvent>,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        trace!("raft_role:tick");
        self.state_mut().tick(role_tx, event_tx, peer_channels, ctx).await
    }

    pub(crate) async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        self.state_mut()
            .handle_raft_event(raft_event, peer_channels, ctx, role_tx)
            .await
    }

    #[cfg(test)]
    pub(crate) fn follower_role_i32() -> i32 {
        0
    }

    /// Verifies leadership validity in the new term by attempting to replicate a no-op entry.
    ///
    /// This critical safety check ensures the new leader can actually communicate with a quorum of
    /// cluster nodes before accepting client requests. The verification is done by replicating
    /// a special no-op log entry and confirming its commitment.
    ///
    /// # Key Behaviors
    /// - **Non-blocking verification**: Operates asynchronously without stalling main Raft loop
    /// - **Term-aware retries**: Automatically aborts if higher term is detected
    ///
    /// # Error Semantics
    /// Returned errors indicate **technical failures in the verification process**, not quorum
    /// rejection:
    /// - Network errors (gRPC failures, unreachable nodes)
    /// - Storage I/O errors (failed to persist no-op entry)
    /// - Internal channel errors (message queue overflows)
    ///
    /// # Success Conditions
    /// A successful `Ok(())` return only means:
    /// The noop request has been successfully enqueued into the leader’s replication batch queue.
    ///
    /// # Arguments
    /// - `peer_channels`: Network channels to cluster peers
    /// - `ctx`: Shared Raft state context
    /// - `role_tx`: Role transition event channel
    pub(crate) async fn verify_leadership_in_new_term(
        &mut self,
        peer_channels: Arc<POF<T>>,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()> {
        self.state_mut()
            .verify_leadership_in_new_term(peer_channels, ctx, role_tx)
            .await
    }
}

impl Serialize for HardState {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HardState", 2)?;
        state.serialize_field("current_term", &self.current_term)?;
        state.serialize_field("voted_for", &self.voted_for)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for HardState {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct HardStateDe {
            current_term: u64,
            voted_for: Option<VotedFor>,
        }

        let hard_state_de = HardStateDe::deserialize(deserializer)?;

        Ok(HardState {
            current_term: hard_state_de.current_term,
            voted_for: hard_state_de.voted_for,
        })
    }
}
