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
use crate::proto::common::EntryPayload;
use crate::proto::election::VotedFor;
use crate::Result;
use crate::TypeConfig;

/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
#[repr(i32)]
pub enum RaftRole<T: TypeConfig> {
    Follower(Box<FollowerState<T>>),
    Candidate(Box<CandidateState<T>>),
    Leader(Box<LeaderState<T>>),
    Learner(Box<LearnerState<T>>),
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

    /// === Persistent State (MUST be on disk)
    pub hard_state: HardState,

    /// === Volatile state on all servers:
    /// index of highest log entry known to be committed (initialized to 0,
    /// increases monotonically)
    pub commit_index: u64,
}

#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub role: i32,
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
        let hard_state = if let Some(s) = hard_state_from_db {
            s
        } else {
            HardState {
                current_term: 1,
                voted_for: None,
            }
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
            RaftRole::Follower(state) => state.as_ref(),
            RaftRole::Candidate(state) => state.as_ref(),
            RaftRole::Leader(state) => state.as_ref(),
            RaftRole::Learner(state) => state.as_ref(),
        }
    }

    pub(crate) fn state_mut(&mut self) -> &mut dyn RaftRoleState<T = T> {
        match self {
            RaftRole::Follower(state) => state.as_mut(),
            RaftRole::Candidate(state) => state.as_mut(),
            RaftRole::Leader(state) => state.as_mut(),
            RaftRole::Learner(state) => state.as_mut(),
        }
    }

    pub(crate) fn is_timer_expired(&self) -> bool {
        self.state().is_timer_expired()
    }

    pub(crate) fn reset_timer(&mut self) {
        self.state_mut().reset_timer()
    }

    pub(crate) async fn join_cluster(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        self.state().join_cluster(ctx).await
    }

    pub(crate) async fn fetch_initial_snapshot(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        self.state().fetch_initial_snapshot(ctx).await
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        self.state().next_deadline()
    }

    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) fn is_follower(&self) -> bool {
        self.state().is_follower()
    }
    #[allow(dead_code)]
    pub(crate) fn is_candidate(&self) -> bool {
        self.state().is_candidate()
    }
    #[allow(dead_code)]
    pub(crate) fn is_leader(&self) -> bool {
        self.state().is_leader()
    }
    #[allow(dead_code)]
    pub(crate) fn is_learner(&self) -> bool {
        self.state().is_learner()
    }

    pub(crate) fn current_term(&self) -> u64 {
        self.state().current_term()
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.state().voted_for()
    }
    #[allow(dead_code)]
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

    pub(crate) fn init_peers_next_index_and_match_index(
        &mut self,
        last_entry_id: u64,
        peer_ids: Vec<u32>,
    ) -> Result<()> {
        self.state_mut().init_peers_next_index_and_match_index(last_entry_id, peer_ids)
    }

    pub(crate) async fn tick(
        &mut self,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
        event_tx: &mpsc::Sender<RaftEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        trace!("raft_role:tick");
        self.state_mut().tick(role_tx, event_tx, ctx).await
    }

    pub(crate) async fn handle_raft_event(
        &mut self,
        raft_event: RaftEvent,
        ctx: &RaftContext<T>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        self.state_mut().handle_raft_event(raft_event, ctx, role_tx).await
    }

    #[cfg(test)]
    pub(crate) fn follower_role_i32() -> i32 {
        0
    }

    pub(crate) async fn verify_leadership_persistent(
        &mut self,
        payloads: Vec<EntryPayload>,
        bypass_queue: bool,
        ctx: &RaftContext<T>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
    ) -> Result<bool> {
        self.state_mut()
            .verify_leadership_persistent(payloads, bypass_queue, ctx, role_tx)
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
    where
        D: Deserializer<'de>,
    {
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

#[derive(Debug, PartialEq)]
pub(crate) enum QuorumVerificationResult {
    Success,        // Leadership confirmation successful
    LeadershipLost, // Confirmation of leadership loss (need to abdicate)
    RetryRequired,  // Retry required (leadership still exists)
}
