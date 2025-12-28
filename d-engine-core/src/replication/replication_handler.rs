use std::cmp;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use prost::Message;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::AppendResponseWithUpdates;
use super::ReplicationCore;
use crate::AppendResults;
use crate::IdAllocationError;
use crate::LeaderStateSnapshot;
use crate::PeerUpdate;
use crate::RaftContext;
use crate::RaftLog;
use crate::ReplicationError;
use crate::Result;
use crate::StateSnapshot;
use crate::Transport;
use crate::TypeConfig;
use crate::alias::ROF;
use crate::scoped_timer::ScopedTimer;
use crate::utils::cluster::is_majority;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::ConflictResult;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;

#[derive(Clone)]
pub struct ReplicationHandler<T>
where
    T: TypeConfig,
{
    pub my_id: u32,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T> ReplicationCore<T> for ReplicationHandler<T>
where
    T: TypeConfig,
{
    async fn handle_raft_request_in_batch(
        &self,
        entry_payloads: Vec<EntryPayload>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        cluster_metadata: &crate::raft_role::ClusterMetadata,
        ctx: &RaftContext<T>,
    ) -> Result<AppendResults> {
        let _timer = ScopedTimer::new("handle_raft_request_in_batch");

        debug!("-------- handle_raft_request_in_batch --------");

        // ----------------------
        // Phase 1: Pre-Checks and Cluster Topology Detection
        // ----------------------
        // Use cached replication targets from cluster metadata (zero-cost)
        let replication_targets = &cluster_metadata.replication_targets;

        // Separate Voters and Learners
        // Use role (not status) to distinguish: Follower/Candidate are voters, Learner are learners
        // This is more robust than using status, which can be temporarily non-Active
        let (voters, learners): (Vec<_>, Vec<_>) = replication_targets
            .iter()
            .partition(|node| node.role != NodeRole::Learner as i32);

        if !learners.is_empty() {
            trace!(
                "handle_raft_request_in_batch - voters: {:?}, learners: {:?}",
                voters, learners
            );
        }

        // ----------------------
        // Phase 2: Process Client Commands
        // ----------------------

        // Record down the last index before new inserts, to avoid duplicated entries, bugfix#48
        let raft_log = ctx.raft_log();
        let leader_last_index_before = raft_log.last_entry_id();

        let new_entries = self
            .generate_new_entries(entry_payloads, state_snapshot.current_term, raft_log)
            .await?;

        // ----------------------
        // Phase 3: Prepare Replication Data
        // ----------------------
        let replication_data = ReplicationData {
            leader_last_index_before,
            current_term: state_snapshot.current_term,
            commit_index: state_snapshot.commit_index,
            peer_next_indices: leader_state_snapshot.next_index,
        };

        let entries_per_peer = self.prepare_peer_entries(
            &new_entries,
            &replication_data,
            ctx.node_config.raft.replication.append_entries_max_entries_per_replication,
            raft_log,
        );

        // ----------------------
        // Phase 4: Build Requests
        // ----------------------
        let requests = replication_targets
            .iter()
            .map(|m| {
                self.build_append_request(raft_log, m.id, &entries_per_peer, &replication_data)
            })
            .collect();

        // ----------------------
        // Phase 5: Replication
        // ----------------------

        // No peers: logs already written in Phase 2, return immediately
        // No replication needed, quorum is automatically achieved (standalone node)
        if replication_targets.is_empty() {
            debug!(
                "Standalone node (leader={}): logs persisted, quorum automatically achieved",
                self.my_id
            );
            return Ok(AppendResults {
                commit_quorum_achieved: true,
                peer_updates: HashMap::new(),
                learner_progress: HashMap::new(),
            });
        }

        // Multi-node cluster: perform replication to peers
        let leader_current_term = state_snapshot.current_term;
        let mut successes = 1; // Include leader itself
        let mut peer_updates = HashMap::new();
        let mut learner_progress = HashMap::new();

        let membership = ctx.membership();
        match ctx
            .transport()
            .send_append_requests(
                requests,
                &ctx.node_config.retry,
                membership,
                ctx.node_config.raft.rpc_compression.replication_response,
            )
            .await
        {
            Ok(append_result) => {
                for response in append_result.responses {
                    match response {
                        Ok(append_response) => {
                            // Skip responses from stale terms
                            if append_response.term < leader_current_term {
                                info!(%append_response.term, %leader_current_term, "append_response.term < leader_current_term");
                                continue;
                            }

                            match append_response.result {
                                Some(append_entries_response::Result::Success(success_result)) => {
                                    // Only count successful responses from Voters
                                    if voters.iter().any(|n| n.id == append_response.node_id) {
                                        successes += 1;
                                    }

                                    let update = self.handle_success_response(
                                        append_response.node_id,
                                        append_response.term,
                                        success_result,
                                        leader_current_term,
                                    )?;

                                    // Record Learner progress
                                    if learners.iter().any(|n| n.id == append_response.node_id) {
                                        learner_progress
                                            .insert(append_response.node_id, update.match_index);
                                    }

                                    peer_updates.insert(append_response.node_id, update);
                                }

                                Some(append_entries_response::Result::Conflict(
                                    conflict_result,
                                )) => {
                                    let current_next_index = replication_data
                                        .peer_next_indices
                                        .get(&append_response.node_id)
                                        .copied()
                                        .unwrap_or(1);

                                    let update = self.handle_conflict_response(
                                        append_response.node_id,
                                        conflict_result,
                                        raft_log,
                                        current_next_index,
                                    )?;

                                    // Record Learner progress
                                    if learners.iter().any(|n| n.id == append_response.node_id) {
                                        learner_progress
                                            .insert(append_response.node_id, update.match_index);
                                    }

                                    peer_updates.insert(append_response.node_id, update);
                                }

                                Some(append_entries_response::Result::HigherTerm(higher_term)) => {
                                    // Only handle higher term if it's greater than current term
                                    if higher_term > leader_current_term {
                                        return Err(
                                            ReplicationError::HigherTerm(higher_term).into()
                                        );
                                    }
                                }

                                None => {
                                    error!("TODO: need to figure out the reason of this cluase");
                                    unreachable!();
                                }
                            }
                        }
                        Err(e) => {
                            // Timeouts and network errors are logged but not added to peer_updates
                            warn!("Peer request failed: {:?}", e);
                        }
                    }
                }
                let peer_ids = append_result.peer_ids;
                debug!(
                    "send_append_requests to: {:?} with succeed number = {}",
                    &peer_ids, successes
                );

                let total_voters = voters.len() + 1; // Leader + voter peers
                let commit_quorum_achieved = is_majority(successes, total_voters);
                Ok(AppendResults {
                    commit_quorum_achieved,
                    peer_updates,
                    learner_progress,
                })
            }
            Err(e) => return Err(e),
        }
    }

    fn handle_success_response(
        &self,
        peer_id: u32,
        peer_term: u64,
        success_result: SuccessResult,
        leader_term: u64,
    ) -> Result<PeerUpdate> {
        let _timer = ScopedTimer::new("handle_success_response");

        debug!(
            ?success_result,
            "Received success response from peer {}", peer_id
        );

        let match_log = success_result.last_match.unwrap_or(LogId { term: 0, index: 0 });

        // Verify Term consistency
        if peer_term > leader_term {
            return Err(ReplicationError::HigherTerm(peer_term).into());
        }

        let peer_match_index = match_log.index;
        let peer_next_index = peer_match_index + 1;

        Ok(PeerUpdate {
            match_index: Some(peer_match_index),
            next_index: peer_next_index,
            success: true,
        })
    }

    fn handle_conflict_response(
        &self,
        peer_id: u32,
        conflict_result: ConflictResult,
        raft_log: &Arc<ROF<T>>,
        current_next_index: u64,
    ) -> Result<PeerUpdate> {
        let _timer = ScopedTimer::new("handle_conflict_response");

        debug!("Handling conflict from peer {}", peer_id);

        // Calculate next_index based on conflict information
        let next_index = match (
            conflict_result.conflict_term,
            conflict_result.conflict_index,
        ) {
            (Some(term), Some(index)) => {
                if let Some(last_index_for_term) = raft_log.last_index_for_term(term) {
                    last_index_for_term + 1
                } else {
                    // Term not found, fallback to conflict index
                    index
                }
            }
            (None, Some(index)) => index,
            _ => current_next_index.saturating_sub(1), // Return to the initial position
        };

        // Make sure next_index is not less than 1
        let next_index = next_index.max(1);
        Ok(PeerUpdate {
            match_index: None, // Unknown after conflict
            next_index,
            success: false,
        })
    }

    fn retrieve_to_be_synced_logs_for_peers(
        &self,
        new_entries: Vec<Entry>,
        leader_last_index_before_inserting_new_entries: u64,
        max_legacy_entries_per_peer: u64, //Maximum number of entries
        peer_next_indices: &HashMap<u32, u64>,
        raft_log: &Arc<ROF<T>>,
    ) -> DashMap<u32, Vec<Entry>> {
        let _timer = ScopedTimer::new("retrieve_to_be_synced_logs_for_peers");

        let peer_entries: DashMap<u32, Vec<Entry>> = DashMap::new();
        trace!(
            "retrieve_to_be_synced_logs_for_peers::leader_last_index: {}",
            leader_last_index_before_inserting_new_entries
        );
        peer_next_indices.keys().for_each(|&id| {
            if id == self.my_id {
                return;
            }
            let peer_next_id = peer_next_indices.get(&id).copied().unwrap_or(1);

            debug!("peer: {} next: {}", id, peer_next_id);
            let mut entries = Vec::new();
            if leader_last_index_before_inserting_new_entries >= peer_next_id {
                let until_index = if (leader_last_index_before_inserting_new_entries - peer_next_id)
                    >= max_legacy_entries_per_peer
                {
                    peer_next_id + max_legacy_entries_per_peer - 1
                } else {
                    leader_last_index_before_inserting_new_entries
                };

                let legacy_entries = match raft_log.get_entries_range(peer_next_id..=until_index) {
                    Ok(entries) => entries,
                    Err(e) => {
                        error!("Failed to get legacy entries for peer {}: {:?}", id, e);
                        Vec::new()
                    }
                };

                if !legacy_entries.is_empty() {
                    trace!("legacy_entries: {:?}", &legacy_entries);
                    entries.extend(legacy_entries);
                }
            }

            if !new_entries.is_empty() {
                entries.extend(new_entries.clone()); // Add new entries
            }
            if !entries.is_empty() {
                peer_entries.insert(id, entries);
            }
        });

        peer_entries
    }

    /// As Follower only
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
        state_snapshot: &StateSnapshot,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<AppendResponseWithUpdates> {
        let _timer = ScopedTimer::new("handle_append_entries");

        debug!(
            "[F-{:?}] >> receive leader append request {:?}",
            self.my_id, request
        );
        let current_term = state_snapshot.current_term;
        let mut last_log_id_option = raft_log.last_log_id();

        //if there is no new entries need to insert, we just return the last local log index
        let mut commit_index_update = None;

        let response = self.check_append_entries_request_is_legal(current_term, &request, raft_log);

        // Handle illegal requests (return conflict or higher Term)
        if response.is_conflict() || response.is_higher_term() {
            debug!("Rejecting AppendEntries: {:?}", &response);

            return Ok(AppendResponseWithUpdates {
                response,
                commit_index_update,
            });
        }

        //switch to follower listening state
        debug!("switch to follower listening state");

        let success = true;

        if !request.entries.is_empty() {
            last_log_id_option = raft_log
                .filter_out_conflicts_and_append(
                    request.prev_log_index,
                    request.prev_log_term,
                    request.entries.clone(),
                )
                .await?;
        }

        if let Some(new_commit_index) = Self::if_update_commit_index_as_follower(
            state_snapshot.commit_index,
            raft_log.last_entry_id(),
            request.leader_commit_index,
        ) {
            debug!("new commit index received: {:?}", new_commit_index);
            commit_index_update = Some(new_commit_index);
        }

        debug!(
            "success: {:?}, current_term: {:?}, last_matched_id: {:?}",
            success, current_term, last_log_id_option
        );

        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(self.my_id, current_term, last_log_id_option),
            commit_index_update,
        })
    }

    ///If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
    /// of last new entry)
    fn if_update_commit_index_as_follower(
        my_commit_index: u64,
        last_raft_log_id: u64,
        leader_commit_index: u64,
    ) -> Option<u64> {
        debug!(
            "Should I update my commit index? leader_commit_index:{:?} > state.commit_index:{:?} = {:?}",
            leader_commit_index,
            my_commit_index,
            leader_commit_index > my_commit_index
        );

        if leader_commit_index > my_commit_index {
            return Some(cmp::min(leader_commit_index, last_raft_log_id));
        }
        None
    }

    #[tracing::instrument(skip(self, raft_log))]
    fn check_append_entries_request_is_legal(
        &self,
        my_term: u64,
        request: &AppendEntriesRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> AppendEntriesResponse {
        let _timer = ScopedTimer::new("check_append_entries_request_is_legal");

        // Rule 1: Term check
        if my_term > request.term {
            warn!(" my_term({}) >= req.term({}) ", my_term, request.term);
            return AppendEntriesResponse::higher_term(self.my_id, my_term);
        }

        let last_log_id_option = raft_log.last_log_id();
        let last_log_id = last_log_id_option.unwrap_or(LogId { term: 0, index: 0 }).index;

        // Rule 2: Special handling for virtual log
        if request.prev_log_index == 0 && request.prev_log_term == 0 {
            // Accept virtual log request (regardless of whether the local log is empty)
            return AppendEntriesResponse::success(self.my_id, my_term, last_log_id_option);
        }

        // Rule 3: General log matching check
        match raft_log.entry_term(request.prev_log_index) {
            Some(term) if term == request.prev_log_term => AppendEntriesResponse::success(
                self.my_id,
                my_term,
                Some(LogId {
                    term: request.prev_log_term,
                    index: request.prev_log_index,
                }),
            ),
            Some(conflict_term) => {
                // Find first index of conflict term
                // TODO:Upcoming feature #45 in v0.2.0
                // let conflict_index = raft_log.first_index_for_term(conflict_term);
                let conflict_index = if request.prev_log_index < last_log_id {
                    request.prev_log_index.saturating_sub(1)
                } else {
                    last_log_id + 1
                };
                AppendEntriesResponse::conflict(
                    self.my_id,
                    my_term,
                    Some(conflict_term),
                    Some(conflict_index),
                )
            }
            None => {
                // prev_log_index not exist, return next expected index
                let conflict_index = last_log_id + 1;
                AppendEntriesResponse::conflict(self.my_id, my_term, None, Some(conflict_index))
            }
        }
    }
}

#[derive(Debug)]
pub struct ReplicationData {
    pub leader_last_index_before: u64,
    pub current_term: u64,
    pub commit_index: u64,
    pub peer_next_indices: HashMap<u32, u64>,
}

impl<T> ReplicationHandler<T>
where
    T: TypeConfig,
{
    pub fn new(my_id: u32) -> Self {
        Self {
            my_id,
            _phantom: PhantomData,
        }
    }

    /// Generate a new log entry
    ///     including insert them into local raft log
    pub async fn generate_new_entries(
        &self,
        entry_payloads: Vec<EntryPayload>,
        current_term: u64,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<Vec<Entry>> {
        let _timer = ScopedTimer::new("generate_new_entries");

        // Handle empty case early
        if entry_payloads.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-allocate ID range in one atomic operation
        let id_range = raft_log.pre_allocate_id_range(entry_payloads.len() as u64);
        assert!(!id_range.is_empty());

        let mut next_index = *id_range.start();

        let mut entries = Vec::with_capacity(entry_payloads.len());

        for payload in entry_payloads {
            // Ensure we don't exceed allocated range
            if next_index > *id_range.end() {
                return Err(IdAllocationError::Overflow {
                    start: next_index,
                    end: *id_range.end(),
                }
                .into());
            }

            entries.push(Entry {
                index: next_index,
                term: current_term,
                payload: Some(payload),
            });

            next_index += 1;
        }

        if !entries.is_empty() {
            trace!(
                "RaftLog insert_batch: {}..={}",
                entries[0].index,
                entries.last().unwrap().index
            );
            raft_log.insert_batch(entries.clone()).await?;
        }

        Ok(entries)
    }

    /// Prepare the items that need to be synchronized for each node
    pub fn prepare_peer_entries(
        &self,
        new_entries: &[Entry],
        data: &ReplicationData,
        max_legacy_entries: u64,
        raft_log: &Arc<ROF<T>>,
    ) -> DashMap<u32, Vec<Entry>> {
        self.retrieve_to_be_synced_logs_for_peers(
            new_entries.to_vec(),
            data.leader_last_index_before,
            max_legacy_entries,
            &data.peer_next_indices,
            raft_log,
        )
    }

    /// Build an append request for a single node
    pub fn build_append_request(
        &self,
        raft_log: &Arc<ROF<T>>,
        peer_id: u32,
        entries_per_peer: &DashMap<u32, Vec<Entry>>,
        data: &ReplicationData,
    ) -> (u32, AppendEntriesRequest) {
        let _timer = ScopedTimer::new("build_append_request");
        // Calculate prev_log metadata
        let (prev_log_index, prev_log_term) =
            data.peer_next_indices.get(&peer_id).map_or((0, 0), |next_id| {
                let prev_index = next_id.saturating_sub(1);
                let term = raft_log.entry_term(prev_index).unwrap_or(0);
                (prev_index, term)
            });

        // Get the items to be sent
        let entries = entries_per_peer.get(&peer_id).map(|e| e.clone()).unwrap_or_default();

        debug!(
            "[Leader {} -> Follower {}] Replicating {} entries",
            self.my_id,
            peer_id,
            entries.len()
        );

        (
            peer_id,
            AppendEntriesRequest {
                term: data.current_term,
                leader_id: self.my_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit_index: data.commit_index,
            },
        )
    }
}

impl<T> Debug for ReplicationHandler<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("ReplicationHandler").field("my_id", &self.my_id).finish()
    }
}

/// Converts a vector of client WriteCommands into a vector of EntryPayloads.
/// Each WriteCommand is serialized into bytes and wrapped in an EntryPayload::Command variant.
///
/// # Arguments
/// * `commands` - A vector of WriteCommand to be converted
///
/// # Returns
/// A vector of EntryPayload containing the serialized commands
pub fn client_command_to_entry_payloads(commands: Vec<WriteCommand>) -> Vec<EntryPayload> {
    commands
        .into_iter()
        .map(|cmd| {
            let mut buf = BytesMut::with_capacity(cmd.encoded_len());
            cmd.encode(&mut buf).unwrap();

            EntryPayload {
                payload: Some(Payload::Command(buf.freeze())),
            }
        })
        .collect()
}
