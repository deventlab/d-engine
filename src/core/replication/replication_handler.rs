use std::cmp;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use autometrics::autometrics;
use dashmap::DashMap;
use prost::Message;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;

use super::AppendResponseWithUpdates;
use super::ReplicationCore;
use crate::alias::POF;
use crate::alias::ROF;
use crate::proto::append_entries_response;
use crate::proto::AppendEntriesRequest;
use crate::proto::AppendEntriesResponse;
use crate::proto::ClientCommand;
use crate::proto::ConflictResult;
use crate::proto::Entry;
use crate::proto::LogId;
use crate::proto::SuccessResult;
use crate::utils::cluster::is_majority;
use crate::AppendResults;
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::LeaderStateSnapshot;
use crate::PeerUpdate;
use crate::RaftContext;
use crate::RaftLog;
use crate::ReplicationError;
use crate::Result;
use crate::StateSnapshot;
use crate::Transport;
use crate::TypeConfig;
use crate::API_SLO;

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
    async fn handle_client_proposal_in_batch(
        &self,
        commands: Vec<ClientCommand>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        ctx: &RaftContext<T>,
        peer_channels: Arc<POF<T>>,
    ) -> Result<AppendResults> {
        debug!("-------- handle_client_proposal_in_batch --------");
        trace!("commands: {:?}", &commands);

        // ----------------------
        // Phase 1: Pre-Checks
        // ----------------------
        let replication_members = ctx.voting_members(peer_channels);
        if replication_members.is_empty() {
            warn!("no peer found for leader({})", self.my_id);
            return Err(ReplicationError::NoPeerFound { leader_id: self.my_id }.into());
        }

        // ----------------------
        // Phase 2: Process Client Commands
        // ----------------------

        // Record down the last index before new inserts, to avoid duplicated entries, bugfix#48
        let raft_log = ctx.raft_log();
        let leader_last_index_before = raft_log.last_entry_id();

        let new_entries = self.generate_new_entries(commands, state_snapshot.current_term, raft_log)?;

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
            ctx.node_config
                .raft
                .replication
                .append_entries_max_entries_per_replication,
            raft_log,
        );

        // ----------------------
        // Phase 4: Build Requests
        // ----------------------
        let requests = replication_members
            .iter()
            .map(|peer| self.build_append_request(raft_log, peer, &entries_per_peer, &replication_data))
            .collect();

        // ----------------------
        // Phase 5: Send Requests
        // ----------------------
        let leader_current_term = state_snapshot.current_term;
        let mut successes = 1; // Include leader itself
        let mut peer_updates = HashMap::new();
        match ctx
            .transport()
            .send_append_requests(requests, &ctx.node_config.retry)
            .await
        {
            Ok(append_result) => {
                for response in append_result.responses {
                    match response {
                        Ok(append_response) => {
                            // Skip responses from stale terms
                            if append_response.term < leader_current_term {
                                continue;
                            }

                            match append_response.result {
                                Some(append_entries_response::Result::Success(success_result)) => {
                                    successes += 1;
                                    let update = self.handle_success_response(
                                        append_response.node_id,
                                        append_response.term,
                                        success_result,
                                        leader_current_term,
                                    )?;
                                    peer_updates.insert(append_response.node_id, update);
                                }

                                Some(append_entries_response::Result::Conflict(conflict_result)) => {
                                    let update = self.handle_conflict_response(
                                        append_response.node_id,
                                        conflict_result,
                                        raft_log,
                                    )?;

                                    peer_updates.insert(append_response.node_id, update);
                                }

                                Some(append_entries_response::Result::HigherTerm(higher_term)) => {
                                    // Only handle higher term if it's greater than current term
                                    if higher_term > leader_current_term {
                                        return Err(ReplicationError::HigherTerm(higher_term).into());
                                    }
                                }

                                None => {
                                    error!("TODO: need to figure out the reason of this cluase");
                                    unreachable!();
                                }
                            }
                        }
                        Err(e) => {
                            error!("send_append_requests error: {:?}", e);
                        }
                    }
                }
                let peer_ids = append_result.peer_ids;
                debug!(
                    "send_append_requests to: {:?} with succeed number = {}",
                    &peer_ids, successes
                );

                let commit_quorum_achieved = is_majority(successes, peer_ids.len() + 1);
                Ok(AppendResults {
                    commit_quorum_achieved,
                    peer_updates,
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
        debug!("Received success response from peer {}", peer_id);

        let match_log = success_result.last_match.unwrap_or(LogId { term: 0, index: 0 });

        // Verify Term consistency
        if peer_term > leader_term {
            return Err(ReplicationError::HigherTerm(peer_term).into());
        }

        let peer_match_index = match_log.index;
        let peer_next_index = peer_match_index + 1;

        Ok(PeerUpdate {
            match_index: peer_match_index,
            next_index: peer_next_index,
            success: true,
        })
    }

    fn handle_conflict_response(
        &self,
        peer_id: u32,
        conflict_result: ConflictResult,
        _raft_log: &Arc<ROF<T>>,
    ) -> Result<PeerUpdate> {
        debug!("Handling conflict from peer {}", peer_id);

        // Calculate next_index based on conflict information
        let next_index = match (conflict_result.conflict_term, conflict_result.conflict_index) {
            (Some(_term), Some(index)) => {
                // Find the last log that matches term
                // TODO: feature in v0.2.0
                // raft_log
                //     .last_index_for_term(term)
                //     .map(|last_index| last_index + 1)
                //     .unwrap_or(index)
                index.saturating_sub(1)
            }
            (None, Some(index)) => index,
            _ => 1, // Return to the initial position
        };

        // Make sure next_index is not less than 1
        let next_index = next_index.max(1);
        // Update peer status (at least go back 1 position)
        Ok(PeerUpdate {
            match_index: next_index.saturating_sub(1),
            next_index,
            success: false,
        })
    }

    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
    fn retrieve_to_be_synced_logs_for_peers(
        &self,
        new_entries: Vec<Entry>,
        leader_last_index_before_inserting_new_entries: u64,
        max_legacy_entries_per_peer: u64, //Maximum number of entries
        peer_next_indices: &HashMap<u32, u64>,
        raft_log: &Arc<ROF<T>>,
    ) -> DashMap<u32, Vec<Entry>> {
        let peer_entries: DashMap<u32, Vec<Entry>> = DashMap::new();
        debug!(
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
                let until_index =
                    if (leader_last_index_before_inserting_new_entries - peer_next_id) >= max_legacy_entries_per_peer {
                        peer_next_id + max_legacy_entries_per_peer - 1
                    } else {
                        leader_last_index_before_inserting_new_entries
                    };

                let legacy_entries = raft_log.get_entries_between(peer_next_id..=until_index);

                if !legacy_entries.is_empty() {
                    debug!("legacy_entries: {:?}", &legacy_entries);
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

        return peer_entries;
    }

    /// As Follower only
    #[tracing::instrument]
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
        state_snapshot: &StateSnapshot,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<AppendResponseWithUpdates> {
        debug!("[F-{:?}] >> receive leader append request {:?}", self.my_id, request);
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
            last_log_id_option = raft_log.filter_out_conflicts_and_append(
                request.prev_log_index,
                request.prev_log_term,
                request.entries.clone(),
            )?;
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
    #[autometrics(objective = API_SLO)]
    #[tracing::instrument]
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

    #[tracing::instrument]
    fn check_append_entries_request_is_legal(
        &self,
        my_term: u64,
        request: &AppendEntriesRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> AppendEntriesResponse {
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
                AppendEntriesResponse::conflict(self.my_id, my_term, Some(conflict_term), Some(conflict_index))
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
pub(super) struct ReplicationData {
    pub(super) leader_last_index_before: u64,
    pub(super) current_term: u64,
    pub(super) commit_index: u64,
    pub(super) peer_next_indices: HashMap<u32, u64>,
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
    pub(super) fn generate_new_entries(
        &self,
        commands: Vec<ClientCommand>,
        current_term: u64,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<Vec<Entry>> {
        let mut entries = Vec::with_capacity(commands.len());

        for command in commands {
            let index = raft_log.pre_allocate_raft_logs_next_index();
            debug!("Allocated log index: {}", index);

            entries.push(Entry {
                index,
                term: current_term,
                command: command.encode_to_vec(),
            });
        }

        if !entries.is_empty() {
            raft_log.insert_batch(entries.clone())?;
        }

        Ok(entries)
    }

    /// Prepare the items that need to be synchronized for each node
    pub(super) fn prepare_peer_entries(
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
    #[tracing::instrument]
    pub(super) fn build_append_request(
        &self,
        raft_log: &Arc<ROF<T>>,
        peer: &ChannelWithAddressAndRole,
        entries_per_peer: &DashMap<u32, Vec<Entry>>,
        data: &ReplicationData,
    ) -> (u32, ChannelWithAddress, AppendEntriesRequest) {
        let peer_id = peer.id;

        // Calculate prev_log metadata
        let (prev_log_index, prev_log_term) = data.peer_next_indices.get(&peer_id).map_or((0, 0), |next_id| {
            let prev_index = next_id.saturating_sub(1);
            let term = raft_log.prev_log_term(peer_id, prev_index);
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
            peer.channel_with_address.clone(),
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
        f.debug_struct("ReplicationHandler")
            .field("my_id", &self.my_id)
            .finish()
    }
}
