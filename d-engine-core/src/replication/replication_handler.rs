use std::cmp;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::ConflictResult;
use d_engine_proto::server::replication::SuccessResult;
use prost::Message;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;

use super::AppendResponseWithUpdates;
use super::PrepareResult;
use super::ReplicationCore;
use crate::IdAllocationError;
use crate::LeaderStateSnapshot;
use crate::PeerUpdate;
use crate::RaftLog;
use crate::ReplicationError;
use crate::Result;
use crate::StateSnapshot;
use crate::TypeConfig;
use crate::alias::ROF;
use crate::scoped_timer::ScopedTimer;

pub struct ReplicationHandler<T>
where
    T: TypeConfig,
{
    pub my_id: u32,
    _phantom: PhantomData<T>,
}

// Manual Clone: PhantomData<T> is always Clone regardless of T, so no T: Clone bound needed.
impl<T: TypeConfig> Clone for ReplicationHandler<T> {
    fn clone(&self) -> Self {
        ReplicationHandler {
            my_id: self.my_id,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> ReplicationCore<T> for ReplicationHandler<T>
where
    T: TypeConfig,
{
    async fn prepare_batch_requests(
        &self,
        entry_payloads: Vec<EntryPayload>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        cluster_metadata: &crate::raft_role::ClusterMetadata,
        ctx: &crate::RaftContext<T>,
    ) -> Result<PrepareResult> {
        let replication_targets = &cluster_metadata.replication_targets;

        let raft_log = ctx.raft_log();
        let leader_last_index_before = raft_log.last_entry_id();

        // Phase 1: Write new entries to local log (must stay serial in Raft loop).
        let new_entries = self
            .generate_new_entries(entry_payloads, state_snapshot.current_term, raft_log)
            .await?;

        if replication_targets.is_empty() {
            return Ok(PrepareResult::default());
        }

        // Purge boundary: peers with next_index below this need snapshot, not AppendEntries.
        let min_log_index = raft_log.first_entry_id();

        // Phase 2: Build per-peer payloads.
        let replication_data = ReplicationData {
            leader_last_index_before,
            current_term: state_snapshot.current_term,
            commit_index: state_snapshot.commit_index,
            peer_next_indices: leader_state_snapshot.next_index,
        };

        let mut entries_per_peer = self.prepare_peer_entries(
            &new_entries,
            &replication_data,
            ctx.node_config.raft.replication.append_entries_max_entries_per_replication,
            raft_log,
        );

        let mut append_requests = Vec::with_capacity(replication_targets.len());
        let mut snapshot_targets = Vec::new();

        for peer in replication_targets {
            let peer_next_id =
                replication_data.peer_next_indices.get(&peer.id).copied().unwrap_or(1);

            // Peer is behind the purge boundary: must receive snapshot, not AppendEntries.
            if min_log_index > 1 && peer_next_id < min_log_index {
                snapshot_targets.push(peer.id);
                continue;
            }

            append_requests.push(self.build_append_request(
                raft_log,
                peer.id,
                &mut entries_per_peer,
                &replication_data,
            ));
        }

        Ok(PrepareResult {
            append_requests,
            snapshot_targets,
        })
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
        new_entries: &[Entry],
        leader_last_index_before_inserting_new_entries: u64,
        max_legacy_entries_per_peer: u64,
        peer_next_indices: &HashMap<u32, u64>,
        raft_log: &Arc<ROF<T>>,
    ) -> HashMap<u32, Vec<Entry>> {
        let _timer = ScopedTimer::new("retrieve_to_be_synced_logs_for_peers");

        let peer_count = peer_next_indices.len().saturating_sub(1); // exclude self
        let mut peer_entries: HashMap<u32, Vec<Entry>> = HashMap::with_capacity(peer_count);
        trace!(
            "retrieve_to_be_synced_logs_for_peers::leader_last_index: {}",
            leader_last_index_before_inserting_new_entries
        );
        for (&id, &peer_next_id) in peer_next_indices {
            if id == self.my_id {
                continue;
            }

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
                entries.extend_from_slice(new_entries);
            }
            if !entries.is_empty() {
                peer_entries.insert(id, entries);
            }
        }

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
                // Skip entire conflicting term: give Leader the first index of conflict_term
                // so it jumps to the term boundary in one RPC instead of stepping back one-by-one.
                // Raft §5.3 optimization (#346).
                let conflict_index = raft_log
                    .first_index_for_term(conflict_term)
                    .unwrap_or_else(|| request.prev_log_index.saturating_sub(1));
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
    ) -> HashMap<u32, Vec<Entry>> {
        self.retrieve_to_be_synced_logs_for_peers(
            new_entries,
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
        entries_per_peer: &mut HashMap<u32, Vec<Entry>>,
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

        // Move entries out of the map — avoids Vec clone
        let entries = entries_per_peer.remove(&peer_id).unwrap_or_default();

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
