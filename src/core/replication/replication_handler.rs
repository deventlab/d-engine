use super::{AppendResponseWithUpdates, ReplicationCore};
use crate::{
    alias::{ROF, TROF},
    grpc::rpc_service::{AppendEntriesRequest, ClientCommand, Entry},
    AppendResults, ChannelWithAddress, ChannelWithAddressAndRole, Error, LeaderStateSnapshot,
    RaftLog, RaftSettings, Result, StateSnapshot, Transport, TypeConfig, API_SLO,
};
use autometrics::autometrics;
use dashmap::DashMap;
use log::{debug, error, warn};
use prost::Message;
use std::{cmp, collections::HashMap, marker::PhantomData, sync::Arc};

use tonic::async_trait;

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
    /// As Leader, send replications to peers.
    /// (combined regular heartbeat and client proposals)
    ///
    /// Each time handle_client_proposal_in_batch is called, perform peer synchronization check
    /// 1. Verify if any peer's next_id <= leader's commit_index
    /// 2. For non-synced peers meeting this condition:
    ///    a. Retrieve all unsynced log entries
    ///    b. Buffer these entries before processing real entries
    /// 3. Ensure unsynced entries are prepended to the entries queue
    ///    before actual entries get pushed
    ///
    /// Leader state will be updated by LeaderState only(follows SRP).
    ///
    async fn handle_client_proposal_in_batch(
        &self,
        commands: Vec<ClientCommand>,
        state_snapshot: StateSnapshot,
        leader_state_snapshot: LeaderStateSnapshot,
        replication_members: &Vec<ChannelWithAddressAndRole>,
        raft_log: &Arc<ROF<T>>,
        transport: &Arc<TROF<T>>,
        raft_settings: RaftSettings,
    ) -> Result<AppendResults> {
        debug!("-------- handle_client_proposal_in_batch --------");

        if replication_members.len() < 1 {
            warn!("no peer found for leader({})", self.my_id);
            return Err(Error::AppendEntriesNoPeerFound);
        }

        let leader_last_index_before_inserting_new_entries = raft_log.last_entry_id();

        let mut entries: Vec<Entry> = Vec::new();

        let current_term = state_snapshot.current_term;
        let commit_index = state_snapshot.commit_index;
        let peer_next_indices = leader_state_snapshot.next_index;

        for c in commands {
            let raft_log_new_index = raft_log.pre_allocate_raft_logs_next_index();
            debug!("raft_log_new_index: {:?}", raft_log_new_index);
            entries.push(Entry {
                index: raft_log_new_index,
                term: current_term,
                command: c.encode_to_vec(),
            });
        }

        if entries.len() > 0 {
            // Step 1: insert into local log
            if let Err(e) = raft_log.insert_batch(entries.clone()) {
                error!("insert_batch_commands failed: {:?}", e);
                return Err(Error::GeneralLocalLogIOError);
            }
        }

        let peer_entries: DashMap<u32, Vec<Entry>> = self.retrieve_to_be_synced_logs_for_peers(
            entries,
            leader_last_index_before_inserting_new_entries,
            raft_settings.append_entries_max_entries_per_replication,
            &peer_next_indices,
            raft_log,
        );

        let mut peer_ids = vec![];

        //(peer_id, peer_address, peer_request)
        let append_entries_requests_with_peer_address: Vec<(
            u32,
            ChannelWithAddress,
            AppendEntriesRequest,
        )> = replication_members
            .iter()
            .map(|peer| {
                peer_ids.push(peer.id);
                //TODO: prev_log_index from state might be an bug?
                let peer_next_id = peer_next_indices.get(&peer.id).copied().unwrap_or(0);
                let prev_log_index = if peer_next_id > 0 {
                    peer_next_id - 1
                } else {
                    0
                };
                let prev_log_term = raft_log.prev_log_term(peer.id, prev_log_index);
                let entries = peer_entries
                    .get(&peer.id)
                    .map(|v| v.clone())
                    .unwrap_or_else(Vec::new);

                debug!(
                    "[L_{}->F_{}]going to replicate, entries:{:?}",
                    self.my_id, peer.id, &entries
                );
                (
                    peer.id,
                    peer.channel_with_address.clone(),
                    AppendEntriesRequest {
                        term: current_term,
                        leader_id: self.my_id,
                        prev_log_index,
                        prev_log_term,
                        entries: entries, // Assuming entries need to be cloned for each request
                        leader_commit_index: commit_index,
                    },
                )
            })
            .collect();

        debug!("start append_entries..");
        transport
            .send_append_requests(
                current_term,
                append_entries_requests_with_peer_address,
                raft_settings.clone(),
            )
            .await
    }

    #[autometrics(objective = API_SLO)]
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

                let legacy_entries = raft_log.get_entries_between(peer_next_id..=until_index);

                if !legacy_entries.is_empty() {
                    debug!("legacy_entries: {:?}", &legacy_entries);
                    entries.extend(legacy_entries);
                }
            }

            if new_entries.len() > 0 {
                entries.extend(new_entries.clone()); // Add new entries
            }
            if entries.len() > 0 {
                peer_entries.insert(id, entries);
            }
        });

        return peer_entries;
    }

    /// As Follower only
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
        state_snapshot: &StateSnapshot,
        last_applied: u64,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<AppendResponseWithUpdates> {
        debug!(
            "[F-{:?}] >> receive leader append request {:?}",
            self.my_id, request
        );
        let current_term = state_snapshot.current_term;
        let raft_log_last_index = raft_log.last_entry_id();
        let success;
        //if there is no new entries need to insert, we just return the last local log index
        let mut last_matched_id = raft_log_last_index;
        let mut commit_index_update = None;

        if current_term > request.term {
            debug!(
                " current_term({}) >= req.term({}) ",
                current_term, request.term
            );
            return Ok(AppendResponseWithUpdates {
                success: false,
                current_term,
                last_matched_id,
                commit_index_update,
            });
        }

        if raft_log.prev_log_ok(request.prev_log_index, request.prev_log_term, last_applied) {
            //switch to follower listening state
            debug!("switch to follower listening state");

            success = true;

            if !request.entries.is_empty() {
                last_matched_id = raft_log.filter_out_conflicts_and_append(
                    request.prev_log_index,
                    request.entries.clone(),
                );
            }

            if let Some(new_commit_index) = Self::if_update_commit_index_as_follower(
                state_snapshot.commit_index,
                raft_log.last_entry_id(),
                request.leader_commit_index,
            ) {
                debug!("new commit index received: {:?}", new_commit_index);
                commit_index_update = Some(new_commit_index);
            }
        } else {
            warn!("prev log is not ok on req");
            //bugfix: #112
            last_matched_id = if request.prev_log_index < raft_log_last_index {
                request.prev_log_index.saturating_sub(1)
            } else {
                raft_log_last_index
            };
            success = false;
        }

        debug!(
            "success: {:?}, current_term: {:?}, last_matched_id: {:?}",
            success, current_term, last_matched_id
        );

        Ok(AppendResponseWithUpdates {
            success,
            current_term,
            last_matched_id,
            commit_index_update,
        })
    }

    ///If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    #[autometrics(objective = API_SLO)]
    fn if_update_commit_index_as_follower(
        my_commit_index: u64,
        last_raft_log_id: u64,
        leader_commit_index: u64,
    ) -> Option<u64> {
        debug!("Should I update my commit index? leader_commit_index:{:?} > state.commit_index:{:?} = {:?}", leader_commit_index, my_commit_index, leader_commit_index > my_commit_index);

        if leader_commit_index > my_commit_index {
            return Some(cmp::min(leader_commit_index, last_raft_log_id));
        }
        None
    }
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
}
