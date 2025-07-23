//! It works as RAFT storage layer
//!
//! For the get operation, we should check it from memory cache firslty.
//! the reason, we could catch the entry in memory is because:
//!     no entry could be deleted or modified in RAFT.

use crate::convert::safe_kv;
use crate::convert::safe_vk;
use crate::convert::vki;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::storage::sled_adapter::RAFT_LOG_NAMESPACE;
use crate::BufferedRaftLog;
use crate::FlushCommand;
use crate::LocalLogBatch;
use crate::RaftLog;
use crate::Result;
use crate::StorageError;
use crate::API_SLO;
use crate::MESSAGE_SIZE_IN_BYTES_METRIC;
use autometrics::autometrics;
use dashmap::DashMap;
use prost::Message;
use sled::Batch;
use sled::IVec;
use sled::Subscriber;
use std::ops::RangeInclusive;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

const ID_BATCH_SIZE: u64 = 100;

pub struct SledRaftLog {
    node_id: u32,
    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
    pub(super) buffer: Arc<BufferedRaftLog>,
}

impl std::fmt::Debug for SledRaftLog {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledRaftLog")
            .field("tree_len", &self.tree.len())
            .finish()
    }
}

impl Drop for SledRaftLog {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => info!("Successfully flush RaftLog"),
            Err(e) => error!(?e, "Failed to flush RaftLog"),
        }
    }
}

/// This data structure is designed to improve read and write performance.
/// It has to be refreshed when there is a write
#[derive(Debug)]
pub(super) struct RaftLogMemCache {
    length: AtomicU64,
    mapped_entries: DashMap<u64, Entry>, //mapped from disk
    pub(super) next_id: AtomicU64, /* because of the batch apply feature, we have to pre allocated some entry index before insert successfully */
}

impl RaftLogMemCache {
    fn refresh(
        &self,
        new_disk_entries: &LocalLogBatch,
        disk_len: u64,
    ) -> Result<()> {
        for (key, value) in &new_disk_entries.writes {
            let index = safe_vk(key)?;
            match value {
                Some(ivec) => {
                    if let Ok(entry) = Entry::decode(ivec.as_ref()) {
                        self.mapped_entries.insert(index, entry);
                    } else {
                        error!("Failed to decode entry at index {}", index);
                    }
                }
                None => {
                    self.mapped_entries.remove(&index);
                }
            }
        }
        self.length.store(disk_len, Ordering::Release);
        debug!("RaftLog::refresh local raft log next log index: {}", disk_len + 1);
        self.next_id.store(disk_len + 1, Ordering::Release);

        Ok(())
    }
}

#[async_trait]
impl RaftLog for SledRaftLog {
    /// This function can not be used to decide last local log entry logically
    ///  because it might be cleaned due to snapshot generating or
    ///  it might be the learner who has already applied the snapshot.
    #[autometrics(objective = API_SLO)]
    fn last_entry_id(&self) -> u64 {
        self.buffer.last_entry_id()
    }

    fn last_entry(&self) -> Option<Entry> {
        self.buffer.last_entry()
    }

    fn last_log_id(&self) -> Option<LogId> {
        self.last_entry().map(|entry| LogId {
            term: entry.term,
            index: entry.index,
        })
    }

    #[autometrics(objective = API_SLO)]
    fn first_entry_id(&self) -> u64 {
        self.buffer.first_entry_id()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        self.buffer.get_entry(entry_id).map(|entry| entry.term)
    }

    fn pre_allocate_raft_logs_next_index(&self) -> u64 {
        // self.get_raft_logs_length() + 1
        let next_id = self.buffer.next_id.fetch_add(1, Ordering::SeqCst);
        trace!(
            "[Node-{}] RaftLog pre_allocate_raft_logs_next_index: {}",
            self.node_id,
            next_id
        );
        next_id
    }

    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64> {
        // Calculate required batch size (minimum 1 batch)
        let batches = count.div_ceil(ID_BATCH_SIZE).max(1);
        let total = batches * ID_BATCH_SIZE;

        let start = self.buffer.next_id.fetch_add(total, Ordering::SeqCst);
        start..=start + total - 1
    }

    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.buffer.first_index_for_term(term)
    }

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.buffer.last_index_for_term(term)
    }

    /// start is 'get_last_synced_commit_index', it might be zero
    /// Note: start: start_entry_id
    #[autometrics(objective = API_SLO)]
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Vec<Entry> {
        self.buffer.get_entries_range(range)
    }

    // fn filter_out_conflicts_and_append(
    //     &self,
    //     prev_log_index: u64,
    //     prev_log_term: u64,
    //     new_entries: Vec<Entry>,
    // ) -> Result<Option<LogId>> {
    //     debug!(
    //         "Handling AppendEntries: prev_log_index={}, prev_log_term={}",
    //         prev_log_index, prev_log_term
    //     );

    //     let mut batch = LocalLogBatch::default();
    //     // Process virtual log (prev_log_index=0)
    //     if prev_log_index == 0 && prev_log_term == 0 {
    //         // Clear all local logs
    //         self.clear()?;

    //         // Last entry after insert
    //         let last = new_entries
    //             .last()
    //             .map(|e| {
    //                 Some(LogId {
    //                     term: e.term,
    //                     index: e.index,
    //                 })
    //             })
    //             .unwrap_or(None);

    //         // Append new log
    //         for entry in new_entries {
    //             let encoded = IVec::from(entry.encode_to_vec());
    //             batch.insert(safe_kv(entry.index), encoded);
    //         }
    //         self.apply(&batch)?;
    //         return Ok(last);
    //     }

    //     // Check if prev_log_index matches
    //     match self.entry(prev_log_index) {
    //         Some(entry) if entry.term == prev_log_term => {
    //             // Delete all logs after prev_log_index
    //             for index in (prev_log_index + 1)..=self.last_entry_id() {
    //                 batch.remove(safe_kv(index));
    //             }
    //             // Append new log
    //             for entry in new_entries {
    //                 let encoded = IVec::from(entry.encode_to_vec());
    //                 batch.insert(safe_kv(entry.index), encoded);
    //             }
    //         }
    //         _ => {
    //             // Mismatch, refuse to append (the upper-level logic handles the conflict response)
    //             return Ok(self.last_log_id());
    //         }
    //     }

    //     // Apply batch processing
    //     if !batch.is_empty() {
    //         self.apply(&batch)?;
    //     }

    //     Ok(self.last_log_id())
    // }
    //
    fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>> {
        // Virtual log handling (snapshot installation)
        if prev_log_index == 0 && prev_log_term == 0 {
            self.reset()?;
            self.buffer.add_entries(new_entries.clone());
            return Ok(new_entries.last().map(|e| LogId {
                term: e.term,
                index: e.index,
            }));
        }

        // Check log match
        match self.entry_term(prev_log_index) {
            Some(term) if term == prev_log_term => {
                // Remove conflicting entries from buffer
                for index in (prev_log_index + 1)..=self.buffer.last_entry_id() {
                    self.buffer.entries_cache.remove(&index);
                }

                // Update flush state
                let mut state = self.buffer.flush_state.lock().unwrap();
                state.pending_flush.retain(|&id| id <= prev_log_index);

                // Prepare disk batch
                let mut batch = LocalLogBatch::default();
                for index in (prev_log_index + 1)..=self.buffer.durable_index.load(Ordering::Acquire) {
                    batch.remove(safe_kv(index));
                }

                // Add new entries to buffer
                self.buffer.add_entries(new_entries.clone());

                // Apply to disk asynchronously
                let tree = Arc::clone(&self.tree);
                tokio::spawn(async move {
                    if let Err(e) = tree.apply_batch(batch) {
                        error!("Conflict resolution batch failed: {}", e);
                    }
                });

                // Return last log ID
                Ok(new_entries.last().map(|e| LogId {
                    term: e.term,
                    index: e.index,
                }))
            }
            _ => {
                // Conflict - return current state
                let last_id = self.buffer.last_entry_id();
                Ok(self.buffer.entries_cache.get(&last_id).map(|e| LogId {
                    term: e.term,
                    index: e.index,
                }))
            }
        }
    }

    #[autometrics(objective = API_SLO)]
    fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()> {
        self.buffer.add_entries(logs);
        Ok(())
    }

    // #[autometrics(objective = API_SLO)]
    // fn reset(&self) -> Result<()> {
    //     if let Err(e) = self.tree.clear() {
    //         error!("error: {:?}", e);
    //         Err(StorageError::DbError(e.to_string()).into())
    //     } else {
    //         Ok(())
    //     }
    // }
    #[autometrics(objective = API_SLO)]
    fn reset(&self) -> Result<()> {
        // Clear in-memory buffer
        self.buffer.entries_cache.clear();
        self.buffer.durable_index.store(0, Ordering::Release);
        self.buffer.next_id.store(1, Ordering::Release);

        // Reset flush state
        let mut state = self.buffer.flush_state.lock().unwrap();
        state.pending_flush.clear();
        state.flushing = false;

        // Clear persistent storage
        self.tree.clear().map_err(|e| {
            error!("Storage reset failed: {}", e);
            StorageError::DbError(e.to_string())
        })?;

        Ok(())
    }

    // #[autometrics(objective = API_SLO)]
    // fn purge_logs_up_to(
    //     &self,
    //     cutoff: LogId,
    // ) -> Result<()> {
    //     debug!(?cutoff, "purge_logs_up_to");

    //     let cutoff = cutoff.index;

    //     let mut batch = LocalLogBatch::default();

    //     for result in self.tree.range(..safe_kv(cutoff + 1)) {
    //         let (key, _value) = result?;
    //         batch.remove(key.to_vec());
    //     }

    //     if let Err(e) = self.apply(&batch) {
    //         error!("purge_logs_up_to error: {}", e);
    //         return Err(StorageError::DbError(e.to_string()).into());
    //     }

    //     // Bugfix: #55: Flushing can take quite a lot of time,
    //     //  and you should measure the performance impact of using it
    //     //  on realistic sustained workloads running on realistic hardware.
    //     // self.tree.flush_async().await?;

    //     Ok(())
    // }
    #[autometrics(objective = API_SLO)]
    fn purge_logs_up_to(
        &self,
        cutoff: LogId,
    ) -> Result<()> {
        debug!(?cutoff, "purge_logs_up_to");

        let cutoff_index = cutoff.index;

        // Step 1: Remove purged entries from buffer
        {
            let mut entries_to_remove = Vec::new();
            for entry_ref in self.buffer.entries_cache.iter() {
                if entry_ref.index <= cutoff_index {
                    entries_to_remove.push(entry_ref.index);
                }
            }

            for index in entries_to_remove {
                self.buffer.entries_cache.remove(&index);
            }

            // Update durable index if needed
            let current_durable = self.buffer.durable_index.load(Ordering::Acquire);
            if cutoff_index > current_durable {
                self.buffer.durable_index.store(cutoff_index, Ordering::Release);
            }
        }

        // Step 2: Batch remove from disk storage
        let mut batch = LocalLogBatch::default();
        for result in self.tree.range(..safe_kv(cutoff_index + 1)) {
            let (key, _) = result?;
            batch.remove(key.to_vec());
        }

        // Step 3: Apply deletion asynchronously
        let tree = Arc::clone(&self.tree);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = tree.apply_batch(batch) {
                error!("Async purge failed: {:?}", e);
            }
        });

        // Step 4: Update buffer metadata
        let new_length = self.buffer.entries_cache.iter().map(|e| e.index).max().unwrap_or(0);

        self.buffer.length.store(new_length, Ordering::Release);
        self.buffer.next_id.store(new_length + 1, Ordering::Release);

        Ok(())
    }
    /// If there exists an N such that N >= commitIndex, a majority
    /// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex =
    /// N (§5.3, §5.4). @param Peer_id - not include leader id itself.
    ///
    /// return None, when majority_matched_index < current commit index or
    ///     the log term which index is majority_matched_index, doesn't equal to
    /// the current leader term.

    // #[autometrics(objective = API_SLO)]
    // fn calculate_majority_matched_index(
    //     &self,
    //     current_term: u64,
    //     commit_index: u64,
    //     mut peer_matched_ids: Vec<u64>, // only contain peer's matched index
    // ) -> Option<u64> {
    //     // Get the current term and commit index
    //     // let current_term = self.persistent_state().current_term(); // Leader's
    //     // current term let commit_index = self.commit_index(); // Leader's
    //     // current commit index
    //     let my_raft_log_last_index = self.last_entry_id();
    //     trace!("my_raft_log_last_index: {}", my_raft_log_last_index);

    //     // Collect match indices for all peers
    //     // let mut matched_ids: Vec<u64> = peer_ids
    //     //     .iter()
    //     //     .map(|&id| self.match_index(id).unwrap_or(0))
    //     //     .collect();
    //     // Include the leader's own last log index
    //     peer_matched_ids.push(my_raft_log_last_index);

    //     // Sort the indices in descending order
    //     peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

    //     // Calculate the majority index (floor of (n + 1) / 2)
    //     let majority_index = peer_matched_ids.len() / 2;

    //     // Get the potential majority-matched index
    //     let majority_matched_index = peer_matched_ids.get(majority_index)?;
    //     debug!("calculate_majority_matched_index:peer_matched_ids: {:?}. majority_index: {:?}, majority_matched_index: {:?}", peer_matched_ids, majority_index, majority_matched_index);
    //     // Check if this index satisfies the conditions:
    //     // 1. It's greater than or equal to the current commit index.
    //     // 2. The term of the log at this index equals the current term.
    //     if *majority_matched_index >= commit_index {
    //         if let Some(term) = self.get_entry_term(*majority_matched_index) {
    //             if term == current_term {
    //                 return Some(*majority_matched_index);
    //             }
    //         }
    //     }

    //     None
    // }

    #[autometrics(objective = API_SLO)]
    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        mut peer_matched_ids: Vec<u64>,
    ) -> Option<u64> {
        // Include leader's last index (from buffer)
        let leader_last_index = self.buffer.last_entry_id();
        peer_matched_ids.push(leader_last_index);

        // Sort in descending order
        peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

        // Calculate majority index
        let majority_index = peer_matched_ids.len() / 2;
        let majority_matched_index = *peer_matched_ids.get(majority_index)?;

        debug!(
            "Majority calculation: peers={:?}, majority_index={}, value={}",
            peer_matched_ids, majority_index, majority_matched_index
        );

        // Check commit conditions
        if majority_matched_index >= commit_index {
            // Use buffer's get_entry_term which checks both durable and pending entries
            if let Some(term) = self.entry_term(majority_matched_index) {
                if term == current_term {
                    return Some(majority_matched_index);
                }
            }
        }

        None
    }

    // fn flush(&self) -> Result<()> {
    //     match self.db.flush() {
    //         Ok(bytes) => {
    //             info!("Successfully flushed Local Log, bytes flushed: {}", bytes);
    //             println!("Successfully flushed Local Log, bytes flushed: {bytes}");
    //         }
    //         Err(e) => {
    //             error!("Failed to flush Local Log: {}", e);
    //             eprintln!("Failed to flush Local Log: {e}");
    //         }
    //     }
    //     Ok(())
    // }
    #[autometrics(objective = API_SLO)]
    fn flush(&self) -> Result<()> {
        // Trigger immediate flush of all pending entries
        let (tx, rx) = oneshot::channel();
        self.buffer
            .flush_sender
            .blocking_send(FlushCommand::Immediate(tx))
            .map_err(|_| StorageError::ChannelClosed)?;

        // Wait for flush completion
        rx.blocking_recv().map_err(|_| StorageError::ChannelClosed)?;

        // Now flush the underlying sled DB
        match self.db.flush() {
            Ok(bytes) => {
                info!("Successfully flushed DB, bytes written: {}", bytes);
                Ok(())
            }
            Err(e) => {
                error!("DB flush failed: {}", e);
                Err(StorageError::DbError(e.to_string()).into())
            }
        }
    }

    /// db_size_cache_duration - how long the cache will be valid (since last
    /// activity) #[deprecated]
    #[cfg(test)]
    #[autometrics(objective = API_SLO)]
    fn db_size(
        &self,
        _node_id: u32,
        _db_size_cache_window: u128,
    ) -> Result<u64> {
        // let db_size_cache = self.db_size_cache.clone();
        // if let Some(last_activity) = db_size_cache.last_activity.get(&node_id) {
        //     let diff_in_mil = now.duration_since(*last_activity).as_millis();
        //     println!("now: {:?}, last_activity: {:?}", now, last_activity);
        //     println!("diff: {}", diff_in_mil);
        //     if diff_in_mil <= db_size_cache_window {
        //         let cached_size = db_size_cache.size.load(Ordering::Acquire);
        //         debug!("retrieved db cached size: {}", cached_size);
        //         println!("retrieved db cached size: {}", cached_size);
        //         return Ok(cached_size);
        //     }
        // }

        // if nothing found in cache, we will query the size from the db directly
        match SledRaftLog::db_size(self.db.clone()) {
            Ok(size) => {
                // db_size_cache.size.store(size, Ordering::Release);
                // db_size_cache.last_activity.insert(node_id, now);
                debug!("retrieved the real db size: {size}",);
                println!("retrieved the real db size: {size}",);
                Ok(size)
            }
            Err(e) => {
                error!("db_size() failed: {e:?}");
                eprintln!("db_size() failed: {e:?}");
                Err(e)
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tree.len()
    }
}

impl SledRaftLog {
    ///performance stupid
    /// TODO
    #[autometrics(objective = API_SLO)]
    pub(self) fn db_size(db: Arc<sled::Db>) -> Result<u64> {
        //bug fix: do flush before calculating.
        db.flush()?;

        match db.size_on_disk() {
            Ok(size) => Ok(size),
            Err(e) => {
                error!("db.size_on_disk failed: {:?}", e);
                Err(StorageError::DbError(e.to_string()).into())
            }
        }
    }
    #[autometrics(objective = API_SLO)]
    pub fn new(
        node_id: u32,
        raft_log_db: Arc<sled::Db>,
        commit_index: u64,
    ) -> Self {
        match raft_log_db.open_tree(RAFT_LOG_NAMESPACE) {
            Ok(raft_log_tree) => {
                let disk_len = raft_log_tree.len() as u64;
                debug!("RaftLog disk length: {}", disk_len);
                println!("RaftLog disk length: {disk_len}");

                let l = SledRaftLog {
                    node_id,
                    db: raft_log_db,
                    tree: Arc::new(raft_log_tree),
                    buffer: BufferedRaftLog::new(disk_len),
                };
                l.load_uncommitted_from_db_to_cache(commit_index, disk_len);
                l
            }
            Err(e) => panic!("failed to open sled tree: {:?}", e),
        }
    }

    // #[autometrics(objective = API_SLO)]
    // fn last_entry(&self) -> Option<(u64, Vec<u8>)> {
    //     debug!("getting last entry as matched index");
    //     if let Ok(Some((key, value))) = self.tree.last() {
    //         let key: u64 = vki(&key);
    //         let value = value.to_vec();
    //         trace!("last entry, key: {:?}, value: {:?}", &key, &value);

    //         Some((key, value))
    //     } else {
    //         debug!("no last entry found");
    //         None
    //     }
    // }

    // #[autometrics(objective = API_SLO)]
    // fn first_entry(&self) -> Option<(u64, Vec<u8>)> {
    //     debug!("getting first entry as matched index");
    //     if let Ok(Some((key, value))) = self.tree.first() {
    //         let key: u64 = vki(&key);
    //         let value = value.to_vec();
    //         debug!("first entry, key: {:?}, value: {:?}", &key, &value);

    //         Some((key, value))
    //     } else {
    //         debug!("no first entry found");
    //         None
    //     }
    // }

    // #[autometrics(objective = API_SLO)]
    // fn get(
    //     &self,
    //     key: u64,
    // ) -> Option<Entry> {
    //     if let Some(v) = self.get_from_cache(key) {
    //         return Some(v);
    //     }
    //     if let Ok(Some(bytes)) = self.tree.get(safe_kv(key)) {
    //         if let Ok(e) = Entry::decode(&*bytes) {
    //             return Some(e);
    //         } else {
    //             error!("get():: decode error!");
    //         }
    //     }
    //     return None;
    // }

    // fn get_as_vec(&self, key: u64) -> Option<Vec<u8>> {
    //     if let Some(ivec) = self.get(key).unwrap() {
    //         Some(ivec.to_vec())
    //     } else {
    //         None
    //     }
    // }

    ///Start and End might not within the local log length
    /// $start = get_last_synced_commit_index()
    #[autometrics(objective = API_SLO)]
    fn get_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Vec<Entry> {
        let start = safe_kv(*range.start());
        let end = safe_kv(*range.end());

        self.tree
            .range(start..=end)
            .filter_map(|res| res.ok().and_then(|(_, v)| Entry::decode(&*v).ok()))
            .collect()
    }

    #[autometrics(objective = API_SLO)]
    fn remove(
        &self,
        key: u64,
    ) -> sled::Result<Option<IVec>> {
        self.tree.remove(safe_kv(key))
    }
    #[autometrics(objective = API_SLO)]
    fn clear(&self) -> sled::Result<()> {
        info!("Local raft log been cleared.");
        self.tree.clear()
    }

    // /// @Write - For Leader
    // #[autometrics(objective = API_SLO)]
    // fn insert(&self, key: u64, value: Entry) -> sled::Result<Option<IVec>> {
    //     let k = safe_kv(key);
    //     debug!("insert local log: key: {:?} - value: {:?}", k, value);
    //     let r = self.tree.insert(k.clone(), value.encode_to_vec());
    //     if let Err(ref e) = r {
    //         error!("insert error: {:?}", e);
    //     } else {
    //         let mut batch = LocalLogBatch::default();
    //         batch.insert(k, value);
    //         self.post_write_actions(&batch);
    //     }
    //     return r;
    // }

    /// @Write - For Followers
    /// No matter it is insert or remove, both of them are client commands
    /// e.g. if we `insert log_1` and then `remove log_1`, then the log length
    /// should not be changed at all; e.g. if we `insert log_2` and then
    /// `remove log_1`, then the log length will be 2 if there is no log_1 in
    /// SLED,     or log length will be 1 if there is log_1 entry in the
    /// SLED
    ///
    /// RAFT Leader log principle: No leader log entry could be deleted.
    /// RAFT Follower log principle: if there is conflict entry in follower log,
    ///     we should delete them by following leader log as single source of
    /// truth.
    #[autometrics(objective = API_SLO)]
    pub(self) fn apply(
        &self,
        batch: &LocalLogBatch,
    ) -> sled::Result<()> {
        debug!("apply local log batch length = {:?}", batch.writes.len());

        let mut b = Batch::default();

        for (key, value) in &batch.writes {
            let k: &[u8] = key.as_ref(); // avoid cloning, use reference
            if let Some(v) = value {
                // Performance bug fix: remove value clone.
                // let v = v.clone();
                b.insert(k, v);
                trace!("batch insert local log: key: {:?} - value: {:?}", k, v);
            } else {
                b.remove(k);
                debug!("remove local log: key: {:?}", k);
            }
        }
        let r = self.tree.apply_batch(b);
        if let Err(ref e) = r {
            error!("apply with error: {:?}", e);
        } else {
            debug!("apply successfully!");
            if let Err(e) = self.post_write_actions(batch) {
                error!("post write actions with error: {:?}", e);
            }
        }
        return r;
    }

    #[autometrics(objective = API_SLO)]
    fn post_write_actions(
        &self,
        batch: &LocalLogBatch,
    ) -> Result<()> {
        self.buffer.refresh(batch, self.last_entry_id())
    }

    #[autometrics(objective = API_SLO)]
    pub fn recover(&self) {
        let disk_len = self.tree.len() as u64;
        let durable_index = disk_len;

        // Load all disk entries into cache
        for entry in self.get_entries_range(1..=disk_len) {
            self.buffer.pending_entries.insert(entry.index, entry);
        }

        // Reset state
        self.buffer.durable_index.store(durable_index, Ordering::Release);
        self.buffer.next_id.store(durable_index + 1, Ordering::Release);

        // Rebuild flush state
        let mut state = self.buffer.flush_state.blocking_lock();
        state.pending_flush.clear();
        state.flushing = false;
    }
}
