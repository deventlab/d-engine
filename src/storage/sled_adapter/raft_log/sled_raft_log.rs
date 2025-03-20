//! It works as RAFT storage layer
//!
//! For the get operation, we should check it from memory cache firslty.
//! the reason, we could catch the entry in memory is because:
//!     no entry could be deleted or modified in RAFT.
//!
use crate::storage::sled_adapter::RAFT_LOG_NAMESPACE;
use crate::{grpc::rpc_service::Entry, API_SLO, MESSAGE_SIZE_IN_BYTES_METRIC};
use crate::{
    util::{kv, vki},
    Error, LocalLogBatch, RaftLog, Result,
};
use autometrics::autometrics;
use dashmap::DashMap;
use log::{debug, error, info, trace};
use prost::Message;
use sled::{Batch, IVec, Subscriber};
use std::{
    mem,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::time::Instant;
use tonic::async_trait;

use log::warn;

///To check DB size is costy operation.
/// We need to cache it
#[derive(Debug)]
pub(self) struct CachedDbSize {
    size: AtomicU64,
    last_activity: DashMap<u64, Instant>, //<node_id, ms>
}

#[derive(Debug)]
pub struct SledRaftLog {
    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
    cache: RaftLogMemCache,
    // db_size_cache: Arc<CachedDbSize>,
}

/// This data structure is designed to improve read and write performance.
/// It has to be refreshed when there is a write
///
#[derive(Debug)]
struct RaftLogMemCache {
    length: AtomicU64,
    mapped_entries: DashMap<Vec<u8>, Entry>, //mapped from disk
    next_id: AtomicU64, //because of the batch apply feature, we have to pre allocated some entry index before insert successfully
}

impl RaftLogMemCache {
    fn refresh(&self, new_disk_entries: &LocalLogBatch, disk_len: u64) {
        for (key, value) in &new_disk_entries.writes {
            let k = key.clone();
            if let Some(v) = value {
                let v = v.clone();
                self.mapped_entries.insert(k.clone(), v);
            } else {
                self.mapped_entries.remove(&k);
            }
        }
        self.length.store(disk_len, Ordering::Release);
        self.next_id.store(disk_len + 1, Ordering::Release);
    }
}
//sized traits are not object-safe:https://doc.rust-lang.org/reference/items/traits.html#object-safety

#[async_trait]
impl RaftLog for SledRaftLog {
    /// This function can not be used to decide last local log entry logically
    ///  because it might be cleaned due to snapshot generating or
    ///  it might be the learner who has already applied the snapshot.
    ///
    #[autometrics(objective = API_SLO)]
    fn last_entry_id(&self) -> u64 {
        if let Some(v) = self.last_entry() {
            v.0
        } else {
            0
        }
    }

    #[autometrics(objective = API_SLO)]
    fn first_entry_id(&self) -> u64 {
        if let Some(v) = self.first_entry() {
            v.0
        } else {
            0
        }
    }

    #[autometrics(objective = API_SLO)]
    fn span_between_first_entry_and_last_entry(&self) -> u64 {
        let last = self.last_entry_id();
        let first = self.first_entry_id();
        if last < first {
            error!(
                "local log: last entry:{} < first entry:{}'s id",
                last, first
            );
            return 0;
        } else {
            debug!(
                "local log: last entry:{} >= first entry:{}'s id",
                last, first
            );
        }

        return last - first + 1;
    }

    #[autometrics(objective = API_SLO)]
    fn pre_allocate_raft_logs_next_index(&self) -> u64 {
        // self.get_raft_logs_length() + 1
        self.cache.next_id.fetch_add(1, Ordering::SeqCst)
    }

    #[autometrics(objective = API_SLO)]
    fn last(&self) -> Option<Entry> {
        if let Some(pair) = self.last_entry() {
            if let Ok(e) = Entry::decode(&*pair.1) {
                return Some(e);
            } else {
                error!("last decode error!");
            }
        }
        return None;
    }

    #[autometrics(objective = API_SLO)]
    fn get_entry_by_index(&self, index: u64) -> Option<Entry> {
        self.get(index)
    }

    /// start is 'get_last_synced_commit_index', it might be zero
    /// Note: start: start_entry_id
    #[autometrics(objective = API_SLO)]
    fn get_entries_between(&self, range: RangeInclusive<u64>) -> Vec<Entry> {
        self.get_range(range)
    }

    #[autometrics(objective = API_SLO)]
    fn get_entry_term_by_index(&self, index: u64) -> Option<u64> {
        let e = self.get_entry_by_index(index);
        e.map(|entry| entry.term)
    }

    /// TODO: process duplicated `new_ones`
    ///
    /// If an existing entry conflicts with a new one (same index
    ///     but different terms), delete the existing entry and all that
    ///     follow it (§5.3)
    ///
    /// @return: last_matched_id
    #[autometrics(objective = API_SLO)]
    fn filter_out_conflicts_and_append(
        &self,
        _prev_log_index: u64,
        mut new_ones: Vec<Entry>,
    ) -> u64 {
        debug!("Start filter_out_conflicts_and_append");

        let mut update = false;
        let mut batch = LocalLogBatch::default();
        let last_raft_log_entry_id = self.last_entry_id();
        debug!(
            "filter_out_conflicts_and_append/local log length: {:?}",
            last_raft_log_entry_id
        );

        while !new_ones.is_empty() {
            let new = new_ones.remove(0);
            let new_index = new.index;
            if new_index < last_raft_log_entry_id {
                if let Some(entry) = self.get_entry_by_index(new_index) {
                    if entry.term != new.term {
                        debug!("entry.term({}) != new.term({})", entry.term, new.term);
                        for key_to_remove in new_index - 1..(last_raft_log_entry_id + 1) {
                            batch.remove(kv(key_to_remove));
                        }
                    }
                } else {
                    continue;
                }
            }

            batch.insert(kv(new.index), new);
            update = true;
        }

        if update {
            if let Err(e) = self.apply(&batch) {
                error!("local insert commit entry into kv store failed: {:?}", e);
            }
        }

        if let Some(last) = self.last_entry() {
            last.0
        } else {
            0
        }
    }

    // fn prev_log_index(&self, follower_id: u32, next_index: u64) -> u64 {
    //     let next_id = self.next_index(follower_id).await;
    //     if next_id > 0 {next_id - 1} else {0}

    // }

    #[autometrics(objective = API_SLO)]
    fn prev_log_term(&self, _follower_id: u32, prev_log_index: u64) -> u64 {
        // let id = self.prev_log_index(follower_id).await;
        if let Some(t) = self.get_entry_term_by_index(prev_log_index) {
            t
        } else {
            0
        }
    }

    /// used as binary rpc communication
    #[autometrics(objective = API_SLO)]
    fn retrieve_one_entry_for_this_follower(&self, follower_id: u32, next_id: u64) -> Vec<u8> {
        if let Some(log) = self.get_entry_by_index(next_id) {
            log.encode_to_vec()
        } else {
            Vec::new()
        }
    }

    /// About checking duplicates. D-Engine will treat every client command as unique command,
    /// maybe from client business logic view, there are duplicated commands.
    /// But D-Engine make sure the command event will be applied in sequence so the event the same client
    /// command will not convert two multi entries, it will only be entry, but might be converted several times
    /// because D-Engine receive duplicated command several times.
    ///
    /// I think this is D-Engine pricinple: Only be event recorder.
    ///
    /// In RAFT this function will only be used by leader.
    ///
    // #[autometrics(objective = API_SLO)]
    // fn insert_client_commands(
    //     &self,
    //     term: u64,
    //     commands: Vec<ClientCommand>,
    //     leader_raft_log_batch_sender: mpsc::UnboundedSender<(u64, ClientCommand)>,
    // ) -> Result<()> {
    //     for c in commands {
    //         if let Err(e) = leader_raft_log_batch_sender.send((term, c)) {
    //             error!("insert_client_commands with e: {:?}", e);
    //             return Err(ErrorType::IOError);
    //         }
    //     }
    //     Ok(())
    // }

    #[autometrics(objective = API_SLO)]
    fn insert_batch(&self, logs: Vec<Entry>) -> Result<()> {
        debug!("starting insert_client_batch_commands...");

        let mut batch = LocalLogBatch::default();
        for entry in logs.into_iter() {
            let index = entry.index;
            let size = mem::size_of_val(&entry);

            batch.insert(kv(index), entry);
            //-------------------
            //performance testing: /*
            MESSAGE_SIZE_IN_BYTES_METRIC
                .with_label_values(&[&index.to_string()])
                .observe(size as f64);
            //performance testing: */
        }
        if let Err(e) = self.apply(&batch) {
            error!("apply batch error: {:?}", e);
            return Err(Error::GeneralLocalLogIOError);
        }
        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    fn prev_log_ok(
        &self,
        req_prev_log_index: u64,
        req_prev_log_term: u64,
        last_applied: u64,
    ) -> bool {
        debug!(
            "start checking if prev_log_ok: req_prev_log_index: {:?}, req_prev_log_term: {:?}",
            req_prev_log_index, req_prev_log_term
        );

        // Cache the last local log entry ID to avoid multiple calls
        let last_raft_log_entry_id = self.last_entry_id();

        //unexpected
        if req_prev_log_index > 0 && last_raft_log_entry_id < 1 {
            error!(
                "prev_log_ok(&self, req_prev_log_index: {}, req_prev_log_term: {},) failed.",
                req_prev_log_index, req_prev_log_term,
            );
            return false;
        }

        // Early exit for the case when req_prev_log_index is zero
        if req_prev_log_index == 0 {
            debug!("req_prev_log_index == 0, return true");
            return true;
        }

        // Check if the index is within bounds
        if req_prev_log_index > 0 && req_prev_log_index <= last_raft_log_entry_id {
            // Use pattern matching to handle Option more efficiently
            if let Some(matched_raft_log_entry) = self.get_entry_by_index(req_prev_log_index) {
                // Check term directly from the matched log entry
                if req_prev_log_term == matched_raft_log_entry.term {
                    debug!("prev_log_ok return true.");
                    return true;
                } else {
                    warn!(
                        "req_prev_log_term({}) != matched_raft_log_entry.term({})",
                        req_prev_log_term, matched_raft_log_entry.term
                    );
                }
            } else {
                warn!(
                    "can not find entry by index({}) in my local log.",
                    req_prev_log_index
                );
            }
        }

        // Log details only when returning false
        debug!(
            "prev_log_ok return false. local log length: {:?}, req_prev_log_index: {:?}, req_prev_log_term: {:?}",
            last_raft_log_entry_id,
            req_prev_log_index,
            req_prev_log_term
        );

        false
    }

    // #[autometrics(objective = API_SLO)]
    // fn prev_log_ok_2(&self, req_prev_log_index: u64, req_prev_log_term: u64) -> bool {
    //     debug!(
    //         "start checking if prev_log_ok: req_prev_log_index: {:?}, req_prev_log_term: {:?}",
    //         req_prev_log_index, req_prev_log_term
    //     );
    //     let matched_raft_log_entry = self.get_entry_by_index(req_prev_log_index);
    //     let mlle_clone = matched_raft_log_entry.clone();
    //     debug!("matched_raft_log_entry: {:?}", matched_raft_log_entry);

    //     if req_prev_log_index == 0 {
    //         debug!("req_prev_log_index == 0, return true");
    //         //req_prev_log_index means it is first entry of this leader append request.
    //         return true;
    //     } else if req_prev_log_index > 0
    //         && req_prev_log_index <= self.last_entry_id()
    //         && matched_raft_log_entry.is_some()
    //         && req_prev_log_term == matched_raft_log_entry.unwrap().term
    //     {
    //         //means we found conflict logs inside this follower's local log since index: req_prev_log_index
    //         // we will do filter conflict operation later.
    //         debug!("prev_log_ok return true.");
    //         return true;
    //     }
    //     debug!(
    //         "prev_log_ok return false. local log length: {:?}, matched_raft_log_entry: {:?}",
    //         self.last_entry_id(),
    //         mlle_clone
    //     );

    //     false
    // }

    #[autometrics(objective = API_SLO)]
    fn reset(&self) -> Result<()> {
        if let Err(e) = self.tree.clear() {
            error!("error: {:?}", e);
            Err(Error::NotFound)
        } else {
            Ok(())
        }
    }

    #[autometrics(objective = API_SLO)]
    fn retrieve_subscriber(&self, watch_key: &Vec<u8>) -> Subscriber {
        self.tree.watch_prefix(watch_key)
    }

    #[autometrics(objective = API_SLO)]
    fn delete_entries_before(&self, last_applied: u64) -> Result<()> {
        let mut batch = LocalLogBatch::default();

        for result in self.tree.range(..kv(last_applied + 1)) {
            let (key, _value) = result?;
            batch.remove(key.to_vec());
        }

        if let Err(e) = self.apply(&batch) {
            error!("delete_entries_before error: {}", e);
            return Err(Error::SledError(e));
        }

        // Bugfix: #55: Flushing can take quite a lot of time,
        //  and you should measure the performance impact of using it
        //  on realistic sustained workloads running on realistic hardware.
        // self.tree.flush_async().await?;

        Ok(())
    }

    /// Load entries from disk to memory.
    #[autometrics(objective = API_SLO)]
    fn load_uncommitted_from_db_to_cache(&self, commit_id: u64, len: u64) {
        info!(
            "load_uncommitted_from_db_to_cache, commit_id: {}, len: {} ",
            commit_id, len
        );
        for l in self.get_entries_between((commit_id + 1)..=len) {
            self.cache.mapped_entries.insert(l.index.encode_to_vec(), l);
        }
    }

    #[autometrics(objective = API_SLO)]
    fn get_from_cache(&self, key: &Vec<u8>) -> Option<Entry> {
        if let Some(v) = self.cache.mapped_entries.get(key) {
            Some(v.clone())
        } else {
            None
        }
    }
    /// If there exists an N such that N >= commitIndex, a majority
    /// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
    /// @param Peer_id - not include leader id itself.
    ///
    /// return None, when majority_matched_index < current commit index or
    ///     the log term which index is majority_matched_index, doesn't equal to the current leader term.
    ///
    #[autometrics(objective = API_SLO)]
    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        mut peer_matched_ids: Vec<u64>, // only contain peer's matched index
    ) -> Option<u64> {
        // Get the current term and commit index
        // let current_term = self.persistent_state().current_term(); // Leader's current term
        // let commit_index = self.commit_index(); // Leader's current commit index
        let my_raft_log_last_index = self.last_entry_id();
        trace!("my_raft_log_last_index: {}", my_raft_log_last_index);

        // Collect match indices for all peers
        // let mut matched_ids: Vec<u64> = peer_ids
        //     .iter()
        //     .map(|&id| self.match_index(id).unwrap_or(0))
        //     .collect();
        // Include the leader's own last log index
        peer_matched_ids.push(my_raft_log_last_index);

        // Sort the indices in descending order
        peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

        // Calculate the majority index (floor of (n + 1) / 2)
        let majority_index = (peer_matched_ids.len() / 2) as usize;

        // Get the potential majority-matched index
        let majority_matched_index = peer_matched_ids.get(majority_index)?;
        debug!("calculate_majority_matched_index:peer_matched_ids: {:?}. majority_index: {:?}, majority_matched_index: {:?}", peer_matched_ids, majority_index, majority_matched_index);
        // Check if this index satisfies the conditions:
        // 1. It's greater than or equal to the current commit index.
        // 2. The term of the log at this index equals the current term.
        if *majority_matched_index >= commit_index {
            if let Some(term) = self.get_entry_term_by_index(*majority_matched_index) {
                if term == current_term {
                    return Some(*majority_matched_index);
                }
            }
        }

        None
    }

    fn flush(&self) -> Result<()> {
        match self.db.flush() {
            Ok(bytes) => {
                info!("Successfully flushed Local Log, bytes flushed: {}", bytes);
                println!("Successfully flushed Local Log, bytes flushed: {}", bytes);
            }
            Err(e) => {
                error!("Failed to flush Local Log: {}", e);
                eprintln!("Failed to flush Local Log: {}", e);
            }
        }
        Ok(())
    }

    /// db_size_cache_duration - how long the cache will be valid (since last activity)
    /// #[deprecated]
    #[cfg(test)]
    #[autometrics(objective = API_SLO)]
    fn db_size(&self, node_id: u32, db_size_cache_window: u128) -> Result<u64> {
        let now = Instant::now();
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
                debug!("retrieved the real db size: {}", size);
                println!("retrieved the real db size: {}", size);
                Ok(size)
            }
            Err(e) => {
                error!("db_size() failed: {:?}", e);
                eprintln!("db_size() failed: {:?}", e);
                Err(e)
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tree.len()
    }

    // #[autometrics(objective = API_SLO)]
    // fn load_from_db(&self, commit_index: u64) {
    //     //step1: load local log length
    //     let len = self.last_entry_id();
    //     debug!("load local log length from database: {:?}", len);
    //     if let Some(l) = &self.cache.length {
    //         l.store(len as u64, Ordering::Release);
    //     } else {
    //         error!("local log length is not inited yet");
    //     }

    //     //step3: load any uncommited entries into Cache
    //     debug!(
    //         "load_from_db, commit_id: {:?}, len: {:?}",
    //         commit_index, len
    //     );
    //     self.;
    // }

    #[cfg(test)]
    fn delete_entries(&self, range: RangeInclusive<u64>) -> Result<()> {
        let mut batch = LocalLogBatch::default();

        for key in range {
            batch.remove(key.encode_to_vec());
        }

        if let Err(e) = self.apply(&batch) {
            error!("delete_entries error: {}", e);
            return Err(Error::SledError(e));
        }

        Ok(())
    }

    #[cfg(test)]
    fn cached_length(&self) -> u64 {
        self.cache.length.load(Ordering::Acquire)
    }
    #[cfg(test)]
    fn cached_mapped_entries_len(&self) -> usize {
        self.cache.mapped_entries.len()
    }
    #[cfg(test)]
    fn cached_next_id(&self) -> u64 {
        self.cache.next_id.load(Ordering::Acquire)
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
                Err(Error::SledError(e))
            }
        }
    }
    #[autometrics(objective = API_SLO)]
    pub(crate) fn new(raft_log_db: Arc<sled::Db>, commit_index: Option<u64>) -> Self {
        match raft_log_db.open_tree(RAFT_LOG_NAMESPACE) {
            Ok(raft_log_tree) => {
                let disk_len = raft_log_tree.len() as u64;
                debug!("RaftLog disk length: {}", disk_len);
                println!("RaftLog disk length: {}", disk_len);

                let l = SledRaftLog {
                    db: raft_log_db,
                    tree: Arc::new(raft_log_tree),
                    cache: RaftLogMemCache {
                        length: AtomicU64::new(disk_len),
                        mapped_entries: DashMap::new(),
                        next_id: AtomicU64::new(disk_len + 1),
                    },
                };
                l.load_uncommitted_from_db_to_cache(commit_index.unwrap_or(0), disk_len);
                l
            }
            Err(e) => {
                error!("Failed to open logs db: {}", e);
                panic!("failed to open sled tree: {}", e);
            }
        }
    }

    #[autometrics(objective = API_SLO)]
    fn last_entry(&self) -> Option<(u64, Vec<u8>)> {
        debug!("getting last entry as matched index");
        if let Ok(Some((key, value))) = self.tree.last() {
            let key: u64 = vki(&key);
            let value = value.to_vec();
            debug!("last entry, key: {:?}, value: {:?}", &key, &value);

            Some((key, value))
        } else {
            debug!("no last entry found");
            None
        }
    }

    #[autometrics(objective = API_SLO)]
    fn first_entry(&self) -> Option<(u64, Vec<u8>)> {
        debug!("getting first entry as matched index");
        if let Ok(Some((key, value))) = self.tree.first() {
            let key: u64 = vki(&key);
            let value = value.to_vec();
            debug!("first entry, key: {:?}, value: {:?}", &key, &value);

            Some((key, value))
        } else {
            debug!("no first entry found");
            None
        }
    }

    #[autometrics(objective = API_SLO)]
    fn get(&self, key: u64) -> Option<Entry> {
        let k = kv(key);
        if let Some(v) = self.get_from_cache(&k) {
            return Some(v);
        }
        if let Ok(Some(bytes)) = self.tree.get(k) {
            if let Ok(e) = Entry::decode(&*bytes) {
                return Some(e);
            } else {
                error!("get():: decode error!");
            }
        }
        return None;
    }

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
    fn get_range(&self, range: RangeInclusive<u64>) -> Vec<Entry> {
        let mut r = Vec::new();

        for i in range {
            if let Some(buffer) = self.get(i) {
                r.push(buffer);
            } else {
                warn!("get_range() item can not found: {:?}", i);
            }
        }
        return r;
    }

    #[autometrics(objective = API_SLO)]
    fn remove(&self, key: u64) -> sled::Result<Option<IVec>> {
        self.tree.remove(kv(key))
    }
    #[autometrics(objective = API_SLO)]
    fn clear(&self) -> sled::Result<()> {
        self.tree.clear()
    }

    // /// @Write - For Leader
    // #[autometrics(objective = API_SLO)]
    // fn insert(&self, key: u64, value: Entry) -> sled::Result<Option<IVec>> {
    //     let k = kv(key);
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
    /// e.g. if we `insert log_1` and then `remove log_1`, then the log length should not be changed at all;
    /// e.g. if we `insert log_2` and then `remove log_1`, then the log length will be 2 if there is no log_1 in SLED,
    ///     or log length will be 1 if there is log_1 entry in the SLED
    ///
    /// RAFT Leader log principle: No leader log entry could be deleted.
    /// RAFT Follower log principle: if there is conflict entry in follower log,
    ///     we should delete them by following leader log as single source of truth.
    ///
    #[autometrics(objective = API_SLO)]
    pub(self) fn apply(&self, batch: &LocalLogBatch) -> sled::Result<()> {
        let mut b = Batch::default();

        for (key, value) in &batch.writes {
            let k = key.clone();
            if let Some(v) = value {
                // Performance bug fix: remove value clone.
                // let v = v.clone();
                b.insert(k.clone(), v.encode_to_vec());
                debug!("batch insert local log: key: {:?} - value: {:?}", k, v);
            } else {
                b.remove(k.clone());
                debug!("remove local log: key: {:?}", k);
            }
        }
        let r = self.tree.apply_batch(b);
        if let Err(ref e) = r {
            error!("apply with error: {:?}", e);
        } else {
            debug!("apply successfully!");
            self.post_write_actions(batch);
        }
        return r;
    }

    #[autometrics(objective = API_SLO)]
    fn post_write_actions(&self, batch: &LocalLogBatch) {
        self.cache.refresh(&batch, self.last_entry_id());
    }
}
