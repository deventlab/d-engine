//! Direct, single-threaded implementation of `RaftLog`: `persist_entries`/
//! `replace_range`/`purge`/`reset` execute inline on the caller's own task —
//! no dedicated OS thread, no command channel. Only the physical fsync is
//! handed off, to `FsyncWorker`'s own execution context (see fsync_worker.rs).

use crate::{
    Error, HardState, InternalEvent, LogStore, MetaStore, NetworkError, RaftLog, Result,
    StorageEngine, TypeConfig, alias::SOF, fsync_worker::FsyncWorker, scoped_timer::ScopedTimer,
};
use crossbeam_skiplist::SkipMap;
use d_engine_proto::common::{Entry, LogId};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    ops::RangeInclusive,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tonic::async_trait;
use tracing::{debug, error, warn};

pub struct RaftLogCore<T>
where
    T: TypeConfig,
{
    node_id: u32,

    pub(crate) log_store: Arc<<SOF<T> as StorageEngine>::LogStore>,
    pub(crate) meta_store: Arc<<SOF<T> as StorageEngine>::MetaStore>,

    /// Max time `close()` waits for a final flush before giving up. The
    /// in-flight fsync task itself is never cancelled — it keeps running in
    /// the background regardless; this only bounds how long the caller waits.
    shutdown_timeout_ms: u64,

    // --- In-memory state ---
    // The Raft log itself: every entry this node currently holds in memory.
    // This is the single source of truth — every other field below is either
    // a boundary marker or a lookup accelerator derived from what's here.
    pub(crate) entries: RwLock<SkipMap<u64, Entry>>,

    // How far the log has actually been made crash-safe (fsynced to disk).
    // Raft must not tell a client or a peer a write is safe ahead of this point,
    // regardless of what's already visible in `entries`.
    pub(crate) durable_index: AtomicU64,

    // How far entries have been written to the log store (page cache, not
    // yet fsynced) — the frontier `persist_entries` scans forward from.
    // Advance with `fetch_max`, never a plain store: a slower concurrent
    // call must not regress a value a faster call already advanced past.
    pub(crate) persisted_index: AtomicU64,

    // The next index to be allocated
    pub(crate) next_id: AtomicU64,

    // --- In-memory index ---
    // O(1) answer to "is this index currently held in memory" — lets callers
    // (e.g. entry_term()) reject an out-of-range index without touching `entries`.
    min_index: AtomicU64,        // Smallest log index (0 if empty)
    memory_max_index: AtomicU64, // Largest log index held in memory (0 if empty) — may be ahead of what's persisted/durable

    // The term of the last entry ever purged (compacted away after a snapshot).
    // Raft's AppendEntries consistency check needs the term at prev_log_index
    // even when that entry no longer physically exists in `entries` — without
    // this, a follower can't tell "purged, but we agree" apart from "conflict".
    //
    // Must be published in the same critical section as the entries removal
    // and the min_index/memory_max_index advance it corresponds to — a reader must
    // never be able to observe the entry gone but this boundary not yet set.
    last_purged_index: AtomicU64,
    last_purged_term: AtomicU64,

    // Where each term starts/ends. Lets a leader/follower jump straight to a
    // term boundary during post-election conflict backtracking, instead of
    // walking the log entry by entry to find where history diverged.
    term_first_index: SkipMap<u64, AtomicU64>,
    term_last_index: SkipMap<u64, AtomicU64>,

    // Same question as term_first/last_index ("what term is at index i"), but
    // optimized for the by-far most common case: a query near the current tail
    // of the log, asked on essentially every AppendEntries in normal replication.
    term_segments: TermSegments,

    // --- P0: LogFlushed event notification ---
    // Sends InternalEvent::LogFlushed(durable) to Raft loop after each fsync.
    // None in tests; Some(internal_event_tx) in production.
    internal_event_tx: Option<mpsc::UnboundedSender<crate::InternalEvent>>,

    fsync_worker: Arc<FsyncWorker<<SOF<T> as StorageEngine>::LogStore>>,

    // set once on first fsync failure, never cleared for this instance's lifetime
    pub(super) poisoned: Arc<AtomicBool>,
}

#[async_trait]
impl<T> RaftLog for RaftLogCore<T>
where
    T: TypeConfig,
{
    ///TODO: not considered the order of configured storage rule
    /// also should we remove Result<>?
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        Ok(self.entries.read().get(&index).map(|e| e.value().clone()))
    }

    fn first_entry_id(&self) -> u64 {
        self.min_index.load(Ordering::Acquire)
    }

    fn last_entry_id(&self) -> u64 {
        self.memory_max_index.load(Ordering::Acquire)
    }

    fn durable_index(&self) -> u64 {
        self.durable_index.load(Ordering::Acquire)
    }

    fn last_entry(&self) -> Option<Entry> {
        let last_index = self.last_entry_id();
        if last_index > 0 {
            self.entry(last_index).ok().flatten()
        } else {
            None
        }
    }

    // #446: this is what election-eligibility comparisons (is_target_log_more_recent)
    // read. It must keep reflecting the in-memory tail, never durable_index — a node
    // with an un-fsynced entry must still be able to reject a less-up-to-date candidate.
    fn last_log_id(&self) -> Option<LogId> {
        let last_index = self.last_entry_id();
        if last_index > 0 {
            self.entry(last_index).ok().flatten().map(|entry| LogId {
                term: entry.term,
                index: entry.index,
            })
        } else {
            // Log is empty (e.g. immediately after snapshot install).
            // Return the snapshot boundary so the leader sees the correct
            // last-log position and sends entries starting from index+1.
            let purged_index = self.last_purged_index.load(Ordering::Acquire);
            if purged_index > 0 {
                Some(LogId {
                    index: purged_index,
                    term: self.last_purged_term.load(Ordering::Acquire),
                })
            } else {
                None
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        // Bounds check: skip TermSegments entirely for out-of-range queries.
        let max = self.memory_max_index.load(Ordering::Acquire);
        let min = self.min_index.load(Ordering::Acquire);
        if max == 0 || entry_id < min || entry_id > max {
            // Cold path: check purge boundary so that AppendEntries built with
            // prev_log_index == last_purged_index carries the correct term.
            let purged_index = self.last_purged_index.load(Ordering::Acquire);
            if purged_index > 0 && entry_id == purged_index {
                return Some(self.last_purged_term.load(Ordering::Acquire));
            }
            return None;
        }
        // Hot path: TermSegments — O(1) for current segment, O(log k) for history.
        // No SkipMap lookup, no epoch::pin(), no CAS.
        if let Some(term) = self.term_segments.get(entry_id) {
            return Some(term);
        }
        // Fallback: SkipMap — guards against rare transient inconsistency.
        self.entries.read().get(&entry_id).map(|e| e.value().term)
    }

    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.term_first_index.get(&term).map(|e| e.value().load(Ordering::Acquire))
    }

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.term_last_index.get(&term).map(|e| e.value().load(Ordering::Acquire))
    }

    fn pre_allocate_raft_logs_next_index(&self) -> u64 {
        // self.get_raft_logs_length() + 1
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64> {
        match count {
            0 => u64::MAX..=u64::MAX, // Standard empty range
            _ => {
                // Overflow checking (enable on demand)
                let cur = self.next_id.load(Ordering::SeqCst);
                assert!(cur <= u64::MAX - count, "ID overflow");

                let start = self.next_id.fetch_add(count, Ordering::SeqCst);
                start..=(start + count - 1)
            }
        }
    }

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        let entries = self.entries.read();

        // OPTIMIZED: SkipMap range scan O(k + log n); pre-allocate to avoid realloc
        let capacity =
            (range.end().saturating_sub(*range.start()) + 1).min(entries.len() as u64) as usize;
        let mut result = Vec::with_capacity(capacity);

        result.extend(entries.range(range).map(|e| e.value().clone()));
        Ok(result)
    }

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        // Fast-fail optimization, not a correctness gate — the real durability
        // boundary is persist_pending_range()/fsync_worker. Poisoned data
        // reaching memory here is harmless as long as it never gets marked durable.
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }

        let _timer = ScopedTimer::new("append_entries");
        if entries.is_empty() {
            return Ok(());
        }

        self.insert_to_memory(&entries);

        // A write landed. Persist the new tail and submit it. No queue
        // drain: a burst's redundant `Persist`s find `persisted_index`
        // already at `memory_max_index` and no-op here (#446).
        let start = self.persisted_index.load(Ordering::Acquire) + 1;
        let end = self.memory_max_index.load(Ordering::Acquire);
        match self.persist_pending_range(start, end, "persist").await {
            Ok(Some(mark)) => {
                self.persisted_index.fetch_max(mark.index, Ordering::AcqRel);
                self.fsync_worker.submit(mark, Vec::new());
            }
            Ok(None) => {}
            Err(e) => return Err(e), // poisoned — same exit convention as the mutations below
        }

        Ok(())
    }

    async fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()> {
        self.append_entries(logs).await?;
        Ok(())
    }

    async fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>> {
        let _timer = ScopedTimer::new("filter_out_conflicts_and_append");

        // prev_log_index==0 has no real entry to compare against, not a reset signal
        let is_virtual_log_start = prev_log_index == 0 && prev_log_term == 0;
        if !is_virtual_log_start {
            // Check log consistency: use entry_term() so purge-boundary entries
            // (entries removed from the SkipMap but recorded in last_purged_index/term)
            // are still recognised as valid prev_log positions after snapshot install.
            if self.entry_term(prev_log_index) != Some(prev_log_term) {
                return Ok(self.last_log_id());
            }
        }

        let last_current_index = self.last_entry_id();

        // Step 1: partition_point — O(log n_batch), zero SkipMap lookups.
        // Locates the overlap boundary without touching entry_term() at all.
        let skip = new_entries.partition_point(|e| e.index <= last_current_index);
        let overlap = &new_entries[..skip];
        let tail = &new_entries[skip..];

        // Step 2: Overlap safety check — O(1), two atomic loads.
        //
        // Overlap is safe to skip entirely when all three conditions hold:
        //   (a) The entire overlap range falls within follower's current term segment
        //       (first.index >= last_term_start) — no older-term entries below overlap start.
        //   (b) Incoming overlap entries all carry follower's current term (first.term == last_term).
        //   (c) No term change within incoming overlap (last.term == last_term); guaranteed by
        //       Raft non-decreasing term property when combined with (b).
        //
        // Covers the steady-state pipeline path (99%+ of calls). Election recovery (new leader,
        // term conflict in overlap) is caught by condition (b) failing → slow path.
        let overlap_safe = match overlap.first() {
            None => true,
            Some(first) => {
                let ft = self.term_segments.last_term.load(Ordering::Acquire);
                let fs = self.term_segments.last_term_start.load(Ordering::Acquire);
                first.index >= fs
                    && first.term == ft
                    && overlap.last().is_none_or(|last| last.term == ft)
            }
        };

        let last_log_id = if overlap_safe {
            // Fast path: skip entire overlap, append tail only.
            if tail.is_empty() {
                new_entries.last().map(|e| LogId {
                    term: e.term,
                    index: e.index,
                })
            } else {
                self.append_entries(tail.to_vec()).await?;
                tail.last().map(|e| LogId {
                    term: e.term,
                    index: e.index,
                })
            }
        } else {
            // Slow path: term boundary detected in overlap — scan for first conflict.
            // entry_term() uses TermSegments (O(1)) with SkipMap fallback, so each
            // check is cheap even on this rare election-recovery path.
            let diverge_pos = new_entries.iter().position(|e| {
                e.index > last_current_index || self.entry_term(e.index) != Some(e.term)
            });

            match diverge_pos {
                None => {
                    // All terms matched — idempotent RPC, nothing to do.
                    new_entries.last().map(|e| LogId {
                        term: e.term,
                        index: e.index,
                    })
                }
                Some(pos) => {
                    let tail = &new_entries[pos..];
                    let diverge_index = new_entries[pos].index;

                    if diverge_index <= last_current_index {
                        // Real term conflict: truncate from diverge_index, replace with tail.
                        // Await the done channel so callers can flush() knowing the truncation
                        // is durable — durable_index may exceed memory_max_index after truncation,
                        // which would cause flush() to short-circuit before the replace lands.
                        self.remove_range(diverge_index..=u64::MAX);
                        self.insert_to_memory(tail);
                        self.replace_range_and_submit(diverge_index, tail.to_vec()).await?;
                    } else {
                        // No conflict — pipeline overlap consumed, append only the new tail.
                        self.append_entries(tail.to_vec()).await?;
                    }

                    tail.last().map(|e| LogId {
                        term: e.term,
                        index: e.index,
                    })
                }
            }
        };

        Ok(last_log_id)
    }

    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        mut peer_matched_ids: Vec<u64>,
    ) -> Option<u64> {
        let _timer = ScopedTimer::new("calculate_majority_matched_index");
        // RPO=0 (#446): leader's own contribution must be its own durable (fsynced)
        // position, not the in-memory tail — otherwise a majority-looking commit can
        // still lose data on correlated power loss.
        peer_matched_ids.push(self.durable_index());

        // Sort in descending order
        peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

        // Calculate median as majority index
        let majority_index = peer_matched_ids[peer_matched_ids.len() / 2];

        debug!(
            ?self.node_id,
            "Majority calculation: peers={:?}, majority_index={}",
            peer_matched_ids, majority_index,
        );

        // Verify commit conditions
        if majority_index < commit_index {
            return None;
        }

        // Check term consistency
        match self.entry(majority_index) {
            Ok(Some(entry)) if entry.term == current_term => Some(majority_index),
            _ => None,
        }
    }

    async fn purge_logs_up_to(
        &self,
        cutoff_index: LogId,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("purge_logs_up_to");
        debug!(?self.node_id, ?cutoff_index, "purge_logs_up_to");

        // Remove range + publish last_purged_* atomically in the same lock (#442).
        self.purge_prefix(cutoff_index);

        // Purged entries are backed by the snapshot; treat cutoff as durable.
        // Already running on the single owner (called from role_state.rs, same
        // thread as remove_range) — safe to apply directly, no message hop needed.
        if let Some(new_durable) = self.try_advance_durable_index(cutoff_index)
            && let Some(ref tx) = self.internal_event_tx
        {
            let _ = tx.send(crate::InternalEvent::LogFlushed {
                durable_index: new_durable,
            });
        }
        self.purge_and_advance(cutoff_index).await
    }

    fn try_advance_durable_index(
        &self,
        mark: LogId,
    ) -> Option<u64> {
        let prev = self.durable_index.load(Ordering::Acquire);
        if mark.index <= prev {
            return None;
        }
        if self.entry_term(mark.index) != Some(mark.term) {
            return None;
        }
        let safe = mark.index.min(
            self.memory_max_index
                .load(Ordering::Acquire)
                .max(self.last_purged_index.load(Ordering::Acquire)),
        );
        if safe <= prev {
            return None;
        }
        self.durable_index.fetch_max(safe, Ordering::AcqRel);
        Some(safe)
    }

    async fn flush(&self) -> Result<()> {
        let target = self.memory_max_index.load(Ordering::Acquire);
        if target == 0 || self.durable_index.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        let persisted = self.persisted_index.load(Ordering::Acquire);
        if persisted < target
            && let Ok(Some(m)) = self.persist_pending_range(persisted + 1, target, "flush").await
        {
            self.persisted_index.fetch_max(m.index, Ordering::AcqRel);
        }
        let (tx, rx) = oneshot::channel();
        let term = self.entry_term(target).unwrap_or(0);
        self.fsync_worker.submit(
            LogId {
                term,
                index: target,
            },
            vec![tx],
        );
        rx.await
            .map_err(|_| NetworkError::SingalSendFailed("flush channel closed".into()))?
    }

    async fn reset(&self) -> Result<()> {
        let _timer = ScopedTimer::new("raft_log_core::reset");
        self.reset_internal().await
    }

    fn load_hard_state(&self) -> Result<Option<HardState>> {
        self.meta_store.load_hard_state()
    }

    fn save_hard_state(
        &self,
        hard_state: &HardState,
    ) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }
        self.meta_store.save_hard_state(hard_state).inspect_err(|e| {
            error!(?self.node_id, "save_hard_state failed (fatal): {e:?}");
            self.mark_poisoned_and_notify(format!("save_hard_state failed: {e:?}"));
        })
    }

    async fn close(&self) {
        let timed_out = tokio::time::timeout(
            Duration::from_millis(self.shutdown_timeout_ms),
            self.flush(),
        )
        .await
        .is_err();
        if timed_out {
            error!(
                ?self.node_id,
                "close(): flush() did not complete within {}ms — returning anyway; the \
                 in-flight fsync keeps running and any queued flush() replies will still \
                 resolve once it does",
                self.shutdown_timeout_ms
            );
        }
        let _ = self.meta_store.flush();
    }
}

impl<T> RaftLogCore<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        storage: Arc<SOF<T>>,
        internal_event_tx: Option<mpsc::UnboundedSender<InternalEvent>>,
        shutdown_timeout_ms: u64,
    ) -> Arc<Self> {
        let log_store = storage.log_store();
        let meta_store = storage.meta_store();
        let disk_len = log_store.last_index();

        let entries = SkipMap::new();

        // Initialize term indexes
        let term_first_index = SkipMap::new();
        let term_last_index = SkipMap::new();
        let term_segments = TermSegments::new();

        // Load all entries from disk to memory
        let mut loaded_count = 0;
        if disk_len > 0 {
            match log_store.get_entries(1..=disk_len) {
                Ok(all_entries) if !all_entries.is_empty() => {
                    loaded_count = all_entries.len();
                    debug!(
                        ?node_id,
                        "Successfully loaded {} entries from disk", loaded_count
                    );

                    for entry in &all_entries {
                        let index = entry.index;
                        entries.insert(index, entry.clone());

                        // MODIFIED: Initialize term indexes for each loaded entry
                        // Update first index for term
                        term_first_index
                            .get_or_insert(entry.term, AtomicU64::new(u64::MAX))
                            .value()
                            .fetch_min(index, Ordering::AcqRel);
                        // Update last index for term
                        term_last_index
                            .get_or_insert(entry.term, AtomicU64::new(0))
                            .value()
                            .fetch_max(index, Ordering::AcqRel);
                    }
                    term_segments.on_append(&all_entries);
                }
                Ok(_empty_entries) => {
                    warn!(
                        ?node_id,
                        "Disk reported length {} but loaded 0 entries", disk_len
                    );
                }
                Err(e) => {
                    error!(?node_id, "Failed to load entries from storage: {:?}", e);
                    // Handle critical error if needed
                }
            }
        }

        // Initialize atomic boundaries
        let min_index = entries.front().map(|e| *e.key()).unwrap_or(0);
        let memory_max_index = entries.back().map(|e| *e.key()).unwrap_or(0);

        if disk_len > 0 && loaded_count == 0 {
            warn!(
                ?node_id,
                "Inconsistent state: disk_len={} but loaded_count=0", disk_len
            );
        }

        // Restore purge boundary from storage so entry_term() returns the correct
        // term for the snapshot boundary entry even after a restart.
        let (last_purged_index_val, last_purged_term_val) = match log_store.load_purge_boundary() {
            Ok(Some(lid)) => (lid.index, lid.term),
            Ok(None) => (0, 0),
            Err(e) => {
                warn!(
                    ?node_id,
                    "Failed to load purge boundary: {:?}, defaulting to 0", e
                );
                (0, 0)
            }
        };

        let poisoned = Arc::new(AtomicBool::new(false));
        let fsync_worker = Arc::new(FsyncWorker::new(
            node_id,
            log_store.clone(),
            poisoned.clone(),
            internal_event_tx.clone(),
        ));

        Arc::new(Self {
            node_id,
            log_store,
            meta_store,
            shutdown_timeout_ms,
            entries: RwLock::new(entries),
            min_index: AtomicU64::new(min_index),
            memory_max_index: AtomicU64::new(memory_max_index),
            last_purged_index: AtomicU64::new(last_purged_index_val),
            last_purged_term: AtomicU64::new(last_purged_term_val),
            durable_index: AtomicU64::new(disk_len),
            persisted_index: AtomicU64::new(disk_len),
            next_id: AtomicU64::new(disk_len + 1),
            term_first_index,
            term_last_index,
            term_segments,
            internal_event_tx,
            fsync_worker,
            poisoned,
        })
    }

    /// Insert entries into the in-memory index (SkipMap + term indexes + atomics).
    fn insert_to_memory(
        &self,
        entries: &[Entry],
    ) {
        {
            let log = self.entries.write();
            for entry in entries {
                log.insert(entry.index, entry.clone());
            }
        }

        self.update_term_indexes(entries);
        self.term_segments.on_append(entries);

        let max_index = entries.iter().map(|e| e.index).max().unwrap_or(0);
        let current_next = self.next_id.load(Ordering::Acquire);
        if max_index >= current_next {
            self.next_id.store(max_index + 1, Ordering::Release);
        }

        if let Some(first_entry) = entries.first() {
            let mut current_min = self.min_index.load(Ordering::Relaxed);
            while first_entry.index < current_min || current_min == 0 {
                match self.min_index.compare_exchange_weak(
                    current_min,
                    first_entry.index,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(e) => current_min = e,
                }
            }
        }

        if let Some(last_entry) = entries.last() {
            let mut current_max = self.memory_max_index.load(Ordering::Relaxed);
            while last_entry.index > current_max {
                match self.memory_max_index.compare_exchange_weak(
                    current_max,
                    last_entry.index,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(e) => current_max = e,
                }
            }
        }
    }

    // Update the term index (completely lock-free)
    fn update_term_indexes(
        &self,
        entries: &[Entry],
    ) {
        for entry in entries {
            let term = entry.term;

            // Update first index
            self.term_first_index
                .get_or_insert(term, AtomicU64::new(u64::MAX))
                .value()
                .fetch_min(entry.index, Ordering::AcqRel);

            // Update last index
            self.term_last_index
                .get_or_insert(term, AtomicU64::new(0))
                .value()
                .fetch_max(entry.index, Ordering::AcqRel);
        }
    }

    /// Persists entries in `(from, to]` that aren't in the OS page cache yet
    /// (no fsync). `from` / `to` are only scan bounds — the caller passes
    /// `persisted_index + 1` and a `memory_max_index` snapshot.
    ///
    /// The SkipMap range scan returns only entries that still exist. If a
    /// concurrent term-conflict truncation removed the top of `(from, to]`
    /// between the caller's `memory_max_index` read and this scan, those
    /// indices are simply absent and never written.
    ///
    /// Returns `Some((term, index))` of the last entry written — its index may
    /// be *below* `to` in that truncation-race case — or `None` when the scan
    /// found nothing. Callers advance `persisted_index` and submit this mark to
    /// the fsync coordinator, so neither points past a real entry.
    async fn persist_pending_range(
        &self,
        from: u64,
        to: u64,
        ctx: &str,
    ) -> Result<Option<LogId>> {
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".to_string()));
        }

        if from > to {
            return Ok(None);
        }
        let entries = self.get_entries_range(from..=to)?;
        let Some(mark) = entries.last().map(|e| LogId {
            term: e.term,
            index: e.index,
        }) else {
            return Ok(None);
        };

        let t0 = std::time::Instant::now();
        self.log_store.persist_entries(entries).await.inspect_err(|e| {
            error!(
                ?self.node_id,
                persist_path = ctx,
                from, to, "persist_entries failed: {e:?}"
            );
            self.mark_poisoned_and_notify(format!("{ctx}: persist_entries failed: {e:?}"));
        })?;
        metrics::histogram!("core.raft.log_store.persist_entries_duration_ms")
            .record(t0.elapsed().as_secs_f64() * 1_000.0);
        Ok(Some(mark))
    }

    /// Efficient range removal with targeted term index updates
    /// O(k + t) where k = number of entries removed, t = number of affected terms
    pub fn remove_range(
        &self,
        range: RangeInclusive<u64>,
    ) {
        let entries = self.entries.write();
        let (new_min, new_max) = self.remove_range_locked(&entries, range);
        self.min_index.store(new_min, Ordering::Release);
        self.memory_max_index.store(new_max, Ordering::Release);

        self.durable_index.fetch_min(new_max, Ordering::AcqRel);
        self.fsync_worker.bump_generation();
        // `entries` guard drops here (end of scope) — write lock released.
    }

    /// Shared deletion logic for `remove_range`/`purge_prefix`. Caller already
    /// holds the write lock; this only touches entries + term_first/last_index
    /// and returns the new (min, max) without storing them, so a caller that
    /// needs to publish additional state under the same lock (purge_prefix's
    /// last_purged_*) can do so before the lock is released.
    fn remove_range_locked(
        &self,
        entries: &SkipMap<u64, Entry>,
        range: RangeInclusive<u64>,
    ) -> (u64, u64) {
        let (start, end) = range.into_inner();

        // Track affected terms and their min/max indexes in the removal range
        let mut affected_terms: HashMap<u64, (Option<u64>, Option<u64>)> = HashMap::new();

        // Remove entries in range and track affected terms
        let mut current = start;
        while current <= end {
            if let Some(entry) = entries.range(current..=end).next() {
                let key = *entry.key();
                let term = entry.value().term;

                let (min_idx, max_idx) = affected_terms.entry(term).or_insert((None, None));
                if min_idx.is_none() || key < min_idx.unwrap() {
                    *min_idx = Some(key);
                }
                if max_idx.is_none() || key > max_idx.unwrap() {
                    *max_idx = Some(key);
                }

                entries.remove(&key);
                current = key + 1;
            } else {
                break;
            }
        }

        // Update only affected term indexes
        for (term, (removed_min, removed_max)) in affected_terms {
            // Update first index if the removed entry was the first for this term
            if let Some(term_first) = self.term_first_index.get(&term) {
                let current_first = term_first.value().load(Ordering::Acquire);
                if removed_min.is_some() && current_first >= removed_min.unwrap() {
                    // Find new first index for this term
                    let new_first =
                        entries.iter().find(|e| e.value().term == term).map(|e| *e.key());

                    if let Some(idx) = new_first {
                        term_first.value().store(idx, Ordering::Release);
                    } else {
                        self.term_first_index.remove(&term);
                    }
                }
            }

            // Update last index if the removed entry was the last for this term
            if let Some(term_last) = self.term_last_index.get(&term) {
                let current_last = term_last.value().load(Ordering::Acquire);
                if removed_max.is_some() && current_last <= removed_max.unwrap() {
                    // Find new last index for this term
                    let new_last =
                        entries.iter().rev().find(|e| e.value().term == term).map(|e| *e.key());

                    if let Some(idx) = new_last {
                        term_last.value().store(idx, Ordering::Release);
                    } else {
                        self.term_last_index.remove(&term);
                    }
                }
            }
        }

        let new_min = entries.front().map(|e| *e.key()).unwrap_or(0);
        let new_max = entries.back().map(|e| *e.key()).unwrap_or(0);
        (new_min, new_max)
    }

    /// Purge entries at/below `cutoff.index`, publishing `last_purged_index`/
    /// `last_purged_term` in the SAME critical section as the entries removal
    /// and the min_index/memory_max_index advance. Only place that should ever write
    /// `last_purged_*` — a reader must never observe the entries gone but the
    /// boundary not yet recorded (#442).
    pub fn purge_prefix(
        &self,
        cutoff: LogId,
    ) {
        let entries = self.entries.write();
        let (new_min, new_max) = self.remove_range_locked(&entries, 0..=cutoff.index);

        self.min_index.store(new_min, Ordering::Release);
        self.memory_max_index.store(new_max, Ordering::Release);

        // Write term before index (Release) so readers that load index first
        // then term (Acquire) always observe a consistent pair.
        self.last_purged_term.store(cutoff.term, Ordering::Release);
        self.last_purged_index.store(cutoff.index, Ordering::Release);
    }

    /// Truncate the log from `truncate_from` and replace with `new_entries`,
    /// then submit the new tail for fsync. fdatasync is whole-WAL, so this also
    /// covers any leading persist from the same turn.
    async fn replace_range_and_submit(
        &self,
        truncate_from: u64,
        new_entries: Vec<Entry>,
    ) -> Result<()> {
        // Result is relied on immediately for protocol answers (entry_term(),
        // AppendEntries consistency checks) — must not proceed on an
        // already-untrusted disk.
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }

        // Capture the new tail's term before `new_entries` is moved.
        let new_tail_term = new_entries.last().map(|e| e.term).unwrap_or(0);
        let new_tail = match self.log_store.replace_range(truncate_from, new_entries).await {
            Ok(new_tail) => new_tail,
            Err(e) => {
                error!(?self.node_id, "replace_range failed (fatal): {e:?}");
                self.mark_poisoned_and_notify(format!("replace_range failed: {e:?}"));
                return Err(e);
            }
        };

        if new_tail >= truncate_from {
            self.fsync_worker.submit(
                LogId {
                    term: new_tail_term,
                    index: new_tail,
                },
                vec![],
            );
        }
        self.persisted_index.store(new_tail, Ordering::Release);

        Ok(())
    }

    async fn purge_and_advance(
        &self,
        cutoff: LogId,
    ) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }
        if let Err(e) = self.log_store.purge(cutoff).await {
            error!(?self.node_id, "purge failed (fatal): {e:?}");
            self.mark_poisoned_and_notify(format!("Purge failed: {e:?}"));
            return Err(e);
        }
        self.persisted_index.fetch_max(cutoff.index, Ordering::AcqRel);
        Ok(())
    }

    /// Returns `true` if a storage-layer failure has permanently poisoned this
    /// log — no further writes/commands will be attempted, and callers above
    /// the storage layer (e.g. the Raft protocol loop) must stop dispatching
    /// new work to this node.
    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Mark the log as permanently poisoned and notify the Raft driving loop.
    /// Single choke point for both failure surfaces (persist_entries write
    /// failure and fsync failure) — do not set `poisoned` or call `notify_fatal`
    /// directly from anywhere else.
    pub(super) fn mark_poisoned_and_notify(
        &self,
        error: String,
    ) {
        self.poisoned.store(true, Ordering::Release);
        self.notify_fatal(error);
    }

    /// Send `InternalEvent::FatalError` to the Raft driving loop, mirroring the
    /// pattern SM-worker failures already use (state_machine_handler/worker.rs).
    /// Idempotent in effect: even if called multiple times (e.g. both
    /// persist_entries and a later fsync fail), raft.rs::run() only needs to
    /// see it once to exit.
    pub(super) fn notify_fatal(
        &self,
        error: String,
    ) {
        if let Some(ref tx) = self.internal_event_tx
            && tx
                .send(crate::InternalEvent::FatalError {
                    source: "RaftLog".to_string(),
                    error,
                })
                .is_err()
        {
            error!(
                ?self.node_id,
                "FatalError delivery failed (channel closed) — node may be poisoned \
                     but still running; durability is no longer guaranteed"
            );
        }
    }

    async fn reset_internal(&self) -> Result<()> {
        // Fence first: bump generation before clearing state, so any fsync task
        // still in flight will observe a mismatch and discard its stale result.
        self.fsync_worker.fence_reset();

        self.entries.write().clear();
        self.durable_index.store(0, Ordering::Release);
        self.next_id.store(1, Ordering::Release);

        // Reset boundaries
        self.min_index.store(0, Ordering::Release);
        self.memory_max_index.store(0, Ordering::Release);

        // Clear term indexes to ensure consistency after reset
        self.term_first_index.clear();
        self.term_last_index.clear();
        self.term_segments.clear();

        if let Err(e) = self.log_store.reset().await {
            error!(?self.node_id, "reset failed (fatal): {e:?}");
            self.mark_poisoned_and_notify(format!("Reset failed: {e:?}"));
            return Err(e);
        }
        self.persisted_index.store(0, Ordering::Release);

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Returns reference to next_id atomic for test verification (test-only).
    #[cfg(test)]
    pub fn next_id(&self) -> &std::sync::atomic::AtomicU64 {
        &self.next_id
    }

    #[cfg(test)]
    pub(super) fn set_memory_max_index_for_test(
        &self,
        value: u64,
    ) {
        self.memory_max_index.store(value, Ordering::Release);
    }
}

/// Maximum number of historical term segments (one per leader election).
/// 1024 is far more than any realistic cluster lifetime.
const MAX_TERM_SEGMENTS: usize = 1024;

/// Compact term boundary index for O(1) `entry_term()` lookups.
///
/// Hot path (99%+ of queries): two `Acquire` loads, no lock, no CAS.
/// Cold path (election recovery): atomic array reverse-scan, no lock, no unsafe.
///
/// When the array is full, new segments are silently dropped and `entry_term()`
/// falls back to the SkipMap (O(log n)) — correct but slower.
///
/// Memory: 2 AtomicU64 + 1 AtomicUsize + 2×1024 AtomicU64 = ~16KB, all inline.
pub(crate) struct TermSegments {
    /// Term of the most-recently appended segment.
    pub(crate) last_term: AtomicU64,
    /// First log index belonging to `last_term`.
    pub(crate) last_term_start: AtomicU64,
    /// Number of valid historical segments stored in `seg_starts`/`seg_terms`.
    seg_count: AtomicUsize,
    /// First index of each historical segment, in append order.
    seg_starts: [AtomicU64; MAX_TERM_SEGMENTS],
    /// Term of each historical segment, parallel to `seg_starts`.
    seg_terms: [AtomicU64; MAX_TERM_SEGMENTS],
}

impl TermSegments {
    pub(crate) fn new() -> Self {
        Self {
            last_term: AtomicU64::new(0),
            last_term_start: AtomicU64::new(0),
            seg_count: AtomicUsize::new(0),
            seg_starts: std::array::from_fn(|_| AtomicU64::new(0)),
            seg_terms: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Return the term for `index`, or `None` if the log is empty or index is out of range.
    ///
    /// Hot path: two `Acquire` loads, no lock, no CAS — O(1).
    /// Cold path: reverse-scan atomic array, k = election count (typically < 10) — O(k).
    pub(crate) fn get(
        &self,
        index: u64,
    ) -> Option<u64> {
        let last_start = self.last_term_start.load(Ordering::Acquire);
        let last_term = self.last_term.load(Ordering::Acquire);
        if last_term == 0 {
            return None;
        }
        if index >= last_start {
            return Some(last_term);
        }
        // Cold path: reverse-scan historical segments.
        let count = self.seg_count.load(Ordering::Acquire);
        (0..count).rev().find_map(|i| {
            let start = self.seg_starts[i].load(Ordering::Acquire);
            if start <= index {
                Some(self.seg_terms[i].load(Ordering::Acquire))
            } else {
                None
            }
        })
    }

    /// Update after appending entries at the tail. Entries must be in ascending index order.
    ///
    /// Common case (same term): one `Acquire` load, no write — O(1).
    /// Term change: two atomic stores to next slot, no lock — O(1).
    pub(crate) fn on_append(
        &self,
        entries: &[Entry],
    ) {
        for entry in entries {
            let lt = self.last_term.load(Ordering::Acquire);
            if entry.term == lt {
                // Same term: pull segment start back if needed (truncation + re-insert).
                let ls = self.last_term_start.load(Ordering::Acquire);
                if entry.index < ls {
                    self.last_term_start.store(entry.index, Ordering::Release);
                }
                continue;
            }
            if lt == 0 {
                // First entries ever: initialise hot atomics.
                self.last_term_start.store(entry.index, Ordering::Release);
                self.last_term.store(entry.term, Ordering::Release);
                continue;
            }
            // New term boundary: archive current segment into the next slot.
            // When the array is full, skip the write — entry_term() falls back
            // to the SkipMap cold path for any overflow segments.
            let ls = self.last_term_start.load(Ordering::Acquire);
            let i = self.seg_count.fetch_add(1, Ordering::AcqRel);
            if i < MAX_TERM_SEGMENTS {
                self.seg_starts[i].store(ls, Ordering::Release);
                self.seg_terms[i].store(lt, Ordering::Release);
            }
            // Always update hot atomics so the current term remains O(1).
            self.last_term_start.store(entry.index, Ordering::Release);
            self.last_term.store(entry.term, Ordering::Release);
        }
    }

    /// Reset to empty. Called on log reset (snapshot install / full rewind).
    pub(crate) fn clear(&self) {
        self.seg_count.store(0, Ordering::Release);
        self.last_term.store(0, Ordering::Release);
        self.last_term_start.store(0, Ordering::Release);
    }
}

impl<T> std::fmt::Debug for RaftLogCore<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftLogCore").finish()
    }
}

#[cfg(test)]
#[path = "raft_log_core_test/basic_operations_test.rs"]
mod basic_operations_test;

#[cfg(test)]
#[path = "raft_log_core_test/concurrent_fsync_test.rs"]
mod concurrent_fsync_test;

#[cfg(test)]
#[path = "raft_log_core_test/concurrent_operations_test.rs"]
mod concurrent_operations_test;

#[cfg(test)]
#[path = "raft_log_core_test/drain_fsync_test.rs"]
mod drain_fsync_test;

#[cfg(test)]
#[path = "raft_log_core_test/durable_index_test.rs"]
mod durable_index_test;

#[cfg(test)]
#[path = "raft_log_core_test/edge_cases_test.rs"]
mod edge_cases_test;

#[cfg(test)]
#[path = "raft_log_core_test/flush_strategy_test.rs"]
mod flush_strategy_test;

#[cfg(test)]
#[path = "raft_log_core_test/id_allocation_test.rs"]
mod id_allocation_test;

#[cfg(test)]
#[path = "raft_log_core_test/performance_test.rs"]
mod performance_test;

#[cfg(test)]
#[path = "raft_log_core_test/durable_index_truncation_clamp_test.rs"]
mod durable_index_truncation_clamp_test;

#[cfg(test)]
#[path = "raft_log_core_test/pipeline_overlap_test.rs"]
mod pipeline_overlap_test;

#[cfg(test)]
#[path = "raft_log_core_test/quorum_durability_test.rs"]
mod quorum_durability_test;

#[cfg(test)]
#[path = "raft_log_core_test/raft_properties_test.rs"]
mod raft_properties_test;

#[cfg(test)]
#[path = "raft_log_core_test/remove_range_test.rs"]
mod remove_range_test;

#[cfg(test)]
#[path = "raft_log_core_test/replace_range_fsync_test.rs"]
mod replace_range_fsync_test;

#[cfg(test)]
#[path = "raft_log_core_test/shutdown_test.rs"]
mod shutdown_test;

#[cfg(test)]
#[path = "raft_log_core_test/term_index_test.rs"]
mod term_index_test;

#[cfg(test)]
#[path = "raft_log_core_test/term_segments_test.rs"]
mod term_segments_test;

#[cfg(test)]
#[path = "raft_log_core_test/truncation_fsync_fence_test.rs"]
mod truncation_fsync_fence_test;

#[cfg(test)]
#[path = "raft_log_core_test/content_validated_watermark_test.rs"]
mod content_validated_watermark_test;

#[cfg(test)]
#[path = "raft_log_core_test/prev_log_index_zero_idempotency_test.rs"]
mod prev_log_index_zero_idempotency_test;
