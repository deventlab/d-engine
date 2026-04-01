//! High-performance buffered Raft log — notify-then-fsync architecture.
//!
//! ## Durability guarantee (MemFirst strategy)
//!
//! **Level 2 (current)**: `db.write()` → OS page cache (no fsync every write)
//! - Process crash safe (RocksDB replays WAL on restart)
//! - Power loss unsafe (OS page cache lost)
//! - Tradeoff: Skip per-write fsync (1–5ms) for ~3x throughput vs Level 3
//!
//! **DiskFirst removed** (was Level 3: fsync every write = blocking, safe but slow)
//! **MemFirst + idle timer** (current = Level 2: batch fsync, natural window from IO work)
//!
//! ## Write path
//!
//! `append_entries` inserts entries into in-memory SkipMap, calls `write_notify.notify_one()`.
//! Multiple concurrent writers coalesce into single IO thread wakeup (no channel, no per-write syscall).
//!
//! ## IO thread (notify-then-fsync)
//!
//! On wakeup from `write_notify`:
//! 1. **Read** — scan SkipMap range `(durable_index, max_index]`
//! 2. **Persist** — write to OS page cache (no fsync)
//! 3. **Fsync once** — call `log_store.flush()` (WAL sync to disk)
//!    Entries arriving during fsync batch into next wakeup
//! 4. **Advance durable_index** — wake all `WaitDurable` callers
//!
//! Fsync execution time is the natural batch window — no timer needed.
//!
//! ## Fsync triggers
//!
//! 1. **Notify-driven** (normal): Multiple writes → single IO thread wakeup → batch fsync
//! 2. **Explicit** (flush API): `flush()` → immediate fsync + `wait_durable()`
//! 3. **Idle timer** (safety net): No activity for `idle_flush_interval_ms` → fsync pending
//!
//! ## Durability contract
//!
//! `durable_index` advanced only after fsync.
//! **Assumption**: Host crashes gracefully (UPS-backed, OS cache writeback guaranteed on shutdown).
//! **Not safe for**: Power loss scenarios without UPS or host without battery-backed cache.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::FlushPolicy;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::NetworkError;
use crate::PersistenceConfig;
use crate::RaftLog;
use crate::Result;
use crate::StorageEngine;
use crate::TypeConfig;
use crate::alias::SOF;
use crate::scoped_timer::ScopedTimer;
use crossbeam_skiplist::SkipMap;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::warn;

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

/// IO tasks for the dedicated raft-io thread.
///
/// All blocking storage operations route through this channel so they never run
/// on tokio worker threads or the Raft event loop.
/// Normal write path uses `write_notify` (tokio::sync::Notify) instead of a
/// channel message — see `BufferedRaftLog::write_notify`.
#[derive(Debug)]
pub enum IOTask {
    /// Wait until specific index is durable
    WaitDurable(u64, oneshot::Sender<()>),
    /// Atomically truncate from `truncate_from` then persist `new_entries`.
    /// Conflict-resolution path: truncate + write are a single atomic IO unit.
    ReplaceRange {
        truncate_from: u64,
        new_entries: Vec<Entry>,
    },
    /// Purge log entries up to `cutoff` from storage.
    Purge {
        cutoff: LogId,
        done: oneshot::Sender<()>,
    },
    /// Reset log storage (snapshot install). Routes through IO thread so reset
    /// never runs on the Raft event loop.
    Reset { done: oneshot::Sender<Result<()>> },
    /// Trigger an immediate drain-then-fsync without new entries.
    /// Sent by `flush()` so the caller does not wait for the safety-net timer.
    FlushNow,
    /// Shutdown the IO thread
    Shutdown,
}

/// High-performance buffered Raft log with event-driven architecture
///
/// This implementation provides in-memory first access with configurable
/// persistence strategies while ensuring thread safety and avoiding deadlocks.
///
/// Key design principles:
/// - Lock-free reads for 99% of operations
/// - Event-driven asynchronous processing
/// - Deadlock prevention through proper error handling
/// - Memory-efficient batch operations
pub struct BufferedRaftLog<T>
where
    T: TypeConfig,
{
    #[allow(dead_code)]
    node_id: u32,

    pub(crate) log_store: Arc<<SOF<T> as StorageEngine>::LogStore>,
    pub(crate) meta_store: Arc<<SOF<T> as StorageEngine>::MetaStore>,

    /// Safety-net timer interval (ms). Normal-path latency is determined by fsync time.
    idle_flush_interval_ms: u64,

    // --- In-memory state ---
    // Pending entries
    pub(crate) entries: SkipMap<u64, Entry>,
    // Tracks the highest index that has been persisted to disk
    pub(crate) durable_index: AtomicU64,
    // The next index to be allocated
    pub(crate) next_id: AtomicU64,

    // --- In-memory index ---
    min_index: AtomicU64, // Smallest log index (0 if empty)
    max_index: AtomicU64, // Largest log index (0 if empty)

    // Purge boundary: last log entry that was purged (index=0 means never purged).
    // entry_term() checks these when a SkipMap lookup misses, so that
    // prev_log_term for the snapshot boundary index is always correct.
    // Write ordering: term stored (Release) before index (Release);
    // reader loads index (Acquire) then term (Acquire) — ensures consistency.
    last_purged_index: AtomicU64,
    last_purged_term: AtomicU64,

    term_first_index: SkipMap<u64, AtomicU64>,
    term_last_index: SkipMap<u64, AtomicU64>,

    /// Compact term boundary index — avoids SkipMap lookups in entry_term().
    term_segments: TermSegments,

    // --- Flush coordination ---
    /// Coalesced write notification. `append_entries` calls `notify_one()` after
    /// inserting into the SkipMap. Multiple concurrent writers coalesce into a
    /// single IO thread wakeup, eliminating per-write kernel cond_signal overhead.
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) command_sender: mpsc::UnboundedSender<IOTask>,
    pub(crate) waiters: DashMap<u64, Vec<oneshot::Sender<()>>>,

    // --- P0: LogFlushed event notification ---
    // Sends RoleEvent::LogFlushed(durable) to Raft loop after each fsync.
    // None in tests; Some(role_tx) in production.
    log_flush_tx: Option<mpsc::UnboundedSender<crate::RoleEvent>>,

    // IO thread handle — set by start(), used by close() to join before returning.
    io_thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[async_trait]
impl<T> RaftLog for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    ///TODO: not considered the order of configured storage rule
    /// also should we remove Result<>?
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        Ok(self.entries.get(&index).map(|e| e.value().clone()))
    }

    fn first_entry_id(&self) -> u64 {
        self.min_index.load(Ordering::Acquire)
    }

    fn last_entry_id(&self) -> u64 {
        self.max_index.load(Ordering::Acquire)
    }

    fn durable_index(&self) -> u64 {
        // fsync is async; only entries confirmed by batch_processor are crash-safe.
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
        self.entries.is_empty()
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        // Bounds check: skip TermSegments entirely for out-of-range queries.
        let max = self.max_index.load(Ordering::Acquire);
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
        self.entries.get(&entry_id).map(|e| e.value().term)
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
        // OPTIMIZED: SkipMap range scan O(k + log n); pre-allocate to avoid realloc
        let capacity = (range.end().saturating_sub(*range.start()) + 1) as usize;
        let mut result = Vec::with_capacity(capacity);
        result.extend(self.entries.range(range).map(|e| e.value().clone()));
        Ok(result)
    }

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("append_entries");
        if entries.is_empty() {
            return Ok(());
        }

        self.insert_to_memory(&entries);
        // Signal IO thread to persist. Multiple concurrent notify_one() calls
        // while the IO thread is busy coalesce into one wakeup — no per-write
        // kernel cond_signal. IO thread reads from SkipMap via max_index.
        self.write_notify.notify_one();

        Ok(())
    }

    async fn wait_durable(
        &self,
        index: u64,
    ) -> Result<()> {
        let durable_index = self.durable_index.load(Ordering::Acquire);
        if index <= durable_index {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.command_sender.send(IOTask::WaitDurable(index, tx)).map_err(|e| {
            NetworkError::SingalSendFailed(format!("wait_durable send failed: {e:?}"))
        })?;

        rx.await
            .map_err(|_| NetworkError::SingalSendFailed("wait_durable channel closed".into()))?;

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
        // prev_log_index == 0 means the leader wants the follower to start from scratch
        // (e.g. new follower joining, or follower log fully diverged). Reset and replace.
        if prev_log_index == 0 && prev_log_term == 0 {
            self.reset().await?;
            self.append_entries(new_entries.clone()).await?;
            return Ok(new_entries.last().map(|e| LogId {
                term: e.term,
                index: e.index,
            }));
        }

        // Check log consistency: use entry_term() so purge-boundary entries
        // (entries removed from the SkipMap but recorded in last_purged_index/term)
        // are still recognised as valid prev_log positions after snapshot install.
        if self.entry_term(prev_log_index) != Some(prev_log_term) {
            return Ok(self.last_log_id());
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
                        self.remove_range(diverge_index..=u64::MAX);
                        self.insert_to_memory(tail);
                        self.command_sender
                            .send(IOTask::ReplaceRange {
                                truncate_from: diverge_index,
                                new_entries: tail.to_vec(),
                            })
                            .map_err(|e| {
                                NetworkError::SingalSendFailed(format!(
                                    "Failed to send ReplaceRange: {e:?}"
                                ))
                            })?;
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
        // Leader's contribution: last_entry_id (in-memory). With MemFirst (Level 2), db.write()
        // returns once data reaches OS page cache — durable_index advances immediately.
        // Followers also ACK after OS page cache write (no fsync wait). Crash safety is
        // OS page cache level: process crash is recoverable, power loss is not.
        peer_matched_ids.push(self.last_entry_id());

        // Sort in descending order
        peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

        // Calculate median as majority index
        let majority_index = peer_matched_ids[peer_matched_ids.len() / 2];

        debug!(
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
        debug!(?cutoff_index, "purge_logs_up_to");

        // Remove range in O(k log n)
        self.remove_range(0..=cutoff_index.index);

        // Update boundaries
        let new_min = self.entries.front().map(|e| *e.key()).unwrap_or(0);
        self.min_index.store(new_min, Ordering::Release);

        let new_max = self.entries.back().map(|e| *e.key()).unwrap_or(0);
        self.max_index.store(new_max, Ordering::Release);

        // Update durable index if needed
        let current_durable = self.durable_index.load(Ordering::Acquire);

        // TODO: to be double thinking
        if cutoff_index.index >= current_durable {
            self.durable_index.store(cutoff_index.index, Ordering::Release);
        }

        // Record purge boundary so entry_term() can return the correct term for
        // prev_log_index == cutoff_index.index after the entry has been removed.
        // Write term before index (Release) so readers that load index first then
        // term (Acquire) always observe a consistent pair.
        self.last_purged_term.store(cutoff_index.term, Ordering::Release);
        self.last_purged_index.store(cutoff_index.index, Ordering::Release);

        // Route purge through the IO thread so it never blocks the Raft event loop.
        // Also writes the purge boundary to META_CF in the RocksDB implementation.
        let (done_tx, done_rx) = oneshot::channel();
        self.command_sender
            .send(IOTask::Purge {
                cutoff: cutoff_index,
                done: done_tx,
            })
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send Purge: {e:?}")))?;
        done_rx
            .await
            .map_err(|_| NetworkError::SingalSendFailed("Purge channel closed".into()))?;

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let max_index = self.max_index.load(Ordering::Acquire);
        if max_index == 0 {
            return Ok(());
        }
        if self.durable_index.load(Ordering::Acquire) >= max_index {
            return Ok(());
        }
        // Trigger an immediate drain-then-fsync cycle in the IO thread.
        let _ = self.command_sender.send(IOTask::FlushNow);
        self.wait_durable(max_index).await
    }

    async fn reset(&self) -> Result<()> {
        let _timer = ScopedTimer::new("buffered_raft_log::reset");
        self.reset_internal().await
    }

    fn load_hard_state(&self) -> Result<Option<HardState>> {
        self.meta_store.load_hard_state()
    }

    fn save_hard_state(
        &self,
        hard_state: &HardState,
    ) -> Result<()> {
        self.meta_store.save_hard_state(hard_state)
    }

    async fn close(&self) {
        // Signal the IO thread to flush remaining data and exit.
        let _ = self.command_sender.send(IOTask::Shutdown);
        // Take the handle — idempotent; second call is a no-op.
        let handle = self.io_thread_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            // Join on a spawn_blocking thread so we don't block a tokio worker.
            tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await
            .ok();
        }
    }
}

impl<T> BufferedRaftLog<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        persistence_config: PersistenceConfig,
        storage: Arc<SOF<T>>,
    ) -> (Self, mpsc::UnboundedReceiver<IOTask>) {
        let log_store = storage.log_store();
        let meta_store = storage.meta_store();
        let disk_len = log_store.last_index();

        let FlushPolicy::Batch {
            idle_flush_interval_ms,
        } = persistence_config.flush_policy;
        debug!(
            "Creating BufferedRaftLog with node_id: {}, strategy: {:?}, idle_flush_interval_ms: {:?}, disk_len: {:?}",
            node_id, persistence_config.strategy, idle_flush_interval_ms, disk_len
        );

        //TODO: if switch to UnboundedChannel?
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
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
                    debug!("Successfully loaded {} entries from disk", loaded_count);

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
                    warn!("Disk reported length {} but loaded 0 entries", disk_len);
                }
                Err(e) => {
                    error!("Failed to load entries from storage: {:?}", e);
                    // Handle critical error if needed
                }
            }
        }

        // Initialize atomic boundaries
        let min_index = entries.front().map(|e| *e.key()).unwrap_or(0);
        let max_index = entries.back().map(|e| *e.key()).unwrap_or(0);

        if disk_len > 0 && loaded_count == 0 {
            warn!(
                "Inconsistent state: disk_len={} but loaded_count=0",
                disk_len
            );
        }

        // Restore purge boundary from storage so entry_term() returns the correct
        // term for the snapshot boundary entry even after a restart.
        let (last_purged_index_val, last_purged_term_val) = match log_store.load_purge_boundary() {
            Ok(Some(lid)) => (lid.index, lid.term),
            Ok(None) => (0, 0),
            Err(e) => {
                warn!("Failed to load purge boundary: {:?}, defaulting to 0", e);
                (0, 0)
            }
        };

        (
            Self {
                node_id,
                log_store,
                meta_store,
                idle_flush_interval_ms,
                entries,
                min_index: AtomicU64::new(min_index),
                max_index: AtomicU64::new(max_index),
                last_purged_index: AtomicU64::new(last_purged_index_val),
                last_purged_term: AtomicU64::new(last_purged_term_val),
                durable_index: AtomicU64::new(disk_len),
                next_id: AtomicU64::new(disk_len + 1),
                write_notify: Arc::new(Notify::new()),
                command_sender: command_sender.clone(),
                waiters: DashMap::new(),
                term_first_index,
                term_last_index,
                term_segments,
                log_flush_tx: None,                            // set in start()
                io_thread_handle: std::sync::Mutex::new(None), // set in start()
            },
            command_receiver,
        )
    }

    /// Start the command processor and return an Arc-wrapped instance.
    ///
    /// `batch_processor` runs on a dedicated OS thread (not a tokio worker) so that
    /// synchronous RocksDB calls (`db.write`, `flush_wal`) never block the async runtime.
    /// The thread drives the async batch_processor via `Handle::block_on`, which lets the
    /// internal tokio channels and timers work unchanged.
    pub fn start(
        mut self,
        receiver: mpsc::UnboundedReceiver<IOTask>,
        log_flush_tx: Option<mpsc::UnboundedSender<crate::RoleEvent>>,
    ) -> Arc<Self> {
        self.log_flush_tx = log_flush_tx;
        let arc_self = Arc::new(self);
        let weak_self = Arc::downgrade(&arc_self);

        let idle_flush_interval_ms = arc_self.idle_flush_interval_ms;
        let node_id = arc_self.node_id;
        let handle = tokio::runtime::Handle::current();
        let io_handle = std::thread::Builder::new()
            .name(format!("raft-io-{}", node_id))
            .spawn(move || {
                handle.block_on(Self::batch_processor(
                    weak_self,
                    receiver,
                    idle_flush_interval_ms,
                ));
            })
            .expect("failed to spawn raft-io thread");

        *arc_self.io_thread_handle.lock().unwrap() = Some(io_handle);

        arc_self
    }

    /// Notify-driven IO loop.
    ///
    /// Waits on `write_notify.notified()` for new entries in the SkipMap.
    /// Multiple `notify_one()` calls while the IO thread is busy (persisting or
    /// fsyncing) coalesce into a single wakeup, reducing kernel cond_signal overhead
    /// from one-per-write to one-per-burst.
    ///
    /// On each wakeup:
    ///   1. Read entries in `(durable_index, max_index]` from SkipMap.
    ///   2. persist_entries to OS page cache (no fsync).
    ///   3. Drain any pending control commands from the mpsc channel.
    ///   4. fsync once — advance durable_index, wake WaitDurable callers.
    ///
    /// Safety-net timer fires after `idle_flush_interval_ms` of inactivity.
    async fn batch_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<IOTask>,
        idle_flush_interval_ms: u64,
    ) {
        // Upgrade once at entry — Arc stays alive until Shutdown (the real exit signal).
        let Some(this) = this.upgrade() else { return };

        let start = tokio::time::Instant::now() + Duration::from_millis(idle_flush_interval_ms);
        let mut safety_timer =
            tokio::time::interval_at(start, Duration::from_millis(idle_flush_interval_ms));
        safety_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Highest index in OS page cache, awaiting fsync. Reset to 0 after each fsync.
        let mut pending_max: u64 = 0;

        loop {
            tokio::select! {
                _ = this.write_notify.notified() => {
                    // Persist all entries written to SkipMap since last fsync.
                    let end = this.max_index.load(Ordering::Acquire);
                    let start = this.durable_index.load(Ordering::Acquire) + 1;
                    if start <= end {
                        match this.get_entries_range(start..=end) {
                            Ok(entries) if !entries.is_empty() => {
                                if let Err(e) = this.log_store.persist_entries(entries).await {
                                    error!("write_notify persist_entries failed: {e:?}");
                                } else {
                                    pending_max = pending_max.max(end);
                                }
                            }
                            Ok(_) => {}
                            Err(e) => error!("write_notify get_entries_range failed: {e:?}"),
                        }
                    }
                    // Drain any pending control commands before fsync.
                    // Shutdown in the drain path must be handled here — it won't reach
                    // the outer receiver arm if consumed in try_recv().
                    let mut seen_shutdown = false;
                    while let Ok(cmd) = receiver.try_recv() {
                        if matches!(cmd, IOTask::Shutdown) {
                            seen_shutdown = true;
                            break;
                        }
                        Self::handle_non_write_cmd(cmd, &this, &mut pending_max).await;
                    }
                    if pending_max > 0 {
                        if let Err(e) = this.advance_durable_after_write(pending_max).await {
                            error!("write_notify fsync failed: {e:?}");
                        } else {
                            pending_max = 0;
                        }
                    }
                    if seen_shutdown {
                        let _ = this.log_store.flush();
                        let _ = this.meta_store.flush();
                        break;
                    }
                }
                cmd = receiver.recv() => {
                    let Some(cmd) = cmd else { break };
                    match cmd {
                        IOTask::Shutdown => {
                            // Persist any entries not yet in page cache before final flush.
                            let end = this.max_index.load(Ordering::Acquire);
                            let start = this.durable_index.load(Ordering::Acquire) + 1;
                            if start <= end
                                && let Ok(entries) = this.get_entries_range(start..=end)
                                && !entries.is_empty()
                            {
                                if let Err(e) = this.log_store.persist_entries(entries).await {
                                    error!("Shutdown persist_entries failed: {e:?}");
                                } else {
                                    pending_max = pending_max.max(end);
                                }
                            }
                            if pending_max > 0 {
                                let _ = this.advance_durable_after_write(pending_max).await;
                            }
                            let _ = this.log_store.flush();
                            let _ = this.meta_store.flush();
                            break;
                        }
                        IOTask::FlushNow => {
                            // Persist any entries not yet in page cache, then fsync.
                            let end = this.max_index.load(Ordering::Acquire);
                            let start = this.durable_index.load(Ordering::Acquire) + 1;
                            if start <= end
                                && let Ok(entries) = this.get_entries_range(start..=end)
                                && !entries.is_empty()
                            {
                                if let Err(e) = this.log_store.persist_entries(entries).await {
                                    error!("FlushNow persist_entries failed: {e:?}");
                                } else {
                                    pending_max = pending_max.max(end);
                                }
                            }
                            // Drain remaining control cmds.
                            let mut seen_shutdown = false;
                            while let Ok(cmd) = receiver.try_recv() {
                                if matches!(cmd, IOTask::Shutdown) {
                                    seen_shutdown = true;
                                    break;
                                }
                                Self::handle_non_write_cmd(cmd, &this, &mut pending_max).await;
                            }
                            if pending_max > 0 {
                                if let Err(e) = this.advance_durable_after_write(pending_max).await {
                                    error!("FlushNow fsync failed: {e:?}");
                                } else {
                                    pending_max = 0;
                                }
                            }
                            if seen_shutdown {
                                let _ = this.log_store.flush();
                                let _ = this.meta_store.flush();
                                break;
                            }
                        }
                        cmd => {
                            Self::handle_non_write_cmd(cmd, &this, &mut pending_max).await;
                        }
                    }
                }
                _ = safety_timer.tick() => {
                    // Safety-net: persist and fsync any entries not yet durable.
                    let end = this.max_index.load(Ordering::Acquire);
                    let start = this.durable_index.load(Ordering::Acquire) + 1;
                    if start <= end
                        && let Ok(entries) = this.get_entries_range(start..=end)
                        && !entries.is_empty()
                    {
                        if let Err(e) = this.log_store.persist_entries(entries).await {
                            error!("safety-net persist_entries failed: {e:?}");
                        } else {
                            pending_max = pending_max.max(end);
                        }
                    }
                    if pending_max > 0 {
                        if let Err(e) = this.advance_durable_after_write(pending_max).await {
                            error!("safety-net timer fsync failed: {e:?}");
                        } else {
                            pending_max = 0;
                        }
                    }
                }
            }
        }
    }

    /// Handle IOTask variants that are NOT `FlushNow` or `Shutdown`.
    ///
    /// Callers (`batch_processor`) dispatch `FlushNow` and `Shutdown` directly in the outer
    /// `match` before this function is ever called — those two arms are unreachable here.
    async fn handle_non_write_cmd(
        cmd: IOTask,
        this: &Arc<Self>,
        pending_max: &mut u64,
    ) {
        match cmd {
            IOTask::FlushNow => {
                // FlushNow encountered in the write_notify drain path — the outer loop
                // will handle it on the next iteration. No action needed here.
            }
            IOTask::Shutdown => {
                unreachable!("Shutdown is always filtered out before reaching handle_non_write_cmd")
            }
            IOTask::WaitDurable(index, ack) => {
                this.handle_wait_durable(index, ack).await;
            }
            IOTask::ReplaceRange {
                truncate_from,
                new_entries,
            } => {
                if let Err(e) = this.log_store.truncate(truncate_from).await {
                    error!("IOTask::ReplaceRange truncate({truncate_from}) failed: {e:?}");
                    return;
                }
                if !new_entries.is_empty() {
                    let max_idx = new_entries.last().map(|e| e.index).unwrap_or(0);
                    if let Err(e) = this.log_store.persist_entries(new_entries).await {
                        error!("IOTask::ReplaceRange persist_entries failed: {e:?}");
                        return;
                    }
                    *pending_max = (*pending_max).max(max_idx);
                }
            }
            IOTask::Purge { cutoff, done } => {
                let _ = this.log_store.purge(cutoff).await;
                let _ = done.send(());
            }
            IOTask::Reset { done } => {
                let result = this.log_store.reset().await;
                *pending_max = 0; // disk wiped — pending page-cache watermark must be zeroed
                let _ = done.send(result);
            }
        }
    }

    /// Advance durable_index to `max_index` after data has been written to OS page cache (Level 2).
    ///
    /// For Level 2 (current): `is_write_durable=true` → skip flush_wal, advance immediately.
    /// For Level 3 (future):  `is_write_durable=false` → call flush_wal(sync=true) first.
    async fn advance_durable_after_write(
        &self,
        max_index: u64,
    ) -> Result<()> {
        if !self.log_store.is_write_durable() {
            self.log_store.flush()?;
        }
        self.advance_durable_and_notify(max_index);
        Ok(())
    }

    async fn reset_internal(&self) -> Result<()> {
        self.entries.clear();
        self.durable_index.store(0, Ordering::Release);
        self.next_id.store(1, Ordering::Release);
        // Explicitly unblock all pending waiters: after reset the entries they were
        // waiting on no longer exist. Sending () is cleaner than silently dropping
        // the senders, which would cause wait_durable callers to receive a
        // channel-closed error instead of a clean return.
        self.notify_waiters(u64::MAX);

        // Reset boundaries
        self.min_index.store(0, Ordering::Release);
        self.max_index.store(0, Ordering::Release);

        // Clear term indexes to ensure consistency after reset
        self.term_first_index.clear();
        self.term_last_index.clear();
        self.term_segments.clear();

        let (done_tx, done_rx) = oneshot::channel();
        self.command_sender
            .send(IOTask::Reset { done: done_tx })
            .map_err(|e| crate::Error::Fatal(format!("IOTask::Reset send failed: {e:?}")))?;
        done_rx
            .await
            .map_err(|e| crate::Error::Fatal(format!("IOTask::Reset recv failed: {e:?}")))??;

        Ok(())
    }

    async fn handle_wait_durable(
        &self,
        index: u64,
        ack: oneshot::Sender<()>,
    ) {
        let durable_index = self.durable_index.load(Ordering::Acquire);
        if index <= durable_index {
            let _ = ack.send(());
        } else {
            self.waiters.entry(index).or_default().push(ack);
        }
    }

    /// Insert entries into the in-memory index (SkipMap + term indexes + atomics).
    fn insert_to_memory(
        &self,
        entries: &[Entry],
    ) {
        for entry in entries {
            self.entries.insert(entry.index, entry.clone());
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
            let mut current_max = self.max_index.load(Ordering::Relaxed);
            while last_entry.index > current_max {
                match self.max_index.compare_exchange_weak(
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

    /// Advance `durable_index` to `new_durable` (monotonically) and wake waiters.
    fn advance_durable_and_notify(
        &self,
        new_durable: u64,
    ) {
        let prev = self.durable_index.fetch_max(new_durable, Ordering::AcqRel);
        if new_durable > prev {
            self.notify_waiters(new_durable);
            if let Some(ref tx) = self.log_flush_tx {
                let _ = tx.send(crate::RoleEvent::LogFlushed {
                    durable_index: new_durable,
                });
            }
        }
    }

    /// Notify waiters for completed flush operations
    fn notify_waiters(
        &self,
        flushed_index: u64,
    ) {
        let mut to_remove = Vec::new();

        for entry in self.waiters.iter() {
            if *entry.key() <= flushed_index {
                to_remove.push(*entry.key());
            }
        }

        for index in to_remove {
            if let Some((_, waiters)) = self.waiters.remove(&index) {
                for waiter in waiters {
                    let _ = waiter.send(());
                }
            }
        }
    }

    /// Efficient range removal with targeted term index updates
    /// O(k + t) where k = number of entries removed, t = number of affected terms
    pub fn remove_range(
        &self,
        range: RangeInclusive<u64>,
    ) {
        let (start, end) = range.into_inner();

        // MODIFIED: Track affected terms and their min/max indexes in the removal range
        let mut affected_terms: HashMap<u64, (Option<u64>, Option<u64>)> = HashMap::new();

        // Remove entries in range and track affected terms
        let mut current = start;
        while current <= end {
            if let Some(entry) = self.entries.range(current..=end).next() {
                let key = *entry.key();
                let term = entry.value().term;

                // Track min/max indexes for each affected term
                let (min_idx, max_idx) = affected_terms.entry(term).or_insert((None, None));
                if min_idx.is_none() || key < min_idx.unwrap() {
                    *min_idx = Some(key);
                }
                if max_idx.is_none() || key > max_idx.unwrap() {
                    *max_idx = Some(key);
                }

                self.entries.remove(&key);
                current = key + 1;
            } else {
                break;
            }
        }

        // MODIFIED: Update only affected term indexes
        for (term, (removed_min, removed_max)) in affected_terms {
            // Update first index if the removed entry was the first for this term
            if let Some(term_first) = self.term_first_index.get(&term) {
                let current_first = term_first.value().load(Ordering::Acquire);
                if removed_min.is_some() && current_first >= removed_min.unwrap() {
                    // Find new first index for this term
                    let new_first =
                        self.entries.iter().find(|e| e.value().term == term).map(|e| *e.key());

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
                    let new_last = self
                        .entries
                        .iter()
                        .rev()
                        .find(|e| e.value().term == term)
                        .map(|e| *e.key());

                    if let Some(idx) = new_last {
                        term_last.value().store(idx, Ordering::Release);
                    } else {
                        self.term_last_index.remove(&term);
                    }
                }
            }
        }

        // Always update boundaries after removal
        let new_min = self.entries.front().map(|e| *e.key()).unwrap_or(0);
        let new_max = self.entries.back().map(|e| *e.key()).unwrap_or(0);

        self.min_index.store(new_min, Ordering::Release);
        self.max_index.store(new_max, Ordering::Release);
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

    /// Returns the number of entries in the buffer.
    ///
    /// # Visibility
    /// This method is only available in test builds.
    #[cfg(any(test, feature = "__test_support"))]
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns reference to next_id atomic for test verification (test-only).
    ///
    /// This accessor allows tests to verify ID allocation behavior.
    #[cfg(test)]
    pub fn next_id(&self) -> &std::sync::atomic::AtomicU64 {
        &self.next_id
    }

    /// Returns true if the buffer contains no entries.
    ///
    /// This complements the `len()` method to follow Rust API conventions.
    /// Clippy requires: Any struct with a public `len` method should also
    /// have a public `is_empty` method.
    #[cfg(any(test, feature = "__test_support"))]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<T> Drop for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        if let Err(e) = self.command_sender.clone().send(IOTask::Shutdown) {
            debug!(
                "Shutdown command send failed (receiver already closed): {:?}",
                e
            );
        }
    }
}

impl<T> std::fmt::Debug for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("BufferedRaftLog").finish()
    }
}
