//! High-performance buffered Raft log with static dispatch
//!
//! PersistenceStrategy defines durability guarantees:
//!
//! - DiskFirst: Entries persisted before becoming visible (SAFE, slower)
//! - MemFirst: Entries visible before persistence (FAST, crash risk) WARNING: Violates Raft
//!   durability. Use only if:
//!   - You accept potential data loss on crash
//!   - Replication provides redundancy
//!   - Performance > strict durability
//!
//! - Lock-free in-memory index
//! - Batch I/O operations
//! - Async persistence pipeline
//! - Generic storage integration
//!
//! ## Durability contract
//!
//! `durable_index` means **data is crash-safe**. It is only advanced after the storage
//! backend confirms durability: either because `LogStore::is_write_durable()` returns
//! `true` (backend auto-syncs), or after an explicit `LogStore::flush()` call.
//!
//! ## Batch flush design
//!
//! `FlushPolicy::Batch` has two flush triggers:
//!
//! - **Threshold trigger** (`handle_persist_entries`): calls `process_flush` inline when
//!   `pending_indexes` reaches the configured threshold. Inline execution is required to
//!   preserve ordering with `flush()` — dispatching to the worker pool would create an
//!   async gap. On failure the indexes are re-enqueued so the next timer tick retries them.
//! - **Timer trigger** (`batch_processor`): fires every `interval_ms` and dispatches a
//!   `FlushTask` to the flush worker pool. On a transient error the worker retries once after
//!   100 ms. If the retry also fails, the worker exits and **the batch is permanently lost** —
//!   `durable_index` will not advance and callers on `WaitDurable` will time out. This is
//!   intentional: a storage failure that survives two attempts is treated as unrecoverable.
//!
//! ## flush() semantics
//!
//! `flush()` waits until `durable_index >= max_index`. Because `durable_index` is only
//! advanced after crash-safety is confirmed, this is a true durability barrier regardless
//! of which path (threshold, timer, or direct write) performed the flush.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::Error;
use crate::FlushPolicy;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::NetworkError;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
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
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use tokio::time::Instant;
use tokio::time::interval;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::warn;

pub(crate) struct FlushWorkerPool<T>
where
    T: TypeConfig,
{
    sender: Option<mpsc::Sender<FlushTask<T>>>,
}

pub(crate) struct FlushTask<T>
where
    T: TypeConfig,
{
    indexes: Vec<u64>,
    this: Arc<BufferedRaftLog<T>>,
}

/// Commands for the log processor
#[derive(Debug)]
pub enum LogCommand {
    /// Request to wait until specific index is durable
    WaitDurable(u64, oneshot::Sender<()>),
    /// Request to persist specific log entries
    PersistEntries(Vec<u64>),
    /// Reset the log storage
    Reset(oneshot::Sender<Result<()>>),
    /// Shutdown command processor
    Shutdown,
}

pub(crate) struct FlushState {
    pending_indexes: Vec<u64>, // Indexes pending flush
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

    pub(crate) strategy: PersistenceStrategy,
    pub(crate) flush_policy: FlushPolicy,

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

    term_first_index: SkipMap<u64, AtomicU64>,
    term_last_index: SkipMap<u64, AtomicU64>,

    // --- Flush coordination ---
    // Channel to trigger flushes
    pub(crate) command_sender: mpsc::UnboundedSender<LogCommand>,
    // Track flush state
    pub(crate) flush_state: Mutex<FlushState>,
    pub(crate) waiters: DashMap<u64, Vec<oneshot::Sender<()>>>,

    // --- Flush worker pool ---
    pub(crate) flush_workers: FlushWorkerPool<T>,
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
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        self.entry(entry_id).ok().flatten().map(|entry| entry.term)
    }

    // fn first_index_for_term(
    //     &self,
    //     term: u64,
    // ) -> Option<u64> {
    //     // OPTIMIZED: Check last entry first for common case
    //     if let Some(front) = self.entries.front() {
    //         if front.value().term == term {
    //             return Some(*front.key());
    //         }
    //     }

    //     // OPTIMIZED: Forward scan stops at first match (O(k) where k is position of first term
    //     // match)
    //     self.entries.iter().find(|entry| entry.value().term == term).map(|e| *e.key())
    // }

    // fn last_index_for_term(
    //     &self,
    //     term: u64,
    // ) -> Option<u64> {
    //     // OPTIMIZED: Check last entry first for common case
    //     if let Some(last) = self.entries.back() {
    //         if last.value().term == term {
    //             return Some(*last.key());
    //         }
    //     }

    //     // OPTIMIZED: Reverse scan stops at last match (O(k) where k is position of last term
    // match)     self.entries
    //         .iter()
    //         .rev()
    //         .find(|entry| entry.value().term == term)
    //         .map(|e| *e.key())
    // }

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
        // OPTIMIZED: SkipMap range scan O(k + log n)
        Ok(self.entries.range(range).map(|e| e.value().clone()).collect())
    }

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("append_entries");

        let max_index = entries.iter().map(|e| e.index).max().unwrap_or(0);

        // Update memory based on strategy
        match self.strategy {
            PersistenceStrategy::DiskFirst => {
                // For DiskFirst, persist first then update memory
                self.persist_entries(&entries).await?;
                for entry in &entries {
                    self.entries.insert(entry.index, entry.clone());
                }

                self.update_term_indexes(&entries);
            }
            PersistenceStrategy::MemFirst => {
                // For MemFirst, update memory first then persist async
                for entry in &entries {
                    self.entries.insert(entry.index, entry.clone());
                }

                self.update_term_indexes(&entries);

                let indexes: Vec<u64> = entries.iter().map(|e| e.index).collect();
                self.command_sender.send(LogCommand::PersistEntries(indexes)).map_err(|e| {
                    NetworkError::SingalSendFailed(format!("Failed to send signal: {e:?}",))
                })?;
            }
        }

        // Update next index
        let current_next = self.next_id.load(Ordering::Acquire);
        if max_index >= current_next {
            self.next_id.store(max_index + 1, Ordering::Release);
        }
        // Update atomic indexes
        if let Some(first_entry) = entries.first() {
            // Atomic min update
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
            // Atomic max update
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
        self.command_sender.send(LogCommand::WaitDurable(index, tx)).map_err(|e| {
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
        // Virtual log handling (snapshot installation)
        if prev_log_index == 0 && prev_log_term == 0 {
            self.reset().await?;
            self.append_entries(new_entries.clone()).await?;
            return Ok(new_entries.last().map(|e| LogId {
                term: e.term,
                index: e.index,
            }));
        }

        // Check log consistency
        let prev_term = self.entry(prev_log_index)?;
        if prev_term.map(|e| e.term) != Some(prev_log_term) {
            return Ok(self.last_log_id());
        }

        // OPTIMIZATION: Skip removal if no conflicts
        let last_current_index = self.last_entry_id();
        if prev_log_index >= last_current_index {
            // Directly append new entries if no conflicts
            self.append_entries(new_entries.clone()).await?;
            return Ok(new_entries.last().map(|e| LogId {
                term: e.term,
                index: e.index,
            }));
        }

        // OPTIMIZATION: Bulk removal
        let start_index = prev_log_index + 1;
        if start_index <= last_current_index {
            // Efficient range removal without retain
            self.remove_range(start_index..=u64::MAX);

            // Truncate storage
            self.log_store.truncate(start_index).await?;
        }

        // Append new entries
        self.append_entries(new_entries.clone()).await?;

        // Return new log head
        Ok(new_entries.last().map(|e| LogId {
            term: e.term,
            index: e.index,
        }))
    }

    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        mut peer_matched_ids: Vec<u64>,
    ) -> Option<u64> {
        let _timer = ScopedTimer::new("calculate_majority_matched_index");
        // Include leader's last index
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

        // Persist to storage
        self.log_store.purge(cutoff_index).await?;

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // Wait until every entry currently in the log is durable.
        // durable_index is only advanced after the backend guarantees crash-safety
        // (see process_flush / persist_entries), so this is a true durability barrier
        // regardless of which flush path (threshold or timer) performs the actual write.
        let max_index = self.max_index.load(Ordering::Acquire);
        if max_index == 0 {
            return Ok(());
        }
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
}

impl<T> BufferedRaftLog<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        persistence_config: PersistenceConfig,
        storage: Arc<SOF<T>>,
    ) -> (Self, mpsc::UnboundedReceiver<LogCommand>) {
        let log_store = storage.log_store();
        let meta_store = storage.meta_store();
        let disk_len = log_store.last_index();

        debug!(
            "Creating BufferedRaftLog with node_id: {}, strategy: {:?}, flush: {:?}, disk_len: {:?}",
            node_id, persistence_config.strategy, persistence_config.flush_policy, disk_len
        );

        //TODO: if switch to UnboundedChannel?
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let entries = SkipMap::new();

        // Initialize term indexes
        let term_first_index = SkipMap::new();
        let term_last_index = SkipMap::new();

        // Load all entries from disk to memory
        let mut loaded_count = 0;
        if disk_len > 0 {
            match log_store.get_entries(1..=disk_len) {
                Ok(all_entries) if !all_entries.is_empty() => {
                    loaded_count = all_entries.len();
                    debug!("Successfully loaded {} entries from disk", loaded_count);

                    for entry in all_entries {
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

        let flush_workers = Self::create_flush_worker_pool(persistence_config.channel_capacity);
        (
            Self {
                node_id,
                log_store,
                meta_store,
                strategy: persistence_config.strategy,
                flush_policy: persistence_config.flush_policy,
                entries,
                min_index: AtomicU64::new(min_index),
                max_index: AtomicU64::new(max_index),
                durable_index: AtomicU64::new(disk_len),
                next_id: AtomicU64::new(disk_len + 1),
                command_sender: command_sender.clone(),
                flush_state: Mutex::new(FlushState {
                    pending_indexes: Vec::new(),
                }),
                waiters: DashMap::new(),
                term_first_index,
                term_last_index,
                flush_workers,
            },
            command_receiver,
        )
    }

    /// Start the command processor and return an Arc-wrapped instance
    pub fn start(
        self,
        receiver: mpsc::UnboundedReceiver<LogCommand>,
    ) -> Arc<Self> {
        let arc_self = Arc::new(self);
        let weak_self = Arc::downgrade(&arc_self);

        let FlushPolicy::Batch { interval_ms, .. } = arc_self.flush_policy;
        tokio::spawn(Self::batch_processor(weak_self, receiver, interval_ms));

        arc_self
    }

    async fn batch_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<LogCommand>,
        interval_ms: u64,
    ) {
        // interval_ms=0 means threshold-only flushing; use 1ms floor to satisfy tokio's
        // non-zero period requirement. The timer path is effectively dead when threshold=1.
        let mut interval = interval(Duration::from_millis(interval_ms.max(1)));
        let mut shutdown_requested = false;

        while !shutdown_requested {
            tokio::select! {
                // Priority 1: immediate command processing
                cmd = receiver.recv() => {
                    match cmd {
                        Some(LogCommand::Shutdown) => {
                            shutdown_requested = true;
                            if let Some(this) = this.upgrade() {
                                let _ = this.force_flush().await;
                                let _ = this.log_store.flush();
                                let _ = this.meta_store.flush();
                            }
                        }
                        Some(cmd) => {
                            if let Some(this) = this.upgrade() {
                                this.handle_command(cmd).await;
                            }
                        }
                        None => break,
                    }
                }
                // Priority 2: non-blocking refresh trigger
                _ = interval.tick() => {
                    if let Some(this) = this.upgrade() {
                        // Quickly get the index to be refreshed (non-blocking)
                        let indexes = this.get_pending_indexes().await;

                        if !indexes.is_empty() {

                            // Send to flush worker pool instead of spawning new task
                            let flush_task = FlushTask {
                                indexes,
                                this: this.clone(),
                            };

                            // Non-blocking send: drop the task if the channel is full
                            // (backpressure) rather than blocking the batch_processor loop.
                            match this
                                .flush_workers
                                .sender
                                .as_ref()
                                .expect("sender should exist")
                                .try_send(flush_task)
                            {
                                Ok(_) => {
                                    metrics::counter!("flush_tasks.enqueued").increment(1);
                                }
                                Err(e) => {
                                    metrics::counter!("flush_tasks.dropped").increment(1);
                                    error!("Flush worker pool full, dropping task: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(
        self: &Arc<Self>,
        cmd: LogCommand,
    ) {
        match cmd {
            LogCommand::PersistEntries(indexes) => {
                if !indexes.is_empty() {
                    self.handle_persist_entries(&indexes).await;
                }
            }
            LogCommand::WaitDurable(index, ack) => {
                self.handle_wait_durable(index, ack).await;
            }
            LogCommand::Reset(ack) => {
                if let Err(e) = self.reset_internal().await {
                    error!("Failed to reset internal state: {}", e);
                    let _ = ack.send(Err(e));
                } else {
                    //reset disk
                    let result = self.log_store.reset().await;
                    let _ = ack.send(result);
                }
            }
            LogCommand::Shutdown => {
                let _ = self.force_flush().await;
                let _ = self.log_store.flush();
                let _ = self.meta_store.flush();
            }
        }
    }

    async fn handle_persist_entries(
        self: &Arc<Self>,
        indexes: &[u64],
    ) {
        // Filter out already persisted indices
        let durable_index = self.durable_index.load(Ordering::Acquire);
        let indexes_to_process: Vec<u64> =
            indexes.iter().filter(|&&idx| idx > durable_index).cloned().collect();

        if indexes_to_process.is_empty() {
            return;
        }
        metrics::counter!("persist_entries_calls").increment(1);
        metrics::histogram!("persist_entries_batch_size").record(indexes_to_process.len() as f64);

        match self.flush_policy {
            FlushPolicy::Batch { threshold, .. } => {
                // Accumulate indexes into pending_indexes, then check threshold.
                let mut flush_now = false;
                {
                    // lock scope small for performance
                    let mut state = self.flush_state.lock().await;
                    state.pending_indexes.extend_from_slice(indexes_to_process.as_slice());
                    if state.pending_indexes.len() >= threshold {
                        flush_now = true;
                    }
                }

                if flush_now {
                    // Threshold reached: drain and flush inline.
                    //
                    // NOTE: This path calls process_flush directly (not via the worker pool) to
                    // preserve flush ordering with force_flush / LogCommand::Flush. Dispatching
                    // to the worker pool would introduce an async gap: force_flush drains
                    // pending_indexes first, so a concurrent worker-pool dispatch would race and
                    // the explicit flush could return before the data is persisted.
                    //
                    // On failure the indexes are re-enqueued so the next timer tick retries them.
                    let pending = self.get_pending_indexes().await;
                    if !pending.is_empty()
                        && let Err(e) = self.process_flush(&pending).await
                    {
                        error!(
                            "Batch persist failed, re-enqueuing {} indexes: {:?}",
                            pending.len(),
                            e
                        );
                        // Re-enqueue for retry on next timer tick.
                        let mut state = self.flush_state.lock().await;
                        state.pending_indexes.extend_from_slice(&pending);
                    }
                }
            }
        }
    }

    async fn get_pending_indexes(&self) -> Vec<u64> {
        let mut state = self.flush_state.lock().await;
        std::mem::take(&mut state.pending_indexes)
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

        self.log_store.reset().await?;

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

    /// Process entries for flush operation
    /// Separated to make error handling clearer
    pub async fn process_flush(
        &self,
        indexes: &[u64],
    ) -> Result<()> {
        // Collect entries to persist
        let entries: Vec<Entry> = indexes
            .iter()
            .filter_map(|idx| self.entries.get(idx).map(|e| e.value().clone()))
            .collect();

        // Persist to storage
        self.log_store.persist_entries(entries).await?;

        // Flush only when the backend does not guarantee per-write durability.
        if !self.log_store.is_write_durable() {
            self.log_store.flush()?;
        }

        // Update durable index
        if !indexes.is_empty() {
            let mut cur = self.durable_index.load(Ordering::Acquire);
            let mut v = indexes.to_vec();
            v.sort_unstable();
            v.dedup();
            for idx in v {
                if idx == cur + 1 {
                    cur = idx;
                } else if idx > cur + 1 {
                    break;
                }
            }
            let prev = self.durable_index.fetch_max(cur, Ordering::AcqRel);
            if cur > prev {
                self.notify_waiters(cur);
            }
        }

        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        // process_flush now ensures flush() is called before advancing durable_index,
        // so no explicit log_store.flush() is needed here.
        let indexes = self.get_pending_indexes().await;
        self.process_flush(&indexes).await
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

    async fn persist_entries(
        &self,
        entries: &[Entry],
    ) -> Result<()> {
        self.log_store.persist_entries(entries.to_vec()).await?;

        // Flush only when the backend does not guarantee per-write durability.
        if !self.log_store.is_write_durable() {
            self.log_store.flush()?;
        }

        // Update durable index
        if let Some(max_index) = entries.iter().map(|e| e.index).max() {
            let prev = self.durable_index.fetch_max(max_index, Ordering::AcqRel);
            if max_index > prev {
                self.notify_waiters(max_index);
            }
        }

        Ok(())
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

    /// Creates a flush worker pool with configurable number of workers.
    ///
    fn create_flush_worker_pool(channel_capacity: usize) -> FlushWorkerPool<T> {
        let (sender, receiver) = mpsc::channel::<FlushTask<T>>(channel_capacity);
        tokio::spawn(async move {
            let mut rx = receiver;
            loop {
                let task = match rx.recv().await {
                    Some(task) => task,
                    None => break, // sender dropped — shutdown
                };
                if !Self::run_flush_task(task).await {
                    break; // persistent failure — exit
                }
            }
            debug!("Flush worker shutting down");
        });
        FlushWorkerPool {
            sender: Some(sender),
        }
    }

    /// Returns `true` if the worker should continue, `false` on persistent failure.
    async fn run_flush_task(task: FlushTask<T>) -> bool {
        let FlushTask { indexes, this } = task;
        let start_time = Instant::now();
        metrics::counter!("flush_worker.requests").increment(1);
        metrics::histogram!("flush_worker.batch_size").record(indexes.len() as f64);

        match this.process_flush(&indexes).await {
            Ok(_) => {
                let duration = start_time.elapsed();
                metrics::histogram!("flush_worker.success_duration")
                    .record(duration.as_micros() as f64);
                debug!(
                    "Flush worker processed {} entries in {:?}",
                    indexes.len(),
                    duration
                );
                true
            }
            Err(e) => {
                metrics::counter!("flush_worker.errors").increment(1);
                error!("Flush worker failed: {}", e);

                if Self::is_transient_error(&e) {
                    warn!("Flush worker retrying after 100ms");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    match this.process_flush(&indexes).await {
                        Ok(_) => {
                            debug!("Flush worker retry succeeded");
                            true
                        }
                        Err(retry_err) => {
                            error!("Flush worker retry failed: {}", retry_err);
                            false
                        }
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Helper to determine if an error is transient and worth retrying
    fn is_transient_error(error: &Error) -> bool {
        // Implement logic based on your error types
        // For example, network timeouts, temporary IO errors, etc.
        error.to_string().contains("timeout")
            || error.to_string().contains("temporary")
            || error.to_string().contains("busy")
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

    /// Returns the current durable index (test-only).
    ///
    /// This accessor allows tests to verify persistence progress without
    /// accessing the internal AtomicU64 directly.
    #[cfg(any(test, feature = "__test_support"))]
    pub fn durable_index(&self) -> u64 {
        self.durable_index.load(Ordering::Acquire)
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
        if let Err(e) = self.command_sender.clone().send(LogCommand::Shutdown) {
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
