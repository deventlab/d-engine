use autometrics::autometrics;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use std::ops::RangeInclusive;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::time::interval;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::trace;

use super::RaftLog;
use super::StorageEngine;
use crate::alias::SOF;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::scoped_timer::ScopedTimer;
use crate::FlushPolicy;
use crate::NetworkError;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
use crate::Result;
use crate::TypeConfig;
use crate::API_SLO;

/// Commands for the log processor
#[derive(Debug)]
pub enum LogCommand {
    /// Request to wait until specific index is durable
    WaitDurable(u64, oneshot::Sender<()>),
    /// Request to persist specific log entries
    PersistEntries(Vec<u64>),
    /// Trigger immediate flush with result notification
    Flush(oneshot::Sender<Result<()>>),
    /// Reset the log storage
    Reset(oneshot::Sender<Result<()>>),
    /// Shutdown command processor
    Shutdown,
}

pub struct FlushState {
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

    pub(crate) storage: Arc<SOF<T>>,
    strategy: PersistenceStrategy,
    flush_policy: FlushPolicy,

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

    // --- Flush coordination ---
    // Channel to trigger flushes
    pub(crate) command_sender: mpsc::UnboundedSender<LogCommand>,
    // Track flush state
    pub(crate) flush_state: Mutex<FlushState>,
    pub(crate) waiters: DashMap<u64, Vec<oneshot::Sender<()>>>,
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

    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        // OPTIMIZED: Check last entry first for common case
        if let Some(front) = self.entries.front() {
            if front.value().term == term {
                return Some(*front.key());
            }
        }

        // OPTIMIZED: Forward scan stops at first match (O(k) where k is position of first term match)
        self.entries.iter().find(|entry| entry.value().term == term).map(|e| *e.key())
    }

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        // OPTIMIZED: Check last entry first for common case
        if let Some(last) = self.entries.back() {
            if last.value().term == term {
                return Some(*last.key());
            }
        }

        // OPTIMIZED: Reverse scan stops at last match (O(k) where k is position of last term match)
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.value().term == term)
            .map(|e| *e.key())
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
            }
            PersistenceStrategy::MemFirst => {
                // For MemFirst, update memory first then persist async
                for entry in &entries {
                    self.entries.insert(entry.index, entry.clone());
                }
                let indexes: Vec<u64> = entries.iter().map(|e| e.index).collect();
                self.command_sender.send(LogCommand::PersistEntries(indexes)).map_err(|e| {
                    NetworkError::SingalSendFailed(format!("Failed to send signal: {:?}", e))
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
            self.storage.truncate(start_index)?;
        }

        // Append new entries
        self.append_entries(new_entries.clone()).await?;

        // Return new log head
        Ok(new_entries.last().map(|e| LogId {
            term: e.term,
            index: e.index,
        }))
    }

    #[autometrics(objective = API_SLO)]
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

    #[autometrics(objective = API_SLO)]
    fn purge_logs_up_to(
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
        self.storage.purge_logs(cutoff_index)?;

        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    async fn flush(&self) -> Result<()> {
        // Trigger immediate flush of all pending entries
        let (tx, rx) = oneshot::channel();
        self.command_sender.send(LogCommand::Flush(tx)).map_err(|e| {
            NetworkError::SingalSendFailed(format!("Failed to send flush command: {:?}", e))
        })?;
        let _result = rx
            .await
            .map_err(|_| NetworkError::SingalSendFailed("Flush ack channel closed".into()))?;
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let _timer = ScopedTimer::new("buffered_raft_log::reset");
        self.reset_internal()?;

        let storage_clone = self.storage.clone();
        tokio::spawn(async move {
            let _ = storage_clone.reset();
        });

        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<crate::HardState>> {
        self.storage.load_hard_state()
    }

    fn save_hard_state(
        &self,
        hard_state: crate::HardState,
    ) -> Result<()> {
        self.storage.save_hard_state(hard_state)
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
        // if nothing found in cache, we will query the size from the db directly
        match self.storage.db_size() {
            Ok(size) => {
                // db_size_cache.size.store(size, Ordering::Release);
                // db_size_cache.last_activity.insert(node_id, now);
                debug!("retrieved the real db size: {size}",);
                println!("retrieved the real db size: {size}",);
                return Ok(size);
            }
            Err(e) => {
                error!("db_size() failed: {e:?}");
                eprintln!("db_size() failed: {e:?}");
                return Err(e);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
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
        let disk_len = storage.last_index();

        debug!(
            "Creating BufferedRaftLog with node_id: {}, strategy: {:?}, flush: {:?}, disk_len: {:?}",
            node_id, persistence_config.strategy, persistence_config.flush_policy, disk_len
        );

        println!(
            "Creating BufferedRaftLog with node_id: {}, strategy: {:?}, flush: {:?}, disk_len: {:?}",
            node_id, persistence_config.strategy, persistence_config.flush_policy, disk_len
        );

        //TODO: if switch to UnboundedChannel?
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let entries = SkipMap::new();

        // Load all entries from disk to memory
        if disk_len > 0 {
            match storage.get_entries_range(1..=disk_len) {
                Ok(all_entries) => {
                    for entry in all_entries {
                        entries.insert(entry.index, entry);
                    }
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

        (
            Self {
                node_id,
                storage,
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

        // Start background processor based on flush policy
        if let FlushPolicy::Batch { interval_ms, .. } = arc_self.flush_policy {
            tokio::spawn(Self::batch_processor(weak_self, receiver, interval_ms));
        } else {
            tokio::spawn(Self::command_processor(weak_self, receiver));
        }

        arc_self
    }

    async fn command_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<LogCommand>,
    ) {
        trace!("Starting command processor");
        while let Some(cmd) = receiver.recv().await {
            let Some(this) = this.upgrade() else { break };
            this.handle_command(cmd).await;
        }
        trace!("Command processor shutting down");
    }

    async fn batch_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<LogCommand>,
        interval_ms: u64,
    ) {
        let mut interval = interval(Duration::from_millis(interval_ms));

        loop {
            tokio::select! {
                // Priority 1: immediate command processing
                cmd = receiver.recv() => {
                    if let Some(cmd) = cmd {
                        if let Some(this) = this.upgrade() {
                            // Process immediately without delay
                            this.handle_command(cmd).await;
                        }
                    }
                }
                // Priority 2: non-blocking refresh trigger
                _ = interval.tick() => {
                    if let Some(this) = this.upgrade() {
                        // Quickly get the index to be refreshed (non-blocking)
                        let indexes = this.get_pending_indexes().await;

                        if !indexes.is_empty() {
                            // Background asynchronous refresh (do not block the command processor)
                            let this_clone = this.clone();
                            tokio::spawn(async move {
                                let _ = this_clone.process_flush(&indexes).await;
                            });
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(
        &self,
        cmd: LogCommand,
    ) {
        match cmd {
            LogCommand::PersistEntries(indexes) => {
                self.handle_persist_entries(&indexes).await;
            }
            LogCommand::WaitDurable(index, ack) => {
                self.handle_wait_durable(index, ack).await;
            }
            LogCommand::Flush(ack) => {
                let result = self.force_flush().await;
                let _ = ack.send(result);
            }
            LogCommand::Reset(ack) => {
                let result = self.reset_internal();
                let _ = ack.send(result);
            }
            LogCommand::Shutdown => {
                let _ = self.force_flush().await;
                let _ = self.storage.flush();
            }
        }
    }

    async fn handle_persist_entries(
        &self,
        indexes: &[u64],
    ) {
        let should_flush = match self.flush_policy {
            FlushPolicy::Immediate => true,
            FlushPolicy::Batch { threshold, .. } => {
                let mut state = self.flush_state.lock().await;
                state.pending_indexes.extend_from_slice(indexes);
                state.pending_indexes.len() >= threshold
            }
        };

        if should_flush {
            let indexes = self.get_pending_indexes().await;
            if !indexes.is_empty() {
                let _ = self.process_flush(&indexes).await;
            }
        }
    }

    async fn get_pending_indexes(&self) -> Vec<u64> {
        let mut state = self.flush_state.lock().await;
        std::mem::take(&mut state.pending_indexes)
    }

    fn reset_internal(&self) -> Result<()> {
        self.entries.clear();
        self.durable_index.store(0, Ordering::Release);
        self.next_id.store(1, Ordering::Release);
        self.waiters.clear();

        // Reset boundaries
        self.min_index.store(0, Ordering::Release);
        self.max_index.store(0, Ordering::Release);

        self.storage.reset()?;

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
    pub(super) async fn process_flush(
        &self,
        indexes: &[u64],
    ) -> Result<()> {
        // Collect entries to persist
        let entries: Vec<Entry> = indexes
            .iter()
            .filter_map(|idx| self.entries.get(idx).map(|e| e.value().clone()))
            .collect();

        trace!("Collected {} entries for persistence", entries.len());

        // Persist to storage
        self.storage.persist_entries(entries)?;

        // Handle immediate flush policy
        if matches!(self.flush_policy, FlushPolicy::Immediate) {
            self.storage.flush()?;
        }

        // Update durable index
        if let Some(max_index) = indexes.iter().max() {
            self.durable_index.store(*max_index, Ordering::Release);
            self.notify_waiters(*max_index);
        }

        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        let indexes = self.get_pending_indexes().await;
        let result = self.process_flush(&indexes).await;

        self.storage.flush()?;

        result
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
        self.storage.persist_entries(entries.to_vec())?;

        // Handle flush policy
        match self.flush_policy {
            FlushPolicy::Immediate => self.storage.flush()?,
            FlushPolicy::Batch { .. } => {} // Defer flush
        }

        // Update durable index
        if let Some(max_index) = entries.iter().map(|e| e.index).max() {
            self.durable_index.store(max_index, Ordering::Release);
        }

        Ok(())
    }

    /// Efficient range removal without locks
    /// O(k log n) where k = number of entries in range
    ///
    /// range: [start, end]
    pub(crate) fn remove_range(
        &self,
        range: RangeInclusive<u64>,
    ) {
        let (start, end) = range.into_inner();

        // Remove entries in range
        let mut current = start;
        while current <= end {
            if let Some(entry) = self.entries.range(current..=end).next() {
                let key = *entry.key();
                self.entries.remove(&key);
                current = key + 1;
            } else {
                break;
            }
        }

        // Always update boundaries after removal
        let new_min = self.entries.front().map(|e| *e.key()).unwrap_or(0);
        let new_max = self.entries.back().map(|e| *e.key()).unwrap_or(0);

        self.min_index.store(new_min, Ordering::Release);
        self.max_index.store(new_max, Ordering::Release);
    }
}

impl<T> Drop for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        if let Err(e) = self.command_sender.clone().send(LogCommand::Shutdown) {
            error!("Failed to send shutdown command: {:?}", e);
        }

        trace!("BufferedRaftLog dropped");
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
