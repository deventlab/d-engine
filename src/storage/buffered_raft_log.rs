use super::RaftLog;
use super::StorageEngine;
use crate::alias::SOF;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::FlushPolicy;
use crate::PersistenceConfig;
use crate::PersistenceStrategy;
use crate::Result;
use crate::API_SLO;
use crate::{NetworkError, TypeConfig};
use autometrics::autometrics;
use dashmap::DashMap;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::instrument;
use tracing::trace;

const ID_BATCH_SIZE: u64 = 100;

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

    storage: Option<Arc<SOF<T>>>,
    strategy: PersistenceStrategy,
    flush_policy: FlushPolicy,

    // --- In-memory state ---
    // Pending entries
    pub entries: DashMap<u64, Entry>,
    // Tracks the highest index that has been persisted to disk
    pub durable_index: AtomicU64,
    // The next index to be allocated
    pub next_id: AtomicU64,

    // --- Flush coordination ---
    // Channel to trigger flushes
    pub command_sender: mpsc::UnboundedSender<LogCommand>,
    // Track flush state
    pub flush_state: Mutex<FlushState>,
    pub waiters: DashMap<u64, Vec<oneshot::Sender<()>>>,
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
        // Memory-first synchronous read (99% of cases)
        if let Some(entry_ref) = self.entries.get(&index) {
            return Ok(Some(entry_ref.clone()));
        }

        // Disk fallback only for DiskFirst strategy
        if let PersistenceStrategy::DiskFirst = self.strategy {
            if let Some(storage) = &self.storage {
                if let Some(entry) = storage.entry(index)? {
                    self.entries.insert(index, entry.clone());
                    return Ok(Some(entry));
                }
            }
        }

        Ok(None)
    }

    fn first_entry_id(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| *e.key())
            .min()
            .unwrap_or_else(|| self.durable_index.load(Ordering::Acquire))
    }

    fn last_entry_id(&self) -> u64 {
        match self.strategy {
            // For DiskFirst: storage has the most accurate last index
            PersistenceStrategy::DiskFirst => self.storage.as_ref().map(|s| s.last_index()).unwrap_or(0),
            // For MemFirst: memory has the complete log
            PersistenceStrategy::MemFirst => self.entries.iter().map(|e| *e.key()).max().unwrap_or(0),
        }
    }

    fn last_entry(&self) -> Option<Entry> {
        // Disk fallback only for DiskFirst strategy
        let last_index = match self.strategy {
            PersistenceStrategy::DiskFirst => self.storage.as_ref().map(|s| s.last_index()).unwrap_or(0),
            PersistenceStrategy::MemFirst => self.entries.iter().map(|e| *e.key()).max().unwrap_or(0),
        };

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
        self.entries
            .iter()
            .filter(|entry| entry.value().term == term) // Filter entries by term
            .map(|entry| *entry.key()) // Get the index of each entry
            .min() // Find the minimum index
    }

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.entries
            .iter()
            .filter(|entry| entry.value().term == term) // Filter entries by term
            .map(|entry| *entry.key()) // Get the index of each entry
            .max() // Find the maximum index
    }

    fn pre_allocate_raft_logs_next_index(&self) -> u64 {
        // self.get_raft_logs_length() + 1
        let next_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        trace!(" RaftLog pre_allocate_raft_logs_next_index: {}", next_id);
        next_id
    }

    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64> {
        // Calculate required batch size (minimum 1 batch)
        let batches = count.div_ceil(ID_BATCH_SIZE).max(1);
        let total = batches * ID_BATCH_SIZE;

        let start = self.next_id.fetch_add(total, Ordering::SeqCst);
        trace!("RaftLog pre_allocate_id_range: {}..={}", start, start + total - 1);
        start..=start + total - 1
    }

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        // For MemFirst: all entries are in memory
        if matches!(self.strategy, PersistenceStrategy::MemFirst) {
            let mut result: Vec<Entry> = self
                .entries
                .iter()
                .filter(|e| range.contains(e.key()))
                .map(|e| e.value().clone())
                .collect();
            result.sort_by_key(|e| e.index);
            return Ok(result);
        }

        // For DiskFirst: hybrid memory + disk
        let mut result: Vec<Entry> = self
            .entries
            .iter()
            .filter(|e| range.contains(e.key()))
            .map(|e| e.value().clone())
            .collect();

        if let Some(storage) = &self.storage {
            let storage_entries = storage.get_entries_range(range.clone())?;
            for entry in storage_entries {
                if !self.entries.contains_key(&entry.index) {
                    result.push(entry);
                }
            }
        }

        result.sort_by_key(|e| e.index);
        Ok(result)
    }

    #[instrument(skip(self))]
    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
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
                self.command_sender
                    .send(LogCommand::PersistEntries(indexes))
                    .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send signal: {:?}", e)))?;
            }
        }

        // Update next index
        let current_next = self.next_id.load(Ordering::Acquire);
        if max_index >= current_next {
            self.next_id.store(max_index + 1, Ordering::Release);
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

        // Remove conflicting entries
        for index in (prev_log_index + 1)..=self.last_entry_id() {
            self.entries.remove(&index);
        }

        // Truncate storage if exists
        if let Some(storage) = &self.storage {
            storage.truncate(prev_log_index + 1)?;
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
        debug!(?cutoff_index, "purge_logs_up_to");

        // Remove from memory
        self.entries.retain(|index, _| *index > cutoff_index.index);

        // Update durable index if needed
        let current_durable = self.durable_index.load(Ordering::Acquire);

        // TODO: to be double thinking
        if cutoff_index.index >= current_durable {
            self.durable_index.store(cutoff_index.index, Ordering::Release);
        }

        // Persist to storage
        if let Some(storage) = &self.storage {
            storage.purge_logs(cutoff_index)?;
        }

        Ok(())
    }

    #[autometrics(objective = API_SLO)]
    async fn flush(&self) -> Result<()> {
        // Trigger immediate flush of all pending entries
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(LogCommand::Flush(tx))
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send flush command: {:?}", e)))?;
        let _result = rx
            .await
            .map_err(|_| NetworkError::SingalSendFailed("Flush ack channel closed".into()))?;
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        // Use non-blocking send since we'll await below
        self.command_sender
            .send(LogCommand::Reset(tx))
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send reset command: {:?}", e)))?;

        // Receive the response
        let r = rx
            .await
            .map_err(|_| NetworkError::SingalSendFailed("Reset ack channel closed".to_string()))?;

        debug!("Reset completed: {:?}", r);

        Ok(())
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
        if let Some(ref storage) = self.storage {
            match storage.db_size() {
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
        Ok(0)
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
        storage: Option<Arc<SOF<T>>>,
    ) -> (Self, mpsc::UnboundedReceiver<LogCommand>) {
        let disk_len = if let Some(storage) = &storage {
            storage.last_index()
        } else {
            0
        };

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

        let entries = DashMap::new();

        // Load all entries from disk to memory
        if let PersistenceStrategy::MemFirst = persistence_config.strategy {
            if let Some(storage) = &storage {
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
            }
        }

        (
            Self {
                node_id,
                storage,
                strategy: persistence_config.strategy,
                flush_policy: persistence_config.flush_policy,
                entries,
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
        let mut pending_commands = Vec::new();

        loop {
            tokio::select! {
                cmd = receiver.recv() => {
                    if let Some(cmd) = cmd {
                        pending_commands.push(cmd);

                        // Size-based flush trigger
                        if let Some(this) = this.upgrade() {
                            if let FlushPolicy::Batch { threshold, .. } = this.flush_policy {
                                let state = this.flush_state.lock().await;
                                if state.pending_indexes.len() >= threshold {
                                    let indexes = this.get_pending_indexes().await;
                                    if !indexes.is_empty() {
                                        let _ = this.process_flush(&indexes).await;
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    if let Some(this) = this.upgrade() {
                        // Process all pending commands first
                        for cmd in pending_commands.drain(..) {
                            this.handle_command(cmd).await;
                        }

                        // Time-based flush trigger
                        let indexes = this.get_pending_indexes().await;
                        if !indexes.is_empty() {
                            let _ = this.process_flush(&indexes).await;
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
                if let Some(storage) = &self.storage {
                    let _ = storage.flush();
                }
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

        if let Some(storage) = &self.storage {
            storage.reset()?;
        }

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
    async fn process_flush(
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
        if let Some(storage) = &self.storage {
            storage.persist_entries(entries)?;

            // Handle immediate flush policy
            if matches!(self.flush_policy, FlushPolicy::Immediate) {
                storage.flush()?;
            }
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

        if let Some(storage) = &self.storage {
            storage.flush()?;
        }

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
        if let Some(storage) = &self.storage {
            storage.persist_entries(entries.to_vec())?;

            // Handle flush policy
            match self.flush_policy {
                FlushPolicy::Immediate => storage.flush()?,
                FlushPolicy::Batch { .. } => {} // Defer flush
            }
        }

        // Update durable index
        if let Some(max_index) = entries.iter().map(|e| e.index).max() {
            self.durable_index.store(max_index, Ordering::Release);
        }

        Ok(())
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
