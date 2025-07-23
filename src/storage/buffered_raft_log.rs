use super::RaftLog;
use super::StorageEngine;
use crate::alias::SOF;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::PersistenceStrategy;
use crate::Result;
use crate::API_SLO;
use crate::{NetworkError, TypeConfig};
use autometrics::autometrics;
use dashmap::DashMap;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex};
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::trace;

const ID_BATCH_SIZE: u64 = 100;

/// Commands for the log processor
pub enum LogCommand {
    /// Request to wait until specific index is durable
    WaitDurable(oneshot::Sender<()>),
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
    flushing: bool,            // Is a flush in progress?
}

pub struct BufferedRaftLog<T>
where
    T: TypeConfig,
{
    #[allow(dead_code)]
    node_id: u32,

    storage: Option<Arc<SOF<T>>>,
    strategy: PersistenceStrategy,

    // --- In-memory state ---
    // Pending entries
    pub entries: DashMap<u64, Entry>,
    // Tracks the highest index that has been persisted to disk
    pub durable_index: AtomicU64,
    // The next index to be allocated
    pub next_id: AtomicU64,

    // --- Flush coordination ---
    // Channel to trigger flushes
    pub command_sender: mpsc::Sender<LogCommand>,
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

        // Fallback to storage only if configured
        match &self.storage {
            Some(storage) => {
                // Async only when absolutely necessary
                let storage = storage.clone();
                tokio::task::block_in_place(|| storage.get_entry(index))
            }
            None => Ok(None),
        }
    }

    fn first_entry_id(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| *e.key())
            .min()
            .unwrap_or_else(|| self.durable_index.load(Ordering::Acquire))
    }

    fn last_entry_id(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| *e.key())
            .max()
            .unwrap_or_else(|| self.durable_index.load(Ordering::Acquire))
    }

    fn last_entry(&self) -> Option<Entry> {
        self.entries
            .iter()
            // Find the entry with the maximum key (index)
            .max_by_key(|entry| *entry.key())
            // Clone the entry value if found
            .map(|entry| entry.value().clone())
    }

    fn last_log_id(&self) -> Option<LogId> {
        self.last_entry().map(|entry| LogId {
            term: entry.term,
            index: entry.index,
        })
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
        trace!("RaftLog pre_allocate_id_range: {}", start);
        start..=start + total - 1
    }

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        let mut result: Vec<Entry> = self
            .entries
            .iter()
            .filter(|e| range.contains(e.key()))
            .map(|e| e.value().clone())
            .collect();

        // Early return if memory-only
        if self.storage.is_none() {
            result.sort_by_key(|e| e.index);
            return Ok(result);
        }

        // Hybrid mode: fill gaps from storage
        let storage_entries = if let Some(storage) = &self.storage {
            let storage = storage.clone();
            tokio::task::block_in_place(|| storage.get_entries_range(range.clone()))?
        } else {
            Vec::new()
        };

        // Merge results
        for entry in storage_entries {
            if !self.entries.contains_key(&entry.index) {
                result.push(entry);
            }
        }

        result.sort_by_key(|e| e.index);
        Ok(result)
    }

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let max_index = entries.iter().map(|e| e.index).max().unwrap_or(0);

        // Add to memory entries
        for entry in &entries {
            self.entries.insert(entry.index, entry.clone());
        }

        // Update next index
        let current_next = self.next_id.load(Ordering::Acquire);
        if max_index >= current_next {
            self.next_id.store(max_index + 1, Ordering::Release);
        }

        // Handle persistence strategy
        match self.strategy {
            PersistenceStrategy::DiskFirst => {
                // Block until persisted
                self.persist_entries(entries.iter().map(|e| e.index).collect()).await?;
            }
            PersistenceStrategy::MemFirst | PersistenceStrategy::Batched(_, _) => {
                // Queue for async persistence
                let indexes: Vec<u64> = entries.iter().map(|e| e.index).collect();
                self.command_sender
                    .send(LogCommand::PersistEntries(indexes))
                    .await
                    .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send signal: {:?}", e)))?;
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
        // Virtual log handling (snapshot installation)
        if prev_log_index == 0 && prev_log_term == 0 {
            self.reset()?;
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
    fn flush(&self) -> Result<()> {
        // Trigger immediate flush of all pending entries
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .blocking_send(LogCommand::Flush(tx))
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send flush command: {:?}", e)))?;

        // Wait for flush completion
        let _ = rx
            .blocking_recv()
            .map_err(|e| NetworkError::SingalSendFailed(format!("Flush ack channel closed: {:?}", e)))?;

        Ok(())
    }

    fn reset(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .blocking_send(LogCommand::Reset(tx))
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send reset command: {:?}", e)))?;

        rx.blocking_recv()
            .map_err(|_| NetworkError::SingalSendFailed("Reset ack channel closed".into()))?
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
        strategy: PersistenceStrategy,
        storage: Option<Arc<SOF<T>>>,
    ) -> Arc<Self> {
        let disk_len = storage
            .as_ref()
            .map(|s| tokio::task::block_in_place(|| s.last_index()))
            .unwrap_or(0);

        let (command_sender, command_receiver) = mpsc::channel(100);

        let instance = Arc::new(Self {
            node_id,
            storage,
            strategy,
            entries: DashMap::new(),
            durable_index: AtomicU64::new(disk_len),
            next_id: AtomicU64::new(disk_len + 1),
            command_sender: command_sender.clone(),
            flush_state: Mutex::new(FlushState {
                pending_indexes: Vec::new(),
                flushing: false,
            }),
            waiters: DashMap::new(),
        });

        // Start command processor
        tokio::spawn(Self::command_processor(Arc::downgrade(&instance), command_receiver));

        instance
    }

    async fn command_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::Receiver<LogCommand>,
    ) {
        while let Some(cmd) = receiver.recv().await {
            let Some(this) = this.upgrade() else { break };

            match cmd {
                LogCommand::PersistEntries(indexes) => {
                    this.handle_persist_command(indexes).await;
                }
                LogCommand::WaitDurable(ack) => {
                    this.handle_wait_command(ack).await;
                }
                LogCommand::Flush(ack) => {
                    let result = this.flush_pending().await;
                    if let Some(storage) = &this.storage {
                        if let Err(e) = storage.flush() {
                            error!("Storage flush failed: {:?}", e);
                        }
                    }
                    let _ = ack.send(result);
                }
                LogCommand::Reset(ack) => {
                    let result = this.reset_internal();
                    let _ = ack.send(result);
                }
                LogCommand::Shutdown => break,
            }
        }
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

    async fn handle_persist_command(
        &self,
        indexes: Vec<u64>,
    ) {
        let mut state = self.flush_state.lock().await;
        state.pending_indexes.extend(indexes);

        match self.strategy {
            PersistenceStrategy::Batched(batch_size, _) if state.pending_indexes.len() >= batch_size => {
                if let Err(e) = self.flush_pending().await {
                    error!("Batch flush failed: {:?}", e);
                }
            }
            _ => {
                let _ = self.flush_pending().await;
            }
        }
    }

    async fn handle_wait_command(
        &self,
        ack: oneshot::Sender<()>,
    ) {
        let durable_index = self.durable_index.load(Ordering::Acquire);
        if durable_index >= self.last_entry_id() {
            let _ = ack.send(());
        } else {
            self.waiters.entry(self.last_entry_id()).or_default().push(ack);
        }
    }

    async fn flush_pending(&self) -> Result<()> {
        let indexes = {
            let mut state = self.flush_state.lock().await;
            if state.flushing {
                return Ok(());
            }
            state.flushing = true;
            std::mem::take(&mut state.pending_indexes)
        };

        if !indexes.is_empty() {
            // Collect entries to persist
            let entries: Vec<Entry> = indexes
                .iter()
                .filter_map(|idx| self.entries.get(idx).map(|e| e.value().clone()))
                .collect();

            // Persist to storage
            if let Some(storage) = &self.storage {
                storage.persist_entries(entries)?;
            }

            // Update durable index
            let max_index = indexes.iter().max().copied().unwrap_or(0);
            self.durable_index.store(max_index, Ordering::Release);

            // Notify waiters
            for index in indexes {
                if let Some((_, waiters)) = self.waiters.remove(&index) {
                    for waiter in waiters {
                        let _ = waiter.send(());
                    }
                }
            }
        }

        let mut state = self.flush_state.lock().await;
        state.flushing = false;
        Ok(())
    }

    async fn persist_entries(
        &self,
        indexes: Vec<u64>,
    ) -> Result<()> {
        self.command_sender
            .send(LogCommand::PersistEntries(indexes.clone()))
            .await
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send persist command: {:?}", e)))?;

        if matches!(self.strategy, PersistenceStrategy::DiskFirst) {
            if let Some(&last_index) = indexes.last() {
                self.wait_durable(last_index).await?;
            }
        }

        Ok(())
    }

    async fn wait_durable(
        &self,
        index: u64,
    ) -> Result<()> {
        if index <= self.durable_index.load(Ordering::Acquire) {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.waiters.entry(index).or_default().push(tx);

        rx.await
            .map_err(|_| NetworkError::SingalSendFailed("Wait for durable ack channel closed".into()))?;
        Ok(())
    }
}

impl<T> Drop for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        let _ = self.command_sender.blocking_send(LogCommand::Shutdown);
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
