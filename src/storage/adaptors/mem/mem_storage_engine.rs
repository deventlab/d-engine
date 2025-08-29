use crate::proto::common::{Entry, LogId};
use crate::Result;
use crate::{HardState, LogStore, MetaStore, StorageEngine, StorageError, HARD_STATE_KEY};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::async_trait;
use tracing::{info, trace};

/// In-memory log store implementation
#[derive(Debug)]
pub struct MemoryLogStore {
    entries: RwLock<BTreeMap<u64, Entry>>,
    last_index: AtomicU64,
}

/// In-memory metadata store implementation
#[derive(Debug)]
pub struct MemoryMetaStore {
    data: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

/// Unified in-memory storage engine
#[derive(Debug)]
pub struct MemoryStorageEngine {
    log_store: Arc<MemoryLogStore>,
    meta_store: Arc<MemoryMetaStore>,
}

impl StorageEngine for MemoryStorageEngine {
    type LogStore = MemoryLogStore;
    type MetaStore = MemoryMetaStore;

    #[inline]
    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    #[inline]
    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        trace!("persist_entries len = {:?}", entries.len());

        let mut store = self.entries.write();
        let mut max_index = 0;

        for entry in entries {
            let index = entry.index;
            store.insert(index, entry);
            max_index = max_index.max(index);
        }

        if max_index > 0 {
            self.last_index.store(max_index, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        let store = self.entries.read();
        Ok(store.get(&index).cloned())
    }

    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        let store = self.entries.read();
        let mut result = Vec::new();

        for (_, entry) in store.range(range) {
            result.push(entry.clone());
        }

        Ok(result)
    }

    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<()> {
        let mut store = self.entries.write();
        let keys_to_remove: Vec<u64> =
            store.range(..=cutoff_index.index).map(|(k, _)| *k).collect();

        for key in keys_to_remove {
            store.remove(&key);
        }

        Ok(())
    }

    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<()> {
        let mut store = self.entries.write();
        let keys_to_remove: Vec<u64> = store.range(from_index..).map(|(k, _)| *k).collect();

        for key in keys_to_remove {
            store.remove(&key);
        }

        // Update last index if needed
        if let Some(last) = store.keys().last() {
            self.last_index.store(*last, Ordering::SeqCst);
        } else {
            self.last_index.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    fn flush(&self) -> Result<()> {
        trace!("MemoryLogStore flush (no-op)");
        Ok(())
    }

    async fn flush_async(&self) -> Result<()> {
        trace!("MemoryLogStore flush_async (no-op)");
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let mut store = self.entries.write();
        store.clear();
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<()> {
        let serialized = bincode::serialize(state).map_err(|e| StorageError::BincodeError(e))?;

        let mut data = self.data.write();
        data.insert(HARD_STATE_KEY.to_vec(), serialized);

        info!("Persisted hard state successfully");
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>> {
        let data = self.data.read();

        match data.get(HARD_STATE_KEY) {
            Some(bytes) => {
                let state =
                    bincode::deserialize(bytes).map_err(|e| StorageError::BincodeError(e))?;
                info!("Loaded hard state from memory");
                Ok(Some(state))
            }
            None => {
                info!("No hard state found in memory");
                Ok(None)
            }
        }
    }

    fn flush(&self) -> Result<()> {
        trace!("MemoryMetaStore flush (no-op)");
        Ok(())
    }

    async fn flush_async(&self) -> Result<()> {
        trace!("MemoryMetaStore flush_async (no-op)");
        Ok(())
    }
}

impl MemoryStorageEngine {
    /// Creates new in-memory storage engine
    pub fn new() -> Self {
        Self {
            log_store: Arc::new(MemoryLogStore {
                entries: RwLock::new(BTreeMap::new()),
                last_index: AtomicU64::new(0),
            }),
            meta_store: Arc::new(MemoryMetaStore {
                data: RwLock::new(HashMap::new()),
            }),
        }
    }
}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

// Test helper methods
#[cfg(test)]
impl MemoryLogStore {
    pub fn insert(
        &self,
        index: u64,
        entry: Entry,
    ) -> Result<()> {
        let mut store = self.entries.write();
        store.insert(index, entry);
        self.last_index.store(index, Ordering::SeqCst);
        Ok(())
    }

    pub fn get(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        let store = self.entries.read();
        Ok(store.get(&index).cloned())
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

#[cfg(test)]
impl MemoryMetaStore {
    pub fn insert(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        let mut data = self.data.write();
        data.insert(key, value);
        Ok(())
    }

    pub fn get(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }
}
