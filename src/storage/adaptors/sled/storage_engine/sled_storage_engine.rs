use crate::convert::safe_vk;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::Error;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::ProstError;
use crate::StorageEngine;
use crate::StorageError;
use bytes::Bytes;
use prost::Message;
use sled::Batch;
use sled::IVec;
use sled::Tree;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tonic::async_trait;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;

pub const HARD_STATE_KEY: &[u8] = b"hard_state";

/// Dedicated log store implementation
pub struct SledLogStore {
    tree: sled::Tree,
}

/// Dedicated metadata store implementation
pub struct SledMetaStore {
    tree: sled::Tree,
}

/// Unified storage engine
pub struct SledStorageEngine {
    log_store: Arc<SledLogStore>,
    meta_store: Arc<SledMetaStore>,
}

impl StorageEngine for SledStorageEngine {
    type LogStore = SledLogStore;
    type MetaStore = SledMetaStore;

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
impl LogStore for SledLogStore {
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let mut batch = Batch::default();
        let mut max_index = 0;

        trace!("persist_entries len = {:?}", entries.len(),);

        for entry in entries {
            let key = SledStorageEngine::index_to_key(entry.index);
            let value = entry.encode_to_vec();
            batch.insert(&key, value);
            max_index = max_index.max(entry.index);
        }

        self.tree.apply_batch(batch)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let key = SledStorageEngine::index_to_key(index);
        match self.tree.get(key)? {
            Some(bytes) => {
                Entry::decode(&*bytes).map(Some).map_err(|e| ProstError::Decode(e).into())
            }

            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let start = SledStorageEngine::index_to_key(*range.start());
        let end = SledStorageEngine::index_to_key(*range.end());
        let mut entries = Vec::new();

        for item in self.tree.range(start..=end) {
            let (_, value) = item?;
            let entry = Entry::decode(&*value).map_err(|e| Error::from(ProstError::Decode(e)))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    #[instrument(skip(self))]
    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let start = SledStorageEngine::index_to_key(0);
        let end = SledStorageEngine::index_to_key(cutoff_index.index);
        let mut batch = Batch::default();

        for item in self.tree.range(start..=end) {
            let (key, _) = item?;
            batch.remove(key);
        }

        self.tree.apply_batch(batch)?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let start_key = SledStorageEngine::index_to_key(from_index);
        let mut batch = Batch::default();

        for item in self.tree.range(start_key..) {
            let (key, _) = item?;
            batch.remove(key);
        }

        self.tree.apply_batch(batch)?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn flush(&self) -> Result<(), Error> {
        trace!("LogStore flush");
        self.tree.flush()?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        trace!("LogStore flush");
        self.tree.flush_async().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn reset(&self) -> Result<(), Error> {
        self.tree.clear()?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn last_index(&self) -> u64 {
        match self.tree.last() {
            Ok(Some((key, _))) => match safe_vk(&key) {
                Ok(index) => index,
                Err(err) => {
                    tracing::warn!("Invalid key format in sled: {:?}", err);
                    0
                }
            },
            _ => 0,
        }
    }
}

#[async_trait]
impl MetaStore for SledMetaStore {
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        // let serialized = bincode::serialize(state)?;
        // self.tree.insert(HARD_STATE_KEY, serialized)?;
        // Ok(())
        match bincode::serialize(state) {
            Ok(v) => {
                self.tree.insert(HARD_STATE_KEY, IVec::from(v.as_ref() as &[u8]))?;

                info!("persistent_state_into_db successfully!");
                println!("persistent_state_into_db successfully!");
            }
            Err(e) => {
                error!("persistent_state_into_db: {}", e);
                eprintln!("persistent_state_into_db: {e}");
                return Err(StorageError::BincodeError(e).into());
            }
        }
        match self.flush() {
            Ok(()) => {
                info!("Successfully flushed sled DB");
            }
            Err(e) => {
                error!("Failed to flush sled DB: {}", e);
            }
        }

        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        info!(
            "pending load_role_hard_state_from_db with key: {:?}",
            HARD_STATE_KEY
        );

        match self.tree.get(HARD_STATE_KEY)? {
            Some(ivec) => {
                let bytes = Bytes::copy_from_slice(ivec.as_ref());
                info!("found node state from DB with key: {:?}", HARD_STATE_KEY);

                match bincode::deserialize::<HardState>(bytes.as_ref()) {
                    Ok(hard_state) => {
                        info!(
                            "load_role_hard_state_from_db: current_term={:?}",
                            hard_state.current_term
                        );
                        Ok(Some(hard_state))
                    }
                    Err(e) => {
                        error!(
                            "state:load_role_hard_state_from_db deserialize error. {}",
                            e
                        );
                        Ok(None)
                    }
                }
            }
            None => {
                info!(
                    "no hard state found from db with key: {:?}.",
                    HARD_STATE_KEY
                );
                Ok(None)
            }
        }
    }

    fn flush(&self) -> Result<(), Error> {
        self.tree.flush()?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        trace!("MetaStore flush");
        self.tree.flush_async().await?;
        Ok(())
    }
}

impl std::fmt::Debug for SledStorageEngine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledRaftLog").finish()
    }
}

impl Drop for SledStorageEngine {
    fn drop(&mut self) {
        match self.log_store.flush() {
            Ok(_) => info!("Successfully flush log_store"),
            Err(e) => error!(?e, "Failed to flush log_store"),
        }
        match self.meta_store.flush() {
            Ok(_) => info!("Successfully flush meta_store"),
            Err(e) => error!(?e, "Failed to flush meta_store"),
        }
    }
}

impl SledStorageEngine {
    /// Creates new storage engine with physically separated stores
    ///
    /// # Panics
    /// If log and meta trees are the same
    pub fn new(
        log_tree: Tree,
        meta_tree: Tree,
    ) -> Self {
        assert!(
            log_tree.name() != meta_tree.name(),
            "CRITICAL: Log and metadata must use different trees"
        );

        Self {
            log_store: Arc::new(SledLogStore { tree: log_tree }),
            meta_store: Arc::new(SledMetaStore { tree: meta_tree }),
        }
    }

    /// Helper: convert index to big-endian bytes
    #[inline]
    pub fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Helper: convert key bytes to index
    #[allow(dead_code)]
    #[inline]
    pub fn key_to_index(key: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[0..8]);
        u64::from_be_bytes(bytes)
    }
}

impl SledLogStore {
    #[cfg(test)]
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]> + 'static,
        V: AsRef<[u8]> + 'static,
    {
        match self.tree.insert(key, IVec::from(value.as_ref()))? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn get<K>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, Error>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        match self.tree.get(key.as_ref())? {
            Some(ivec) => Ok(Some(Bytes::copy_from_slice(ivec.as_ref()))),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SledMetaStore {
    #[cfg(test)]
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]> + 'static,
        V: AsRef<[u8]> + 'static,
    {
        match self.tree.insert(key, IVec::from(value.as_ref()))? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn get<K>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, Error>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        match self.tree.get(key.as_ref())? {
            Some(ivec) => Ok(Some(Bytes::copy_from_slice(ivec.as_ref()))),
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
