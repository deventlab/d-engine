use crate::convert::safe_vk;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::storage::RAFT_LOG_NAMESPACE;
use crate::Error;
use crate::ProstError;
use crate::Result;
use crate::StorageEngine;
use prost::Message;
use sled::Batch;
use sled::Db;
use std::ops::RangeInclusive;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;

pub struct SledStorageEngine {
    #[allow(dead_code)]
    db: sled::Db,

    pub(super) tree: sled::Tree,
}

impl StorageEngine for SledStorageEngine {
    fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        let mut batch = Batch::default();

        trace!("persist_entries len = {:?}", entries.len(),);

        for entry in entries {
            let key = Self::index_to_key(entry.index);
            let value = entry.encode_to_vec();
            batch.insert(&key, value);
        }

        self.tree.apply_batch(batch)?;
        trace!("last_index = {}", self.last_index());
        Ok(())
    }

    #[instrument(skip(self))]
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        let key = Self::index_to_key(index);
        match self.tree.get(key)? {
            Some(bytes) => Entry::decode(&*bytes)
                .map(Some)
                .map_err(|e| ProstError::Decode(e).into()),
            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        let start = Self::index_to_key(*range.start());
        let end = Self::index_to_key(*range.end());
        let mut entries = Vec::new();

        for item in self.tree.range(start..=end) {
            let (_, value) = item?;
            let entry = Entry::decode(&*value).map_err(|e| Error::from(ProstError::Decode(e)))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    #[instrument(skip(self))]
    fn purge_logs(
        &self,
        cutoff_index: LogId,
    ) -> Result<()> {
        let start = Self::index_to_key(0);
        let end = Self::index_to_key(cutoff_index.index);
        let mut batch = Batch::default();

        for item in self.tree.range(start..=end) {
            let (key, _) = item?;
            batch.remove(key);
        }

        self.tree.apply_batch(batch)?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn flush(&self) -> Result<()> {
        trace!("SledStorageEngine flush");
        self.tree.flush()?;
        Ok(())
    }

    #[instrument(skip(self))]
    fn reset(&self) -> Result<()> {
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

    #[instrument(skip(self))]
    fn truncate(
        &self,
        from_index: u64,
    ) -> Result<()> {
        let start_key = Self::index_to_key(from_index);
        let mut batch = Batch::default();

        for item in self.tree.range(start_key..) {
            let (key, _) = item?;
            batch.remove(key);
        }

        self.tree.apply_batch(batch)?;
        Ok(())
    }

    #[cfg(test)]
    fn db_size(&self) -> Result<u64> {
        use crate::StorageError;

        self.db
            .size_on_disk()
            .map_err(|e| StorageError::DbError(e.to_string()).into())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tree.len()
    }
}

impl std::fmt::Debug for SledStorageEngine {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledRaftLog")
            .field("tree_len", &self.tree.len())
            .finish()
    }
}

impl Drop for SledStorageEngine {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => info!("Successfully flush RaftLog"),
            Err(e) => error!(?e, "Failed to flush RaftLog"),
        }
    }
}

impl SledStorageEngine {
    /// Creates a new Sled storage engine
    pub fn new(
        node_id: u32,
        db: Db,
    ) -> Result<Self> {
        let tree_name = format!("raft_log_{}_{}", RAFT_LOG_NAMESPACE, node_id);
        let tree = db.open_tree(&tree_name)?;
        Ok(Self { db, tree })
    }

    /// Helper: convert index to big-endian bytes
    pub fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Helper: convert key bytes to index
    #[allow(dead_code)]
    pub fn key_to_index(key: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[0..8]);
        u64::from_be_bytes(bytes)
    }
}
