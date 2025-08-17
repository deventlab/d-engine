use std::sync::Arc;

use crate::{MockLogStore, MockMetaStore, StorageEngine};

#[derive(Debug, Clone)]
pub struct MockStorageEngine {
    log_store: Arc<MockLogStore>,
    meta_store: Arc<MockMetaStore>,
}

impl MockStorageEngine {
    pub fn new() -> Self {
        Self {
            log_store: Arc::new(MockLogStore::new()),
            meta_store: Arc::new(MockMetaStore::new()),
        }
    }
    pub fn from(
        log_store: MockLogStore,
        meta_store: MockMetaStore,
    ) -> Self {
        Self {
            log_store: Arc::new(log_store),
            meta_store: Arc::new(meta_store),
        }
    }
}
impl Default for MockStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MockStorageEngine {
    type LogStore = MockLogStore;
    type MetaStore = MockMetaStore;

    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}
