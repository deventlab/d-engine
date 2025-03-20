use crate::grpc::rpc_service::Entry;
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub(crate) struct LocalLogBatch {
    pub(crate) writes: HashMap<Vec<u8>, Option<Entry>>,
}

impl LocalLogBatch {
    /// Set a key to a new value
    pub(crate) fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<Vec<u8>>,
        V: Into<Entry>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub(crate) fn remove<K>(&mut self, key: K)
    where
        K: Into<Vec<u8>>,
    {
        self.writes.insert(key.into(), None);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.writes.len() < 1
    }
    pub(crate) fn clear(&mut self) {
        self.writes.clear();
    }
}
