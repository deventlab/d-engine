use sled::IVec;
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub(crate) struct LocalLogBatch {
    pub(crate) writes: HashMap<Box<[u8]>, Option<IVec>>,
}

impl LocalLogBatch {
    /// Set a key to a new value
    #[allow(dead_code)]
    pub(crate) fn insert<K, V>(
        &mut self,
        key: K,
        value: V,
    ) where
        K: Into<Box<[u8]>>,
        V: Into<IVec>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    #[allow(dead_code)]
    pub(crate) fn remove<K>(
        &mut self,
        key: K,
    ) where
        K: Into<Box<[u8]>>,
    {
        self.writes.insert(key.into(), None);
    }
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.len() < 1
    }

    #[allow(dead_code)]
    pub(crate) fn clear(&mut self) {
        self.writes.clear();
    }
}
