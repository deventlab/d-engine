use std::sync::Arc;

use log::error;
use log::info;

use super::STATE_STORAGE_NAMESPACE;
use crate::convert::skv;
use crate::Error;
use crate::HardState;
use crate::Result;
use crate::StateStorage;
use crate::HARD_STATE_KEY;

#[derive(Clone)]
pub struct SledStateStorage {
    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
}

impl std::fmt::Debug for SledStateStorage {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledStateStorage")
            .field("tree_len", &self.tree.len())
            .finish()
    }
}

impl StateStorage for SledStateStorage {
    fn get(
        &self,
        key: Vec<u8>,
    ) -> crate::Result<Option<Vec<u8>>> {
        match self.tree.get(key)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn insert(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> crate::Result<Option<Vec<u8>>> {
        match self.tree.insert(key, value)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn flush(&self) -> crate::Result<usize> {
        self.tree.flush().map_err(Error::SledError)
    }

    fn load_hard_state(&self) -> Option<crate::HardState> {
        info!("pending load_role_hard_state_from_db with key: {}", HARD_STATE_KEY);
        if let Ok(Some(v)) = self.get(skv(HARD_STATE_KEY.to_string())) {
            info!("found node state from DB with key: {}", HARD_STATE_KEY);

            let v = v.to_vec();
            match bincode::deserialize::<HardState>(&v) {
                Ok(hard_state) => {
                    info!(
                        "load_role_hard_state_from_db: current_term={:?}",
                        hard_state.current_term
                    );
                    return Some(hard_state);
                }
                Err(e) => {
                    error!("state:load_role_hard_state_from_db deserialize error. {}", e);
                }
            }
        } else {
            info!("no hard state found from db with key: {}.", HARD_STATE_KEY);
        }
        None
    }

    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> Result<()> {
        match bincode::serialize(&hard_state) {
            Ok(v) => {
                if let Err(e) = self.insert(skv(HARD_STATE_KEY.to_string()), v) {
                    error!("self.node_state_metadata_db.insert error: {}", e);
                    return Err(e);
                }
                info!("persistent_state_into_db successfully!");
                println!("persistent_state_into_db successfully!");
            }
            Err(e) => {
                error!("persistent_state_into_db: {}", e);
                eprintln!("persistent_state_into_db: {}", e);
                return Err(Error::BincodeError(e));
            }
        }
        match self.flush() {
            Ok(bytes) => {
                info!("Successfully flushed sled DB, bytes flushed: {}", bytes);
            }
            Err(e) => {
                error!("Failed to flush sled DB: {}", e);
            }
        }

        // self.raft_log.flush()?;

        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tree.len()
    }
}

impl SledStateStorage {
    pub fn new(db: Arc<sled::Db>) -> Self {
        match db.open_tree(STATE_STORAGE_NAMESPACE) {
            Ok(tree) => SledStateStorage {
                db,
                tree: Arc::new(tree),
            },
            Err(e) => {
                error!("Failed to open state machine db tree: {}", e);
                panic!("failed to open sled tree: {}", e);
            }
        }
    }
}
