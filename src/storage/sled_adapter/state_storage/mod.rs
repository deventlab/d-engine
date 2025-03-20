use autometrics::autometrics;
use log::{error, info};
use std::sync::Arc;

use crate::{utils::util::skv, Error, HardState, Result, StateStorage, API_SLO, HARD_STATE_KEY};

use super::STATE_STORAGE_NAMESPACE;

#[derive(Clone, Debug)]
pub struct SledStateStorage {
    db: Arc<sled::Db>,
    tree: Arc<sled::Tree>,
}

impl StateStorage for SledStateStorage {
    fn get(&self, key: Vec<u8>) -> crate::Result<Option<Vec<u8>>> {
        match self.tree.get(key)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<Option<Vec<u8>>> {
        match self.tree.insert(key, value)? {
            Some(ivec) => Ok(Some(ivec.to_vec())),
            None => Ok(None),
        }
    }

    fn flush(&self) -> crate::Result<usize> {
        self.tree.flush().map_err(|e| Error::SledError(e))
    }

    fn load_hard_state(&self) -> Option<crate::HardState> {
        info!(
            "pending load_role_hard_state_from_db with key: {}",
            HARD_STATE_KEY
        );
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
                    error!(
                        "state:load_role_hard_state_from_db deserialize error. {}",
                        e
                    );
                }
            }
        } else {
            info!("no hard state found from db with key: {}.", HARD_STATE_KEY);
        }
        return None;
    }

    fn save_hard_state(&self, hard_state: HardState) -> Result<()> {
        match bincode::serialize(&hard_state) {
            Ok(v) => {
                if let Err(e) = self.insert(skv(HARD_STATE_KEY.to_string()), v) {
                    error!("self.node_state_metadata_db.insert error: {}", e);
                    return Err(Error::NodeStateError(format!(
                        "self.node_state_metadata_db.insert error: {}",
                        e
                    )));
                }
                info!("persistent_state_into_db successfully!");
                println!("persistent_state_into_db successfully!");
            }
            Err(e) => {
                error!("persistent_state_into_db: {}", e);
                eprintln!("persistent_state_into_db: {}", e);
                return Err(Error::NodeStateError(format!(
                    "persistent_state_into_db: {}",
                    e
                )));
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
    #[autometrics(objective = API_SLO)]
    pub(crate) fn new(db: Arc<sled::Db>) -> Self {
        match db.open_tree(STATE_STORAGE_NAMESPACE) {
            Ok(tree) => {
                return SledStateStorage {
                    db,
                    tree: Arc::new(tree),
                }
            }
            Err(e) => {
                error!("Failed to open state machine db tree: {}", e);
                panic!("failed to open sled tree: {}", e);
            }
        }
    }
}
