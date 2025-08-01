use crate::alias::SOF;
use crate::constants::STATE_STORAGE_HARD_STATE_KEY;
use crate::convert::skv;
use crate::HardState;
use crate::Result;
use crate::StateStorage;
use crate::StorageEngine;
use crate::StorageError;
use crate::TypeConfig;
use bytes::Bytes;
use std::sync::Arc;
use tracing::error;
use tracing::info;

#[allow(dead_code)]
#[derive(Clone)]
pub struct SledStateStorage<T>
where
    T: TypeConfig,
{
    node_id: u32,
    storage: Arc<SOF<T>>,
}

impl<T> StateStorage for SledStateStorage<T>
where
    T: TypeConfig,
{
    fn get(
        &self,
        key: Vec<u8>,
    ) -> crate::Result<Option<Bytes>> {
        self.storage.get(key)
    }

    fn insert(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.storage.insert(key, value)
    }

    fn flush(&self) -> Result<()> {
        self.storage.flush()
    }

    fn load_hard_state(&self) -> Option<crate::HardState> {
        info!(
            "pending load_role_hard_state_from_db with key: {}",
            STATE_STORAGE_HARD_STATE_KEY
        );
        let result = self.get(skv(STATE_STORAGE_HARD_STATE_KEY.to_string()));

        match result {
            Ok(Some(bytes)) => {
                info!("found node state from DB with key: {}", STATE_STORAGE_HARD_STATE_KEY);

                match bincode::deserialize::<HardState>(bytes.as_ref()) {
                    Ok(hard_state) => {
                        info!(
                            "load_role_hard_state_from_db: current_term={:?}",
                            hard_state.current_term
                        );
                        Some(hard_state)
                    }
                    Err(e) => {
                        error!("state:load_role_hard_state_from_db deserialize error. {}", e);
                        None
                    }
                }
            }
            Ok(None) => {
                info!(
                    "no hard state found from db with key: {}.",
                    STATE_STORAGE_HARD_STATE_KEY
                );
                None
            }
            Err(e) => {
                error!("Failed to get hard state: {}", e);
                None
            }
        }
    }

    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> Result<()> {
        match bincode::serialize(&hard_state) {
            Ok(v) => {
                if let Err(e) = self.insert(skv(STATE_STORAGE_HARD_STATE_KEY.to_string()), v) {
                    error!("self.node_state_metadata_db.insert error: {}", e);
                    return Err(e);
                }
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

        // self.raft_log.flush()?;

        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.storage.len()
    }
}

impl<T> SledStateStorage<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        storage: Arc<SOF<T>>,
    ) -> Self {
        Self { node_id, storage }
    }
}

impl<T> std::fmt::Debug for SledStateStorage<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SledStateStorage")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl<T> Drop for SledStateStorage<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => info!("Successfully flush StateStorage"),
            Err(e) => error!(?e, "Failed to flush StateStorage"),
        }
    }
}
