use uuid::Uuid;

use crate::{HardState, MockLogStore, MockMetaStore, StorageEngine};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

// Use thread-local storage to simulate persistence across instances
thread_local! {
    static MOCK_STORAGE_DATA: RefCell<HashMap<String, Vec<u8>>> = RefCell::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct MockStorageEngine {
    log_store: Arc<MockLogStore>,
    meta_store: Arc<MockMetaStore>,

    // Add an ID to differentiate between instances while sharing data
    instance_id: String,
}

impl MockStorageEngine {
    pub fn new() -> Self {
        Self::with_id(Uuid::new_v4().to_string())
    }

    pub fn with_id(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        // Configure mock behaviors with persistence simulation
        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    pub fn from(
        log_store: MockLogStore,
        meta_store: MockMetaStore,
    ) -> Self {
        Self {
            log_store: Arc::new(log_store),
            meta_store: Arc::new(meta_store),
            instance_id: Uuid::new_v4().to_string(),
        }
    }

    fn configure_mocks(
        log_store: &mut MockLogStore,
        meta_store: &mut MockMetaStore,
        instance_id: &str,
    ) {
        // Mock implementation with thread-local persistence simulation
        let instance_id = instance_id.to_string();

        // Last index implementation with persistence simulation
        log_store.expect_last_index().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let instance_id_ref = instance_id_ref.clone();
                MOCK_STORAGE_DATA.with(|data| {
                    let binding = data.borrow();
                    let key = format!("{}_last_index", instance_id_ref);
                    binding
                        .get(&key)
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_slice().try_into().unwrap();
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0)
                })
            }
        });

        // Persist entries implementation with persistence simulation
        log_store.expect_persist_entries().returning({
            let instance_id_ref = instance_id.clone();
            move |entries| {
                let instance_id_ref = instance_id_ref.clone();
                MOCK_STORAGE_DATA.with(|data| {
                    let mut binding = data.borrow_mut();

                    // Store each entry
                    for entry in &entries {
                        let key = format!("{}_entry_{}", instance_id_ref, entry.index);
                        let value = bincode::serialize(entry).unwrap();
                        binding.insert(key, value);
                    }

                    // Update last index
                    if let Some(last_entry) = entries.last() {
                        let key = format!("{}_last_index", instance_id_ref);
                        binding.insert(key, last_entry.index.to_be_bytes().to_vec());
                    }

                    Ok(())
                })
            }
        });

        // Get entry implementation with persistence simulation
        log_store.expect_entry().returning({
            let instance_id_ref = instance_id.clone();
            move |index| {
                let instance_id_ref = instance_id_ref.clone();
                Ok(MOCK_STORAGE_DATA.with(|data| {
                    let binding = data.borrow();
                    let key = format!("{}_entry_{}", instance_id_ref, index);
                    binding.get(&key).map(|v| bincode::deserialize(v).unwrap())
                }))
            }
        });

        // Get entries range implementation with persistence simulation
        log_store.expect_get_entries().returning({
            let instance_id_ref = instance_id.clone();
            move |range| {
                let instance_id_ref = instance_id_ref.clone();

                MOCK_STORAGE_DATA.with(|data| {
                    let binding = data.borrow();
                    let mut result = Vec::new();

                    for index in *range.start()..=*range.end() {
                        let key = format!("{}_entry_{}", instance_id_ref, index);
                        if let Some(value) = binding.get(&key) {
                            result.push(bincode::deserialize(value).unwrap());
                        }
                    }

                    Ok(result)
                })
            }
        });

        // Other mock implementations remain similar but should use the thread-local storage
        log_store.expect_purge().returning(|_| Ok(()));
        log_store.expect_truncate().returning(|_| Ok(()));
        log_store.expect_flush().returning(|| Ok(()));
        log_store.expect_flush_async().returning(|| Ok(()));
        log_store.expect_reset().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let instance_id_ref = instance_id_ref.clone();
                MOCK_STORAGE_DATA.with(|data| {
                    let mut binding = data.borrow_mut();
                    // Only remove log entries (keys starting with `"{id}_entry_"` or `"{id}_last_index"`)
                    let last_index_key = format!("{}_last_index", instance_id_ref);
                    let keys: Vec<String> = binding
                        .keys()
                        .filter(|k| {
                            k.starts_with(&format!("{}_entry_", instance_id_ref))
                                || k.as_str() == last_index_key
                        })
                        .cloned()
                        .collect();
                    for key in keys {
                        binding.remove(&key);
                    }

                    Ok(())
                })
            }
        });

        // Similar implementations for meta_store
        meta_store.expect_save_hard_state().returning({
            let instance_id_ref = instance_id.clone();
            move |state| {
                MOCK_STORAGE_DATA.with(|data| {
                    let mut binding = data.borrow_mut();
                    let key = format!("{}_meta_hard_state", instance_id_ref);
                    let value = bincode::serialize(state).expect("serialize hard_state");
                    binding.insert(key, value);
                    Ok(())
                })
            }
        });
        meta_store.expect_load_hard_state().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                Ok(MOCK_STORAGE_DATA.with(|data| {
                    let binding = data.borrow();
                    let key = format!("{}_meta_hard_state", instance_id_ref);
                    binding.get(&key).map(|bytes| {
                        bincode::deserialize::<HardState>(bytes).expect("deserialize hard_state")
                    })
                }))
            }
        });
        meta_store.expect_flush().returning(|| Ok(()));
        meta_store.expect_flush_async().returning(|| Ok(()));
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
