use crate::proto::common::{Entry, LogId};
use crate::MetaStore;
use crate::{Error, HardState, LogStore, StorageEngine, StorageError, HARD_STATE_KEY};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use tracing::info;

// Constants for file structure
// const LOG_FILE_EXTENSION: &str = "log";
// const META_FILE_NAME: &str = "meta.bin";
const HARD_STATE_FILE_NAME: &str = "hard_state.bin";
// const SNAPSHOT_DIR_NAME: &str = "snapshots";

/// File-based log store implementation
#[derive(Debug)]
pub struct FileLogStore {
    data_dir: PathBuf,
    entries: Mutex<BTreeMap<u64, Entry>>,
    last_index: AtomicU64,
    file_handle: Mutex<File>,

    index_positions: Mutex<BTreeMap<u64, u64>>, // Maps index to file position
}

/// File-based metadata store implementation
#[derive(Debug)]
pub struct FileMetaStore {
    data_dir: PathBuf,
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

/// Unified file-based storage engine
#[derive(Debug)]
pub struct FileStorageEngine {
    log_store: Arc<FileLogStore>,
    meta_store: Arc<FileMetaStore>,
    data_dir: PathBuf,
}

impl StorageEngine for FileStorageEngine {
    type LogStore = FileLogStore;
    type MetaStore = FileMetaStore;

    #[inline]
    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    #[inline]
    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

impl FileStorageEngine {
    /// Creates new file-based storage engine
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        // Ensure data directory exists
        fs::create_dir_all(&data_dir)?;

        // Create log store
        let log_store = Arc::new(FileLogStore::new(data_dir.join("logs"))?);

        // Create meta store
        let meta_store = Arc::new(FileMetaStore::new(data_dir.join("meta"))?);

        Ok(Self {
            log_store,
            meta_store,
            data_dir,
        })
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

impl FileLogStore {
    /// Creates new file-based log store
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        // Ensure directory exists
        fs::create_dir_all(&data_dir)?;

        // Open or create the log file
        let log_file_path = data_dir.join("log.data");
        let file = OpenOptions::new().read(true).write(true).create(true).open(log_file_path)?;

        // Load existing entries from file
        let entries = Mutex::new(BTreeMap::new());
        let last_index = AtomicU64::new(0);
        let index_positions = Mutex::new(BTreeMap::new());

        let store = Self {
            data_dir,
            entries,
            last_index,
            file_handle: Mutex::new(file),
            index_positions,
        };

        // Load existing data
        store.load_from_file()?;

        Ok(store)
    }

    /// Load entries from file
    fn load_from_file(&self) -> Result<(), Error> {
        let mut file = self.file_handle.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;

        let mut entries = self.entries.lock().unwrap();
        let mut index_positions = self.index_positions.lock().unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut pos = 0;
        let mut max_index = 0;

        while pos < buffer.len() {
            // Record the position of this entry
            let entry_position = pos as u64;

            // Read entry length
            if pos + 8 > buffer.len() {
                break;
            }

            let len_bytes = &buffer[pos..pos + 8];
            let entry_len = u64::from_be_bytes([
                len_bytes[0],
                len_bytes[1],
                len_bytes[2],
                len_bytes[3],
                len_bytes[4],
                len_bytes[5],
                len_bytes[6],
                len_bytes[7],
            ]) as usize;

            pos += 8;

            // Read entry data
            if pos + entry_len > buffer.len() {
                break;
            }

            let entry_data = &buffer[pos..pos + entry_len];
            match Entry::decode(entry_data) {
                Ok(entry) => {
                    entries.insert(entry.index, entry.clone());
                    index_positions.insert(entry.index, entry_position);
                    max_index = max_index.max(entry.index);
                }
                Err(e) => {
                    eprintln!("Failed to decode entry: {}", e);
                    // Continue with next entry
                }
            }

            pos += entry_len;
        }

        self.last_index.store(max_index, Ordering::SeqCst);
        Ok(())
    }

    /// Append entry to file
    fn append_to_file(
        &self,
        entry: &Entry,
    ) -> Result<(), Error> {
        let mut file = self.file_handle.lock().unwrap();

        // Get current file position
        let position = file.seek(SeekFrom::End(0))?;

        let encoded = entry.encode_to_vec();

        // Write entry length (8 bytes)
        let len = encoded.len() as u64;
        file.write_all(&len.to_be_bytes())?;

        // Write entry data
        file.write_all(&encoded)?;

        file.flush()?;

        // Update position index
        let mut index_positions = self.index_positions.lock().unwrap();
        index_positions.insert(entry.index, position);

        Ok(())
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let mut max_index = 0;

        for entry in entries {
            // Append to file
            self.append_to_file(&entry)?;

            // Add to memory
            {
                let mut store = self.entries.lock().unwrap();
                store.insert(entry.index, entry.clone());
            }

            max_index = max_index.max(entry.index);
        }

        if max_index > 0 {
            self.last_index.store(max_index, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let store = self.entries.lock().unwrap();
        Ok(store.get(&index).cloned())
    }

    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let store = self.entries.lock().unwrap();
        let mut result = Vec::new();

        for (_, entry) in store.range(range) {
            result.push(entry.clone());
        }

        Ok(result)
    }

    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let indexes_to_remove: Vec<u64> = {
            let index_positions = self.index_positions.lock().unwrap();
            index_positions.range(0..=cutoff_index.index).map(|(k, _)| *k).collect()
        };

        // Remove from memory
        {
            let mut entries = self.entries.lock().unwrap();
            for index in &indexes_to_remove {
                entries.remove(index);
            }
        }

        // Remove from position index
        {
            let mut index_positions = self.index_positions.lock().unwrap();
            for index in &indexes_to_remove {
                index_positions.remove(index);
            }
        }

        // Note: We don't actually remove from file for performance
        // The file will be compacted during the next snapshot
        Ok(())
    }

    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let indexes_to_remove: Vec<u64> = {
            let index_positions = self.index_positions.lock().unwrap();
            index_positions.range(from_index..).map(|(k, _)| *k).collect()
        };

        // Remove from memory
        {
            let mut entries = self.entries.lock().unwrap();
            for index in &indexes_to_remove {
                entries.remove(index);
            }
        }

        // Remove from position index
        {
            let mut index_positions = self.index_positions.lock().unwrap();
            for index in &indexes_to_remove {
                index_positions.remove(index);
            }
        }

        // Truncate the file
        if let Some(last_keep_position) = self
            .index_positions
            .lock()
            .unwrap()
            .range(..from_index)
            .next_back()
            .map(|(_, pos)| *pos)
        {
            let mut file = self.file_handle.lock().unwrap();

            // Find the end of the last entry to keep
            file.seek(SeekFrom::Start(last_keep_position))?;
            let mut len_buffer = [0u8; 8];
            file.read_exact(&mut len_buffer)?;
            let entry_len = u64::from_be_bytes(len_buffer);

            // Calculate the position after this entry
            let truncate_pos = last_keep_position + 8 + entry_len;

            // Truncate the file
            file.set_len(truncate_pos)?;
        } else {
            // No entries to keep, truncate entire file
            let file = self.file_handle.lock().unwrap();
            file.set_len(0)?;
        }

        // Update last index
        if let Some(new_last_index) = self.index_positions.lock().unwrap().keys().next_back() {
            self.last_index.store(*new_last_index, Ordering::SeqCst);
        } else {
            self.last_index.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        let mut file = self.file_handle.lock().unwrap();
        file.flush()?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    async fn reset(&self) -> Result<(), Error> {
        {
            let mut file = self.file_handle.lock().unwrap();
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.flush()?;
        }
        {
            let mut store = self.entries.lock().unwrap();
            store.clear();
        }
        {
            let mut index_positions = self.index_positions.lock().unwrap();
            index_positions.clear();
        }
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}

impl FileMetaStore {
    /// Creates new file-based metadata store
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        // Ensure directory exists
        fs::create_dir_all(&data_dir)?;

        let store = Self {
            data_dir,
            data: Mutex::new(HashMap::new()),
        };

        // Load existing data
        store.load_from_file()?;

        Ok(store)
    }

    /// Load metadata from file
    fn load_from_file(&self) -> Result<(), Error> {
        let hard_state_path = self.data_dir.join(HARD_STATE_FILE_NAME);

        if hard_state_path.exists() {
            let mut file = File::open(hard_state_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            match bincode::deserialize::<HardState>(&buffer) {
                Ok(_hard_state) => {
                    let mut data = self.data.lock().unwrap();
                    data.insert(HARD_STATE_KEY.to_vec(), buffer);
                    info!("Loaded hard state from file");
                }
                Err(e) => {
                    eprintln!("Failed to decode hard state: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Save metadata to file
    fn save_to_file(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Error> {
        if key == HARD_STATE_KEY {
            let hard_state_path = self.data_dir.join(HARD_STATE_FILE_NAME);
            let mut file = File::create(hard_state_path)?;
            file.write_all(value)?;
            file.flush()?;
        }

        Ok(())
    }
}

#[async_trait]
impl MetaStore for FileMetaStore {
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        let serialized = bincode::serialize(state).map_err(StorageError::BincodeError)?;

        let mut data = self.data.lock().unwrap();
        data.insert(HARD_STATE_KEY.to_vec(), serialized.clone());

        self.save_to_file(HARD_STATE_KEY, &serialized)?;

        info!("Persisted hard state to file");
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        let data = self.data.lock().unwrap();

        match data.get(HARD_STATE_KEY) {
            Some(bytes) => {
                let state = bincode::deserialize(bytes).map_err(StorageError::BincodeError)?;
                info!("Loaded hard state from memory");
                Ok(Some(state))
            }
            None => {
                info!("No hard state found");
                Ok(None)
            }
        }
    }

    fn flush(&self) -> Result<(), Error> {
        // No-op for file-based store as we flush on each write
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
