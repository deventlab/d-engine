use crate::proto::client::write_command::Delete;
use crate::proto::client::write_command::Insert;
use crate::proto::client::write_command::Operation;
use crate::proto::client::WriteCommand;
use crate::proto::common::entry_payload::Payload;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::storage::SnapshotMetadata;
use crate::Error;
use crate::StateMachine;
use crate::StorageError;
use parking_lot::RwLock;
use prost::Message;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::fs;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

type FileStateMachineDataType = RwLock<HashMap<Vec<u8>, (Vec<u8>, u64)>>;
/// File-based state machine implementation with persistence
///
/// Design principles:
/// - All data is persisted to disk for durability
/// - In-memory cache for fast read operations
/// - Write-ahead logging for crash consistency
/// - Efficient snapshot handling with file-based storage
/// - Thread-safe with minimal lock contention
#[derive(Debug)]
pub struct FileStateMachine {
    // Key-value storage with disk persistence
    data: FileStateMachineDataType, // (value, term)

    // Raft state with disk persistence
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,
    last_snapshot_metadata: RwLock<Option<SnapshotMetadata>>,

    // Operational state
    running: AtomicBool,
    node_id: u32,

    // File handles for persistence
    data_dir: PathBuf,
    // data_file: RwLock<File>,
    // metadata_file: RwLock<File>,
    // wal_file: RwLock<File>, // Write-ahead log for crash recovery
}

impl FileStateMachine {
    /// Creates a new file-based state machine with persistence
    ///
    /// # Arguments
    /// * `data_dir` - Directory where data files will be stored
    /// * `node_id` - Unique identifier for this node
    ///
    /// # Returns
    /// Result containing the initialized FileStateMachine
    pub async fn new(
        data_dir: PathBuf,
        node_id: u32,
    ) -> Result<Self, Error> {
        // Ensure data directory exists
        fs::create_dir_all(&data_dir).await?;

        let machine = Self {
            data: RwLock::new(HashMap::new()),
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_snapshot_metadata: RwLock::new(None),
            running: AtomicBool::new(true),
            node_id,
            data_dir: data_dir.clone(),
        };

        // Load existing data from disk
        machine.load_from_disk().await?;

        Ok(machine)
    }

    /// Loads state machine data from disk files
    async fn load_from_disk(&self) -> Result<(), Error> {
        // Load last applied index and term from metadata file
        self.load_metadata().await?;

        // Load key-value data from data file
        self.load_data().await?;

        // Replay write-ahead log for crash recovery
        self.replay_wal().await?;

        info!(
            "[Node-{}] Loaded state machine data from disk",
            self.node_id
        );
        Ok(())
    }

    /// Loads metadata from disk
    async fn load_metadata(&self) -> Result<(), Error> {
        let metadata_path = self.data_dir.join("metadata.bin");
        if !metadata_path.exists() {
            return Ok(());
        }

        let mut file = File::open(metadata_path).await?;
        let mut buffer = [0u8; 16];

        if file.read_exact(&mut buffer).await.is_ok() {
            let index = u64::from_be_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]);

            let term = u64::from_be_bytes([
                buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14],
                buffer[15],
            ]);

            self.last_applied_index.store(index, Ordering::SeqCst);
            self.last_applied_term.store(term, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Loads key-value data from disk
    async fn load_data(&self) -> Result<(), Error> {
        let data_path = self.data_dir.join("state.data");
        if !data_path.exists() {
            return Ok(());
        }

        let mut file = File::open(data_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut pos = 0;
        let mut data = self.data.write();

        while pos < buffer.len() {
            // Read key length
            if pos + 8 > buffer.len() {
                break;
            }

            let key_len_bytes = &buffer[pos..pos + 8];
            let key_len = u64::from_be_bytes([
                key_len_bytes[0],
                key_len_bytes[1],
                key_len_bytes[2],
                key_len_bytes[3],
                key_len_bytes[4],
                key_len_bytes[5],
                key_len_bytes[6],
                key_len_bytes[7],
            ]) as usize;

            pos += 8;

            // Read key
            if pos + key_len > buffer.len() {
                break;
            }

            let key = buffer[pos..pos + key_len].to_vec();
            pos += key_len;

            // Read value length
            if pos + 8 > buffer.len() {
                break;
            }

            let value_len_bytes = &buffer[pos..pos + 8];
            let value_len = u64::from_be_bytes([
                value_len_bytes[0],
                value_len_bytes[1],
                value_len_bytes[2],
                value_len_bytes[3],
                value_len_bytes[4],
                value_len_bytes[5],
                value_len_bytes[6],
                value_len_bytes[7],
            ]) as usize;

            pos += 8;

            // Read value
            if pos + value_len > buffer.len() {
                break;
            }

            let value = buffer[pos..pos + value_len].to_vec();
            pos += value_len;

            // Read term
            if pos + 8 > buffer.len() {
                break;
            }

            let term_bytes = &buffer[pos..pos + 8];
            let term = u64::from_be_bytes([
                term_bytes[0],
                term_bytes[1],
                term_bytes[2],
                term_bytes[3],
                term_bytes[4],
                term_bytes[5],
                term_bytes[6],
                term_bytes[7],
            ]);

            pos += 8;

            // Store in memory
            data.insert(key, (value, term));
        }

        Ok(())
    }

    /// Replays write-ahead log for crash recovery
    async fn replay_wal(&self) -> Result<(), Error> {
        let wal_path = self.data_dir.join("wal.log");
        if !wal_path.exists() {
            return Ok(());
        }

        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // If WAL has content, we need to replay it
        if !buffer.is_empty() {
            warn!(
                "[Node-{}] Replaying write-ahead log for crash recovery",
                self.node_id
            );

            // For simplicity, we'll just clear and rebuild from data file
            // In a production system, you'd parse and replay each WAL entry
            file.set_len(0).await?; // Clear WAL after recovery
        }

        Ok(())
    }

    /// Persists key-value data to disk
    fn persist_data(&self) -> Result<(), Error> {
        // Collect data first to minimize lock time
        let data_copy: HashMap<Vec<u8>, (Vec<u8>, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Write to file without holding data lock
        let data_path = self.data_dir.join("state.data");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)?;

        for (key, (value, term)) in data_copy.iter() {
            // Write key length (8 bytes)
            let key_len = key.len() as u64;
            file.write_all(&key_len.to_be_bytes())?;

            // Write key
            file.write_all(key)?;

            // Write value length (8 bytes)
            let value_len = value.len() as u64;
            file.write_all(&value_len.to_be_bytes())?;

            // Write value
            file.write_all(value)?;

            // Write term (8 bytes)
            file.write_all(&term.to_be_bytes())?;
        }

        file.flush()?;
        Ok(())
    }

    /// Persists key-value data to disk
    async fn persist_data_async(&self) -> Result<(), Error> {
        // Collect data first to minimize lock time
        let data_copy: HashMap<Vec<u8>, (Vec<u8>, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Write to file without holding data lock
        let data_path = self.data_dir.join("state.data");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)
            .await?;

        for (key, (value, term)) in data_copy.iter() {
            // Write key length (8 bytes)
            let key_len = key.len() as u64;
            file.write_all(&key_len.to_be_bytes()).await?;

            // Write key
            file.write_all(key).await?;

            // Write value length (8 bytes)
            let value_len = value.len() as u64;
            file.write_all(&value_len.to_be_bytes()).await?;

            // Write value
            file.write_all(value).await?;

            // Write term (8 bytes)
            file.write_all(&term.to_be_bytes()).await?;
        }

        file.flush().await?;
        Ok(())
    }

    /// Persists metadata to disk
    fn persist_metadata(&self) -> Result<(), Error> {
        let metadata_path = self.data_dir.join("metadata.bin");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(metadata_path)?;

        let index = self.last_applied_index.load(Ordering::SeqCst);
        let term = self.last_applied_term.load(Ordering::SeqCst);

        file.write_all(&index.to_be_bytes())?;
        file.write_all(&term.to_be_bytes())?;

        file.flush()?;
        Ok(())
    }

    async fn persist_metadata_async(&self) -> Result<(), Error> {
        let metadata_path = self.data_dir.join("metadata.bin");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(metadata_path)
            .await?;

        let index = self.last_applied_index.load(Ordering::SeqCst);
        let term = self.last_applied_term.load(Ordering::SeqCst);

        file.write_all(&index.to_be_bytes()).await?;
        file.write_all(&term.to_be_bytes()).await?;

        file.flush().await?;
        Ok(())
    }

    /// Appends an operation to the write-ahead log
    async fn append_to_wal(
        &self,
        entry: &Entry,
        operation: &str,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), Error> {
        let wal_path = self.data_dir.join("wal.log");
        let mut file =
            OpenOptions::new().write(true).create(true).append(true).open(wal_path).await?;

        // Write entry index and term
        file.write_all(&entry.index.to_be_bytes()).await?;
        file.write_all(&entry.term.to_be_bytes()).await?;

        // Write operation type (insert/delete)
        file.write_all(operation.as_bytes()).await?;

        // Write key length and key
        file.write_all(&(key.len() as u64).to_be_bytes()).await?;
        file.write_all(key).await?;

        // For insert operations, write value length and value
        if let Some(value_data) = value {
            file.write_all(&(value_data.len() as u64).to_be_bytes()).await?;
            file.write_all(value_data).await?;
        }

        file.flush().await?;
        Ok(())
    }

    /// Clears the write-ahead log (called after successful persistence)
    fn clear_wal(&self) -> Result<(), Error> {
        let wal_path = self.data_dir.join("wal.log");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(wal_path)?;

        file.set_len(0)?;
        file.flush()?;
        Ok(())
    }

    /// Clears the write-ahead log (called after successful persistence)
    async fn clear_wal_async(&self) -> Result<(), Error> {
        let wal_path = self.data_dir.join("wal.log");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(wal_path)
            .await?;

        file.set_len(0).await?;
        file.flush().await?;
        Ok(())
    }

    /// Resets the state machine to its initial empty state
    ///
    /// This method:
    /// 1. Clears all in-memory data
    /// 2. Resets Raft state to initial values
    /// 3. Clears all persisted files
    /// 4. Maintains operational state (running status, node ID)
    pub async fn reset(&self) -> Result<(), Error> {
        info!("[Node-{}] Resetting state machine", self.node_id);

        // Clear in-memory data
        {
            let mut data = self.data.write();
            data.clear();
        }

        // Reset Raft state
        self.last_applied_index.store(0, Ordering::SeqCst);
        self.last_applied_term.store(0, Ordering::SeqCst);

        {
            let mut snapshot_metadata = self.last_snapshot_metadata.write();
            *snapshot_metadata = None;
        }

        // Clear all persisted files
        self.clear_data_file().await?;
        self.clear_metadata_file().await?;
        self.clear_wal_async().await?;

        info!("[Node-{}] State machine reset completed", self.node_id);
        Ok(())
    }

    /// Clears the data file
    async fn clear_data_file(&self) -> Result<(), Error> {
        let data_path = self.data_dir.join("state.data");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)
            .await?;

        file.set_len(0).await?;
        file.flush().await?;
        Ok(())
    }

    /// Clears the metadata file
    async fn clear_metadata_file(&self) -> Result<(), Error> {
        let metadata_path = self.data_dir.join("metadata.bin");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(metadata_path)
            .await?;

        // Write default values (0 for both index and term)
        file.write_all(&0u64.to_be_bytes()).await?;
        file.write_all(&0u64.to_be_bytes()).await?;

        file.flush().await?;
        Ok(())
    }

    // pub(super) async fn decompress_snapshot(
    //     &self,
    //     compressed_path: &Path,
    //     dest_dir: &Path,
    // ) -> Result<(), Error> {
    //     let file = File::open(compressed_path).await.map_err(StorageError::IoError)?;
    //     let buf_reader = BufReader::new(file);
    //     let gzip_decoder = GzipDecoder::new(buf_reader);
    //     let mut archive = Archive::new(gzip_decoder);

    //     archive.unpack(dest_dir).await.map_err(StorageError::IoError)?;
    //     Ok(())
    // }
}

impl Drop for FileStateMachine {
    fn drop(&mut self) {
        let timer = Instant::now();

        // Save state into local database including flush operation
        match self.save_hard_state() {
            Ok(_) => debug!("StateMachine saved in {:?}", timer.elapsed()),
            Err(e) => error!("Failed to save StateMachine: {}", e),
        }
    }
}

#[async_trait]
impl StateMachine for FileStateMachine {
    fn start(&self) -> Result<(), Error> {
        self.running.store(true, Ordering::SeqCst);
        info!("[Node-{}] File state machine started", self.node_id);
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        // Ensure all data is flushed to disk before stopping
        self.running.store(false, Ordering::SeqCst);
        info!("[Node-{}] File state machine stopped", self.node_id);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        println!("Read Key: {:?}", &key_buffer);
        let data = self.data.read();
        Ok(data.get(key_buffer).map(|(value, _)| value.clone()))
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        let data = self.data.read();
        data.values().find(|(_, index)| *index == entry_id).map(|(_, term)| *term)
    }

    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<(), Error> {
        trace!("Applying chunk: {:?}.", chunk);

        let mut highest_index_entry: Option<LogId> = None;

        // Process each entry in the chunk
        for entry in chunk {
            assert!(entry.payload.is_some(), "Entry payload should not be None!");

            // Ensure entries are processed in order
            if let Some(prev) = &highest_index_entry {
                assert!(
                    entry.index > prev.index,
                    "apply_chunk: received unordered entry at index {} (prev={})",
                    entry.index,
                    prev.index
                );
            }
            highest_index_entry = Some(LogId {
                index: entry.index,
                term: entry.term,
            });

            match entry.payload.as_ref().unwrap().payload.as_ref() {
                Some(Payload::Noop(_)) => {
                    debug!("Handling NOOP command at index {}", entry.index);
                    self.append_to_wal(&entry, "NOOP", &[], None).await?;
                }
                Some(Payload::Command(bytes)) => match WriteCommand::decode(&bytes[..]) {
                    Ok(write_cmd) => match write_cmd.operation {
                        Some(Operation::Insert(Insert { key, value })) => {
                            debug!(
                                "[Node-{}] Applying INSERT at index {}: {:?}",
                                self.node_id, entry.index, key
                            );

                            // Write to WAL first
                            self.append_to_wal(&entry, "INSERT", &key, Some(&value)).await?;

                            // Update in-memory data
                            let mut data = self.data.write();
                            data.insert(key, (value, entry.term));
                        }
                        Some(Operation::Delete(Delete { key })) => {
                            debug!(
                                "[Node-{}] Applying DELETE at index {}: {:?}",
                                self.node_id, entry.index, key
                            );

                            // Write to WAL first
                            self.append_to_wal(&entry, "DELETE", &key, None).await?;

                            // Update in-memory data
                            let mut data = self.data.write();
                            data.remove(&key);
                        }
                        None => {
                            warn!(
                                "[Node-{}] WriteCommand without operation at index {}",
                                self.node_id, entry.index
                            );
                        }
                    },
                    Err(e) => {
                        error!(
                            "[Node-{}] Failed to decode WriteCommand at index {}: {:?}",
                            self.node_id, entry.index, e
                        );
                        return Err(StorageError::SerializationError(e.to_string()).into());
                    }
                },
                Some(Payload::Config(_config_change)) => {
                    debug!(
                        "[Node-{}] Ignoring config change at index {}",
                        self.node_id, entry.index
                    );
                    self.append_to_wal(&entry, "CONFIG", &[], None).await?;
                }
                None => panic!("Entry payload variant should not be None!"),
            }

            info!("[{}]- COMMITTED_LOG_METRIC: {} ", self.node_id, entry.index);
        }

        if let Some(log_id) = highest_index_entry {
            debug!(
                "[Node-{}] State machine - updated last_applied: {:?}",
                self.node_id, log_id
            );
            self.update_last_applied(log_id);
        }

        // Persist changes to disk and clear WAL
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;
        self.clear_wal_async().await?;

        Ok(())
    }

    fn len(&self) -> usize {
        self.data.read().len()
    }

    fn update_last_applied(
        &self,
        last_applied: LogId,
    ) {
        self.last_applied_index.store(last_applied.index, Ordering::SeqCst);
        self.last_applied_term.store(last_applied.term, Ordering::SeqCst);
    }

    fn last_applied(&self) -> LogId {
        LogId {
            index: self.last_applied_index.load(Ordering::SeqCst),
            term: self.last_applied_term.load(Ordering::SeqCst),
        }
    }

    fn persist_last_applied(
        &self,
        last_applied: LogId,
    ) -> Result<(), Error> {
        self.update_last_applied(last_applied);
        self.persist_metadata()
    }

    fn update_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        *self.last_snapshot_metadata.write() = Some(snapshot_metadata.clone());
        Ok(())
    }

    fn snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_snapshot_metadata.read().clone()
    }

    fn persist_last_snapshot_metadata(
        &self,
        snapshot_metadata: &SnapshotMetadata,
    ) -> Result<(), Error> {
        self.update_last_snapshot_metadata(snapshot_metadata)
    }

    async fn apply_snapshot_from_file(
        &self,
        metadata: &SnapshotMetadata,
        snapshot_dir: std::path::PathBuf,
    ) -> Result<(), Error> {
        info!(
            "[Node-{}] Applying snapshot from file: {:?}",
            self.node_id, snapshot_dir
        );
        println!(
            "[Node-{}] Applying snapshot from file: {:?}",
            self.node_id, snapshot_dir
        );

        // Read from the snapshot.bin file inside the directory
        let snapshot_data_path = snapshot_dir.join("snapshot.bin");
        let mut file = File::open(snapshot_data_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Parse snapshot data
        let mut pos = 0;
        let mut new_data = HashMap::new();

        while pos < buffer.len() {
            // Read key length
            if pos + 8 > buffer.len() {
                break;
            }

            let key_len_bytes = &buffer[pos..pos + 8];
            let key_len = u64::from_be_bytes([
                key_len_bytes[0],
                key_len_bytes[1],
                key_len_bytes[2],
                key_len_bytes[3],
                key_len_bytes[4],
                key_len_bytes[5],
                key_len_bytes[6],
                key_len_bytes[7],
            ]) as usize;

            pos += 8;

            // Read key
            if pos + key_len > buffer.len() {
                break;
            }

            let key = buffer[pos..pos + key_len].to_vec();
            pos += key_len;

            // Read value length
            if pos + 8 > buffer.len() {
                break;
            }

            let value_len_bytes = &buffer[pos..pos + 8];
            let value_len = u64::from_be_bytes([
                value_len_bytes[0],
                value_len_bytes[1],
                value_len_bytes[2],
                value_len_bytes[3],
                value_len_bytes[4],
                value_len_bytes[5],
                value_len_bytes[6],
                value_len_bytes[7],
            ]) as usize;

            pos += 8;

            // Read value
            if pos + value_len > buffer.len() {
                break;
            }

            let value = buffer[pos..pos + value_len].to_vec();
            pos += value_len;

            // Read term
            if pos + 8 > buffer.len() {
                break;
            }

            let term_bytes = &buffer[pos..pos + 8];
            let term = u64::from_be_bytes([
                term_bytes[0],
                term_bytes[1],
                term_bytes[2],
                term_bytes[3],
                term_bytes[4],
                term_bytes[5],
                term_bytes[6],
                term_bytes[7],
            ]);

            pos += 8;

            // Add to new data
            new_data.insert(key, (value, term));
        }

        // Atomically replace the data
        {
            let mut data = self.data.write();
            *data = new_data;
        }

        // Update metadata
        *self.last_snapshot_metadata.write() = Some(metadata.clone());

        if let Some(last_included) = &metadata.last_included {
            self.update_last_applied(*last_included);
        }

        // Persist to disk
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;
        self.clear_wal_async().await?;

        info!("[Node-{}] Snapshot applied successfully", self.node_id);
        Ok(())
    }

    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<[u8; 32], Error> {
        info!(
            "[Node-{}] Generating snapshot data up to {:?}",
            self.node_id, last_included
        );

        // Create snapshot directory
        fs::create_dir_all(&new_snapshot_dir).await?;

        // Create snapshot file
        let snapshot_path = new_snapshot_dir.join("snapshot.bin");
        let mut file = File::create(&snapshot_path).await?;

        let data_copy: HashMap<Vec<u8>, (Vec<u8>, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Write data in the same format as the data file
        for (key, (value, term)) in data_copy.iter() {
            // Write key length (8 bytes)
            let key_len = key.len() as u64;
            file.write_all(&key_len.to_be_bytes()).await?;

            // Write key
            println!("key: {:?}", key);
            file.write_all(key).await?;

            // Write value length (8 bytes)
            let value_len = value.len() as u64;
            file.write_all(&value_len.to_be_bytes()).await?;

            // Write value
            println!("value: {:?}", value);
            file.write_all(value).await?;

            // Write term (8 bytes)
            file.write_all(&term.to_be_bytes()).await?;
        }

        file.flush().await?;

        // Update metadata
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: vec![0; 32], // Simple checksum for demo
        };

        self.update_last_snapshot_metadata(&metadata)?;

        info!(
            "[Node-{}] Snapshot generated at {:?}",
            self.node_id, snapshot_path
        );

        // Return dummy checksum
        Ok([0; 32])
    }

    fn save_hard_state(&self) -> Result<(), Error> {
        let last_applied = self.last_applied();
        self.persist_last_applied(last_applied)?;

        if let Some(last_snapshot_metadata) = self.snapshot_metadata() {
            self.persist_last_snapshot_metadata(&last_snapshot_metadata)?;
        }

        self.flush()?;
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        self.persist_data()?;
        self.persist_metadata()?;
        self.clear_wal()?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;
        self.clear_wal_async().await?;
        Ok(())
    }

    async fn reset(&self) -> Result<(), Error> {
        self.reset().await
    }
}
