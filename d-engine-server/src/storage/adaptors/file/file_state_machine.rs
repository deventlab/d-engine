//! File-based state machine implementation with crash recovery
//!
//! This module provides a durable state machine implementation using file-based storage
//! with Write-Ahead Logging (WAL) for crash consistency.
//!
//! # Architecture
//!
//! ## Storage Components
//!
//! - **`state.data`**: Serialized key-value store (persisted after each apply_chunk)
//! - **`wal.log`**: Write-Ahead Log for crash recovery (cleared after successful persistence)
//! - **`ttl_state.bin`**: TTL manager state (persisted alongside state.data)
//! - **`metadata.bin`**: Raft metadata (last_applied_index, last_applied_term)
//!
//! ## Write-Ahead Log (WAL) Design
//!
//! The WAL ensures crash consistency by recording operations before they are applied to
//! in-memory state. Each WAL entry contains:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Entry Index (8 bytes) │ Entry Term (8 bytes) │ OpCode (1 byte) │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Key Length (8 bytes)  │ Key Data (N bytes)                      │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ Value Length (8 bytes)│ Value Data (M bytes, if present)        │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ TTL (4 bytes, 0 = no TTL)                                       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Why TTL is Included in WAL
//!
//! TTL metadata MUST be included in WAL for crash recovery correctness:
//!
//! **Scenario without TTL in WAL:**
//! 1. Client inserts key="foo", value="bar", ttl=3600s
//! 2. Operation written to WAL (without TTL field)
//! 3. Node crashes before persist_data_async() completes
//! 4. On restart, replay_wal() restores key="foo" from WAL
//! 5. **BUG**: Key is restored but TTL manager has no expiration record
//! 6. **Result**: Key becomes permanent (should expire in 3600s)
//!
//! **With TTL in WAL:**
//! 1. WAL includes TTL field (4 bytes, 0 for no TTL)
//! 2. On crash recovery, replay_wal() reads TTL from WAL
//! 3. Calls `lease.register(key, ttl)` to restore expiration
//! 4. **Result**: TTL semantics preserved across crashes
//!
//! ### WAL Lifecycle
//!
//! ```text
//! apply_chunk() → append_to_wal() → [crash safe] → persist_data_async()
//!                                                 → persist_ttl_data()
//!                                                 → clear_wal_async()
//! ```
//!
//! After successful persistence, WAL is cleared since state is now in `state.data`.
//!
//! ## Crash Recovery Flow
//!
//! On node startup (`new()`):
//! 1. `load_metadata()` - Restore Raft state
//! 2. `load_data()` - Load persisted key-value data
//! 3. `load_ttl_data()` - Load persisted TTL state
//! 4. `replay_wal()` - **Critical**: Replay uncommitted operations from WAL
//!    - Restores keys AND their TTL metadata
//!    - Ensures crash consistency (operations are idempotent)
//!
//! ## TTL Cleanup Strategies
//!
//! - **Passive Deletion**: Keys are checked and deleted on read access (`get()`)
//! - **Piggyback Cleanup**: Batch cleanup during `apply_chunk()` (every 100 applies)
//! - **Lazy Activation**: Cleanup skipped if TTL never used (zero overhead)

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use parking_lot::RwLock;
use prost::Message;
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

use crate::storage::DefaultLease;
use d_engine_core::Error;
use d_engine_core::Lease;
use d_engine_core::StateMachine;
use d_engine_core::StorageError;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Delete;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotMetadata;

type FileStateMachineDataType = RwLock<HashMap<Bytes, (Bytes, u64)>>;

/// WAL operation codes for fixed-size encoding
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOpCode {
    Noop = 0,
    Insert = 1,
    Delete = 2,
    Config = 3,
}

impl WalOpCode {
    fn from_str(s: &str) -> Self {
        match s {
            "INSERT" => Self::Insert,
            "DELETE" => Self::Delete,
            "CONFIG" => Self::Config,
            _ => Self::Noop,
        }
    }

    fn from_u8(byte: u8) -> Self {
        match byte {
            1 => Self::Insert,
            2 => Self::Delete,
            3 => Self::Config,
            _ => Self::Noop,
        }
    }
}

/// File-based state machine implementation with persistence
///
/// Design principles:
/// - All data is persisted to disk for durability
/// - In-memory cache for fast read operations
/// - Write-ahead logging for crash consistency
/// - Efficient snapshot handling with file-based storage
/// - Thread-safe with minimal lock contention
/// - TTL support for automatic key expiration
#[derive(Debug)]
pub struct FileStateMachine {
    // Key-value storage with disk persistence
    data: FileStateMachineDataType, // (value, term)

    // Lease management for automatic key expiration
    // DefaultLease is thread-safe internally (uses DashMap + Mutex)
    // Injected by NodeBuilder after construction
    lease: Option<Arc<DefaultLease>>,

    // Raft state with disk persistence
    last_applied_index: AtomicU64,
    last_applied_term: AtomicU64,
    last_snapshot_metadata: RwLock<Option<SnapshotMetadata>>,

    // Operational state
    running: AtomicBool,

    // File handles for persistence
    data_dir: PathBuf,
    // data_file: RwLock<File>,
    // metadata_file: RwLock<File>,
    // wal_file: RwLock<File>, // Write-ahead log for crash recovery
}

impl FileStateMachine {
    /// Creates a new file-based state machine with persistence
    ///
    /// Lease will be injected by NodeBuilder after construction.
    ///
    /// # Arguments
    /// * `data_dir` - Directory where data files will be stored
    ///
    /// # Returns
    /// Result containing the initialized FileStateMachine
    pub async fn new(data_dir: PathBuf) -> Result<Self, Error> {
        // Ensure data directory exists
        fs::create_dir_all(&data_dir).await?;

        let machine = Self {
            data: RwLock::new(HashMap::new()),
            lease: None, // Will be injected by NodeBuilder
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_snapshot_metadata: RwLock::new(None),
            running: AtomicBool::new(true),
            data_dir: data_dir.clone(),
        };

        // Load existing data from disk
        machine.load_from_disk().await?;

        Ok(machine)
    }

    /// Sets the lease manager for this state machine.
    ///
    /// This is an internal method called by NodeBuilder during initialization.
    /// The lease will also be restored from snapshot during `apply_snapshot_from_file()`.
    /// Also available for testing and benchmarks.
    pub fn set_lease(
        &mut self,
        lease: Arc<DefaultLease>,
    ) {
        self.lease = Some(lease);
    }

    /// Loads state machine data from disk files
    async fn load_from_disk(&self) -> Result<(), Error> {
        // Load last applied index and term from metadata file
        self.load_metadata().await?;

        // Load key-value data from data file
        self.load_data().await?;

        // Load TTL data from disk
        self.load_ttl_data().await?;

        // Replay write-ahead log for crash recovery
        self.replay_wal().await?;

        info!("Loaded state machine data from disk");
        Ok(())
    }

    /// Loads TTL data from disk (if lease is configured)
    async fn load_ttl_data(&self) -> Result<(), Error> {
        // Lease will be injected by NodeBuilder later
        // The lease data will be loaded after injection
        // For now, just skip this step during construction
        Ok(())
    }

    /// Loads TTL data into the configured lease
    ///
    /// Called after NodeBuilder injects the lease.
    /// Also available for testing and benchmarks.
    pub async fn load_lease_data(&self) -> Result<(), Error> {
        let Some(ref lease) = self.lease else {
            return Ok(()); // No lease configured
        };

        let ttl_path = self.data_dir.join("ttl_state.bin");
        if !ttl_path.exists() {
            debug!("No TTL state file found");
            return Ok(());
        }

        let ttl_data = tokio::fs::read(&ttl_path).await?;
        lease.reload(&ttl_data)?;

        info!("Loaded TTL state from disk: {} active TTLs", lease.len());
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

            let key = Bytes::from(buffer[pos..pos + key_len].to_vec());
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

            let value = Bytes::from(buffer[pos..pos + value_len].to_vec());
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
            debug!("No WAL file found, skipping replay");
            return Ok(());
        }

        let mut file = File::open(wal_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        if buffer.is_empty() {
            debug!("WAL file is empty, skipping replay");
            return Ok(());
        }

        let mut pos = 0;
        let mut operations = Vec::new();
        let mut replayed_count = 0;

        while pos + 17 < buffer.len() {
            // Read entry index (8 bytes)
            let _index = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // Read entry term (8 bytes)
            let term = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // Read operation code (1 byte)
            let op_code = WalOpCode::from_u8(buffer[pos]);
            pos += 1;

            // Check if we have enough bytes for key length
            if pos + 8 > buffer.len() {
                warn!("Incomplete key length at position {}, stopping replay", pos);
                break;
            }

            // Read key length (8 bytes)
            let key_len = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap()) as usize;
            pos += 8;

            // Check if we have enough data for the key
            if pos + key_len > buffer.len() {
                warn!(
                    "Incomplete key data at position {} (need {} bytes, have {})",
                    pos,
                    key_len,
                    buffer.len() - pos
                );
                break;
            }

            // Read key
            let key = Bytes::from(buffer[pos..pos + key_len].to_vec());
            pos += key_len;

            // Check if we have enough bytes for value length
            if pos + 8 > buffer.len() {
                warn!(
                    "Incomplete value length at position {}, stopping replay",
                    pos
                );
                break;
            }

            // Read value length (8 bytes)
            let value_len = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap()) as usize;
            pos += 8;

            // Read value if present
            let value = if value_len > 0 {
                if pos + value_len > buffer.len() {
                    warn!("Incomplete value data at position {}, stopping replay", pos);
                    break;
                }
                let value_data = Bytes::from(buffer[pos..pos + value_len].to_vec());
                pos += value_len;
                Some(value_data)
            } else {
                None
            };

            // Read TTL (4 bytes) - 0 means no TTL
            let ttl_secs = if pos + 4 <= buffer.len() {
                let ttl = u32::from_be_bytes(buffer[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if ttl > 0 { Some(ttl) } else { None }
            } else {
                // Backward compatibility: if no TTL field, assume no TTL
                debug!(
                    "No TTL field at position {}, assuming no TTL (backward compatibility)",
                    pos
                );
                None
            };

            operations.push((op_code, key, value, term, ttl_secs));
            replayed_count += 1;
        }

        info!(
            "Parsed {} WAL operations, applying to memory",
            operations.len()
        );

        // Apply all collected operations with a single lock acquisition
        let mut applied_count = 0;
        {
            let mut data = self.data.write();

            for (op_code, key, value, term, ttl_secs) in operations {
                match op_code {
                    WalOpCode::Insert => {
                        if let Some(value_data) = value {
                            data.insert(key.clone(), (value_data, term));

                            // Restore TTL from WAL (if lease configured)
                            if let Some(ttl) = ttl_secs {
                                if let Some(ref lease) = self.lease {
                                    lease.register(key.clone(), ttl as u64);
                                    debug!("Replayed INSERT with TTL: key={:?}, ttl={}s", key, ttl);
                                }
                            } else {
                                debug!("Replayed INSERT: key={:?}", key);
                            }

                            applied_count += 1;
                        } else {
                            warn!("INSERT operation without value");
                        }
                    }
                    WalOpCode::Delete => {
                        data.remove(&key);
                        if let Some(ref lease) = self.lease {
                            lease.unregister(&key);
                        }
                        applied_count += 1;
                        debug!("Replayed DELETE: key={:?}", key);
                    }
                    WalOpCode::Noop | WalOpCode::Config => {
                        // No data modification needed
                        applied_count += 1;
                        debug!("Replayed {:?} operation", op_code);
                    }
                }
            }
        }

        info!(
            "WAL replay complete: {} operations replayed_count, {} operations applied",
            replayed_count, applied_count
        );

        // Clear WAL only if replay was successful
        if applied_count > 0 {
            self.clear_wal_async().await?;
            debug!(
                "Cleared WAL after successful replay of {} operations",
                applied_count
            );
        }

        Ok(())
    }

    /// Persists key-value data to disk
    fn persist_data(&self) -> Result<(), Error> {
        // Collect data first to minimize lock time
        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
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
        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
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
            file.write_all(key.as_ref()).await?;

            // Write value length (8 bytes)
            let value_len = value.len() as u64;
            file.write_all(&value_len.to_be_bytes()).await?;

            // Write value
            file.write_all(value.as_ref()).await?;

            // Write term (8 bytes)
            file.write_all(&term.to_be_bytes()).await?;
        }

        file.flush().await?;

        // Persist TTL data alongside state data
        self.persist_ttl_data().await?;

        Ok(())
    }

    /// Persists lease state to disk (if configured)
    async fn persist_ttl_data(&self) -> Result<(), Error> {
        if let Some(ref lease) = self.lease {
            let ttl_snapshot = lease.to_snapshot();
            let ttl_path = self.data_dir.join("ttl_state.bin");
            tokio::fs::write(&ttl_path, ttl_snapshot).await?;
        }

        debug!("Persisted TTL state to disk");
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

    /// Clears the write-ahead log (called after successful persistence)
    #[allow(unused)]
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
        info!("Resetting state machine");

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

        info!("State machine reset completed");
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

    /// Batch WAL writes with proper durability guarantees
    ///
    /// Format per entry:
    /// - 8 bytes: entry index (big-endian u64)
    /// - 8 bytes: entry term (big-endian u64)
    /// - 1 byte: operation code (0=NOOP, 1=INSERT, 2=DELETE, 3=CONFIG)
    /// - 8 bytes: key length (big-endian u64)
    /// - N bytes: key data
    /// - 8 bytes: value length (big-endian u64, 0 if no value)
    /// - M bytes: value data (only if length > 0)
    pub(crate) async fn append_to_wal(
        &self,
        entries: Vec<(Entry, String, Bytes, Option<Bytes>, Option<u64>)>,
    ) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }

        let wal_path = self.data_dir.join("wal.log");

        let mut file =
            OpenOptions::new().write(true).create(true).append(true).open(&wal_path).await?;

        // Pre-allocate buffer with estimated size
        let estimated_size: usize = entries
            .iter()
            .map(|(_, _, key, value, _)| {
                8 + 8 + 1 + 8 + key.len() + 8 + value.as_ref().map_or(0, |v| v.len()) + 4
            })
            .sum();

        // Single batched write instead of multiple small writes
        let mut batch_buffer = Vec::with_capacity(estimated_size);

        for (entry, operation, key, value, ttl_secs) in entries {
            // Write entry index and term (16 bytes total)
            batch_buffer.extend_from_slice(&entry.index.to_be_bytes());
            batch_buffer.extend_from_slice(&entry.term.to_be_bytes());

            // Write operation code (1 byte)
            let op_code = WalOpCode::from_str(&operation);
            batch_buffer.push(op_code as u8);

            // Write key length and data (8 + N bytes)
            batch_buffer.extend_from_slice(&(key.len() as u64).to_be_bytes());
            batch_buffer.extend_from_slice(&key);

            // Write value length and data (8 + M bytes)
            // Always write length field for consistent format
            if let Some(value_data) = value {
                batch_buffer.extend_from_slice(&(value_data.len() as u64).to_be_bytes());
                batch_buffer.extend_from_slice(&value_data);
            } else {
                // Write 0 length for operations without value
                batch_buffer.extend_from_slice(&0u64.to_be_bytes());
            }

            // Write TTL (4 bytes) - 0 means no TTL
            // Convert u64 to u32 for space efficiency (4 bytes is enough for TTL in seconds)
            let ttl_value = ttl_secs.unwrap_or(0).min(u32::MAX as u64) as u32;
            batch_buffer.extend_from_slice(&ttl_value.to_be_bytes());
        }

        file.write_all(&batch_buffer).await?;
        file.flush().await?;

        Ok(())
    }

    /// Piggyback cleanup: Remove expired keys with time budget
    ///
    /// This method is called during apply_chunk to cleanup expired keys
    /// opportunistically (piggyback on existing Raft events).
    ///
    /// # Arguments
    /// * `max_duration_ms` - Maximum time budget for cleanup (milliseconds)
    ///
    /// # Returns
    /// Number of keys deleted
    ///
    /// # Performance
    /// - Fast-path: ~10ns if no TTL keys exist (lazy activation check)
    /// - Cleanup: O(log N + K) where K = expired keys
    /// - Time-bounded: stops after max_duration_ms to avoid blocking Raft
    /// Checkpoint: Persist memory to disk and clear WAL
    /// This is the "safe point" after which WAL is no longer needed
    #[allow(unused)]
    pub(crate) async fn checkpoint(&self) -> Result<(), Error> {
        // 1. Persist current state
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;

        // 2. Clear WAL (data is now in state.data)
        self.clear_wal_async().await?;

        Ok(())
    }
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
        info!("File state machine started");
        Ok(())
    }

    fn stop(&self) -> Result<(), Error> {
        // Ensure all data is flushed to disk before stopping
        self.running.store(false, Ordering::SeqCst);
        info!("File state machine stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        // Passive expiration check: DefaultLease handles expiration logic
        if let Some(ref lease) = self.lease {
            if lease.is_expired(key_buffer) {
                // Key has expired, delete it
                let mut data = self.data.write();
                data.remove(key_buffer);
                lease.unregister(key_buffer);

                debug!("Passive expiration: deleted key {:?}", key_buffer);
                return Ok(None);
            }
        }

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

    /// Thread-safe: called serially by single-task CommitHandler
    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<(), Error> {
        trace!("Applying chunk: {:?}.", chunk);

        let mut highest_index_entry: Option<LogId> = None;
        let mut batch_operations = Vec::new();

        // PHASE 1: Decode all operations and prepare WAL entries
        for entry in chunk {
            let entry_index = entry.index;

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

            // Decode operations without holding locks
            match entry.payload.as_ref().unwrap().payload.as_ref() {
                Some(Payload::Noop(_)) => {
                    debug!("Handling NOOP command at index {}", entry.index);
                    batch_operations.push((entry, "NOOP", Bytes::new(), None, None));
                }
                Some(Payload::Command(bytes)) => match WriteCommand::decode(&bytes[..]) {
                    Ok(write_cmd) => {
                        // Extract operation data for batch processing
                        match write_cmd.operation {
                            Some(Operation::Insert(Insert {
                                key,
                                value,
                                ttl_secs,
                            })) => {
                                batch_operations.push((
                                    entry,
                                    "INSERT",
                                    key,
                                    Some(value),
                                    ttl_secs,
                                ));
                            }
                            Some(Operation::Delete(Delete { key })) => {
                                batch_operations.push((entry, "DELETE", key, None, None));
                            }
                            None => {
                                warn!("WriteCommand without operation at index {}", entry.index);
                                batch_operations.push((entry, "NOOP", Bytes::new(), None, None));
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to decode WriteCommand at index {}: {:?}",
                            entry.index, e
                        );
                        return Err(StorageError::SerializationError(e.to_string()).into());
                    }
                },
                Some(Payload::Config(_config_change)) => {
                    debug!("Ignoring config change at index {}", entry.index);
                    batch_operations.push((entry, "CONFIG", Bytes::new(), None, None));
                }
                None => panic!("Entry payload variant should not be None!"),
            }

            info!("COMMITTED_LOG_METRIC: {}", entry_index);
        }

        // PHASE 2: Batch WAL writes (minimize I/O latency)
        let mut wal_entries = Vec::new();
        for (entry, operation, key, value, ttl_secs) in &batch_operations {
            // Prepare WAL data without immediate I/O - include TTL for crash recovery
            wal_entries.push((
                entry.clone(),
                operation.to_string(),
                key.clone(),
                value.clone(),
                *ttl_secs, // ttl_secs is already Option<u32> from protobuf
            ));
        }

        // Single batch WAL write (reduces I/O overhead)
        self.append_to_wal(wal_entries).await?;

        // PHASE 3: Fast in-memory updates with minimal lock time (ZERO-COPY)
        {
            let mut data = self.data.write();

            // Process all operations without any awaits inside the lock
            for (entry, operation, key, value, ttl_secs) in batch_operations {
                match operation {
                    "INSERT" => {
                        if let Some(value) = value {
                            // ZERO-COPY: Use existing Bytes without cloning if possible
                            data.insert(key.clone(), (value, entry.term));

                            // Register lease if specified and lease is configured
                            if let Some(ref lease) = self.lease {
                                if let Some(ttl) = ttl_secs {
                                    if ttl > 0 {
                                        lease.register(key, ttl as u64);
                                    }
                                }
                            }
                        }
                    }
                    "DELETE" => {
                        data.remove(&key);
                        if let Some(ref lease) = self.lease {
                            lease.unregister(&key);
                        }
                    }
                    "NOOP" | "CONFIG" => {
                        // No data modification needed
                    }
                    _ => warn!("Unknown operation: {}", operation),
                }
            }
        } // Lock released immediately - no awaits inside!

        // PHASE 4: Lease cleanup (DefaultLease handles strategy internally)
        // Zero overhead if lease cleanup is disabled - DefaultLease checks config
        if let Some(ref lease) = self.lease {
            let expired_keys = lease.on_apply();
            if !expired_keys.is_empty() {
                let mut data = self.data.write();
                for key in &expired_keys {
                    data.remove(key);
                }
                trace!("Lease cleanup: deleted {} expired keys", expired_keys.len());
            }
        }

        if let Some(log_id) = highest_index_entry {
            debug!("State machine - updated last_applied: {:?}", log_id);
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
        info!("Applying snapshot from file: {:?}", snapshot_dir);

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

            let key = Bytes::from(buffer[pos..pos + key_len].to_vec());
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

            let value = Bytes::from(buffer[pos..pos + value_len].to_vec());
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

        // Read and reload lease data if present
        if pos + 8 <= buffer.len() {
            let ttl_len_bytes = &buffer[pos..pos + 8];
            let ttl_len = u64::from_be_bytes([
                ttl_len_bytes[0],
                ttl_len_bytes[1],
                ttl_len_bytes[2],
                ttl_len_bytes[3],
                ttl_len_bytes[4],
                ttl_len_bytes[5],
                ttl_len_bytes[6],
                ttl_len_bytes[7],
            ]) as usize;
            pos += 8;

            if pos + ttl_len <= buffer.len() {
                let ttl_data = &buffer[pos..pos + ttl_len];
                if let Some(ref lease) = self.lease {
                    lease.reload(ttl_data)?;
                }
            }
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

        info!("Snapshot applied successfully");
        Ok(())
    }

    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        last_included: LogId,
    ) -> Result<Bytes, Error> {
        info!("Generating snapshot data up to {:?}", last_included);

        // Create snapshot directory
        fs::create_dir_all(&new_snapshot_dir).await?;

        // Create snapshot file
        let snapshot_path = new_snapshot_dir.join("snapshot.bin");
        let mut file = File::create(&snapshot_path).await?;

        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Write data in the same format as the data file
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

        // Write lease state if configured
        let lease_snapshot = if let Some(ref lease) = self.lease {
            lease.to_snapshot()
        } else {
            Vec::new()
        };

        // Write lease data length
        let lease_len = lease_snapshot.len() as u64;
        file.write_all(&lease_len.to_be_bytes()).await?;

        // Write lease data
        file.write_all(&lease_snapshot).await?;

        file.flush().await?;

        // Update metadata
        let metadata = SnapshotMetadata {
            last_included: Some(last_included),
            checksum: Bytes::from(vec![0; 32]), // Simple checksum for demo
        };

        self.update_last_snapshot_metadata(&metadata)?;

        info!("Snapshot generated at {:?}", snapshot_path);

        // Return dummy checksum
        Ok(Bytes::from_static(&[0u8; 32]))
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
        // self.clear_wal()?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;
        // self.clear_wal_async().await?;
        Ok(())
    }

    async fn reset(&self) -> Result<(), Error> {
        self.reset().await
    }
}
