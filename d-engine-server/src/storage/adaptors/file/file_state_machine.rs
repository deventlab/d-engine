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
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! A record cut short by a crash is dropped on replay: replay stops at the first
//! incomplete record.
//!
//! ### WAL Lifecycle
//!
//! WAL is the **primary** crash-safety path. Checkpoints are taken periodically
//! (every 1000 entries or 10s) to bound replay time on recovery — not on every apply.
//!
//! ```text
//! apply_chunk() → append_to_wal() → update memory → [if should_checkpoint()] → checkpoint()
//!                                                                                  ├─ persist_data_async()
//!                                                                                  ├─ persist_metadata_async()
//!                                                                                  └─ clear_wal_async()
//! ```
//!
//! On shutdown (`flush()`), a forced checkpoint ensures no data loss.
//!
//! ## Crash Recovery Flow
//!
//! On node startup (`new()`):
//! 1. `load_metadata()` - Restore Raft state
//! 2. `load_data()` - Load persisted key-value data
//! 3. `replay_wal()` - **Critical**: Replay operations written after the last checkpoint
//!    - Entries at or below `last_applied` are skipped
//!    - Ensures crash consistency (operations are idempotent)

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use d_engine_core::ApplyEntry;
use d_engine_core::ApplyResult;
use d_engine_core::Command;
use d_engine_core::Error;
use d_engine_core::ScanResult;
use d_engine_core::SnapshotApplyResult;
use d_engine_core::SnapshotError;
use d_engine_core::StateMachine;
use d_engine_core::StorageError;
use d_engine_core::file_io::compute_checksum_from_folder_path;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use parking_lot::RwLock;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

type FileStateMachineDataType = RwLock<HashMap<Bytes, (Bytes, u64)>>;

/// WAL operation codes for fixed-size encoding
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOpCode {
    Noop = 0,
    Insert = 1,
    Delete = 2,
    Config = 3,
    CompareAndSwap = 4,
    CasFailed = 5,
}

impl WalOpCode {
    #[allow(dead_code)]
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "NOOP" => Some(Self::Noop),
            "INSERT" => Some(Self::Insert),
            "DELETE" => Some(Self::Delete),
            "CONFIG" => Some(Self::Config),
            "CAS" => Some(Self::CompareAndSwap),
            "CAS_FAILED" => Some(Self::CasFailed),
            _ => None,
        }
    }

    fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Noop),
            1 => Some(Self::Insert),
            2 => Some(Self::Delete),
            3 => Some(Self::Config),
            4 => Some(Self::CompareAndSwap),
            5 => Some(Self::CasFailed),
            _ => None,
        }
    }
}

/// Encodes a single WAL entry into `buf`.
///
/// For `CompareAndSwap`, `cas_success` determines whether to write
/// `Insert` (success, replay applies unconditionally) or
/// `CasFailed` (failure, replay skips).
fn encode_wal_entry(
    buf: &mut Vec<u8>,
    entry: &ApplyEntry,
    cas_success: bool,
) {
    buf.extend_from_slice(&entry.index.to_be_bytes());
    buf.extend_from_slice(&entry.term.to_be_bytes());
    match &entry.command {
        Command::Noop => {
            buf.push(WalOpCode::Noop as u8);
            buf.extend_from_slice(&0u64.to_be_bytes());
            buf.extend_from_slice(&0u64.to_be_bytes());
        }
        Command::Insert { key, value } => {
            buf.push(WalOpCode::Insert as u8);
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
            buf.extend_from_slice(value);
        }
        Command::Delete { key } => {
            buf.push(WalOpCode::Delete as u8);
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&0u64.to_be_bytes()); // val_len = 0
        }
        Command::CompareAndSwap { key, value, .. } => {
            if cas_success {
                buf.push(WalOpCode::Insert as u8);
                buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
                buf.extend_from_slice(value);
            } else {
                buf.push(WalOpCode::CasFailed as u8);
                buf.extend_from_slice(&0u64.to_be_bytes()); // key_len = 0
                buf.extend_from_slice(&0u64.to_be_bytes()); // val_len = 0
            }
        }
        Command::Batch { ops } => {
            for (i, op) in ops.iter().enumerate() {
                // After the first op, each subsequent op needs its own index+term
                // prefix so replay_wal can parse independent records.
                if i > 0 {
                    buf.extend_from_slice(&entry.index.to_be_bytes());
                    buf.extend_from_slice(&entry.term.to_be_bytes());
                }
                match op {
                    d_engine_core::BatchOp::Insert { key, value } => {
                        buf.push(WalOpCode::Insert as u8);
                        buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
                        buf.extend_from_slice(key);
                        buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
                        buf.extend_from_slice(value);
                    }
                    d_engine_core::BatchOp::Delete { key } => {
                        buf.push(WalOpCode::Delete as u8);
                        buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
                        buf.extend_from_slice(key);
                        buf.extend_from_slice(&0u64.to_be_bytes()); // val_len = 0
                    }
                }
            }
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

    // Checkpoint state: WAL is the primary persistence path; checkpoint snapshots
    // data periodically to bound crash-recovery replay time.
    wal_entries_since_checkpoint: AtomicU64,
    last_checkpoint: Mutex<Instant>,

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
    ///
    /// # Returns
    /// Result containing the initialized FileStateMachine
    pub async fn new(data_dir: PathBuf) -> Result<Self, Error> {
        // Ensure data directory exists
        fs::create_dir_all(&data_dir).await?;

        let machine = Self {
            data: RwLock::new(HashMap::new()),
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_snapshot_metadata: RwLock::new(None),
            running: AtomicBool::new(true),
            wal_entries_since_checkpoint: AtomicU64::new(0),
            last_checkpoint: Mutex::new(Instant::now()),
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

        info!("Loaded state machine data from disk");
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

        let last_applied = self.last_applied_index.load(Ordering::SeqCst);

        // A crash can leave the last WAL record only partly written. The length checks below
        // stop replay at the first incomplete record and drop it; records before it are replayed.
        while pos + 17 < buffer.len() {
            // Read entry index (8 bytes)
            let index = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // Read entry term (8 bytes)
            let term = u64::from_be_bytes(buffer[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // Read operation code (1 byte)
            let op_code =
                WalOpCode::from_u8(buffer[pos]).ok_or_else(|| StorageError::DataCorruption {
                    location: format!("WAL opcode {} at byte offset {}", buffer[pos], pos),
                })?;
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

            // A zero length is an empty value, not a missing one. Delete and Noop records
            // also write 0 here; replay ignores their value.
            if pos + value_len > buffer.len() {
                warn!("Incomplete value data at position {}, stopping replay", pos);
                break;
            }
            let value = Bytes::from(buffer[pos..pos + value_len].to_vec());
            pos += value_len;

            if index <= last_applied {
                debug!(
                    "Skipped stale WAL entry at index {} (last_applied={})",
                    index, last_applied
                );
                continue;
            }

            operations.push((op_code, key, value, term));
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

            for (op_code, key, value, term) in operations {
                match op_code {
                    WalOpCode::Insert => {
                        data.insert(key, (value, term));
                        applied_count += 1;
                    }
                    WalOpCode::Delete => {
                        data.remove(&key);
                        applied_count += 1;
                        debug!("Replayed DELETE: key={:?}", key);
                    }
                    WalOpCode::CompareAndSwap => {
                        // Legacy WAL format from before the CAS outcome fix.
                        // Current code writes CAS results as Insert (success) or Noop (failure),
                        // so this branch is only reached when replaying WAL files created before
                        // the fix. Apply new_value unconditionally as best-effort.
                        data.insert(key.clone(), (value, term));
                        applied_count += 1;
                        debug!("Replayed legacy CAS: key={:?}", key);
                    }
                    WalOpCode::Noop | WalOpCode::Config | WalOpCode::CasFailed => {
                        // No data modification needed
                        applied_count += 1;
                        debug!("Replayed {:?} operation", op_code);
                    }
                }
            }
        }

        info!(
            "WAL replay complete: {} operations replayed, {} applied",
            replayed_count, applied_count,
        );

        // Unconditionally clear WAL after replay. load_data() already restored the last
        // checkpoint; WAL is only the post-checkpoint delta. Even if 0 entries were applied
        // (e.g. truncated tail only), the WAL is stale. Keeping it would cause infinite
        // replay-of-the-same-truncated-entry on every subsequent startup.
        self.clear_wal_async().await?;
        debug!(
            "Cleared WAL after replay ({} operations applied)",
            applied_count
        );

        Ok(())
    }

    /// Persists key-value data to disk
    fn persist_data(&self) -> Result<(), Error> {
        // Collect data first to minimize lock time
        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Batch serialize into a single buffer — mirrors persist_data_async().
        let data_path = self.data_dir.join("state.data");

        let estimated: usize =
            data_copy.iter().map(|(k, (v, _))| 8 + k.len() + 8 + v.len() + 8).sum();
        let mut buf = Vec::with_capacity(estimated);

        for (key, (value, term)) in &data_copy {
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(&term.to_be_bytes());
        }

        std::fs::write(data_path, buf)?;
        Ok(())
    }

    /// Persists key-value data to disk
    async fn persist_data_async(&self) -> Result<(), Error> {
        // Collect data under minimal lock scope
        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        let data_path = self.data_dir.join("state.data");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)
            .await?;

        // Batch serialize into a single buffer — eliminates per-entry async yield overhead.
        // Mirrors append_to_wal's approach for consistent I/O pattern.
        let estimated: usize =
            data_copy.iter().map(|(k, (v, _))| 8 + k.len() + 8 + v.len() + 8).sum();
        let mut buf = Vec::with_capacity(estimated);

        for (key, (value, term)) in &data_copy {
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(&term.to_be_bytes());
        }

        file.write_all(&buf).await?;
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

    /// Returns true if a checkpoint should be taken now.
    ///
    /// Triggers on any of:
    /// - WAL has accumulated ≥ 1000 entries since last checkpoint
    /// - Last checkpoint was > 10s ago
    fn should_checkpoint(&self) -> bool {
        const WAL_ENTRY_THRESHOLD: u64 = 1000;
        const TIME_THRESHOLD_SECS: u64 = 10;

        if self.wal_entries_since_checkpoint.load(Ordering::Relaxed) >= WAL_ENTRY_THRESHOLD {
            return true;
        }
        if let Ok(last) = self.last_checkpoint.lock() {
            return last.elapsed().as_secs() >= TIME_THRESHOLD_SECS;
        }
        false
    }

    /// Checkpoint: snapshot full data + metadata to disk, then clear WAL.
    ///
    /// WAL is the primary crash-safety path; checkpoint bounds recovery replay time.
    pub(crate) async fn checkpoint(&self) -> Result<(), Error> {
        self.persist_data_async().await?;
        self.persist_metadata_async().await?;
        self.clear_wal_async().await?;

        self.wal_entries_since_checkpoint.store(0, Ordering::Relaxed);
        if let Ok(mut last) = self.last_checkpoint.lock() {
            *last = Instant::now();
        }
        debug!("Checkpoint complete");
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

    /// Reads are served only while running. After `close()` the developer may still hold a clone
    /// of the state machine, and it must not answer from state that is no longer maintained.
    fn ensure_serving(&self) -> Result<(), Error> {
        if self.running.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StorageError::NotServing("state machine stopped".into()).into())
        }
    }

    /// Batch WAL writes with proper durability guarantees.
    ///
    /// `cas_outcomes[i]` is consulted only when `entries[i].command` is
    /// `CompareAndSwap`. Successful CAS is recorded as `Insert`; failed CAS as
    /// `Noop`, so replay never applies a CAS that failed at runtime.
    ///
    /// Format per entry:
    ///
    /// - 8 bytes: entry index (big-endian u64)
    /// - 8 bytes: entry term (big-endian u64)
    /// - 1 byte: operation code (0=NOOP, 1=INSERT, 2=DELETE, 3=CONFIG)
    /// - 8 bytes: key length (big-endian u64)
    /// - N bytes: key data
    /// - 8 bytes: value length (big-endian u64, 0 if no value)
    /// - M bytes: value data (only if length > 0)
    #[cfg(test)]
    pub(crate) async fn append_to_wal(
        &self,
        entries: &[ApplyEntry],
        cas_outcomes: &[bool],
    ) -> Result<(), Error> {
        assert_eq!(
            entries.len(),
            cas_outcomes.len(),
            "cas_outcomes must have the same length as entries"
        );
        if entries.is_empty() {
            return Ok(());
        }

        let wal_path = self.data_dir.join("wal.log");
        let mut file =
            OpenOptions::new().write(true).create(true).append(true).open(&wal_path).await?;

        let mut batch_buffer = Vec::with_capacity(entries.len() * 64);
        for (i, entry) in entries.iter().enumerate() {
            encode_wal_entry(&mut batch_buffer, entry, cas_outcomes[i]);
        }

        file.write_all(&batch_buffer).await?;
        file.flush().await?;

        Ok(())
    }
}

impl Drop for FileStateMachine {
    fn drop(&mut self) {
        let timer = Instant::now();

        // Save state into local database including flush operation
        match StateMachine::close(self) {
            Ok(()) => debug!("StateMachine closed in {:?}", timer.elapsed()),
            Err(e) => error!("Failed to close StateMachine: {}", e),
        }
    }
}

#[async_trait]
impl StateMachine for FileStateMachine {
    async fn start(&self) -> Result<(), Error> {
        self.running.store(true, Ordering::SeqCst);

        info!("File state machine started");
        Ok(())
    }

    fn close(&self) -> Result<(), Error> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(()); // already closed
        }
        // The developer may keep an `Arc` to this state machine, so `Drop` may never run:
        // persist everything a restart needs here.
        // No exclusive OS lock file to release.
        self.save_hard_state()
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    fn get(
        &self,
        key_buffer: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        self.ensure_serving()?;
        let data = self.data.read();
        Ok(data.get(key_buffer).map(|(value, _)| value.clone()))
    }

    /// Thread-safe: called serially by the state machine worker
    async fn apply_chunk(
        &self,
        chunk: &[ApplyEntry],
    ) -> Result<Vec<ApplyResult>, Error> {
        self.ensure_serving()?;
        let chunk_len = chunk.len();
        let mut results = Vec::with_capacity(chunk_len);
        let mut highest_log_id: Option<LogId> = None;

        // Validate ordering — Command is already decoded, no proto work here
        for entry in chunk {
            if let Some(prev) = &highest_log_id {
                assert!(
                    entry.index > prev.index,
                    "apply_chunk: received unordered entry at index {} (prev={})",
                    entry.index,
                    prev.index
                );
            }
            highest_log_id = Some(LogId {
                index: entry.index,
                term: entry.term,
            });
            info!("COMMITTED_LOG_METRIC: {}", entry.index);
        }

        // PRE-PHASE + PHASE 2 combined: evaluate outcomes and encode WAL in one pass.
        //
        // WAL records outcomes, not intents. We simulate in chunk order so earlier
        // entries are visible to later CAS — matching Raft's serial apply semantics.
        //
        // Key-scoped snapshot: only CAS entries need to read the current value.
        // We fetch only CAS-referenced keys from self.data (read lock held briefly)
        // and track chunk-level mutations in a small `delta` map, avoiding a full
        // HashMap clone regardless of data set size.
        //
        // Encoding happens in the same loop as evaluation, eliminating the second
        // pass over chunk that a separate append_to_wal call would require.
        let (cas_outcomes, wal_buf) = {
            // Collect keys that any CAS in this chunk will compare against.
            let cas_keys: HashSet<&Bytes> = chunk
                .iter()
                .filter_map(|e| match &e.command {
                    Command::CompareAndSwap { key, .. } => Some(key),
                    _ => None,
                })
                .collect();

            // Fetch only those keys; read lock released at end of this block.
            let base: HashMap<Bytes, (Bytes, u64)> = {
                let guard = self.data.read();
                cas_keys
                    .iter()
                    .filter_map(|k| guard.get(k.as_ref()).map(|v| ((*k).clone(), v.clone())))
                    .collect()
            };

            // delta tracks mutations made by earlier entries in this chunk.
            // None = deleted by an earlier Delete in the same chunk.
            let mut delta: HashMap<Bytes, Option<(Bytes, u64)>> = HashMap::new();
            let mut wal_buf: Vec<u8> = Vec::with_capacity(chunk.len() * 64);

            let outcomes: Vec<bool> = chunk
                .iter()
                .map(|entry| {
                    let success = match &entry.command {
                        Command::Insert { key, value, .. } => {
                            delta.insert(key.clone(), Some((value.clone(), entry.term)));
                            false
                        }
                        Command::Delete { key } => {
                            delta.insert(key.clone(), None);
                            false
                        }
                        Command::CompareAndSwap {
                            key,
                            expected,
                            value: new_value,
                        } => {
                            // Look up current value: delta (this chunk) takes precedence over base.
                            let current =
                                delta.get(key).map(|v| v.as_ref()).unwrap_or_else(|| base.get(key));
                            let success = match (current, expected) {
                                (Some((c, _)), Some(e)) => c.as_ref() == e.as_ref(),
                                (None, None) => true,
                                _ => false,
                            };
                            if success {
                                delta.insert(key.clone(), Some((new_value.clone(), entry.term)));
                            }
                            success
                        }
                        Command::Batch { ops } => {
                            ops.iter().for_each(|op| match op {
                                d_engine_core::BatchOp::Insert { key, value } => {
                                    delta.insert(key.clone(), Some((value.clone(), entry.term)));
                                }
                                d_engine_core::BatchOp::Delete { key } => {
                                    delta.insert(key.clone(), None);
                                }
                            });
                            false
                        }
                        Command::Noop => false,
                    };
                    encode_wal_entry(&mut wal_buf, entry, success);
                    success
                })
                .collect();

            (outcomes, wal_buf)
        };

        // PHASE 2: single fsync with buffer built during evaluation above.
        // Successful CAS → WalOpCode::Insert; failed CAS → WalOpCode::CasFailed.
        // Note: wal_buf is never empty here in practice — every entry encodes at least
        // 41 bytes — but the guard is kept as a cheap defensive check.
        if !wal_buf.is_empty() {
            let wal_path = self.data_dir.join("wal.log");
            let mut file =
                OpenOptions::new().write(true).create(true).append(true).open(&wal_path).await?;
            file.write_all(&wal_buf).await?;
            file.flush().await?;
        }

        // PHASE 3: Fast in-memory updates with minimal lock time
        // (CAS uses pre-computed outcomes — no re-evaluation under write lock)
        {
            let mut data = self.data.write();

            for (entry, &cas_success) in chunk.iter().zip(cas_outcomes.iter()) {
                match &entry.command {
                    Command::Noop => {
                        results.push(ApplyResult::success(entry.index));
                    }
                    Command::Insert { key, value } => {
                        data.insert(key.clone(), (value.clone(), entry.term));
                        results.push(ApplyResult::success(entry.index));
                    }
                    Command::Delete { key } => {
                        data.remove(key.as_ref());
                        results.push(ApplyResult::success(entry.index));
                    }
                    Command::CompareAndSwap {
                        key,
                        value: new_value,
                        ..
                    } => {
                        debug!(
                            "CAS at index {}: key={:?}, success={}",
                            entry.index,
                            String::from_utf8_lossy(key),
                            cas_success
                        );
                        results.push(if cas_success {
                            ApplyResult::success(entry.index)
                        } else {
                            ApplyResult::failure(entry.index)
                        });
                        if cas_success {
                            data.insert(key.clone(), (new_value.clone(), entry.term));
                        }
                    }

                    Command::Batch { ops } => {
                        ops.iter().for_each(|op| match op {
                            d_engine_core::BatchOp::Insert { key, value } => {
                                data.insert(key.clone(), (value.clone(), entry.term));
                            }
                            d_engine_core::BatchOp::Delete { key } => {
                                data.remove(key.as_ref());
                            }
                        });
                        results.push(ApplyResult::success(entry.index));
                    }
                }
            }
        } // Lock released immediately - no awaits inside!

        // PHASE 4: Update last applied index and conditionally checkpoint.
        // WAL (written in PHASE 2) is the primary crash-safety path.
        // Checkpoint snapshots full data periodically to bound WAL replay time on recovery.
        if let Some(log_id) = highest_log_id {
            debug!("State machine - updated last_applied: {:?}", log_id);
            self.update_last_applied(log_id);
        }

        self.wal_entries_since_checkpoint.fetch_add(chunk_len as u64, Ordering::Relaxed);

        if self.should_checkpoint() {
            self.checkpoint().await?;
        }

        Ok(results)
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
    ) -> Result<SnapshotApplyResult, Error> {
        // (#436) — every StateMachine backend must honor this contract, not just RocksDB.
        // A missing boundary can't be classified — reject rather than fall through
        // to an unconditional install.
        let incoming = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("Missing last_included in snapshot metadata".into())
        })?;

        let current = self.last_applied();
        match incoming.index.cmp(&current.index) {
            std::cmp::Ordering::Less => {
                info!(?incoming, ?current, "Ignoring stale snapshot");
                return Ok(SnapshotApplyResult::IgnoredStale { current });
            }
            std::cmp::Ordering::Equal if incoming.term == current.term => {
                info!(?incoming, ?current, "Ignoring duplicate snapshot");
                return Ok(SnapshotApplyResult::IgnoredDuplicate { current });
            }
            std::cmp::Ordering::Equal => {
                return Err(SnapshotError::BoundaryConflict {
                    index: current.index,
                    local_term: current.term,
                    incoming_term: incoming.term,
                }
                .into());
            }
            std::cmp::Ordering::Greater => {}
        }

        // Verify the snapshot's end-to-end checksum before installing (#436).
        let computed_checksum = compute_checksum_from_folder_path(&snapshot_dir).await?;
        if metadata.checksum.as_ref() != computed_checksum.as_ref() {
            error!(
                "Snapshot checksum mismatch! Computed: {:?}, Expected: {:?}",
                computed_checksum, metadata.checksum
            );
            return Err(SnapshotError::ChecksumMismatch.into());
        }

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

        // Atomically replace the data
        {
            let mut data = self.data.write();
            *data = new_data;
        }

        // Update metadata
        *self.last_snapshot_metadata.write() = Some(metadata.clone());

        self.update_last_applied(incoming);

        // Persist to disk
        self.persist_data_async().await?;
        self.clear_wal_async().await?;
        self.persist_metadata_async().await?;

        info!("Snapshot applied successfully");

        Ok(SnapshotApplyResult::Applied {
            last_included: incoming,
        })
    }

    async fn generate_snapshot_data(
        &self,
        new_snapshot_dir: std::path::PathBuf,
        _last_included: LogId,
    ) -> Result<Bytes, Error> {
        // Create snapshot directory
        fs::create_dir_all(&new_snapshot_dir).await?;

        // Create snapshot file
        let snapshot_path = new_snapshot_dir.join("snapshot.bin");
        let mut file = File::create(&snapshot_path).await?;

        let data_copy: HashMap<Bytes, (Bytes, u64)> = {
            let data = self.data.read();
            data.iter().map(|(k, (v, t))| (k.clone(), (v.clone(), *t))).collect()
        };

        // Batch serialize into a single buffer — eliminates per-entry async yield overhead.
        let estimated: usize =
            data_copy.iter().map(|(k, (v, _))| 8 + k.len() + 8 + v.len() + 8).sum::<usize>();
        let mut buf = Vec::with_capacity(estimated);

        for (key, (value, term)) in &data_copy {
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
            buf.extend_from_slice(value);
            buf.extend_from_slice(&term.to_be_bytes());
        }

        file.write_all(&buf).await?;

        file.flush().await?;

        info!("Snapshot generated at {:?}", snapshot_path);

        let checksum = compute_checksum_from_folder_path(&new_snapshot_dir).await?;
        Ok(Bytes::copy_from_slice(&checksum))
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
        // Force checkpoint on shutdown to ensure WAL entries are not lost.
        self.checkpoint().await
    }

    async fn reset(&self) -> Result<(), Error> {
        self.reset().await
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<ScanResult, Error> {
        self.ensure_serving()?;
        let data = self.data.read();
        let entries: Vec<(Bytes, Bytes)> = data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect();
        let revision = self.last_applied_index.load(Ordering::SeqCst);
        Ok(ScanResult { entries, revision })
    }

    fn get_multi(
        &self,
        keys: &[Bytes],
    ) -> d_engine_core::Result<Vec<Option<Bytes>>> {
        self.ensure_serving()?;
        // Hold read lock for the full batch so all keys come from the same consistent view.
        // apply_chunk holds the write lock; parking_lot RwLock ensures they don't interleave.
        let data = self.data.read();
        Ok(keys.iter().map(|k| data.get(k).map(|(v, _)| v.clone())).collect())
    }
}
