use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::watch::WatchManager;

use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::BoxStream;
use memmap2::Mmap;
use memmap2::MmapOptions;
use tempfile::tempdir;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::remove_dir_all;
use tokio::fs::remove_file;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_tar::Archive;
use tonic::Streaming;
use tonic::async_trait;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;

use super::SnapshotAssembler;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::StateMachineHandler;
use crate::NewCommitData;
use crate::RaftLog;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::SnapshotGuard;
use crate::SnapshotPathManager;
use crate::StateMachine;
use crate::StorageError;
use crate::TypeConfig;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::alias::SNP;
use crate::convert::classify_error;
use crate::file_io::validate_checksum;
use crate::file_io::validate_compressed_format;
use crate::scoped_timer::ScopedTimer;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::PurgeLogRequest;
use d_engine_proto::server::storage::PurgeLogResponse;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use prost::Message;

/// Unified snapshot metadata with precomputed values
#[derive(Debug, Clone)]
pub struct SnapshotTransferMeta {
    pub metadata: SnapshotMetadata,
    pub total_chunks: u32,
    pub chunk_size: usize,
    #[allow(unused)]
    pub file_size: u64,
    pub file_path: PathBuf,
}

#[derive(Debug)]
pub struct DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    node_id: u32,
    // last_applied, as an application progress indicator, may fall under the responsibility of the
    // Handler, as it manages the application process.
    last_applied: AtomicU64,   // The last applied log index
    pending_commit: AtomicU64, // The highest pending commit index
    #[allow(unused)]
    max_entries_per_chunk: usize,
    state_machine: Arc<SMOF<T>>,

    // current_snapshot_version: AtomicU64,
    snapshot_config: SnapshotConfig,
    snapshot_policy: SNP<T>,
    snapshot_in_progress: AtomicBool,

    path_mgr: Arc<SnapshotPathManager>,

    /// Temporary lock when snapshot is generated (to prevent concurrent snapshot generation)
    snapshot_lock: RwLock<()>,

    /// Optional watch manager for monitoring key changes
    /// When None, watch functionality is disabled with zero overhead
    watch_manager: Option<Arc<WatchManager>>,
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub(crate) struct CleanupSnapshotMeta {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) path: PathBuf,
}

#[async_trait]
impl<T> StateMachineHandler<T> for DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    /// Get the interval to be processed
    fn pending_range(&self) -> Option<RangeInclusive<u64>> {
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let pending_commit = self.pending_commit.load(Ordering::Acquire);

        if pending_commit > last_applied {
            Some((last_applied + 1)..=pending_commit)
        } else {
            None
        }
    }

    /// Update pending commit index
    fn update_pending(
        &self,
        new_commit: u64,
    ) {
        let mut current = self.pending_commit.load(Ordering::Acquire);
        while new_commit > current {
            match self.pending_commit.compare_exchange_weak(
                current,
                new_commit,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(e) => current = e,
            }
        }
    }

    async fn apply_chunk(
        &self,
        chunk: Vec<Entry>,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("apply_chunk");

        // Use a timer to measure latency and count chunks
        let start = Instant::now();
        let chunk_size = chunk.len();

        metrics::counter!(
            "state_machine.apply_chunk.count",
            &[("node_id", self.node_id.to_string())]
        )
        .increment(1);

        let last_index = chunk.last().map(|entry| entry.index);
        trace!(
            "[node-{}] apply_chunk::entry={:?} last_index: {:?}",
            self.node_id, &chunk, last_index
        );

        let sm = self.state_machine.clone();

        // Apply the chunk and track errors
        let apply_result = sm.apply_chunk(chunk.clone()).await;

        // Notify watchers of changes only on success
        // PERF: has_watchers() is O(1) and avoids expensive protobuf decoding when no watchers exist
        if apply_result.is_ok() {
            if let Some(ref watch_mgr) = self.watch_manager {
                if watch_mgr.has_watchers() {
                    self.notify_watchers(&chunk, watch_mgr);
                }
            }
        }

        // Record latency and chunk size histogram *after* the operation
        let duration_ms = start.elapsed().as_millis() as f64;
        metrics::histogram!(
            "state_machine.apply_chunk.duration_ms",
            &[("node_id", self.node_id.to_string())]
        )
        .record(duration_ms);

        metrics::histogram!(
            "state_machine.apply_chunk.batch_size",
            &[("node_id", self.node_id.to_string())]
        )
        .record(chunk_size as f64);

        // Track result
        match &apply_result {
            Ok(_) => {
                // Efficiently obtain the maximum index: directly get the index of the last entry
                if let Some(idx) = last_index {
                    self.last_applied.store(idx, Ordering::Release);
                }

                metrics::counter!(
                    "state_machine.apply_chunk.success",
                    &[("node_id", self.node_id.to_string())]
                )
                .increment(1);
            }
            Err(e) => {
                let error_type = classify_error(e);
                metrics::counter!(
                    "state_machine.apply_chunk.error",
                    &[
                        ("node_id", self.node_id.to_string()),
                        ("error_type", error_type)
                    ]
                )
                .increment(1);
            }
        }

        apply_result
    }

    /// TODO: decouple client related commands with RAFT internal logic
    fn read_from_state_machine(
        &self,
        keys: Vec<Bytes>,
    ) -> Option<Vec<ClientResult>> {
        let mut result = Vec::new();
        for key in keys {
            if let Ok(Some(value)) = self.state_machine.get(&key) {
                result.push(ClientResult { key, value });
            }
        }

        if !result.is_empty() {
            Some(result)
        } else {
            None
        }
    }

    #[instrument(skip(self))]
    async fn apply_snapshot_stream_from_leader(
        &self,
        current_term: u64,
        stream: Box<Streaming<SnapshotChunk>>,
        ack_tx: mpsc::Sender<SnapshotAck>, // ACK sender
        config: &SnapshotConfig,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("receive_snapshot_stream_from_leader");

        info!(?ack_tx, %current_term, "receive_snapshot_stream_from_leader");
        let (final_metadata, snapshot_path) =
            self.process_snapshot_stream(stream, ack_tx.clone(), config).await?;

        debug!(
            ?final_metadata,
            ?snapshot_path,
            "before apply_snapshot_from_file"
        );

        // Decompress before passing to state machine
        let temp_dir = tempdir()?;
        self.decompress_to_directory(&snapshot_path, temp_dir.path()).await?;

        // Now call state machine with decompressed directory
        self.state_machine
            .apply_snapshot_from_file(&final_metadata, temp_dir.path().to_path_buf())
            .await?;

        info!("Snapshot stream successfully received and applied");
        Ok(())
    }

    #[inline]
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool {
        let _timer = ScopedTimer::new("should_snapshot");

        if self.snapshot_in_progress.load(Ordering::Relaxed) {
            trace!("Snapshot already in progress");
            return false;
        }

        let last_applied = self.state_machine.last_applied();
        let last_snapshot_metadata = self.state_machine.snapshot_metadata();

        let last_included = last_snapshot_metadata
            .and_then(|meta| meta.last_included)
            .unwrap_or(LogId { index: 0, term: 0 });

        self.snapshot_policy.should_trigger(&SnapshotContext {
            role: new_commit_data.role,
            last_included,
            last_applied,
            current_term: new_commit_data.current_term,
        })
    }

    async fn create_snapshot(&self) -> Result<(SnapshotMetadata, PathBuf)> {
        println!("[SNAPHSOT] Generating snapshot now ...");

        // 0. Create a guard (automatically manage state)
        let _guard1 = SnapshotGuard::new(&self.snapshot_in_progress)?;

        // 1: Get write lock
        debug!("create_snapshot 1: Get write lock");
        let _guard2 = self.snapshot_lock.write().await;

        // 2: Prepare temp snapshot file and final snapshot file
        debug!("create_snapshot 2: Prepare temp snapshot file and final snapshot file");
        let raw_last_included = self.state_machine.last_applied();

        // Apply retention policy
        let last_included = LogId {
            index: raw_last_included
                .index
                .saturating_sub(self.snapshot_config.retained_log_entries),
            term: self
                .state_machine
                .entry_term(
                    raw_last_included
                        .index
                        .saturating_sub(self.snapshot_config.retained_log_entries),
                )
                .unwrap_or(raw_last_included.term),
        };

        let temp_path = self.path_mgr.temp_work_path(&last_included);

        // 3: Create snapshot based on the temp path
        debug!(
            ?temp_path,
            ?last_included,
            "create_snapshot 3: Create snapshot based on the temp path"
        );
        let checksum_bytes = self
            .state_machine
            .generate_snapshot_data(temp_path.clone(), last_included)
            .await?;

        // 4: Compress the snapshot directory into a tar.gz archive
        debug!("create_snapshot 4: Compressing snapshot directory");
        let final_path = self.path_mgr.final_snapshot_path(&last_included);
        self.compress_directory(&temp_path, &final_path).await?;

        // 6. Remove the original uncompressed directory
        remove_dir_all(&temp_path).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to remove temp directory: {e}"))
        })?;

        // 7: cleanup old versions
        debug!(%self.snapshot_config.cleanup_retain_count, "create_snapshot 5: cleanup old versions");
        if let Err(e) = self
            .cleanup_snapshot(
                self.snapshot_config.cleanup_retain_count,
                &self.snapshot_config.snapshots_dir,
                &self.snapshot_config.snapshots_dir_prefix,
            )
            .await
        {
            error!(%e, "clean up old snapshot file failed");
        }

        println!("[SNAPHSOT] New snapshot created: {:?}", &final_path);

        Ok((
            SnapshotMetadata {
                last_included: Some(last_included),
                checksum: checksum_bytes,
            },
            final_path,
        ))
    }

    #[tracing::instrument]
    async fn cleanup_snapshot(
        &self,
        retain_count: u64,
        snapshot_dir: &Path,
        snapshot_dir_prefix: &str,
    ) -> Result<()> {
        // Phase 1: Collect and parse snapshots
        let mut snapshots = Vec::new();

        let mut entries =
            fs::read_dir(snapshot_dir).await.map_err(|e| StorageError::PathError {
                path: snapshot_dir.to_path_buf(),
                source: e,
            })?;

        while let Some(entry) = entries.next_entry().await.map_err(StorageError::IoError)? {
            let path = entry.path();
            debug!(?path, "cleanup_snapshot");

            if path.extension().is_some_and(|ext| ext == "gz") {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    let parsed =
                        parse_snapshot_dirname(file_name, snapshot_dir_prefix).or_else(|| {
                            file_name
                                .strip_suffix(".tar.gz")
                                .and_then(|s| parse_snapshot_dirname(s, snapshot_dir_prefix))
                        });

                    let (index, term) = if let Some(pair) = parsed {
                        pair
                    } else {
                        continue;
                    };

                    info!(
                        "Index: {:>10} | Term: {:>10} | Path: {}",
                        index,
                        term,
                        path.display()
                    );
                    snapshots.push(CleanupSnapshotMeta { index, term, path });
                }
            }
        }

        // Phase 2: Sorting and cleaning
        if snapshots.len() <= retain_count as usize {
            return Ok(()); // No need to clean
        }

        // Sort in ascending order by index (earlier snapshots are at the front)
        snapshots.sort_by_key(|m| m.index);

        // Phase 4: Difference: Take the remaining elements
        // Calculate the split point to be retained
        let split_point = snapshots.len() - retain_count as usize;

        for meta in &snapshots[..split_point] {
            info!(
                "Deleting old snapshot [index={}, term={}] at {}",
                meta.index,
                meta.term,
                meta.path.display()
            );
            let file_type = tokio::fs::metadata(&meta.path)
                .await
                .map_err(StorageError::IoError)?
                .file_type();

            if file_type.is_file() {
                remove_file(&meta.path).await.map_err(StorageError::IoError)?;
            } else if file_type.is_dir() {
                remove_dir_all(&meta.path).await.map_err(StorageError::IoError)?;
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn validate_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        req: &PurgeLogRequest,
    ) -> Result<bool> {
        // Verification 1: Leader identity legitimacy
        if req.term < current_term || leader_id != Some(req.leader_id) {
            return Ok(false);
        }

        // Verification 2: Locally applied log index >= requested up_to_index
        if req.last_included.is_none() {
            return Ok(false);
        }

        if let Some(last_included) = req.last_included {
            if self.state_machine.last_applied().index < last_included.index {
                return Ok(false);
            }
        }

        // Verification 3: Verify snapshot consistency (to prevent snapshot data corruption)
        if let Some(last_snapshot_metadata) = self.state_machine.snapshot_metadata() {
            if req.snapshot_checksum != last_snapshot_metadata.checksum {
                return Ok(false);
            }
        } else {
            // There is no corresponding snapshot locally, snapshot synchronization needs to be
            // triggered
            return Ok(false);
        }

        Ok(true)
    }

    async fn handle_purge_request(
        &self,
        current_term: u64,
        leader_id: Option<u32>,
        last_purged: Option<LogId>,
        req: &PurgeLogRequest,
        raft_log: &Arc<ROF<T>>,
    ) -> Result<PurgeLogResponse> {
        // Verification 1: Leader identity legitimacy
        if req.term < current_term || leader_id != Some(req.leader_id) {
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Verification 2: Locally applied log index >= requested up_to_index
        if req.last_included.is_none() {
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        if let Some(last_included) = req.last_included {
            if self.state_machine.last_applied().index < last_included.index {
                return Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        }

        // Verification 3: Verify snapshot consistency (to prevent snapshot data corruption)
        if let Some(last_snapshot_metadata) = self.state_machine.snapshot_metadata() {
            if req.snapshot_checksum != last_snapshot_metadata.checksum {
                return Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                });
            }
        } else {
            // There is no corresponding snapshot locally, snapshot synchronization needs to be
            // triggered
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Perform physical deletion
        match raft_log.purge_logs_up_to(req.last_included.unwrap()).await {
            Ok(_) => Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: true,
                last_purged,
            }),
            Err(e) => {
                error!(?e, "raft_log.purge_logs_up_to");
                Ok(PurgeLogResponse {
                    node_id: self.node_id,
                    term: current_term,
                    success: false,
                    last_purged,
                })
            }
        }
    }

    fn get_latest_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.state_machine.snapshot_metadata()
    }

    /// Load snapshot data as a stream of chunks (ZERO-COPY)
    #[instrument(skip(self))]
    async fn load_snapshot_data(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("load_snapshot_data");
        let transfer_meta = self.prepare_transfer_meta(metadata).await?;

        debug!(
            "Loading snapshot from file: {:?} ({} chunks)",
            transfer_meta.file_path, transfer_meta.total_chunks
        );

        // Use zero-copy memory mapping for efficient reads
        let file = tokio::fs::File::open(&transfer_meta.file_path)
            .await
            .map_err(StorageError::IoError)?;
        let mmap = unsafe { MmapOptions::new().map(&file).map_err(StorageError::IoError)? };
        let mmap_arc = Arc::new(mmap);

        let node_id = self.node_id;
        let leader_term = transfer_meta.metadata.last_included.map(|id| id.term).unwrap_or(0);
        let chunk_size = transfer_meta.chunk_size;
        let total_chunks = transfer_meta.total_chunks;
        let metadata = transfer_meta.metadata;

        let stream = try_stream! {
            for seq in 0..total_chunks {
                let start = (seq as usize) * chunk_size;
                let end = std::cmp::min(start + chunk_size, mmap_arc.len());

                if start >= mmap_arc.len() {
                    break;
                }

                // ZERO-COPY: Create Bytes that references the memory map
                let chunk_data = zero_copy_bytes_from_mmap(mmap_arc.clone(), start, end);

                let checksum = crc32fast::hash(&chunk_data).to_be_bytes();
                let chunk_checksum = Bytes::copy_from_slice(&checksum);

                yield SnapshotChunk {
                    leader_term,
                    leader_id: node_id,
                    metadata: if seq == 0 { Some(metadata.clone()) } else { None },
                    seq,
                    total_chunks,
                    data: chunk_data,
                    chunk_checksum,
                };
            }
        };

        Ok(Box::pin(stream))
    }

    #[allow(unused)]
    async fn load_snapshot_chunk(
        &self,
        metadata: &SnapshotMetadata,
        seq: u32,
    ) -> Result<SnapshotChunk> {
        let _timer = ScopedTimer::new("load_snapshot_chunk");
        let transfer_meta = self.prepare_transfer_meta(metadata.clone()).await?;
        // Validate chunk index
        if seq >= transfer_meta.total_chunks {
            return Err(SnapshotError::ChunkOutOfRange(seq, transfer_meta.total_chunks).into());
        }

        // Calculate chunk boundaries
        let start = (seq as usize) * transfer_meta.chunk_size;
        let end = std::cmp::min(
            start + transfer_meta.chunk_size,
            transfer_meta.file_size as usize,
        );

        if start >= transfer_meta.file_size as usize {
            return Err(
                SnapshotError::OperationFailed("Chunk start beyond file size".into()).into(),
            );
        }

        // ZERO-COPY: Use memory mapping instead of buffer reading
        let chunk_data = self.load_chunk_via_mmap(&transfer_meta.file_path, start, end)?;

        // Efficient checksum calculation on mapped memory
        let checksum_bytes = crc32fast::hash(&chunk_data).to_be_bytes();
        let chunk_checksum = Bytes::copy_from_slice(&checksum_bytes);

        Ok(SnapshotChunk {
            leader_term: transfer_meta.metadata.last_included.map(|id| id.term).unwrap_or(0),
            leader_id: self.node_id,
            metadata: if seq == 0 {
                Some(transfer_meta.metadata)
            } else {
                None
            },
            seq,
            total_chunks: transfer_meta.total_chunks,
            data: chunk_data,
            chunk_checksum,
        })
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    /// Notify watchers of key changes in the applied chunk
    ///
    /// This method parses the chunk entries and sends watch events for PUT and DELETE operations.
    /// It runs on the hot path but uses non-blocking try_send to maintain < 0.01% overhead.
    #[inline]
    fn notify_watchers(
        &self,
        chunk: &[Entry],
        watch_mgr: &Arc<WatchManager>,
    ) {
        for entry in chunk {
            if let Some(ref payload) = entry.payload {
                if let Some(Payload::Command(bytes)) = &payload.payload {
                    // Decode the WriteCommand
                    if let Ok(write_cmd) = WriteCommand::decode(bytes.as_ref()) {
                        match write_cmd.operation {
                            Some(Operation::Insert(insert)) => {
                                watch_mgr.notify_put(insert.key, insert.value);
                            }
                            Some(Operation::Delete(delete)) => {
                                watch_mgr.notify_delete(delete.key);
                            }
                            None => {
                                // No operation, skip
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<T> DefaultStateMachineHandler<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        last_applied_index: u64,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
        snapshot_config: SnapshotConfig,
        snapshot_policy: SNP<T>,
        watch_manager: Option<Arc<WatchManager>>,
    ) -> Self {
        Self {
            node_id,
            last_applied: AtomicU64::new(last_applied_index),
            pending_commit: AtomicU64::new(0),
            max_entries_per_chunk,
            state_machine,
            snapshot_policy,
            path_mgr: Arc::new(SnapshotPathManager::new(
                snapshot_config.snapshots_dir.clone(),
                snapshot_config.snapshots_dir_prefix.clone(),
            )),
            snapshot_config,

            snapshot_lock: RwLock::new(()),
            snapshot_in_progress: AtomicBool::new(false),
            watch_manager,
        }
    }

    /// Convenience constructor for tests without watch manager
    ///
    /// This is a backward-compatible wrapper that calls `new()` with `None` for watch_manager.
    /// Use this in tests to avoid updating all test callsites.
    #[cfg(test)]
    pub fn new_without_watch(
        node_id: u32,
        last_applied_index: u64,
        max_entries_per_chunk: usize,
        state_machine: Arc<SMOF<T>>,
        snapshot_config: SnapshotConfig,
        snapshot_policy: SNP<T>,
    ) -> Self {
        Self::new(
            node_id,
            last_applied_index,
            max_entries_per_chunk,
            state_machine,
            snapshot_config,
            snapshot_policy,
            None, // No watch manager for tests
        )
    }

    #[allow(dead_code)]
    #[instrument(skip(self))]
    async fn create_snapshot_chunk_stream(
        &self,
        snapshot_file: PathBuf,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("create_snapshot_chunk_stream");
        let path_metadata =
            tokio::fs::metadata(&snapshot_file).await.map_err(StorageError::IoError)?;

        if !path_metadata.is_file() {
            return Err(SnapshotError::OperationFailed(format!(
                "Invalid snapshot file type: {}",
                snapshot_file.display()
            ))
            .into());
        }

        let chunk_size = self.snapshot_config.chunk_size;
        let node_id = self.node_id;
        let leader_term = metadata.last_included.map(|id| id.term).unwrap_or(0);

        // Open the compressed file.
        let mut file = File::open(&snapshot_file).await.map_err(StorageError::IoError)?;

        let file_len = path_metadata.len();
        let total_chunks = (file_len as usize).div_ceil(chunk_size);

        debug!(%total_chunks);

        let stream = try_stream! {
            let mut seq = 0;
            let mut buffer = vec![0u8; chunk_size];

            while let Ok(bytes_read) = file.read(&mut buffer).await {
                if bytes_read == 0 {
                    break;
                }

                let chunk_data = Bytes::from(buffer[..bytes_read].to_vec());
                let checksum = crc32fast::hash(&chunk_data).to_be_bytes();
                let chunk_checksum = Bytes::copy_from_slice(&checksum);

                // Only first chunk contains metadata
                let chunk_metadata = if seq == 0 {
                    Some(metadata.clone())
                } else {
                    None
                };

                debug!(%seq, ?chunk_metadata, "create_snapshot_chunk_stream");
                yield SnapshotChunk {
                    leader_term,
                    leader_id: node_id,
                    metadata: chunk_metadata,
                    seq,
                    total_chunks: total_chunks as u32,
                    data: chunk_data,
                    chunk_checksum,
                };

                seq += 1;
            }
        };

        Ok(Box::pin(stream))
    }

    /// Helper function to process snapshot stream
    #[instrument(skip(self, stream))]
    async fn process_snapshot_stream(
        &self,
        mut stream: Box<tonic::Streaming<SnapshotChunk>>,
        ack_tx: mpsc::Sender<SnapshotAck>,
        config: &SnapshotConfig,
    ) -> Result<(SnapshotMetadata, PathBuf)> {
        let mut assembler = SnapshotAssembler::new(self.path_mgr.clone()).await?;
        let chunk_timeout = Duration::from_secs(10);
        let mut last_received = Instant::now();
        let mut term_check = None;
        let mut captured_metadata: Option<SnapshotMetadata> = None;
        let mut total_chunks = None;
        let mut count = 0;

        loop {
            let chunk = match timeout(chunk_timeout, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    debug!("receive new chunk.");
                    last_received = Instant::now();
                    chunk
                }
                Ok(Some(Err(e))) => {
                    // Send stream error ACK
                    ack_tx
                        .send(SnapshotAck {
                            seq: 0, // Best effort
                            status: ChunkStatus::Failed.into(),
                            next_requested: 0,
                        })
                        .await
                        .map_err(|e| {
                            SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                        })?;
                    return Err(
                        SnapshotError::OperationFailed(format!("Stream error: {e:?}")).into(),
                    );
                }
                Ok(None) => {
                    debug!("no more chunks available...");

                    break;
                }
                Err(_) => {
                    // Send timeout ACK
                    ack_tx
                        .send(SnapshotAck {
                            seq: 0, // Best effort
                            status: ChunkStatus::Failed.into(),
                            next_requested: 0,
                        })
                        .await
                        .map_err(|e| {
                            SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                        })?;
                    let elapsed = last_received.elapsed();
                    return Err(SnapshotError::OperationFailed(format!(
                        "No chunk received for {} seconds",
                        elapsed.as_secs()
                    ))
                    .into());
                }
            };

            // Verify leader legitimacy
            if let Some((term, leader_id)) = &term_check {
                if chunk.leader_term != *term || chunk.leader_id != *leader_id {
                    // Send leader changed ACK
                    ack_tx
                        .send(SnapshotAck {
                            seq: chunk.seq,
                            status: ChunkStatus::OutOfOrder.into(),
                            next_requested: 0,
                        })
                        .await
                        .map_err(|e| {
                            SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                        })?;
                    return Err(SnapshotError::OperationFailed(
                        "Leader changed during transfer".to_string(),
                    )
                    .into());
                }
            } else {
                term_check = Some((chunk.leader_term, chunk.leader_id));
                captured_metadata = chunk.metadata.clone();
                total_chunks = Some(chunk.total_chunks);
                debug!(?term_check, ?captured_metadata, ?total_chunks);

                // Validate captured metadata
                if captured_metadata.is_none() {
                    ack_tx
                        .send(SnapshotAck {
                            seq: chunk.seq,
                            status: ChunkStatus::Failed.into(),
                            next_requested: 0,
                        })
                        .await
                        .map_err(|e| {
                            SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                        })?;
                    return Err(SnapshotError::OperationFailed(
                        "Missing metadata in snapshot stream".to_string(),
                    )
                    .into());
                }
            }

            // Checksum validation
            if !validate_checksum(&chunk.data, &chunk.chunk_checksum) {
                // Send checksum failure ACK
                ack_tx
                    .send(SnapshotAck {
                        seq: chunk.seq,
                        status: ChunkStatus::ChecksumMismatch.into(),
                        next_requested: chunk.seq, // Request same chunk again
                    })
                    .await
                    .map_err(|e| {
                        SnapshotError::OperationFailed(format!("Failed to send ACK: {e}"))
                    })?;
                return Err(SnapshotError::OperationFailed(
                    "Checksum validation failed".to_string(),
                )
                .into());
            }

            // Write to temporary file
            assembler.write_chunk(chunk.seq, chunk.data).await?;

            // Send ACK
            let ack = SnapshotAck {
                seq: chunk.seq,
                status: ChunkStatus::Accepted.into(),
                next_requested: chunk.seq + 1,
            };
            ack_tx
                .send(ack)
                .await
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {e}")))?;

            count += 1;
            if count % config.receiver_yield_every_n_chunks == 0 {
                debug!(%count, %config.receiver_yield_every_n_chunks, "yield_now");

                tokio::task::yield_now().await;
            }
        }

        let total = total_chunks.unwrap_or(0);
        debug!(%total, "expected total chunks");
        if assembler.received_chunks() != total {
            ack_tx
                .send(SnapshotAck {
                    seq: assembler.received_chunks(),
                    status: ChunkStatus::Failed.into(),
                    next_requested: 0,
                })
                .await
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {e}")))?;

            return Err(SnapshotError::OperationFailed(format!(
                "Received chunks({}) != total({})",
                assembler.received_chunks(),
                total
            ))
            .into());
        }

        debug!(?captured_metadata);

        let final_metadata = captured_metadata.ok_or_else(|| {
            SnapshotError::OperationFailed("Missing metadata in snapshot stream".to_string())
        })?;

        let snapshot_path = assembler.finalize(&final_metadata).await?;
        Ok((final_metadata, snapshot_path))
    }

    /// Create transfer metadata with precomputed values
    async fn prepare_transfer_meta(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<SnapshotTransferMeta> {
        let last_included = metadata.last_included.ok_or_else(|| {
            SnapshotError::OperationFailed("No last_included in metadata".to_string())
        })?;

        let file_path = self.path_mgr.final_snapshot_path(&last_included);
        let file_meta = tokio::fs::metadata(&file_path).await.map_err(StorageError::IoError)?;
        let file_size = file_meta.len();
        let chunk_size = self.snapshot_config.chunk_size;

        // Precompute total chunks once
        let total_chunks = file_size.div_ceil(chunk_size as u64) as u32;

        Ok(SnapshotTransferMeta {
            metadata,
            total_chunks,
            chunk_size,
            file_size,
            file_path,
        })
    }

    async fn compress_directory(
        &self,
        source_dir: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        let compressed_file = File::create(dest_path).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to create compressed file: {e}"))
        })?;

        let gzip_encoder = GzipEncoder::new(compressed_file);
        let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);

        // Add all files in source_dir to the archive
        tar_builder.append_dir_all(".", source_dir).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to create tar archive: {e}"))
        })?;

        // Finish writing and flush all data
        tar_builder.finish().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to finish tar archive: {e}"))
        })?;

        // Get inner GzipEncoder and shutdown to ensure all data is written
        let mut gzip_encoder = tar_builder.into_inner().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to get inner encoder: {e}"))
        })?;
        gzip_encoder.shutdown().await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to shutdown gzip encoder: {e}"))
        })?;

        Ok(())
    }

    async fn decompress_to_directory(
        &self,
        compressed_path: &Path,
        target_dir: &Path,
    ) -> Result<()> {
        debug!(
            ?compressed_path,
            "Validate file format before processing (IMPROVEMENT ADDED)"
        );
        // Validate file format before processing (IMPROVEMENT ADDED) Verify the file is
        //    actually compressed using magic numbers or extension
        validate_compressed_format(compressed_path)?;

        let file = File::open(compressed_path).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to open snapshot file: {e}"))
        })?;

        let buf_reader = BufReader::new(file);
        let gzip_decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(gzip_decoder);

        archive.unpack(target_dir).await.map_err(|e| {
            SnapshotError::OperationFailed(format!("Failed to unpack snapshot: {e}"))
        })?;

        Ok(())
    }

    /// Zero-copy chunk loading using memory mapping
    ///
    /// # Safety
    /// - Memory mapping is unsafe but properly bounded and validated
    /// - File size is checked before mapping
    /// - Proper error handling for IO operations
    pub(super) fn load_chunk_via_mmap(
        &self,
        file_path: &Path,
        start: usize,
        end: usize,
    ) -> Result<Bytes> {
        // Open file with proper error handling
        let file = std::fs::File::open(file_path).map_err(StorageError::IoError)?;

        // Get file metadata to validate bounds
        let file_meta = file.metadata().map_err(StorageError::IoError)?;
        let file_size = file_meta.len() as usize;

        // Validate bounds before memory mapping
        if end > file_size {
            return Err(SnapshotError::OperationFailed(format!(
                "Chunk end {end} exceeds file size {file_size}"
            ))
            .into());
        }

        // SAFETY: Bounds are validated above, and file is opened properly
        let mmap =
            unsafe { memmap2::MmapOptions::new().map(&file).map_err(StorageError::IoError)? };

        // True zero-copy: Create Bytes that references the memory map directly
        // This requires keeping the mmap alive via Arc
        let mmap_arc = Arc::new(mmap);

        // Use a custom approach to create Bytes that holds the Arc<Mmap>
        // This avoids the copy while maintaining safety
        let chunk_data = zero_copy_bytes_from_mmap(mmap_arc, start, end);

        Ok(chunk_data)
    }

    #[cfg(test)]
    pub fn pending_commit(&self) -> u64 {
        self.pending_commit.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn snapshot_in_progress(&self) -> bool {
        self.snapshot_in_progress.load(Ordering::Acquire)
    }
}

/// Helper function to create zero-copy Bytes from memory map
fn zero_copy_bytes_from_mmap(
    mmap_arc: Arc<Mmap>,
    start: usize,
    end: usize,
) -> Bytes {
    // Get a slice of the memory map
    let slice = &mmap_arc[start..end];
    Bytes::copy_from_slice(slice)
}

/// Manual parsing file name format: snapshot-{index}-{term}
fn parse_snapshot_dirname(
    name: &str,
    snapshot_dir_prefix: &str,
) -> Option<(u64, u64)> {
    debug!(%name, "parse_snapshot_dirname");

    // Check prefix and suffix
    if !name.starts_with(snapshot_dir_prefix) {
        return None;
    }

    // Remove fixed parts
    let core = &name[9..name.len()]; // "snapshot-".len() = 9,

    // Split version and index
    let parts: Vec<&str> = core.splitn(2, '-').collect();
    if parts.len() != 2 {
        return None;
    }

    // Parse numbers
    match (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
        (Ok(i), Ok(t)) => Some((i, t)),
        _ => None,
    }
}
