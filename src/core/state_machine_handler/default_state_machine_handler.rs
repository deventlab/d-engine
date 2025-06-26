use super::SnapshotAssembler;
use super::SnapshotContext;
use super::SnapshotPolicy;
use super::StateMachineHandler;
use crate::alias::ROF;
use crate::alias::SMOF;
use crate::alias::SNP;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::validate_checksum;
use crate::proto::client::ClientResult;
use crate::proto::common::LogId;
use crate::proto::storage::snapshot_ack::ChunkStatus;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::scoped_timer::ScopedTimer;
use crate::utils::cluster::error;
use crate::NetworkError;
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
use crate::API_SLO;
use async_compression::tokio::write::GzipEncoder;
use async_stream::try_stream;
use autometrics::autometrics;
use futures::stream::BoxStream;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::fs::remove_dir_all;
use tokio::fs::remove_file;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tonic::async_trait;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;

/// Unified snapshot metadata with precomputed values
#[derive(Debug, Clone)]
pub struct SnapshotTransferMeta {
    pub metadata: SnapshotMetadata,
    pub total_chunks: u32,
    pub chunk_size: usize,
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
    max_entries_per_chunk: usize,
    state_machine: Arc<SMOF<T>>,

    // current_snapshot_version: AtomicU64,
    snapshot_config: SnapshotConfig,
    snapshot_policy: SNP<T>,
    snapshot_in_progress: AtomicBool,

    path_mgr: Arc<SnapshotPathManager>,

    /// Temporary lock when snapshot is generated (to prevent concurrent snapshot generation)
    snapshot_lock: RwLock<()>,
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
    /// Update pending commit index
    fn update_pending(
        &self,
        new_commit: u64,
    ) {
        let mut current = self.pending_commit.load(Ordering::Acquire);
        while new_commit > current {
            match self
                .pending_commit
                .compare_exchange_weak(current, new_commit, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(e) => current = e,
            }
        }
    }

    /// Batch application log
    async fn apply_batch(
        &self,
        raft_log: Arc<ROF<T>>,
    ) -> Result<()> {
        if let Some(range) = self.pending_range() {
            // Read logs in batches
            let range_end = *range.end();
            let entries = raft_log.get_entries_between(range);

            // Apply in parallel
            let chunks = entries.into_iter().collect::<Vec<_>>();
            let handles: Vec<_> = chunks
                .chunks(self.max_entries_per_chunk)
                .map(|chunk| {
                    let sm = self.state_machine.clone();
                    let chunk = chunk.to_vec(); // Transfer ownership of chunk to the closure
                    tokio::spawn(async move { sm.apply_chunk(chunk) })
                })
                .collect();

            // Wait for all batches to complete
            for h in handles {
                if let Err(e) = h.await {
                    error("apply_batch", &e);
                }
            }

            // Atomic update last_applied
            self.last_applied.store(range_end, Ordering::Release);
        }
        Ok(())
    }

    /// TODO: decouple client related commands with RAFT internal logic
    #[autometrics(objective = API_SLO)]
    fn read_from_state_machine(
        &self,
        keys: Vec<Vec<u8>>,
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
        let (final_metadata, snapshot_path) = self.process_snapshot_stream(stream, ack_tx.clone(), config).await?;

        debug!(?final_metadata, ?snapshot_path, "before apply_snapshot_from_file");

        self.state_machine
            .apply_snapshot_from_file(&final_metadata, snapshot_path)
            .await?;

        // Send final ACK
        let final_ack = SnapshotAck {
            seq: u32::MAX, // Special value for completion
            status: ChunkStatus::Accepted.into(),
            next_requested: 0,
        };
        debug!(?final_ack, "going to send final ack");

        ack_tx.send(final_ack).await.map_err(|e| {
            let error_str = format!("{e:?}");
            error!("Failed to send final ACK: {}", error_str);
            NetworkError::SingalSendFailed(error_str)
        })?;

        info!("Snapshot stream successfully received and applied");
        Ok(())
    }

    #[inline]
    fn should_snapshot(
        &self,
        new_commit_data: NewCommitData,
    ) -> bool {
        if self.snapshot_in_progress.load(Ordering::Relaxed) {
            return false;
        }

        let last_applied = self.state_machine.last_applied();
        let last_snapshot_metadata = self.state_machine.snapshot_metadata();

        let last_included = last_snapshot_metadata
            .and_then(|meta| meta.last_included)
            .unwrap_or_else(|| LogId { index: 0, term: 0 });

        self.snapshot_policy.should_trigger(&SnapshotContext {
            role: new_commit_data.role,
            last_included,
            last_applied,
            current_term: new_commit_data.current_term,
        })
    }

    async fn create_snapshot(&self) -> Result<(SnapshotMetadata, PathBuf)> {
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
        debug!(?temp_path, "create_snapshot 3: Create snapshot based on the temp path");
        let checksum = self
            .state_machine
            .generate_snapshot_data(temp_path.clone(), last_included)
            .await?;

        // 4: Compress the snapshot directory into a tar.gz archive
        debug!("create_snapshot 4: Compressing snapshot directory");
        let final_path = self.path_mgr.final_snapshot_path(&last_included);
        // let compressed_path = final_path.with_extension("tar.gz");
        let compressed_file = File::create(&final_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to create compressed file: {}", e)))?;
        let gzip_encoder = GzipEncoder::new(compressed_file);
        let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);

        // Add all files in temp_path to the archive
        tar_builder
            .append_dir_all(".", &temp_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to create tar archive: {}", e)))?;

        // Finish writing and flush all data
        tar_builder
            .finish()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to finish tar archive: {}", e)))?;

        // Get inner GzipEncoder and shutdown to ensure all data is written
        let mut gzip_encoder = tar_builder
            .into_inner()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to get inner encoder: {}", e)))?;
        gzip_encoder
            .shutdown()
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to shutdown gzip encoder: {}", e)))?;

        // 6. Remove the original uncompressed directory
        remove_dir_all(&temp_path)
            .await
            .map_err(|e| SnapshotError::OperationFailed(format!("Failed to remove temp directory: {}", e)))?;

        // 7: cleanup old versions
        debug!(%self.snapshot_config.cleanup_retain_count, "create_snapshot 5: cleanup old versions");
        if let Err(e) = self
            .cleanup_snapshot(
                self.snapshot_config.cleanup_retain_count,
                &self.snapshot_config.snapshots_dir,
            )
            .await
        {
            error!(%e, "clean up old snapshot file failed");
        }

        Ok((
            SnapshotMetadata {
                last_included: Some(last_included),
                checksum: checksum.to_vec(),
            },
            final_path,
        ))
    }

    #[tracing::instrument]
    async fn cleanup_snapshot(
        &self,
        retain_count: u64,
        snapshot_dir: &Path,
    ) -> Result<()> {
        // Phase 1: Collect and parse snapshots
        let mut snapshots = Vec::new();

        let mut entries = fs::read_dir(snapshot_dir).await.map_err(|e| StorageError::PathError {
            path: snapshot_dir.to_path_buf(),
            source: e,
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(StorageError::IoError)? {
            let path = entry.path();
            debug!(?path, "cleanup_snapshot");

            if path.extension().map_or(false, |ext| ext == "gz") {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    let parsed = parse_snapshot_dirname(file_name).or_else(|| {
                        file_name
                            .strip_suffix(".tar.gz")
                            .and_then(|stripped| parse_snapshot_dirname(stripped))
                    });

                    let (index, term) = if let Some(pair) = parsed {
                        pair
                    } else {
                        continue;
                    };

                    info!("Index: {:>10} | Term: {:>10} | Path: {}", index, term, path.display());
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
            // There is no corresponding snapshot locally, snapshot synchronization needs to be triggered
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
            // There is no corresponding snapshot locally, snapshot synchronization needs to be triggered
            return Ok(PurgeLogResponse {
                node_id: self.node_id,
                term: current_term,
                success: false,
                last_purged,
            });
        }

        // Perform physical deletion
        match raft_log.purge_logs_up_to(req.last_included.unwrap()) {
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

    /// Load snapshot data as a stream of chunks
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
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).map_err(StorageError::IoError)? };
        let mmap = Arc::new(mmap);

        let node_id = self.node_id;
        let leader_term = transfer_meta.metadata.last_included.map(|id| id.term).unwrap_or(0);
        let chunk_size = transfer_meta.chunk_size;
        let total_chunks = transfer_meta.total_chunks;
        let metadata = transfer_meta.metadata;

        let stream = try_stream! {
            for seq in 0..total_chunks {
                let start = (seq as usize) * chunk_size;
                let end = std::cmp::min(start + chunk_size, mmap.len());

                if start >= mmap.len() {
                    break;
                }

                // Zero-copy slice from memory map
                let chunk_data = &mmap[start..end];
                let chunk_checksum = crc32fast::hash(chunk_data).to_be_bytes().to_vec();

                yield SnapshotChunk {
                    leader_term,
                    leader_id: node_id,
                    metadata: if seq == 0 { Some(metadata.clone()) } else { None },
                    seq,
                    total_chunks,
                    data: chunk_data.to_vec(),
                    chunk_checksum,
                };
            }
        };

        Ok(Box::pin(stream))
    }

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
        let start = (seq as u64) * transfer_meta.chunk_size as u64;
        let end = std::cmp::min(start + transfer_meta.chunk_size as u64, transfer_meta.file_size);
        let chunk_size = (end - start) as usize;

        // Read directly without mapping the whole file
        let mut file = tokio::fs::File::open(&transfer_meta.file_path)
            .await
            .map_err(StorageError::IoError)?;
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(StorageError::IoError)?;

        let mut buffer = Vec::with_capacity(chunk_size);
        buffer.resize(chunk_size, 0);
        file.read_exact(&mut buffer).await.map_err(StorageError::IoError)?;

        let chunk_checksum = crc32fast::hash(&buffer).to_be_bytes().to_vec();

        Ok(SnapshotChunk {
            leader_term: transfer_meta.metadata.last_included.map(|id| id.term).unwrap_or(0),
            leader_id: self.node_id,
            metadata: if seq == 0 { Some(transfer_meta.metadata) } else { None },
            seq,
            total_chunks: transfer_meta.total_chunks,
            data: buffer,
            chunk_checksum,
        })
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
                SNAPSHOT_DIR_PREFIX.to_string(),
            )),
            snapshot_config,

            snapshot_lock: RwLock::new(()),
            snapshot_in_progress: AtomicBool::new(false),
        }
    }

    /// Get the interval to be processed
    pub fn pending_range(&self) -> Option<RangeInclusive<u64>> {
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let pending_commit = self.pending_commit.load(Ordering::Acquire);

        if pending_commit > last_applied {
            Some((last_applied + 1)..=pending_commit)
        } else {
            None
        }
    }

    #[instrument(skip(self))]
    async fn create_snapshot_chunk_stream(
        &self,
        snapshot_file: PathBuf,
        metadata: SnapshotMetadata,
    ) -> Result<BoxStream<'static, Result<SnapshotChunk>>> {
        let _timer = ScopedTimer::new("create_snapshot_chunk_stream");
        let path_metadata = tokio::fs::metadata(&snapshot_file)
            .await
            .map_err(StorageError::IoError)?;

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
        let total_chunks = ((file_len as usize) + chunk_size - 1) / chunk_size;

        debug!(%total_chunks);

        let stream = try_stream! {
            let mut seq = 0;
            let mut buffer = vec![0u8; chunk_size];

            while let Ok(bytes_read) = file.read(&mut buffer).await {
                if bytes_read == 0 {
                    break;
                }

                let chunk_data = buffer[..bytes_read].to_vec();
                let chunk_checksum = crc32fast::hash(&chunk_data).to_be_bytes().to_vec();

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
                        .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;
                    return Err(SnapshotError::OperationFailed(format!("Stream error: {:?}", e)).into());
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
                        .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;
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
                        .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;
                    return Err(SnapshotError::OperationFailed("Leader changed during transfer".to_string()).into());
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
                        .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;
                    return Err(
                        SnapshotError::OperationFailed("Missing metadata in snapshot stream".to_string()).into(),
                    );
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
                    .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;
                return Err(SnapshotError::OperationFailed("Checksum validation failed".to_string()).into());
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
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;

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
                .map_err(|e| SnapshotError::OperationFailed(format!("Failed to send ACK: {}", e)))?;

            return Err(SnapshotError::OperationFailed(format!(
                "Received chunks({}) != total({})",
                assembler.received_chunks(),
                total
            ))
            .into());
        }

        debug!(?captured_metadata);

        let final_metadata = captured_metadata
            .ok_or_else(|| SnapshotError::OperationFailed("Missing metadata in snapshot stream".to_string()))?;

        let snapshot_path = assembler.finalize(&final_metadata).await?;
        Ok((final_metadata, snapshot_path))
    }

    /// Create transfer metadata with precomputed values
    async fn prepare_transfer_meta(
        &self,
        metadata: SnapshotMetadata,
    ) -> Result<SnapshotTransferMeta> {
        let last_included = metadata
            .last_included
            .ok_or_else(|| SnapshotError::OperationFailed("No last_included in metadata".to_string()))?;

        let file_path = self.path_mgr.final_snapshot_path(&last_included);
        let file_meta = tokio::fs::metadata(&file_path).await.map_err(StorageError::IoError)?;
        let file_size = file_meta.len();
        let chunk_size = self.snapshot_config.chunk_size;

        // Precompute total chunks once
        let total_chunks = ((file_size + chunk_size as u64 - 1) / chunk_size as u64) as u32;

        Ok(SnapshotTransferMeta {
            metadata,
            total_chunks,
            chunk_size,
            file_size,
            file_path,
        })
    }

    #[cfg(test)]
    pub fn pending_commit(&self) -> u64 {
        self.pending_commit.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }
}

/// Manual parsing file name format: snapshot-{index}-{term}
fn parse_snapshot_dirname(name: &str) -> Option<(u64, u64)> {
    debug!(%name, "parse_snapshot_dirname");

    // Check prefix and suffix
    if !name.starts_with(SNAPSHOT_DIR_PREFIX) {
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
