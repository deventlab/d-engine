use crate::proto::storage::{snapshot_ack, SnapshotAck};
use crate::{proto::storage::snapshot_service_client::SnapshotServiceClient, Result, SnapshotError};
use crate::{NetworkError, TypeConfig};
use futures::StreamExt;
use futures::{stream::BoxStream, Stream};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{debug, trace, warn};

use crate::{
    proto::storage::{SnapshotChunk, SnapshotMetadata},
    SnapshotConfig,
};

// Chunk cache for efficient retransmissions
pub(super) struct ChunkCache {
    chunks: HashMap<u32, SnapshotChunk>,
    capacity: usize,
}

impl ChunkCache {
    pub(super) fn new(capacity: usize) -> Self {
        ChunkCache {
            chunks: HashMap::with_capacity(capacity),
            capacity,
        }
    }

    pub(super) fn insert(
        &mut self,
        seq: u32,
        chunk: SnapshotChunk,
    ) {
        if self.chunks.len() >= self.capacity {
            // Simple FIFO eviction
            if let Some(min_key) = self.chunks.keys().min().copied() {
                self.chunks.remove(&min_key);
            }
        }
        self.chunks.insert(seq, chunk);
    }

    pub(super) fn get(
        &self,
        seq: u32,
    ) -> Option<&SnapshotChunk> {
        self.chunks.get(&seq)
    }
}

// Retry manager with exponential backoff and jitter
pub(super) struct RetryManager {
    max_retries: u32,
    retry_states: HashMap<u32, ChunkRetryState>,
}

pub(super) struct ChunkRetryState {
    retry_count: u32,
    last_retry: Instant,
}

impl RetryManager {
    pub(super) fn new(max_retries: u32) -> Self {
        RetryManager {
            max_retries,
            retry_states: HashMap::new(),
        }
    }

    pub(super) fn record_failure(
        &mut self,
        seq: u32,
    ) -> Result<()> {
        let state = self.retry_states.entry(seq).or_insert(ChunkRetryState {
            retry_count: 0,
            last_retry: Instant::now(),
        });

        state.retry_count += 1;
        state.last_retry = Instant::now();

        if state.retry_count > self.max_retries {
            Err(SnapshotError::TransferFailed.into())
        } else {
            Ok(())
        }
    }

    pub(super) fn record_success(
        &mut self,
        seq: u32,
    ) {
        self.retry_states.remove(&seq);
    }
}

pub(crate) struct BackgroundSnapshotTransfer<T> {
    _marker: PhantomData<T>,
}

#[allow(unused)]
impl<T> BackgroundSnapshotTransfer<T>
where
    T: TypeConfig,
{
    // Unified push transfer entry point
    pub(crate) async fn run_push_transfer(
        node_id: u32,
        data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        channel: tonic::transport::Channel,
        config: SnapshotConfig,
    ) -> Result<()> {
        debug!(%node_id, "Starting push snapshot transfer");
        let config = Arc::new(config);

        // Preload the first chunk and verify
        let mut data_stream = data_stream;
        let first_chunk = match data_stream.next().await {
            Some(Ok(chunk)) if chunk.seq == 0 && chunk.metadata.is_some() => chunk,
            Some(Ok(_)) => return Err(SnapshotError::InvalidFirstChunk.into()),
            Some(Err(e)) => return Err(e),
            None => return Err(SnapshotError::EmptySnapshot.into()),
        };

        let client = SnapshotServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        Self::push_transfer_loop(client, first_chunk, data_stream, config).await?;

        debug!(%node_id, "Push snapshot transfer completed");
        Ok(())
    }

    // Dedicated push logic
    async fn push_transfer_loop(
        mut client: SnapshotServiceClient<Channel>,
        first_chunk: SnapshotChunk,
        mut data_stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        config: Arc<SnapshotConfig>,
    ) -> Result<()> {
        // 1. Create a transmission channel
        let (mut request_tx, request_rx) = mpsc::channel(config.push_queue_size);

        // 2. Send the first data block
        request_tx
            .send(first_chunk)
            .await
            .map_err(|e| NetworkError::SingalSendFailed(format!("{:?}", e)))?;

        // 3. Start the background task to send the remaining blocks
        tokio::spawn(async move {
            while let Some(chunk) = data_stream.next().await {
                if let Ok(chunk) = chunk {
                    if let Err(e) = Self::send_chunk_with_retry(&mut request_tx, chunk, &config).await {
                        break;
                    }
                }
            }
        });

        // 4. Create a gRPC request stream
        let request_stream = ReceiverStream::new(request_rx);

        // 5. Initiate a gRPC call (key point!)
        let response = client
            .install_snapshot(request_stream)
            .await
            .map_err(|e| NetworkError::TonicStatusError(Box::new(e)))?;

        // 6. Check the response
        if response.into_inner().success {
            Ok(())
        } else {
            Err(SnapshotError::RemoteRejection.into())
        }
    }

    // Unified pull transfer entry point
    pub(crate) async fn run_pull_transfer(
        ack_stream: Box<tonic::Streaming<SnapshotAck>>,
        chunk_tx: mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
        mut data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        config: SnapshotConfig,
    ) -> Result<()> {
        debug!("Starting pull snapshot transfer");

        // Preload the first chunk and verify
        let first_chunk = match data_stream.next().await {
            Some(Ok(chunk)) if chunk.seq == 0 && chunk.metadata.is_some() => {
                if chunk.total_chunks == 0 {
                    return Err(SnapshotError::InvalidFirstChunk.into());
                }
                chunk
            }
            Some(Ok(_)) => return Err(SnapshotError::InvalidFirstChunk.into()),
            Some(Err(e)) => return Err(e),
            None => return Err(SnapshotError::EmptySnapshot.into()),
        };

        let total_chunks = first_chunk.total_chunks;
        trace!(?total_chunks);
        let config = Arc::new(config);

        Self::pull_transfer_loop(ack_stream, chunk_tx, first_chunk, data_stream, total_chunks, config).await?;

        debug!("Pull snapshot transfer completed");
        Ok(())
    }

    // Dedicated pull logic
    async fn pull_transfer_loop(
        mut ack_stream: Box<tonic::Streaming<SnapshotAck>>,
        chunk_tx: mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
        first_chunk: SnapshotChunk,
        mut data_stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        total_chunks: u32,
        config: Arc<SnapshotConfig>,
    ) -> Result<()> {
        let mut chunk_cache = ChunkCache::new(config.cache_size);
        chunk_cache.insert(0, first_chunk.clone());
        Self::send_chunk(&chunk_tx, first_chunk.clone(), &config).await?;

        let mut next_index = 1;
        let mut retry_manager = RetryManager::new(config.max_retries);
        let mut retransmit_queue = VecDeque::new();
        let mut last_ack_time = Instant::now();

        while next_index < total_chunks || !retransmit_queue.is_empty() {
            trace!(
                "Loop condition: next_index={}/{} retransmit_queue={}",
                next_index,
                total_chunks,
                retransmit_queue.len()
            );

            // Add flow termination detection
            if next_index >= total_chunks && retransmit_queue.is_empty() {
                break;
            }

            tokio::select! {
                // Handle incoming ACKs
                ack = ack_stream.message() => {
                    trace!("Handle incoming ACKs");
                    match ack {
                        Ok(Some(ack)) => {
                            last_ack_time = Instant::now();
                            Self::handle_ack(ack, &mut next_index, &mut retry_manager, &mut retransmit_queue).await?;
                        }
                        Ok(None) => {
                            debug!("ACK stream terminated by learner");
                            break;
                        }
                        Err(e) => return Err(NetworkError::TonicStatusError(Box::new(e)).into()),
                    }
                },

                // Send next chunk or retransmit
                _ = async {
                    if let Some(seq) = retransmit_queue.pop_front() {
                        if let Some(chunk) = chunk_cache.get(seq).cloned() {
                            Self::send_chunk(&chunk_tx, chunk, &config).await?;
                        } else {
                            warn!("Chunk {} not in cache, reloading", seq);
                            if let Some(chunk) = Self::load_specific_chunk(
                                &mut data_stream,
                                seq,
                                first_chunk.leader_term,
                                first_chunk.leader_id,
                                total_chunks
                            ).await? {
                                chunk_cache.insert(seq, chunk.clone());
                                Self::send_chunk(&chunk_tx, chunk, &config).await?;
                            }
                        }
                    } else if next_index < total_chunks {
                        Self::send_next_chunk(
                            next_index,
                            &mut data_stream,
                            &chunk_tx,
                            &config,
                            &mut chunk_cache,
                            total_chunks,
                            first_chunk.leader_term,
                            first_chunk.leader_id,
                        ).await?;
                        next_index += 1;
                    }
                    Ok::<(), Box<dyn Error + Send + Sync>>(())
                } => {},

                // Handle timeout
                _ = sleep(Duration::from_secs(5)) => {
                    if last_ack_time.elapsed() > Duration::from_secs(10) {
                        return Err(SnapshotError::TransferTimeout.into());
                    }
                }
            }
        }
        Ok(())
    }

    // Handle ACK messages with proper error management
    async fn handle_ack(
        ack: SnapshotAck,
        next_index: &mut u32,
        retry_manager: &mut RetryManager,
        retransmit_queue: &mut VecDeque<u32>,
    ) -> Result<()> {
        match ack.status() {
            snapshot_ack::ChunkStatus::Accepted => {
                debug!("Chunk {} accepted", ack.seq);
                retry_manager.record_success(ack.seq);
            }
            snapshot_ack::ChunkStatus::ChecksumMismatch => {
                warn!("Checksum mismatch for chunk {}, resending", ack.seq);
                retransmit_queue.push_back(ack.seq);
            }
            snapshot_ack::ChunkStatus::OutOfOrder => {
                warn!("Out-of-order chunk {}, resetting to {}", ack.seq, ack.next_requested);
                *next_index = ack.next_requested;
                retransmit_queue.clear();
            }
            snapshot_ack::ChunkStatus::Requested => {
                debug!("Explicit request for chunk {}", ack.next_requested);
                if ack.next_requested < *next_index {
                    retransmit_queue.push_back(ack.next_requested);
                } else {
                    *next_index = ack.next_requested;
                    retransmit_queue.clear();
                }
            }
            snapshot_ack::ChunkStatus::Failed => {
                retry_manager.record_failure(ack.seq)?;
                retransmit_queue.push_back(ack.seq);
            }
        }
        Ok(())
    }
    // Send next chunk in sequence
    async fn send_next_chunk(
        seq: u32,
        data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        chunk_tx: &mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
        config: &SnapshotConfig,
        chunk_cache: &mut ChunkCache,
        total_chunks: u32,
        leader_term: u64,
        leader_id: u32,
    ) -> Result<()> {
        let mut chunk = match data_stream.next().await {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => return Err(e),
            None => return Err(SnapshotError::IncompleteSnapshot.into()),
        };

        // Update chunk metadata
        chunk.seq = seq;
        chunk.leader_term = leader_term;
        chunk.leader_id = leader_id;
        chunk.total_chunks = total_chunks;

        // Calculate and set checksum
        let checksum = crc32fast::hash(&chunk.data);
        chunk.chunk_checksum = checksum.to_be_bytes().to_vec();

        chunk_cache.insert(seq, chunk.clone());
        Self::send_chunk(chunk_tx, chunk, config).await?;
        Ok(())
    }

    // Send chunk with retry logic for push mode
    async fn send_chunk_with_retry(
        tx: &mut mpsc::Sender<SnapshotChunk>,
        chunk: SnapshotChunk,
        config: &SnapshotConfig,
    ) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut attempt = 0;
        let mut backoff = Duration::from_millis(100);

        loop {
            debug!(?attempt);
            match tx.try_send(chunk.clone()) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Full(_)) => {
                    if attempt >= MAX_RETRIES {
                        return Err(SnapshotError::Backpressure.into());
                    }

                    // Apply rate limiting before retry
                    Self::apply_rate_limit(&chunk, config).await;

                    sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0).min(Duration::from_secs(1));
                    attempt += 1;
                }
                Err(_) => return Err(SnapshotError::ReceiverDisconnected.into()),
            }
        }
    }

    // Apply rate limiting for chunk transmission
    async fn apply_rate_limit(
        chunk: &SnapshotChunk,
        config: &SnapshotConfig,
    ) {
        if config.max_bandwidth_mbps > 0 {
            let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
            let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
            debug!(?chunk_size_mb, ?min_duration);
            sleep(min_duration).await;
        }
    }

    // Send chunk with proper error handling
    async fn send_chunk(
        chunk_tx: &mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
        chunk: SnapshotChunk,
        config: &SnapshotConfig,
    ) -> Result<()> {
        Self::apply_rate_limit(&chunk, config).await;

        chunk_tx
            .send(Ok(chunk))
            .await
            .map_err(|_| SnapshotError::ReceiverDisconnected.into())
    }

    // Load specific chunk from stream
    pub(super) async fn load_specific_chunk(
        data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        seq: u32,
        leader_term: u64,
        leader_id: u32,
        total_chunks: u32,
    ) -> Result<Option<SnapshotChunk>> {
        while let Some(chunk) = data_stream.next().await {
            let mut chunk = chunk?;
            if chunk.seq == seq {
                chunk.leader_term = leader_term;
                chunk.leader_id = leader_id;
                chunk.total_chunks = total_chunks;
                return Ok(Some(chunk));
            }
        }
        Ok(None)
    }

    // pub(super) async fn send_chunks(
    //     mut stream: impl Stream<Item = Result<SnapshotChunk>> + Unpin,
    //     tx: mpsc::Sender<SnapshotChunk>,
    //     config: SnapshotConfig,
    // ) -> Result<()> {
    //     let mut yield_counter = 0;
    //     while let Some(chunk) = stream.next().await {
    //         let chunk = chunk?;

    //         //Apply rate limit
    //         if config.max_bandwidth_mbps > 0 {
    //             let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
    //             let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
    //             tokio::time::sleep(min_duration).await;
    //         }

    //         // Non-blocking send
    //         if tx.try_send(chunk).is_err() {
    //             warn!("Snapshot receiver lagging, dropping chunk");
    //             return Err(SnapshotError::Backpressure.into());
    //         }

    //         // Yield control every N chunks
    //         yield_counter += 1;
    //         if yield_counter % config.sender_yield_every_n_chunks == 0 {
    //             tokio::task::yield_now().await;
    //         }
    //     }
    //     Ok(())
    // }

    // /// Production-ready handler for streaming snapshot requests
    // pub(crate) async fn run_pull_transfer(
    //     leader_term: u64,
    //     leader_id: u32,
    //     mut ack_stream_request: Box<tonic::Streaming<SnapshotAck>>,
    //     chunk_tx: mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
    //     mut data_stream: BoxStream<'static, Result<SnapshotChunk>>,
    //     metadata: SnapshotMetadata,
    //     config: SnapshotConfig,
    // ) -> Result<()> {
    //     // Create transfer instance for tracking
    //     let transfer = Arc::new(Self {
    //         node_id: 0, // Not used in this context
    //         metadata: metadata.clone(),
    //         config: Arc::new(config.clone()),
    //         status: Arc::new(AtomicU32::new(SnapshotStatus::Pending as u32)),
    //         _marker: PhantomData,
    //     });

    //     // Preload the first chunk and verify
    //     let first_chunk = match data_stream.next().await {
    //         Some(Ok(chunk)) if chunk.seq == 0 && chunk.metadata.is_some() => {
    //             // Verify required fields
    //             if chunk.total_chunks == 0 || chunk.leader_id != 0 || chunk.leader_term != 0 {
    //                 return Err(SnapshotError::InvalidFirstChunk.into());
    //             }
    //             chunk
    //         }
    //         Some(Ok(_)) => return Err(SnapshotError::InvalidFirstChunk.into()),
    //         Some(Err(e)) => return Err(e),
    //         None => return Err(SnapshotError::EmptySnapshot.into()),
    //     };

    //     // Get total_chunks from the first chunk
    //     let total_chunks = first_chunk.total_chunks;

    //     // Send the first chunk (note to update the leader information)
    //     let mut first_chunk = first_chunk;
    //     first_chunk.leader_id = leader_id;
    //     first_chunk.leader_term = leader_term;
    //     let checksum = crc32fast::hash(&first_chunk.data);
    //     first_chunk.chunk_checksum = checksum.to_be_bytes().to_vec();
    //     Self::send_chunk(&chunk_tx, first_chunk, &config).await?;

    //     let mut next_chunk_index = 0; // Start from the second chunk
    //     let mut chunks_sent = 1; // 1 chunk has been sent (the first chunk)

    //     let mut last_ack_time = Instant::now();
    //     let mut retransmit_queue: VecDeque<u32> = VecDeque::new();

    //     let mut retry_states: HashMap<u32, ChunkRetryState> = HashMap::new();
    //     const MAX_RETRIES: u32 = 5;
    //     const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
    //     // Main transfer loop
    //     while next_chunk_index < total_chunks || !retransmit_queue.is_empty() {
    //         tokio::select! {
    //             // Handle incoming ACKs
    //             ack = ack_stream_request.message() => match ack {
    //                 Ok(Some(ack)) => {
    //                     last_ack_time = Instant::now();

    //                     match ack.status() {
    //                         snapshot_ack::ChunkStatus::Accepted => {
    //                             debug!("Chunk {} accepted", ack.seq);
    //                         }
    //                         snapshot_ack::ChunkStatus::ChecksumMismatch => {
    //                             warn!("Checksum mismatch for chunk {}, resending", ack.seq);
    //                             retransmit_queue.push_back(ack.seq);
    //                         }
    //                         snapshot_ack::ChunkStatus::OutOfOrder => {
    //                             warn!("Out-of-order chunk {}, resetting to {}", ack.seq, ack.next_requested);
    //                             next_chunk_index = ack.next_requested;
    //                             retransmit_queue.clear();
    //                         }
    //                         snapshot_ack::ChunkStatus::Requested => {
    //                             debug!("Explicit request for chunk {}", ack.next_requested);
    //                             if ack.next_requested < next_chunk_index {
    //                                 // If the requested chunk has been sent, process it from the retransmit queue
    //                                 retransmit_queue.push_back(ack.next_requested);
    //                             } else {
    //                                 // If the requested chunk has not been sent, jump directly to that position
    //                                 next_chunk_index = ack.next_requested;
    //                                 retransmit_queue.clear();
    //                             }
    //                         }
    //                         snapshot_ack::ChunkStatus::Failed => {
    //                             let retry_state = retry_states.entry(ack.seq).or_insert(ChunkRetryState {
    //                                 retry_count: 0,
    //                                 last_retry: Instant::now() - INITIAL_RETRY_DELAY,
    //                             });

    //                             // Apply exponential backoff
    //                             let next_retry_time = retry_state.last_retry +
    //                                 INITIAL_RETRY_DELAY * 2u32.pow(retry_state.retry_count);

    //                             if Instant::now() >= next_retry_time {
    //                                 retry_state.retry_count += 1;
    //                                 retry_state.last_retry = Instant::now();

    //                                 if retry_state.retry_count > MAX_RETRIES {
    //                                     error!("Chunk {} failed after {} retries, aborting transfer", ack.seq, MAX_RETRIES);
    //                                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                                     return Err(SnapshotError::TransferFailed.into());
    //                                 }

    //                                 warn!("Chunk {} failed (attempt {}/{}), resending",
    //                                 ack.seq, retry_state.retry_count, MAX_RETRIES);
    //                                 retransmit_queue.push_back(ack.seq);
    //                             } else {
    //                                 // Schedule for later retry
    //                                 retransmit_queue.push_back(ack.seq);
    //                             }
    //                         }
    //                     }
    //                 }
    //                 Ok(None) => break, // Stream closed by learner
    //                 Err(e) => {
    //                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                     return Err(NetworkError::TonicStatusError(Box::new(e)).into());
    //                 }
    //             },

    //             // Send next chunk or retransmit
    //             _ = async {
    //                 if let Some(seq) = retransmit_queue.pop_front() {
    //                     // Retransmit specific chunk
    //                     if let Some(chunk) = Self::load_specific_chunk(
    //                         &mut data_stream,
    //                         seq,
    //                         leader_term,
    //                         leader_id,
    //                         total_chunks
    //                     ).await? {
    //                         Self::send_chunk(&chunk_tx, chunk, &config).await?;
    //                     }
    //                 } else if next_chunk_index < total_chunks {
    //                     // Send next chunk in sequence
    //                     if let Some(chunk) = Self::load_next_chunk(
    //                         &mut data_stream,
    //                         next_chunk_index,
    //                         leader_term,
    //                         leader_id,
    //                         &transfer,
    //                         total_chunks
    //                     ).await? {
    //                         Self::send_chunk(&chunk_tx, chunk, &config).await?;
    //                         next_chunk_index += 1;
    //                     }
    //                     chunks_sent += 1;
    //                 }
    //                 Ok::<(), Box<dyn Error + Send + Sync>>(())
    //             } => {},

    //             // Handle timeout
    //             _ = tokio::time::sleep(Duration::from_secs(5)) => {
    //                 if last_ack_time.elapsed() > Duration::from_secs(10) {
    //                     transfer.status.store(SnapshotStatus::Failed as u32, Ordering::Release);
    //                     return Err(SnapshotError::TransferTimeout.into());
    //                 }
    //             }
    //         }
    //     }

    //     transfer
    //         .status
    //         .store(SnapshotStatus::Completed as u32, Ordering::Release);
    //     Ok(())
    // }

    // async fn load_next_chunk(
    //     data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
    //     seq: u32,
    //     leader_term: u64,
    //     leader_id: u32,
    //     transfer: &Arc<Self>,
    //     total_chunks: u32,
    // ) -> Result<Option<SnapshotChunk>> {
    //     match data_stream.next().await {
    //         Some(Ok(mut chunk)) => {
    //             // Update chunk metadata
    //             chunk.leader_id = leader_id;
    //             chunk.leader_term = leader_term;
    //             chunk.seq = seq;
    //             chunk.total_chunks = total_chunks;

    //             // Add metadata only for first chunk
    //             if seq == 0 {
    //                 chunk.metadata = Some(transfer.metadata.clone());
    //             }

    //             // Calculate and set checksum
    //             chunk.chunk_checksum = crc32fast::hash(&chunk.data).to_be_bytes().to_vec();

    //             Ok(Some(chunk))
    //         }
    //         Some(Err(e)) => Err(e),
    //         None => Ok(None),
    //     }
    // }

    // pub(super) async fn load_specific_chunk(
    //     data_stream: &mut Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
    //     seq: u32,
    //     leader_term: u64,
    //     leader_id: u32,
    //     total_chunks: u32,
    // ) -> Result<Option<SnapshotChunk>> {
    //     while let Some(Ok(mut chunk)) = data_stream.next().await {
    //         if chunk.seq == seq {
    //             chunk.leader_term = leader_term;
    //             chunk.leader_id = leader_id;
    //             chunk.total_chunks = total_chunks;
    //             return Ok(Some(chunk));
    //         }
    //     }
    //     Ok(None)
    // }

    // async fn send_chunk(
    //     chunk_tx: &mpsc::Sender<std::result::Result<SnapshotChunk, Status>>,
    //     chunk: SnapshotChunk,
    //     config: &SnapshotConfig,
    // ) -> Result<()> {
    //     // Apply rate limiting
    //     if config.max_bandwidth_mbps > 0 {
    //         let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
    //         let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
    //         tokio::time::sleep(min_duration).await;
    //     }

    //     // Send with bounded channel backpressure
    //     match chunk_tx.send(Ok(chunk)).await {
    //         Ok(_) => Ok(()),
    //         Err(e) => {
    //             warn!(?e, "Failed to send chunk: receiver disconnected");
    //             Err(SnapshotError::ReceiverDisconnected.into())
    //         }
    //     }
    // }

    // / Create snapshot block
    // fn create_chunk(
    //     &self,
    //     leader_term: u64,
    //     leader_id: u32,
    //     seq: u32,
    //     data: Option<Vec<u8>>,
    //     is_first: bool,
    //     total_chunks: u32,
    // ) -> Result<SnapshotChunk> {
    //     let data = data.unwrap_or_default();

    //     // Calculate checksum
    //     let mut hasher = crc32fast::Hasher::new();
    //     hasher.update(&data);
    //     let checksum = hasher.finalize().to_be_bytes().to_vec();

    //     Ok(SnapshotChunk {
    //         leader_term,
    //         leader_id,
    //         seq,
    //         total_chunks,
    //         chunk_checksum: checksum,
    //         metadata: if is_first { Some(self.metadata.clone()) } else { None },
    //         data,
    //     })
    // }
}
