use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::snapshot_service_client::SnapshotServiceClient;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::debug;
use tracing::trace;

use crate::NetworkError;
use crate::Result;
use crate::SnapshotConfig;
use crate::SnapshotError;
use crate::TypeConfig;

pub struct BackgroundSnapshotTransfer<T> {
    _marker: PhantomData<T>,
}

impl<T> BackgroundSnapshotTransfer<T>
where
    T: TypeConfig,
{
    // Unified push transfer entry point
    pub async fn run_push_transfer(
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

        Self::push_transfer_loop(node_id, client, first_chunk, data_stream, config).await?;

        debug!(%node_id, "Push snapshot transfer completed");
        Ok(())
    }

    // Dedicated push logic
    async fn push_transfer_loop(
        node_id: u32,
        mut client: SnapshotServiceClient<Channel>,
        first_chunk: SnapshotChunk,
        mut data_stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>>,
        config: Arc<SnapshotConfig>,
    ) -> Result<()> {
        // 1. Create a transmission channel
        let (mut request_tx, request_rx) =
            mpsc::channel::<Arc<SnapshotChunk>>(config.push_queue_size);

        // 2. Send the first data block
        request_tx
            .send(Arc::new(first_chunk))
            .await
            .map_err(|e| NetworkError::SingalSendFailed(format!("{e:?}")))?;

        // 3. Start the background task to send the remaining blocks
        let (error_tx, mut error_rx) = mpsc::channel(1);

        // # Bug fix: request_tx can not be cloned.
        // let mut bg_request_tx = request_tx.clone();
        let bg_config = config.clone();
        tokio::spawn(async move {
            let result = async {
                while let Some(chunk) = data_stream.next().await {
                    match chunk {
                        Ok(chunk) => {
                            Self::send_chunk_with_retry(
                                &mut request_tx,
                                Arc::new(chunk),
                                &bg_config,
                            )
                            .await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            }
            .await;

            debug!("finished send snapshot stream!");

            // Only send error if one occurred
            if let Err(e) = result {
                let _ = error_tx.send(e).await;
            }
            // Otherwise, let error_tx drop naturally
        });

        // 4. Create a gRPC request stream
        let request_stream = ReceiverStream::new(request_rx).map(|arc_chunk| (*arc_chunk).clone());

        // 5. Initiate gRPC call with timeout and error handling
        let grpc_fut = client.install_snapshot(request_stream);
        tokio::pin!(grpc_fut);

        debug!(config.push_timeout_in_ms);
        let timeout_duration = Duration::from_millis(config.push_timeout_in_ms);
        let timeout_fut = tokio::time::sleep(timeout_duration);
        tokio::pin!(timeout_fut);

        loop {
            tokio::select! {
                // Check for background errors
                bg_error = error_rx.recv() => {
                    match bg_error {
                        Some(e) => return Err(e),
                        None => continue, // No error yet, or background task completed without error, keep waiting
                    }
                }

                response = &mut grpc_fut => {
                    trace!("normal response ...");
                    match response {
                        Ok(response) => {
                            if response.into_inner().success {
                                return Ok(());
                            } else {
                                return Err(SnapshotError::RemoteRejection.into());
                            }
                        }
                        Err(e) => return Err(NetworkError::TonicStatusError(Box::new(e)).into()),
                    }
                }

                // Handle timeout
                 _ = &mut timeout_fut => {
                    trace!("timeout ...");
                    return Err(NetworkError::Timeout{node_id, duration: timeout_duration}.into());
                }
            }
        }
    }

    // Send chunk with retry logic for push mode
    async fn send_chunk_with_retry(
        tx: &mut mpsc::Sender<Arc<SnapshotChunk>>,
        chunk: Arc<SnapshotChunk>,
        config: &SnapshotConfig,
    ) -> Result<()> {
        let mut attempt = 0;
        // let mut backoff = Duration::from_millis(config.snapshot_push_backoff_in_ms);
        let max_retry = config.snapshot_push_max_retry;

        loop {
            trace!(?attempt);
            match tx.try_send(chunk.clone()) {
                Ok(_) => {
                    trace!("send chunk.");
                    return Ok(());
                }
                Err(TrySendError::Full(_)) => {
                    trace!("queue is full!");
                    if attempt >= max_retry {
                        return Err(SnapshotError::Backpressure.into());
                    }
                    let start = Instant::now();
                    // Apply rate limiting before retry
                    Self::apply_rate_limit(&chunk, config.max_bandwidth_mbps).await;
                    let duration = start.elapsed();

                    attempt += 1;
                    trace!(?attempt, ?duration, "apply_rate_limit=");
                }
                Err(e) => {
                    trace!(?e, "unknown error");
                    return Err(SnapshotError::ReceiverDisconnected.into());
                }
            }
        }
    }

    // Apply rate limiting for chunk transmission
    async fn apply_rate_limit(
        chunk: &SnapshotChunk,
        max_bandwidth_mbps: u32,
    ) {
        if max_bandwidth_mbps > 0 {
            let chunk_size_bits = chunk.data.len() as f64 * 8.0;
            let bandwidth_bps = max_bandwidth_mbps as f64 * 1_000_000.0;
            let min_duration_secs = chunk_size_bits / bandwidth_bps;

            let min_duration = Duration::from_secs_f64(min_duration_secs);
            debug!(
                chunk_size_bytes = chunk.data.len(),
                max_bandwidth_mbps,
                min_duration_secs,
                min_duration = ?min_duration
            );
            sleep(min_duration).await;
        }
    }
}
