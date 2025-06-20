use crate::StorageError;
use crate::{proto::storage::snapshot_service_client::SnapshotServiceClient, Result, SnapshotError};
use futures::StreamExt;
use futures::{stream::BoxStream, Stream};
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

use crate::{
    proto::storage::{SnapshotChunk, SnapshotMetadata},
    SnapshotConfig,
};

pub enum SnapshotStatus {
    Completed = 0,
    Pending = 1,
    Failed = 2,
}
pub struct BackgroundSnapshotTransfer {
    node_id: u32,
    metadata: SnapshotMetadata,
    config: Arc<SnapshotConfig>,
    status: Arc<AtomicU32>, //Transmission status
}

impl BackgroundSnapshotTransfer {
    pub async fn start(
        node_id: u32,
        metadata: SnapshotMetadata,
        data_stream: BoxStream<'static, Result<SnapshotChunk>>,
        channel: Channel,
        config: SnapshotConfig,
    ) -> Result<()> {
        let transfer = Self {
            node_id,
            metadata: metadata.clone(),
            config: Arc::new(config.clone()),
            status: Arc::new(AtomicU32::new(SnapshotStatus::Pending as u32)),
        };

        //Register to global manager SNAPSHOT_MANAGER.register(transfer.clone());

        // Execute in a dedicated thread pool
        let handle = tokio::task::spawn_blocking(move || -> Result<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(StorageError::IoError)?;

            Ok(rt
                .block_on(async {
                    let mut client = SnapshotServiceClient::new(channel)
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip);

                    // Set streaming parameters
                    let (request_tx, request_rx) = mpsc::channel(32);
                    let request_stream = ReceiverStream::new(request_rx);

                    // Start gRPC call (do not wait for completion)
                    let response = client.install_snapshot(request_stream);

                    // Handle sending and receiving in parallel
                    tokio::select! {
                        send_result = Self::send_chunks(
                            data_stream,
                            request_tx,
                            config
                        ) => match send_result {
                            Ok(_) =>{ debug!("Send completed");Ok(())},
                            Err(e) => {
                                transfer.status.store( SnapshotStatus::Failed as u32, Ordering::Release );
                                return Err(e);
                            }
                        },
                        recv_result = response => match recv_result {
                            Ok(res) if res.get_ref().success => {
                                transfer.status.store( SnapshotStatus::Completed as u32, Ordering::Release );
                                return Ok(());
                            },
                            _ => {
                                transfer.status.store(SnapshotStatus::Failed as u32,Ordering::Release);
                                return Err(SnapshotError::TransferFailed.into());
                            }
                        }
                    }
                })
                .map_err(|e| SnapshotError::OperationFailed(format!("{:?}", e)))?)
        });

        //Do not wait for transfer to complete
        tokio::spawn(async move {
            if let Err(e) = handle.await {
                error!("Snapshot transfer task crashed: {:?}", e);
            }
        });

        Ok(())
    }

    async fn send_chunks(
        mut stream: impl Stream<Item = Result<SnapshotChunk>> + Unpin,
        mut tx: mpsc::Sender<SnapshotChunk>,
        config: SnapshotConfig,
    ) -> Result<()> {
        let mut yield_counter = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;

            //Apply rate limit
            if config.max_bandwidth_mbps > 0 {
                let chunk_size_mb = chunk.data.len() as f64 / 1_000_000.0;
                let min_duration = Duration::from_secs_f64(chunk_size_mb / config.max_bandwidth_mbps as f64);
                tokio::time::sleep(min_duration).await;
            }

            // Non-blocking send
            if tx.try_send(chunk).is_err() {
                warn!("Snapshot receiver lagging, dropping chunk");
                return Err(SnapshotError::Backpressure.into());
            }

            // Yield control every N chunks
            yield_counter += 1;
            if yield_counter % config.sender_yield_every_n_chunks == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}
