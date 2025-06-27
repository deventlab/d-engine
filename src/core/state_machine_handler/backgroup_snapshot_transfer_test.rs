use std::pin::Pin;
use std::time::Duration;

use super::*;
use crate::proto::common::LogId;
use crate::proto::storage::snapshot_ack::{self, ChunkStatus};
use crate::proto::storage::{SnapshotAck, SnapshotChunk, SnapshotMetadata, SnapshotResponse};
use crate::test_utils::{
    self, crate_test_snapshot_stream, create_snapshot_stream, create_test_chunk, enable_logger, MockNode,
    MockTypeConfig, MOCK_SNAPSHOT_PORT_BASE,
};
use crate::{NetworkError, SnapshotConfig, SnapshotError, StorageError};
use futures::{stream, Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tracing::debug;

#[tokio::test]
async fn test_push_transfer_success() {
    test_utils::enable_logger();
    let config = default_snapshot_config();
    let stream = create_snapshot_stream(3, 1024); // 3 chunks of 1KB each

    // Start mock server
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let addr_channel = MockNode::simulate_snapshot_mock_server(
        MOCK_SNAPSHOT_PORT_BASE + 1,
        Ok(SnapshotResponse {
            term: 1,
            success: true, // always succeed
            next_chunk: 1,
        }),
        shutdown_rx,
    )
    .await
    .unwrap();

    let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
        1,
        Box::pin(stream),
        addr_channel.channel,
        config,
    )
    .await;

    debug!(?result);

    assert!(result.is_ok());
}

// Test: Transfer fails when stream returns an error
#[tokio::test]
async fn test_push_transfer_fails_on_stream_error() {
    let config = default_snapshot_config();
    let stream = create_snapshot_stream(2, 512);
    let error_stream = stream::once(async {
        Err(StorageError::IoError(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Test error")).into())
    });
    let combined_stream = stream.chain(error_stream).boxed();

    // Start mock server
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let addr_channel = MockNode::simulate_snapshot_mock_server(
        MOCK_SNAPSHOT_PORT_BASE + 2,
        Err(Status::unavailable("Service is not ready")),
        shutdown_rx,
    )
    .await
    .unwrap();

    let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
        1,
        combined_stream,
        addr_channel.channel,
        config,
    )
    .await;

    assert!(result.is_err());
}

// Test: Transfer respects bandwidth limit
#[tokio::test]
async fn test_push_transfer_respects_bandwidth_limit() {
    enable_logger();
    let mut config = default_snapshot_config();
    config.max_bandwidth_mbps = 1; // 1 MBps
    config.push_queue_size = 1;

    let stream = create_snapshot_stream(5, 250_000_000); // 5 chunks of 250KB each (1.25MB total)

    // Start mock server
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let addr_channel = MockNode::simulate_snapshot_mock_server(
        MOCK_SNAPSHOT_PORT_BASE + 3,
        Ok(SnapshotResponse {
            term: 1,
            success: true, // always succeed
            next_chunk: 1,
        }),
        shutdown_rx,
    )
    .await
    .unwrap();

    let start = Instant::now();
    let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(
        1,
        Box::pin(stream),
        addr_channel.channel,
        config,
    )
    .await;

    assert!(result.is_ok());
    let duration = start.elapsed();
    debug!(?duration);
    // Should take at least 1.25 seconds for 1.25MB at 1MBps
    assert!(duration >= Duration::from_secs_f32(1.25));
}

// // Test: Transfer handles out-of-order ACKs
// #[tokio::test]
// async fn test_push_transfer_handles_out_of_order_acks() {
//     let config = default_snapshot_config();
//     let stream = create_snapshot_stream(3, 1024);

//     let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

//     // Simulate out-of-order ACKs
//     tokio::spawn(async move {
//         // ACK for chunk 2 first
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 2,
//                 status: ChunkStatus::Accepted.into(),
//                 next_requested: 0,
//             }))
//             .await
//             .unwrap();

//         // Then ACK for chunk 1
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 1,
//                 status: ChunkStatus::Accepted.into(),
//                 next_requested: 0,
//             }))
//             .await
//             .unwrap();

//         // Finally ACK for chunk 0
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 0,
//                 status: ChunkStatus::Accepted.into(),
//                 next_requested: 0,
//             }))
//             .await
//             .unwrap();
//     });

//     let result =
//         BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(1, Box::pin(stream), channel, config).await;

//     assert!(result.is_ok());
// }

// // Test: Transfer handles checksum failures and retries
// #[tokio::test]
// async fn test_push_transfer_handles_checksum_failures() {
//     let config = default_snapshot_config();
//     let stream = create_snapshot_stream(2, 1024);

//     let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

//     tokio::spawn(async move {
//         // First ACK: checksum failure for chunk 0
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 0,
//                 status: ChunkStatus::ChecksumMismatch.into(),
//                 next_requested: 0, // Request retransmission
//             }))
//             .await
//             .unwrap();

//         // Second ACK: accept retransmitted chunk 0
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 0,
//                 status: ChunkStatus::Accepted.into(),
//                 next_requested: 1,
//             }))
//             .await
//             .unwrap();

//         // ACK for chunk 1
//         ack_tx
//             .send(Ok(SnapshotAck {
//                 seq: 1,
//                 status: ChunkStatus::Accepted.into(),
//                 next_requested: 2,
//             }))
//             .await
//             .unwrap();
//     });

//     let result =
//         BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(1, Box::pin(stream), channel, config).await;

//     assert!(result.is_ok());
// }

// // Test: Transfer fails after max retries
// #[tokio::test]
// async fn test_push_transfer_fails_after_max_retries() {
//     let mut config = default_snapshot_config();
//     config.max_retries = 2; // Only allow 2 retries

//     let stream = create_snapshot_stream(1, 1024);

//     let channel = Endpoint::from_static("http://[::]:12345").connect_lazy();

//     tokio::spawn(async move {
//         // Continuously send checksum failures
//         for _ in 0..=config.max_retries {
//             ack_tx
//                 .send(Ok(SnapshotAck {
//                     seq: 0,
//                     status: ChunkStatus::ChecksumMismatch.into(),
//                     next_requested: 0,
//                 }))
//                 .await
//                 .unwrap();
//         }
//     });

//     let result =
//         BackgroundSnapshotTransfer::<MockTypeConfig>::run_push_transfer(1, Box::pin(stream), channel, config).await;

//     assert!(result.is_err());
//     match result.unwrap_err() {
//         SnapshotError::MaxRetriesExceeded => (),
//         _ => panic!("Expected MaxRetriesExceeded error"),
//     }
// }

// // Test: Pull transfer completes successfully
// #[tokio::test]
// async fn test_pull_transfer_completes_successfully() {
//     let config = default_snapshot_config();
//     let data_stream = create_snapshot_stream(3, 1024).boxed();

//     let (ack_tx, ack_rx) = mpsc::channel(10);
//     let (chunk_tx, mut chunk_rx) = mpsc::channel(10);

//     let ack_stream = ReceiverStream::new(ack_rx).boxed();

//     let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
//         Box::pin(ack_stream),
//         chunk_tx,
//         data_stream,
//         config,
//     ));

//     // Simulate ACKs
//     tokio::spawn(async move {
//         for i in 0..3 {
//             ack_tx
//                 .send(Ok(SnapshotAck {
//                     seq: i,
//                     status: ChunkStatus::Accepted.into(),
//                     next_requested: i + 1,
//                 }))
//                 .await
//                 .unwrap();
//         }
//     });

//     // Verify chunks received
//     let mut received = 0;
//     while let Some(chunk) = chunk_rx.recv().await {
//         assert!(chunk.is_ok());
//         received += 1;
//     }
//     assert_eq!(received, 3);

//     transfer.await.unwrap().unwrap();
// }

// // Helper to create mock gRPC channel
// async fn setup_mock_channel() -> Channel {
//     // Implementation would create in-memory channel for testing
//     // This is a simplified placeholder
//     Endpoint::from_static("http://[::1]:50051").connect().await.unwrap()
// }

fn default_snapshot_config() -> SnapshotConfig {
    SnapshotConfig {
        max_bandwidth_mbps: 0,
        sender_yield_every_n_chunks: 2,
        ..Default::default()
    }
}

// fn make_metadata() -> SnapshotMetadata {
//     SnapshotMetadata {
//         last_included: Some(LogId { index: 1, term: 1 }),
//         ..Default::default()
//     }
// }

// fn make_chunk(data: &[u8]) -> SnapshotChunk {
//     SnapshotChunk {
//         data: data.to_vec(),
//         ..Default::default()
//     }
// }

// /// Test: load_specific_chunk correctly retrieves requested chunk
// #[tokio::test]
// async fn test_load_specific_chunk() {
//     let chunks = vec![
//         SnapshotChunk {
//             seq: 0,
//             ..Default::default()
//         },
//         SnapshotChunk {
//             seq: 1,
//             ..Default::default()
//         },
//         SnapshotChunk {
//             seq: 2,
//             ..Default::default()
//         },
//     ];
//     // Create stream that yields Ok(chunk) for each item
//     let mut stream: Pin<Box<dyn Stream<Item = Result<SnapshotChunk>> + Send>> =
//         Box::pin(stream::iter(chunks.into_iter().map(Ok)));

//     let chunk = BackgroundSnapshotTransfer::<MockTypeConfig>::load_specific_chunk(&mut stream, 1, 1, 1, 3)
//         .await
//         .unwrap()
//         .unwrap();

//     assert_eq!(chunk.seq, 1);
// }

// // Helper to create mock handler
// fn create_mock_handler(num_chunks: u32) -> (Arc<MockStateMachineHandler<MockTypeConfig>>, SnapshotMetadata) {
//     let metadata = make_metadata();
//     let cloned_metadata = metadata.clone();
//     let mut chunks: Vec<Result<SnapshotChunk>> = Vec::new();

//     for i in 0..num_chunks {
//         chunks.push(Ok(SnapshotChunk {
//             seq: i,
//             total_chunks: num_chunks,
//             ..Default::default()
//         }));
//     }

//     let mut handler = MockStateMachineHandler::<MockTypeConfig>::new();
//     handler.expect_load_snapshot_data().returning(move |_| {
//         let chunks = vec![create_test_chunk(0, b"data", 1, 1, 1)];
//         let stream = crate_test_snapshot_stream(chunks);
//         Ok(Box::pin(stream.map(|item| {
//             item.map_err(|s| NetworkError::TonicStatusError(Box::new(s)).into())
//         })))
//     });

//     handler.expect_apply_batch().returning(move |_| Ok(()));
//     handler
//         .expect_get_latest_snapshot_metadata()
//         .returning(move || Some(metadata.clone()));

//     (Arc::new(handler), cloned_metadata)
// }

// #[tokio::test]
// async fn test_pull_transfer_with_retries() {
//     let (chunk_tx, _chunk_rx) = mpsc::channel(10);
//     let config = SnapshotConfig {
//         cache_size: 10,
//         max_retries: 3,
//         ..Default::default()
//     };

//     let first_chunk = SnapshotChunk {
//         seq: 0,
//         leader_term: 1,
//         leader_id: 1,
//         total_chunks: 2,
//         chunk_checksum: vec![],
//         metadata: Some(SnapshotMetadata::default()),
//         data: vec![],
//     };

//     let ack_stream = crate_test_snapshot_stream(vec![
//         SnapshotAck {
//             seq: 0,
//             status: snapshot_ack::ChunkStatus::Failed as i32,
//             next_requested: 0,
//         },
//         SnapshotAck {
//             seq: 0,
//             status: snapshot_ack::ChunkStatus::Accepted as i32,
//             next_requested: 1,
//         },
//     ]);

//     let data_stream = Box::pin(stream::iter(vec![
//         Ok(first_chunk.clone()),
//         Ok(SnapshotChunk {
//             seq: 1,
//             leader_term: 1,
//             leader_id: 1,
//             total_chunks: 2,
//             chunk_checksum: vec![],
//             metadata: None,
//             data: vec![],
//         }),
//     ]));

//     let result = BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
//         Box::new(ack_stream),
//         chunk_tx,
//         data_stream,
//         config,
//     )
//     .await;

//     assert!(result.is_ok());
// }

// #[tokio::test]
// async fn test_chunk_cache_eviction() {
//     let mut cache = ChunkCache::new(2);
//     let chunk1 = SnapshotChunk::default();
//     let chunk2 = SnapshotChunk::default();
//     let chunk3 = SnapshotChunk::default();

//     cache.insert(1, chunk1.clone());
//     cache.insert(2, chunk2.clone());
//     cache.insert(3, chunk3.clone());

//     assert!(cache.get(1).is_none());
//     assert!(cache.get(2).is_some());
//     assert!(cache.get(3).is_some());
// }

// #[tokio::test]
// async fn test_retry_manager_max_retries() {
//     let mut manager = RetryManager::new(2);
//     assert!(manager.record_failure(1).is_ok());
//     assert!(manager.record_failure(1).is_ok());
//     assert!(manager.record_failure(1).is_err());
// }

#[cfg(test)]

mod run_pull_transfer_test {
    use crate::{test_utils::crate_test_snapshot_stream_from_receiver, ConsensusError, Error};

    use super::*;

    #[tokio::test]
    async fn test_pull_transfer_successful_transfer() {
        enable_logger();
        let config = default_snapshot_config();
        let data_stream = create_snapshot_stream(3, 1024).boxed();

        let (ack_tx, ack_rx) = mpsc::channel(10);
        let (chunk_tx, mut chunk_rx) = mpsc::channel(10);

        let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            Box::new(crate_test_snapshot_stream_from_receiver(ack_rx)),
            chunk_tx,
            data_stream,
            config,
        ));

        // Simulate proper ACKs
        let ack_tx_clone = ack_tx.clone();
        tokio::spawn(async move {
            for i in 0..3 {
                ack_tx_clone
                    .send(SnapshotAck {
                        seq: i,
                        status: ChunkStatus::Accepted.into(),
                        next_requested: i + 1,
                    })
                    .await
                    .unwrap();
            }
        });

        // Verify all chunks received
        let mut received = 0;
        while let Some(chunk) = chunk_rx.recv().await {
            assert!(chunk.is_ok());
            received += 1;
            if received == 3 {
                break;
            }
        }

        drop(ack_tx);

        tokio::select! {
            _ = async {
                //Original assertion logic
                assert_eq!(received, 3);
                assert!(transfer.await.unwrap().is_ok());
            } => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                panic!("Test timed out after 5 seconds");
            }
        }
    }

    #[tokio::test]
    async fn test_pull_transfer_with_checksum_retries() {
        enable_logger();
        let config = SnapshotConfig {
            max_retries: 3,
            ..default_snapshot_config()
        };

        let data_stream = create_snapshot_stream(2, 512).boxed();
        let (ack_tx, ack_rx) = mpsc::channel(10);
        let (chunk_tx, mut chunk_rx) = mpsc::channel(10);

        let ack_tx_clone = ack_tx.clone();
        tokio::spawn(async move {
            // First ACK: checksum failure
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 0,
                    status: ChunkStatus::ChecksumMismatch.into(),
                    next_requested: 0,
                })
                .await
                .unwrap();

            // Second ACK: accept retransmitted chunk
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 0,
                    status: ChunkStatus::Accepted.into(),
                    next_requested: 1,
                })
                .await
                .unwrap();

            // ACK for chunk 1
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 1,
                    status: ChunkStatus::Accepted.into(),
                    next_requested: 2,
                })
                .await
                .unwrap();
        });

        let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            Box::new(crate_test_snapshot_stream_from_receiver(ack_rx)),
            chunk_tx,
            data_stream,
            config,
        ));

        let mut received = 0;
        while let Some(chunk) = chunk_rx.recv().await {
            assert!(chunk.is_ok());
            received += 1;
        }

        drop(ack_tx);
        assert_eq!(received, 2);
        assert!(transfer.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_pull_transfer_fails_after_max_retries() {
        enable_logger();
        let config = SnapshotConfig {
            max_retries: 2,
            ..default_snapshot_config()
        };

        let data_stream = create_snapshot_stream(1, 512).boxed();
        let (ack_tx, ack_rx) = mpsc::channel(10);
        let (chunk_tx, _chunk_rx) = mpsc::channel(10);

        let ack_tx_clone = ack_tx.clone();
        tokio::spawn(async move {
            // Continuously send checksum failures
            for _ in 0..=config.max_retries {
                ack_tx_clone
                    .send(SnapshotAck {
                        seq: 0,
                        status: ChunkStatus::ChecksumMismatch.into(),
                        next_requested: 0,
                    })
                    .await
                    .unwrap();
            }
        });

        let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            Box::new(crate_test_snapshot_stream_from_receiver(ack_rx)),
            chunk_tx,
            data_stream,
            config,
        ));

        drop(ack_tx);
        let result = transfer.await.unwrap();
        debug!(?result);
        
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Consensus(ConsensusError::Snapshot(SnapshotError::TransferFailed))
        ));
    }

    #[tokio::test]
    async fn test_pull_transfer_handles_out_of_order_acks() {
        enable_logger();
        let config = default_snapshot_config();
        let data_stream = create_snapshot_stream(3, 512).boxed();
        let (ack_tx, ack_rx) = mpsc::channel(10);
        let (chunk_tx, mut chunk_rx) = mpsc::channel(10);

        let ack_tx_clone = ack_tx.clone();
        tokio::spawn(async move {
            // ACK for chunk 2 first
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 2,
                    status: ChunkStatus::Accepted.into(),
                    next_requested: 0,
                })
                .await
                .unwrap();

            // Then ACK for chunk 1
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 1,
                    status: ChunkStatus::Accepted.into(),
                    next_requested: 0,
                })
                .await
                .unwrap();

            // Finally ACK for chunk 0
            ack_tx_clone
                .send(SnapshotAck {
                    seq: 0,
                    status: ChunkStatus::Accepted.into(),
                    next_requested: 3,
                })
                .await
                .unwrap();
        });

        let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            Box::new(crate_test_snapshot_stream_from_receiver(ack_rx)),
            chunk_tx,
            data_stream,
            config,
        ));

        let mut received = 0;
        while let Some(chunk) = chunk_rx.recv().await {
            assert!(chunk.is_ok());
            received += 1;
        }

        drop(ack_tx);
        assert_eq!(received, 3);
        assert!(transfer.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_pull_transfer_timeout_with_no_acks() {
        enable_logger();
        let config = SnapshotConfig {
            ..default_snapshot_config()
        };

        let data_stream = create_snapshot_stream(1, 512).boxed();
        let (ack_tx, ack_rx) = mpsc::channel(10);
        let (chunk_tx, _chunk_rx) = mpsc::channel(10);

        let transfer = tokio::spawn(BackgroundSnapshotTransfer::<MockTypeConfig>::run_pull_transfer(
            Box::new(crate_test_snapshot_stream_from_receiver(ack_rx)),
            chunk_tx,
            data_stream,
            config,
        ));

        // Don't send any ACKs - should timeout
        drop(ack_tx);

        let result = transfer.await.unwrap();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Consensus(ConsensusError::Snapshot(SnapshotError::TransferTimeout))
        ));
    }
}
