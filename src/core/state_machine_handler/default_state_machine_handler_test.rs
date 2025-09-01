use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mockall::predicate::eq;
use mockall::Sequence;
use tempfile::tempdir;
use tempfile::TempDir;
use tokio::fs::create_dir_all;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time;
use tracing::debug;
use tracing_test::traced_test;

use super::DefaultStateMachineHandler;
use super::MockStateMachineHandler;
use super::StateMachineHandler;
use crate::constants::SNAPSHOT_DIR_PREFIX;
// use crate::init_sled_state_machine_db;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::Entry;
use crate::proto::common::LogId;
use crate::proto::election::VotedFor;
use crate::proto::storage::snapshot_ack::ChunkStatus;
use crate::proto::storage::snapshot_service_client::SnapshotServiceClient;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::SnapshotAck;
use crate::proto::storage::SnapshotChunk;
use crate::proto::storage::SnapshotMetadata;
use crate::test_utils::crate_test_snapshot_stream;
use crate::test_utils::create_test_chunk;
use crate::test_utils::create_test_compressed_snapshot;
use crate::test_utils::node_config;
use crate::test_utils::snapshot_config;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::MOCK_STATE_MACHINE_HANDLER_PORT_BASE;
use crate::AppendResults;
use crate::ConsensusError;
use crate::ElectionError;
use crate::Error;
use crate::MockElectionCore;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::Node;
use crate::SnapshotError;
use crate::StateUpdate;
use crate::StorageError;

// Case 1: normal update
#[test]
fn test_update_pending_case1() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case1")),
        MockSnapshotPolicy::new(),
    );
    handler.update_pending(1);
    assert_eq!(handler.pending_commit(), 1);
    handler.update_pending(10);
    assert_eq!(handler.pending_commit(), 10);
}

// Case 2: new commit < existing commit
#[test]
fn test_update_pending_case2() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case2")),
        MockSnapshotPolicy::new(),
    );
    handler.update_pending(10);
    assert_eq!(handler.pending_commit(), 10);

    handler.update_pending(7);
    assert_eq!(handler.pending_commit(), 10);
}
// Case 3: multi thread update
#[tokio::test]
#[traced_test]
async fn test_update_pending_case3() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case3")),
        MockSnapshotPolicy::new(),
    ));

    let mut tasks = vec![];
    for i in 1..=10 {
        let handler = handler.clone();
        tasks.push(tokio::spawn(async move {
            handler.update_pending(i);
        }));
    }
    futures::future::join_all(tasks).await;
    assert_eq!(handler.pending_commit(), 10);
}

// Case 1: pending commit is zero
#[test]
fn test_pending_range_case1() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case1")),
        MockSnapshotPolicy::new(),
    );
    assert_eq!(handler.pending_range(), None);
}

// Case 2: pending commit <= last_applied
#[test]
fn test_pending_range_case2() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case2")),
        MockSnapshotPolicy::new(),
    );
    handler.update_pending(7);
    handler.update_pending(10);
    assert_eq!(handler.pending_range(), None);
}

// Case 3: pending commit > last_applied
#[test]
fn test_pending_range_case3() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case3")),
        MockSnapshotPolicy::new(),
    );
    handler.update_pending(7);
    handler.update_pending(10);
    handler.update_pending(11);
    assert_eq!(handler.pending_range(), Some(11..=11));
}

#[cfg(test)]
mod apply_chunk_test {

    use super::*;

    fn create_test_handler(
        path: &str,
        apply_chunk_error: bool,
        last_applied_index: Option<u64>,
    ) -> DefaultStateMachineHandler<MockTypeConfig> {
        let mut state_machine = MockStateMachine::new();
        if apply_chunk_error {
            state_machine
                .expect_apply_chunk()
                .returning(|_| Err(Error::Fatal("Test error".to_string())));
        } else {
            state_machine.expect_apply_chunk().returning(|_| Ok(()));
        }
        DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            last_applied_index.unwrap_or(0),
            1,
            Arc::new(state_machine),
            snapshot_config(PathBuf::from(path)),
            MockSnapshotPolicy::new(),
        )
    }

    #[tokio::test]
    async fn test_apply_chunk_updates_last_applied_case1() {
        let handler = create_test_handler(
            "/tmp/test_apply_chunk_updates_last_applied_case1",
            false,
            None,
        );

        // Initial last_applied value
        assert_eq!(handler.last_applied(), 0);

        // Create a test chunk with index 100
        let chunk: Vec<Entry> = vec![
            Entry {
                index: 90,
                term: 1,
                payload: None,
            },
            Entry {
                index: 100,
                term: 1,
                payload: None,
            },
        ];

        // Apply the chunk
        let result = handler.apply_chunk(chunk).await;

        // Verify last_applied was updated
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 100);
    }

    #[tokio::test]
    async fn test_apply_chunk_updates_last_applied_case2() {
        let handler = create_test_handler(
            "/tmp/test_apply_chunk_updates_last_applied_case2",
            false,
            None,
        );

        // Initial last_applied value
        assert_eq!(handler.last_applied(), 0);

        // Create a test chunk with index 50
        let chunk = vec![
            Entry {
                index: 50,
                term: 1,
                payload: None,
            },
            Entry {
                index: 70,
                term: 1,
                payload: None,
            },
        ];

        // Apply the chunk
        let result = handler.apply_chunk(chunk).await;

        // Verify last_applied was updated to the higher index
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 70);
    }
    #[tokio::test]
    async fn test_apply_chunk_handles_empty_chunk() {
        let handler =
            create_test_handler("/tmp/test_apply_chunk_handles_empty_chunk", false, Some(2));

        // Initial last_applied value
        let chunk = vec![
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
            Entry {
                index: 2,
                term: 1,
                payload: None,
            },
        ];
        let result = handler.apply_chunk(chunk).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 2);

        // Create an empty chunk
        let chunk = vec![];

        // Apply the empty chunk
        let result = handler.apply_chunk(chunk).await;

        // Verify last_applied was updated
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 2);
    }
    #[tokio::test]
    async fn test_apply_chunk_with_state_machine_io_error() {
        let handler = create_test_handler(
            "/tmp/test_apply_chunk_with_state_machine_io_error",
            true,
            None,
        );

        // Initial last_applied value
        assert_eq!(handler.last_applied(), 0);

        // Create first chunk with index 50
        let chunk1 = vec![Entry {
            index: 50,
            term: 1,
            payload: None,
        }];

        // Apply first chunk
        let result1 = handler.apply_chunk(chunk1).await;
        assert!(result1.is_err());
        assert_eq!(handler.last_applied(), 0);
    }
}

fn listen_addr(port: u32) -> SocketAddr {
    format!("127.0.0.1:{port}",).parse().unwrap()
}

/// Case1: Complete successful snapshot installation
#[tokio::test]
#[traced_test]
async fn test_install_snapshot_case1() {
    let port = MOCK_STATE_MACHINE_HANDLER_PORT_BASE + 1;
    // 1. Simulate node with RPC server running in a new thread
    let (graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node_with_rpc_service(
        "/tmp/test_install_snapshot_case1",
        listen_addr(port),
        false,
        graceful_rx,
        None,
    );
    node.set_ready(true);

    // 3. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let mut raft = node.raft_core.lock().await;
        let _ = time::timeout(Duration::from_millis(100), raft.run()).await;
    });

    // 2. Crate RPC client
    let addr: SocketAddr = format!("[::]:{port}",).parse().unwrap();
    let mut rpc_client = SnapshotServiceClient::connect(format!(
        "grpc://localhost:{}",
        addr.to_string().split(':').next_back().unwrap()
    ))
    .await
    .unwrap();

    // 3. Fake install snapshot request stream
    let total_chunks = 3;
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    // Generate valid chunks with seq 0..2
    let h = tokio::spawn(async move {
        for seq in 0..total_chunks {
            let chunk = create_test_chunk(
                seq,
                &format!("chunk-{seq}",).into_bytes(),
                3, // chunk term (higher than handler's current_term)
                1, // leader_id
                total_chunks,
            );

            tx.send(chunk).await.expect("send failed");
        }
    });
    // Convert mpsc receiver into tonic::Streaming
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // 4. Waiting to receive response
    tokio::time::sleep(Duration::from_millis(20)).await;

    let response = rpc_client.install_snapshot(request_stream).await.unwrap().into_inner();

    assert!(response.success);
    assert_eq!(response.term, 1); // Should reflect handler's current_term
    assert_eq!(response.next_chunk, 0); // Indicates full success

    // Release handler
    graceful_tx.send(()).expect("shutdown successfully!");
    raft_handle.await.expect("should succeed");
    h.await.unwrap();
}

// Helper to create test handler
fn create_test_handler(
    temp_dir: &Path,
    chunk_size: Option<usize>,
) -> DefaultStateMachineHandler<MockTypeConfig> {
    let state_machine = MockStateMachine::new();
    let mut config = snapshot_config(temp_dir.to_path_buf());
    config.chunk_size = chunk_size.unwrap_or(1024); // Default chunk size

    DefaultStateMachineHandler::new(
        1,
        0,
        1,
        Arc::new(state_machine),
        config,
        MockSnapshotPolicy::new(),
    )
}

/// # Case 2: Successfully applies valid chunks
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case2");
    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock
        .expect_apply_snapshot_from_file()
        .times(1)
        .returning(|_, _| Ok(()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(temp_path.to_path_buf()),
        MockSnapshotPolicy::new(),
    );
    // 3. Fake install snapshot request stream
    let total_chunks = 1;
    // Create compressed chunk data
    let mut chunks: Vec<SnapshotChunk> = vec![];
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 2, term: 1 }),
        checksum: vec![2; 32],
    };

    // Create test data
    tokio::fs::create_dir_all(&temp_path).await.unwrap();
    let data_file = temp_path.join("test.txt");
    tokio::fs::write(&data_file, "test content").await.unwrap();

    // Compress to tar.gz
    let compressed_path = temp_path.join("snapshot.tar.gz");
    let file = File::create(&compressed_path).await.unwrap();
    let gzip_encoder = async_compression::tokio::write::GzipEncoder::new(file);
    let mut tar_builder = tokio_tar::Builder::new(gzip_encoder);
    let file_name_in_tar = "test.txt";
    tar_builder.append_path_with_name(&data_file, file_name_in_tar).await.unwrap();
    tar_builder.finish().await.unwrap();
    let mut gzip_encoder = tar_builder.into_inner().await.unwrap();
    gzip_encoder.shutdown().await.unwrap();

    // Read compressed data
    let compressed_data = tokio::fs::read(&compressed_path).await.unwrap();
    let chunk = SnapshotChunk {
        leader_term: 1,
        leader_id: 1,
        metadata: Some(metadata.clone()),
        seq: 0,
        total_chunks,
        data: compressed_data.clone(),
        chunk_checksum: crc32fast::hash(&compressed_data).to_be_bytes().to_vec(),
    };
    chunks.push(chunk);

    let streaming_request = crate_test_snapshot_stream(chunks);
    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);

    // Spawn the handler in a separate task to prevent deadlock
    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(1, Box::new(streaming_request), ack_tx, &config)
                .await
        }
    });

    // Verify intermediate response
    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Accepted as i32);

    // Ensure handler completes successfully
    assert!(handler_task.await.unwrap().is_ok());
}
const TEST_TERM: u64 = 1;
const TEST_LEADER_ID: u32 = 1;

/// # Case 3: Rejects chunk with invalid checksum
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case3() {
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case3");
    create_dir_all(&temp_path).await.unwrap();

    let handler = create_test_handler(&temp_path, None);

    // Create chunk with invalid checksum
    let mut bad_chunk = create_test_chunk(0, b"bad data", TEST_TERM, TEST_LEADER_ID, 1);
    bad_chunk.chunk_checksum = vec![0xde, 0xad, 0xbe, 0xef]; // Corrupt checksum

    // Create ACK channel
    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);
    let stream = crate_test_snapshot_stream(vec![bad_chunk]);

    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(TEST_TERM, Box::new(stream), ack_tx, &config)
                .await
        }
    });

    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::ChecksumMismatch as i32);

    assert!(matches!(
        handler_task.await,
        Ok(Err(Error::Consensus(ConsensusError::Snapshot(SnapshotError::OperationFailed(msg)))))
            if msg == "Checksum validation failed"));
}

/// # Case 4: Aborts when leader changes during stream
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case4() {
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case4");
    create_dir_all(&temp_path).await.unwrap();

    let handler = create_test_handler(&temp_path, None);

    // First chunk with term 1, second with term 2
    let chunks = vec![
        create_test_chunk(0, b"chunk0", TEST_TERM, TEST_LEADER_ID, 2),
        create_test_chunk(1, b"chunk1", TEST_TERM + 1, TEST_LEADER_ID, 2),
    ];

    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);
    let stream = crate_test_snapshot_stream(chunks);

    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(TEST_TERM, Box::new(stream), ack_tx, &config)
                .await
        }
    });

    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Accepted as i32);
    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::OutOfOrder as i32);

    // Verify handler completes successfully
    assert!(matches!(
        handler_task.await,
        Ok(Err(Error::Consensus(ConsensusError::Snapshot(SnapshotError::OperationFailed(msg)))))
            if msg == "Leader changed during transfer"));
}

/// # Case 5: Handles stream errors gracefully
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case5() {
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case5");
    create_dir_all(&temp_path).await.unwrap();

    let handler = create_test_handler(&temp_path, None);

    // Create stream that returns error after first chunk
    // But we specify the total chunks is 2
    let chunks = vec![create_test_chunk(
        0,
        b"chunk0",
        TEST_TERM,
        TEST_LEADER_ID,
        2,
    )];
    let stream = crate_test_snapshot_stream(chunks);
    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);

    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(TEST_TERM, Box::new(stream), ack_tx, &config)
                .await
        }
    });

    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Accepted as i32);
    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Failed as i32);

    // Verify handler completes successfully
    assert!(handler_task.await.unwrap().is_err());
}

/// # Case 6: Rejects chunks with missing metadata
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case6() {
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case6");
    create_dir_all(&temp_path).await.unwrap();

    let handler = create_test_handler(&temp_path, None);

    // First chunk missing metadata
    let mut invalid_chunk = create_test_chunk(0, b"data", TEST_TERM, TEST_LEADER_ID, 1);
    invalid_chunk.metadata = None;

    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);
    let stream = crate_test_snapshot_stream(vec![invalid_chunk]);

    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(TEST_TERM, Box::new(stream), ack_tx, &config)
                .await
        }
    });

    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Failed as i32);

    // Verify handler completes successfully
    assert!(handler_task.await.unwrap().is_err());
}

/// # Case 7: Handles successful snapshot stream with multiple chunks
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_case7() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_apply_snapshot_stream_from_leader_case7");
    create_dir_all(&temp_path).await.unwrap();

    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock
        .expect_apply_snapshot_from_file()
        .times(1)
        .returning(|_, _| Ok(()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(temp_path.to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    // Create a proper compressed snapshot for testing
    let (compressed_data, metadata) = create_test_compressed_snapshot().await;

    // Create multiple chunks for the snapshot stream
    // Split compressed data into chunks
    let total_chunks = 3;
    let chunk_size = compressed_data.len().div_ceil(total_chunks); // Ceiling division

    let mut chunks: Vec<SnapshotChunk> = vec![];

    for seq in 0..total_chunks {
        let start = seq * chunk_size;
        let end = std::cmp::min(compressed_data.len(), (seq + 1) * chunk_size);
        let chunk_data = compressed_data[start..end].to_vec();

        let chunk = SnapshotChunk {
            leader_term: 1,
            leader_id: 1,
            metadata: if seq == 0 {
                Some(metadata.clone())
            } else {
                None
            },
            seq: seq as u32,
            total_chunks: total_chunks as u32,
            data: chunk_data.clone(),
            chunk_checksum: crc32fast::hash(&chunk_data).to_be_bytes().to_vec(),
        };
        chunks.push(chunk);
    }

    let streaming_request = crate_test_snapshot_stream(chunks);
    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);

    // Spawn the handler in a separate task
    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(1, Box::new(streaming_request), ack_tx, &config)
                .await
        }
    });

    // Verify intermediate ACKs
    for seq in 0..total_chunks {
        let ack = ack_rx.recv().await.unwrap();
        assert_eq!(ack.seq, seq as u32);
        assert_eq!(ack.status, ChunkStatus::Accepted as i32);
        assert_eq!(ack.next_requested, seq as u32 + 1);
    }

    // Ensure handler completes successfully
    let handler_result = handler_task.await;
    println!("handler_task.await: {handler_result:?}");
    assert!(handler_result.unwrap().is_ok());
}
mod create_snapshot_tests {
    use super::*;
    use crate::NewCommitData;
    use crate::LEADER;
    /// # Case 1: Basic creation flow
    #[tokio::test]
    async fn test_create_snapshot_case1() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_create_snapshot_case1");
        let mut sm = MockStateMachine::new();

        // Mock state machine behavior
        let mut seq = Sequence::new();
        sm.expect_last_applied()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| LogId { index: 5, term: 1 });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data()
            .times(1)
            .withf(|path, last_included| {
                debug!(?path, ?last_included);
                // Create the directory structure correctly
                fs::create_dir_all(path.clone()).unwrap();
                //Simulate sled to create a subdirectory
                let db_path = path.join("state_machine");
                fs::create_dir(&db_path).unwrap();

                path.ends_with("temp-5-1") && last_included.index == 5 && last_included.term == 1
            })
            .returning(|_, _| Ok([0; 32]));

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            1,
            Arc::new(sm),
            config,
            MockSnapshotPolicy::new(),
        );

        // Execute snapshot creation
        let result = handler.create_snapshot().await;

        debug!(?result);

        assert!(result.is_ok());

        // Verify file system changes
        let (metadata, final_path) = result.unwrap();
        debug!(?final_path);
        assert!(final_path.is_file());
        assert!(final_path.extension().unwrap() == "gz");
        assert!(final_path
            .to_str()
            .unwrap()
            .contains(&format!("{SNAPSHOT_DIR_PREFIX}5-1.tar.gz",)));

        assert_eq!(metadata.last_included, Some(LogId { term: 1, index: 5 }));
    }

    /// # Case 2: Test concurrent protection
    #[tokio::test]
    async fn test_create_snapshot_case2() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_create_snapshot_case2");
        let mut sm = MockStateMachine::new();

        // Use Mutex for safe shared state
        let attempt_counter = Arc::new(std::sync::Mutex::new(0));
        let counter_clone = attempt_counter.clone();

        // Setup slow snapshot generation
        let (tx, _rx) = tokio::sync::oneshot::channel();
        sm.expect_last_applied().returning(|| LogId { term: 1, index: 1 });
        sm.expect_snapshot_metadata().returning(move || {
            Some(SnapshotMetadata {
                last_included: Some(LogId { index: 1, term: 1 }),
                checksum: [1; 8].to_vec(),
            })
        });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data().times(1..=2).returning(move |path, _| {
            // Track invocation count
            let mut count = counter_clone.lock().unwrap();
            *count += 1;

            // Only succeed on first attempt
            if *count == 1 {
                debug!(?path, "generate_snapshot_data");
                fs::create_dir_all(path.clone()).unwrap();
                let db_path = path.join("state_machine");
                fs::create_dir(&db_path).unwrap();
                Ok([0; 32])
            } else {
                Err(SnapshotError::OperationFailed("Concurrency failure".into()).into())
            }
        });

        let mut snapshot_policy = MockSnapshotPolicy::new();
        snapshot_policy.expect_should_trigger().returning(|_| true);

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            1,
            Arc::new(sm),
            config,
            snapshot_policy,
        ));
        tx.send(()).unwrap(); // Unblock the first task

        // Spawn concurrent snapshot creations
        let h1 = handler.clone();
        let t1 = tokio::spawn(async move { h1.create_snapshot().await });

        let h2 = handler.clone();
        let t2 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            h2.create_snapshot().await
        });

        // Wait for both tasks with timeout
        let results = tokio::time::timeout(
            Duration::from_secs(5),
            futures::future::join_all(vec![t1, t2]),
        )
        .await
        .expect("Test timed out");
        println!("{:?}", &results);

        // Verify only one successful creation
        let success_count = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
        assert_eq!(
            success_count, 1,
            "Expected exactly one successful snapshot creation"
        );
        assert_eq!(count_snapshots(&temp_path), 1);

        // Verify flag is reset regardless of task outcome
        let ctx = NewCommitData {
            role: LEADER,
            current_term: 1,
            new_commit_index: 100,
        };
        assert!(handler.should_snapshot(ctx));
    }

    /// # Case 3: Test cleanup old versions
    #[tokio::test]
    #[traced_test]
    async fn test_create_snapshot_case3() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_create_snapshot_case3");

        let mut sm = MockStateMachine::new();
        let mut count = 0;
        sm.expect_last_applied().returning(move || {
            count += 1;
            LogId {
                term: 1,
                index: count,
            }
        });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data().returning(|path, _| {
            debug!(?path, "expect_generate_snapshot_data");
            std::fs::create_dir_all(path).expect("Failed to create directory");
            // let _new_db = init_sled_state_machine_db(path).expect("");
            Ok([0; 32])
        });
        let snapshot_dir = temp_path.to_path_buf();

        let mut config = snapshot_config(snapshot_dir.clone());
        config.retained_log_entries = 0;

        let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            3, // Current version
            1,
            Arc::new(sm),
            config,
            MockSnapshotPolicy::new(),
        );

        // Create new snapshot (version 4)
        handler.create_snapshot().await.unwrap();
        handler.create_snapshot().await.unwrap();
        handler.create_snapshot().await.unwrap();
        handler.create_snapshot().await.unwrap();

        // Verify cleanup results
        let remaining: HashSet<u64> =
            get_snapshot_versions(snapshot_dir.as_path()).into_iter().collect();
        assert_eq!(remaining, [4, 3].into_iter().collect());

        // Verify files are compressed
        for version in &[3, 4] {
            let path = snapshot_dir.join(format!("{SNAPSHOT_DIR_PREFIX}{version}-1.tar.gz",));
            assert!(path.is_file(), "Snapshot file not found: {path:?}",);
        }
    }

    /// # Case 4: Test failure handling
    #[tokio::test]
    async fn test_create_snapshot_case4() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_create_snapshot_case4");
        let mut sm = MockStateMachine::new();

        // Setup failing snapshot generation
        sm.expect_last_applied().returning(|| LogId { term: 1, index: 1 });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data()
            .returning(|_, _| Err(SnapshotError::OperationFailed("test failure".into()).into()));

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            1,
            Arc::new(sm),
            config,
            MockSnapshotPolicy::new(),
        );

        // Attempt snapshot creation
        let result = handler.create_snapshot().await;
        assert!(result.is_err());

        // Verify no files created
        assert_eq!(count_snapshots(&temp_path), 0);
    }

    /// # Case 5: Test snapshot_in_progress flag is reset on success
    #[tokio::test]
    async fn test_create_snapshot_resets_flag_on_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_flag_reset_success");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId { term: 1, index: 5 });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data().returning(|path, _| {
            fs::create_dir_all(path).unwrap();
            Ok([0; 32])
        });

        let config = snapshot_config(temp_path);
        let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            1,
            Arc::new(sm),
            config,
            MockSnapshotPolicy::new(),
        ));

        // Verify initial state
        assert!(!handler.snapshot_in_progress());

        let handler_clone = handler.clone();
        let result = handler_clone.create_snapshot().await;
        assert!(result.is_ok());

        // Critical assertion: Flag must be reset after success
        assert!(!handler.snapshot_in_progress());
    }

    /// # Case 6: Test snapshot_in_progress flag is reset on error
    #[tokio::test]
    async fn test_create_snapshot_resets_flag_on_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_flag_reset_error");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId { term: 1, index: 1 });
        sm.expect_entry_term().returning(|_| Some(1));
        sm.expect_generate_snapshot_data()
            .returning(|_, _| Err(SnapshotError::OperationFailed("test error".into()).into()));

        let config = snapshot_config(temp_path);
        let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            1,
            Arc::new(sm),
            config,
            MockSnapshotPolicy::new(),
        ));

        // Verify initial state
        assert!(!handler.snapshot_in_progress());

        let handler_clone = handler.clone();
        let result = handler_clone.create_snapshot().await;
        assert!(result.is_err());

        // Critical assertion: Flag must be reset even after failure
        assert!(!handler.snapshot_in_progress());
    }
}

// Helper functions
fn count_snapshots(dir: &Path) -> usize {
    debug!(?dir, "count_snapshots");

    // If the directory does not exist or cannot be accessed, just return 0
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };

    entries
        .filter_map(|entry| {
            // Ignore directory entries that cannot be read
            let entry = entry.ok()?;
            // Extract the file name and check the prefix
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.starts_with(SNAPSHOT_DIR_PREFIX).then_some(()))
        })
        .count()
}

fn get_snapshot_versions(dir: &Path) -> Vec<u64> {
    debug!(?dir, "get_snapshot_versions");

    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            debug!(%name, "get_snapshot_versions");

            // Handle compressed snapshots
            if name.ends_with(".tar.gz") {
                let base_name = name.trim_end_matches(".tar.gz");
                base_name.split('-').nth(1).and_then(|v| v.parse().ok())
            }
            // Handle legacy directories (if any)
            else {
                name.split('-').nth(1).and_then(|v| v.parse().ok())
            }
        })
        .collect()
}

/// # Case 1: Test normal deletion
#[tokio::test]
#[traced_test]
async fn test_cleanup_snapshot_case1() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();

    create_test_files(&temp_dir, &[1, 2, 3]).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    handler.cleanup_snapshot(2, temp_dir.path()).await.unwrap();

    // Verify remaining snapshots
    let mut remaining = get_snapshot_versions(temp_dir.path());
    remaining.sort();
    let mut expect = vec![2, 3];
    expect.sort();
    assert_eq!(remaining, expect);

    // Verify files are compressed
    for version in &[2, 3] {
        let path = temp_dir.path().join(format!("{SNAPSHOT_DIR_PREFIX}{version}-1.tar.gz"));
        assert!(path.is_file(), "Snapshot file not found: {path:?}",);
    }
}

/// # Case 2: Test no old versions to be cleaned
#[tokio::test]
#[traced_test]
async fn test_cleanup_snapshot_case2() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    create_test_files(&temp_dir, &[3, 4]).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    handler.cleanup_snapshot(2, temp_dir.path()).await.unwrap();

    // Verify no deletions
    let mut remaining = get_snapshot_versions(temp_dir.path());
    remaining.sort();
    let mut expect = vec![3, 4];
    expect.sort();
    assert_eq!(remaining, expect);
}

/// # Case 3: Test invalid dirnames
#[tokio::test]
#[traced_test]
async fn test_cleanup_snapshot_case3() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    // Create valid and invalid directories
    create_dir(&temp_dir, &format!("{SNAPSHOT_DIR_PREFIX}1-1",)).await;
    create_dir(&temp_dir, "invalid_format").await;
    create_dir(&temp_dir, &format!("{SNAPSHOT_DIR_PREFIX}bad-2-2",)).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    handler.cleanup_snapshot(2, temp_dir.path()).await.unwrap();

    //Verify only valid version 1 is deleted
    let remaining = get_dir_names(temp_dir.path()).await;
    debug!(?remaining);

    assert!(remaining.contains(&"invalid_format".into()));
    assert!(remaining.contains(&format!("{SNAPSHOT_DIR_PREFIX}bad-2-2",)));
    assert!(remaining.contains(&format!("{SNAPSHOT_DIR_PREFIX}1-1",)));
}

/// #Case 1: Reject stale term
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case1() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5, // last_applied
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 3,
        leader_id: 1,
        last_included,
        snapshot_checksum: [1u8; 32].to_vec(),
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(
            5,
            Some(1),
            last_included,
            &req,
            &Arc::new(MockRaftLog::new()),
        )
        .await
        .unwrap();

    assert!(!res.success);
    assert_eq!(res.term, 5);
}

/// # Case 2: Reject if not from current leader
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case2() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 2,
        last_included,
        snapshot_checksum: vec![],
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(
            5,
            Some(1),
            last_included,
            &req,
            &Arc::new(MockRaftLog::new()),
        )
        .await
        .unwrap();

    assert!(!res.success);
}

// # Case 3: Reject if local state is behind
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case3() {
    let temp_dir = TempDir::new().unwrap();
    let mut sm = MockStateMachine::new();
    sm.expect_last_applied().returning(|| LogId { index: 3, term: 1 }); // last applied is 3

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        3,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 1,
        last_included,
        snapshot_checksum: vec![],
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(
            5,
            Some(1),
            last_included,
            &req,
            &Arc::new(MockRaftLog::new()),
        )
        .await
        .unwrap();

    assert!(!res.success);
}

/// # Case 4: Reject on checksum mismatch
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case4() {
    let temp_dir = TempDir::new().unwrap();
    let mut sm = MockStateMachine::new();
    let mut correct_checksum = [0u8; 32];
    correct_checksum[..3].copy_from_slice(&[1, 2, 3]);
    create_test_snapshot(&mut sm, 5, 1, correct_checksum).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let mut wrong_checksum = [0u8; 32];
    wrong_checksum[..3].copy_from_slice(&[4, 5, 6]);
    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 1,
        last_included,
        snapshot_checksum: wrong_checksum.to_vec(), // mismatch
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(
            5,
            Some(1),
            last_included,
            &req,
            &Arc::new(MockRaftLog::new()),
        )
        .await
        .unwrap();

    assert!(!res.success);
}

/// # Case 5: Successful purge
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case5() {
    let temp_dir = TempDir::new().unwrap();
    let mut sm = MockStateMachine::new();
    let expected_checksum = [1u8; 32];
    create_test_snapshot(&mut sm, 5, 1, expected_checksum).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_purge_logs_up_to()
        .with(eq(LogId { index: 5, term: 1 }))
        .times(1)
        .returning(|_| Ok(()));

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 1,
        last_included,
        snapshot_checksum: expected_checksum.to_vec(),
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(raft_log))
        .await
        .unwrap();

    assert!(res.success);
}

/// # Case 6: Handle storage errors during purge
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case6() {
    let temp_dir = TempDir::new().unwrap();
    let mut sm = MockStateMachine::new();

    let mut expected_checksum = [1u8; 32];
    expected_checksum[..3].copy_from_slice(&[1, 2, 3]);
    create_test_snapshot(&mut sm, 5, 1, expected_checksum).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_purge_logs_up_to().returning(|_| {
        Err(StorageError::DbError("expect_purge_logs_up_to failed".to_string()).into())
    });

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 1,
        last_included,
        snapshot_checksum: expected_checksum.to_vec(),
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(raft_log))
        .await
        .unwrap();

    assert!(!res.success);
}

/// # Case 7: Reject when no local snapshot exists
#[tokio::test]
#[traced_test]
async fn test_handle_purge_request_case7() {
    let temp_dir = TempDir::new().unwrap();
    let mut sm = MockStateMachine::new();
    create_test_snapshot(&mut sm, 5, 1, [0_u8; 32]).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        5,
        100,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    let last_included = Some(LogId { index: 5, term: 1 });
    let req = PurgeLogRequest {
        term: 5,
        leader_id: 1,
        last_included,
        snapshot_checksum: vec![1, 2, 3],
        leader_commit: 1,
    };

    let res = handler
        .handle_purge_request(
            5,
            Some(1),
            last_included,
            &req,
            &Arc::new(MockRaftLog::new()),
        )
        .await
        .unwrap();

    assert!(!res.success);
}

// Helper to create test snapshots
async fn create_test_snapshot(
    sm: &mut MockStateMachine,
    index: u64,
    term: u64,
    checksum: [u8; 32],
) {
    sm.expect_last_applied().returning(move || LogId { index, term });
    sm.expect_snapshot_metadata().returning(move || {
        Some(SnapshotMetadata {
            last_included: Some(LogId { term, index }),
            checksum: checksum.to_vec(),
        })
    });
}

async fn create_test_files(
    temp_dir: &TempDir,
    ids: &[u64],
) {
    for id in ids {
        let file_name = format!("{SNAPSHOT_DIR_PREFIX}{id}-1.tar.gz");
        let path = temp_dir.path().join(file_name);
        debug!(?path, "create_test_files");
        let mut file = File::create(&path).await.unwrap();
        // Write some dummy content
        file.write_all(b"dummy snapshot data").await.unwrap();
    }
}

async fn create_dir(
    temp_dir: &TempDir,
    name: &str,
) {
    let path = temp_dir.path().join(name);
    tokio::fs::create_dir_all(&path).await.unwrap();
}

async fn get_dir_names(path: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut entries = tokio::fs::read_dir(path).await.unwrap();

    while let Some(entry) = entries.next_entry().await.unwrap() {
        if let Some(name) = entry.file_name().to_str() {
            names.push(name.to_owned());
        }
    }
    names
}

fn mock_node_with_rpc_service(
    db_path: &str,
    listen_address: SocketAddr,
    is_leader: bool,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Arc<Node<MockTypeConfig>> {
    let mut node_config = node_config(db_path);
    if let Some(peers_meta) = peers_meta_option {
        node_config.cluster.initial_cluster = peers_meta;
    }

    // Update listen address with passed one
    node_config.cluster.listen_address = listen_address;
    if !is_leader {
        // Make sure no election happens
        node_config.raft.election.election_timeout_min = 500000;
        node_config.raft.election.election_timeout_max = 1000000
    }

    // Initializing Shutdown Signal
    let mut replication_handler = MockReplicationCore::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .returning(|_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(),
            })
        });
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| true);
    if is_leader {
        election_handler
            .expect_broadcast_vote_requests()
            .returning(|_, _, _, _, _| Ok(()));
        election_handler
            .expect_handle_vote_request()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(StateUpdate {
                    new_voted_for: Some(VotedFor {
                        voted_for_id: 1,
                        voted_for_term: 1,
                    }),
                    term_update: Some(2),
                })
            });
    } else {
        // Make sure node is Follower
        election_handler.expect_broadcast_vote_requests().returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Election(
                ElectionError::HigherTerm(100),
            )))
        });
    }
    // let state_machine_handler = Arc::new(state_machine_handler);
    let mut mock_state_machine_handler = MockStateMachineHandler::new();
    mock_state_machine_handler.expect_apply_snapshot_stream_from_leader().returning(
        move |_current_term, _stream_request, ack_tx, _| {
            let ack_tx = ack_tx.clone();
            tokio::spawn(async move {
                // Send final ack
                let final_ack = SnapshotAck {
                    status: ChunkStatus::Accepted as i32,
                    seq: u32::MAX,
                    next_requested: 0,
                };
                ack_tx.send(final_ack).await.ok();
            });
            Ok(())
        },
    );

    MockBuilder::new(shutdown_signal)
        .wiht_node_config(node_config)
        .with_replication_handler(replication_handler)
        .with_election_handler(election_handler)
        .with_state_machine_handler(mock_state_machine_handler)
        .turn_on_election(is_leader)
        .build_node_with_rpc_server()
}

// Update test_load_snapshot_data_case1_single_file_single_chunk
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case1_single_file_single_chunk() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create compressed snapshot file
    let snapshot_file = snapshot_dir.join("snapshot-1-1.tar.gz");
    debug!(?snapshot_file, "prepared test snapshot_file");
    let content = b"Hello World";
    tokio::fs::write(&snapshot_file, content).await.unwrap();

    let handler = create_test_handler(temp_dir.path(), None);
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 1, term: 1 }),
        checksum: vec![1; 32],
    };

    // Get snapshot stream
    let mut stream = handler
        .load_snapshot_data(metadata.clone())
        .await
        .expect("Should create stream");

    // Collect all chunks
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk.unwrap());
    }

    // Verify chunks
    assert_eq!(chunks.len(), 1, "Should have one chunk");
    let chunk = &chunks[0];
    assert_eq!(chunk.leader_term, 1);
    assert_eq!(chunk.leader_id, 1);
    assert_eq!(chunk.seq, 0);
    assert_eq!(chunk.total_chunks, 1);
    assert_eq!(chunk.data, content);
    assert_eq!(
        chunk.chunk_checksum,
        crc32fast::hash(content).to_be_bytes().to_vec()
    );
    assert_eq!(chunk.metadata, Some(metadata));
}

// Update test_load_snapshot_data_case2_single_file_multi_chunk
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case2_single_file_multi_chunk() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create large compressed file (3 chunks of 4 bytes each)
    let data = b"1234567890ABCDEF";
    let snapshot_file = snapshot_dir.join("snapshot-2-1.tar.gz");
    tokio::fs::write(&snapshot_file, data).await.unwrap();

    // Handler with small chunk size
    let handler = create_test_handler(temp_dir.path(), Some(4));

    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 2, term: 1 }),
        checksum: vec![2; 32],
    };

    // Get and collect chunks
    let mut stream = handler.load_snapshot_data(metadata.clone()).await.unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk.unwrap());
    }

    // Verify chunk count (16 bytes / 4 = 4 chunks)
    assert_eq!(chunks.len(), 4);

    // Verify sequence numbers and metadata
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.seq, i as u32);
        assert_eq!(chunk.total_chunks, 4);
        // Only first chunk should have metadata
        if i == 0 {
            assert_eq!(chunk.metadata, Some(metadata.clone()));
        } else {
            assert!(chunk.metadata.is_none());
        }
    }

    // Reassemble data
    let mut reassembled = Vec::new();
    for chunk in &chunks {
        reassembled.extend_from_slice(&chunk.data);
    }

    assert_eq!(reassembled, data);
}

// Update test_load_snapshot_data_case3_multiple_files
// This test is no longer relevant since we now have a single compressed file
// Remove or replace with a test for compressed file containing multiple files

// Update test_load_snapshot_data_case4_empty_snapshot
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case4_empty_snapshot() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create empty compressed file
    let snapshot_file = snapshot_dir.join("snapshot-4-1.tar.gz");
    debug!(?snapshot_file);
    File::create(&snapshot_file).await.unwrap();

    let handler = create_test_handler(temp_dir.path(), None);
    let metadata = SnapshotMetadata {
        last_included: None,
        checksum: vec![],
    };

    assert!(handler.load_snapshot_data(metadata).await.is_err());
}

// Update test_load_snapshot_data_case5_checksum
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case5_checksum() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create compressed file
    let content = b"Validate me";
    let snapshot_file = snapshot_dir.join("snapshot-5-1.tar.gz");
    tokio::fs::write(&snapshot_file, content).await.unwrap();

    let handler = create_test_handler(temp_dir.path(), None);
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 5, term: 1 }),
        checksum: vec![5; 32],
    };

    let mut stream = handler.load_snapshot_data(metadata).await.unwrap();
    let chunk = stream.next().await.unwrap().unwrap();

    let expected_checksum = crc32fast::hash(content).to_be_bytes().to_vec();
    assert_eq!(chunk.chunk_checksum, expected_checksum);
}

// Update test_load_snapshot_data_case6_read_error
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case6_read_error() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create invalid file (directory with same name)
    let snapshot_file = snapshot_dir.join("snapshot-6-1.tar.gz");
    tokio::fs::create_dir(&snapshot_file).await.unwrap();

    let handler = create_test_handler(temp_dir.path(), None);
    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 6, term: 1 }),
        checksum: vec![6; 32],
    };

    let result = handler.load_snapshot_data(metadata).await;
    assert!(result.is_err());
}

// Add new test for metadata in first chunk only
#[tokio::test]
#[traced_test]
async fn test_load_snapshot_data_case7_metadata_in_first_chunk_only() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path();
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Create compressed file
    let content = b"Test content for multiple chunks";
    let snapshot_file = snapshot_dir.join("snapshot-7-1.tar.gz");
    tokio::fs::write(&snapshot_file, content).await.unwrap();

    // Handler with small chunk size
    let handler = create_test_handler(temp_dir.path(), Some(10));

    let metadata = SnapshotMetadata {
        last_included: Some(LogId { index: 7, term: 1 }),
        checksum: vec![7; 32],
    };

    // Get and collect chunks
    let mut stream = handler.load_snapshot_data(metadata.clone()).await.unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk.unwrap());
    }

    // Verify metadata only in first chunk
    assert_eq!(chunks.len(), 4);
    assert_eq!(chunks[0].metadata, Some(metadata));
    for chunk in &chunks[1..] {
        assert!(chunk.metadata.is_none());
    }
}

// Add new test for compression functionality
#[tokio::test]
#[traced_test]
async fn test_snapshot_compression() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_snapshot_compression");
    let mut sm = MockStateMachine::new();

    // Mock state machine to create test data
    sm.expect_last_applied().returning(|| LogId { index: 10, term: 2 });
    sm.expect_entry_term().returning(|_| Some(2));
    sm.expect_generate_snapshot_data().returning(|path, _| {
        // Create the directory structure correctly
        fs::create_dir_all(path.clone()).unwrap();
        // Create test files in the temp directory
        let file1 = path.join("test1.txt");
        let file2 = path.join("test2.bin");

        std::fs::write(&file1, "This is a test file").unwrap();
        std::fs::write(&file2, vec![0u8; 1024]).unwrap();

        Ok([0; 32])
    });

    let mut config = snapshot_config(temp_path.to_path_buf());
    config.retained_log_entries = 0;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        config,
        MockSnapshotPolicy::new(),
    );

    // Create snapshot
    let (_, snapshot_path) = handler.create_snapshot().await.unwrap();

    // Verify compressed file exists
    assert!(snapshot_path.is_file());
    assert_eq!(snapshot_path.extension().unwrap(), "gz");

    // Verify file size is smaller than uncompressed (at least 50% smaller)
    let uncompressed_size = 1024 + "This is a test file".len();
    let compressed_size = std::fs::metadata(&snapshot_path).unwrap().len() as usize;
    assert!(
        compressed_size < uncompressed_size / 2,
        "Compression ineffective: {} > {}",
        compressed_size,
        uncompressed_size / 2
    );
}

/// Test that the state machine receives the decompressed directory, not the compressed file.
#[tokio::test]
#[traced_test]
async fn test_apply_snapshot_stream_from_leader_decompresses_before_apply() {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_decompress_before_apply");
    let mut state_machine_mock = MockStateMachine::new();

    // Expect apply_snapshot_from_file to be called with a directory path (decompressed)
    state_machine_mock
        .expect_apply_snapshot_from_file()
        .times(1)
        .withf(|metadata, path| {
            // Check that the path is a directory (decompressed) and not a file
            path.is_dir() && metadata.last_included == Some(LogId { index: 5, term: 1 })
        })
        .returning(|_, _| Ok(()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(temp_path.to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    // Create a compressed snapshot for testing
    let (compressed_data, metadata) = create_test_compressed_snapshot().await;
    let chunk_checksum = crc32fast::hash(&compressed_data).to_be_bytes().to_vec();
    // Create a single chunk for the snapshot stream
    let chunk = SnapshotChunk {
        leader_term: 1,
        leader_id: 1,
        metadata: Some(metadata),
        seq: 0,
        total_chunks: 1,
        data: compressed_data,
        chunk_checksum,
    };

    let streaming_request = crate_test_snapshot_stream(vec![chunk]);
    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);

    let handler_task = tokio::spawn({
        let config = snapshot_config(temp_path.to_path_buf());
        async move {
            handler
                .apply_snapshot_stream_from_leader(1, Box::new(streaming_request), ack_tx, &config)
                .await
        }
    });

    // Verify ACK
    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Accepted as i32);

    // Ensure handler completes successfully
    assert!(handler_task.await.unwrap().is_ok());
}
