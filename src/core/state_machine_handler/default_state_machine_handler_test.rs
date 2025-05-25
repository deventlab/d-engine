use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use bytes::BytesMut;
use crc32fast::Hasher;
use futures::stream;
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use mockall::predicate::eq;
use mockall::Sequence;
use prost::Message;
use tempfile::tempdir;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::time;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use super::DefaultStateMachineHandler;
use super::MockStateMachineHandler;
use super::StateMachineHandler;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::is_dir;
use crate::init_sled_state_machine_db;
use crate::proto::rpc_service_client::RpcServiceClient;
use crate::proto::Entry;
use crate::proto::LogId;
use crate::proto::NodeMeta;
use crate::proto::PurgeLogRequest;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotMetadata;
use crate::proto::SnapshotResponse;
use crate::proto::VotedFor;
use crate::test_utils::enable_logger;
use crate::test_utils::node_config;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::MOCK_STATE_MACHINE_HANDLER_PORT_BASE;
use crate::AppendResults;
use crate::ConsensusError;
use crate::ElectionError;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MockElectionCore;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::NetworkError;
use crate::Node;
use crate::RaftOneshot;
use crate::SnapshotConfig;
use crate::StateUpdate;
use crate::StorageError;
use crate::SystemError;

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

// Case1: No pending commit
#[tokio::test]
async fn test_apply_batch_case1() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock
        .expect_get_entries_between()
        .times(0)
        .returning(|_| vec![]);
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case1")),
        MockSnapshotPolicy::new(),
    );
    assert!(handler.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
}

// Case2: There is pending commit
//
// Criteria:
// - last_applied should be updated
#[tokio::test]
async fn test_apply_batch_case2() {
    // Init Handler
    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock.expect_apply_chunk().times(1).returning(|_| Ok(()));
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock.expect_get_entries_between().times(1).returning(|_| {
        vec![Entry {
            index: 1,
            term: 1,
            command: vec![1; 8],
        }]
    });

    let max_entries_per_chunk = 1;
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case2")),
        MockSnapshotPolicy::new(),
    );

    // Update pending commit
    handler.update_pending(11);

    assert!(handler.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(handler.last_applied(), 11);
}

// Case 3: There is pending commit
//
// Criteria:
// - test the pending commit should be split into 10 chunks
#[tokio::test]
async fn test_apply_batch_case3() {
    // Init Handler
    let mut state_machine_mock = MockStateMachine::new();
    state_machine_mock.expect_apply_chunk().times(10).returning(|_| Ok(()));
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock.expect_get_entries_between().times(1).returning(move |_| {
        let mut entries = vec![];
        for i in 1..=10 {
            entries.push(Entry {
                index: i,
                term: 1,
                command: vec![1; 8],
            });
        }
        entries
    });

    let max_entries_per_chunk = 1;
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case3")),
        MockSnapshotPolicy::new(),
    );

    // Update pending commit
    handler.update_pending(21);

    assert!(handler.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(handler.last_applied(), 21);
}

/// Helper to compute CRC32 checksum for test data
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}

fn listen_addr(port: u64) -> SocketAddr {
    format!("127.0.0.1:{}", port).parse().unwrap()
}

/// Case1: Complete successful snapshot installation
#[tokio::test]
async fn test_install_snapshot_chunk_case1() {
    enable_logger();

    let port = MOCK_STATE_MACHINE_HANDLER_PORT_BASE + 1;
    // 1. Simulate node with RPC server running in a new thread
    let (graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node_with_rpc_service(
        "/tmp/test_install_snapshot_chunk_case1",
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
    let addr: SocketAddr = format!("[::]:{}", port).parse().unwrap();
    let mut rpc_client = RpcServiceClient::connect(format!(
        "grpc://localhost:{}",
        addr.to_string().split(':').next_back().unwrap()
    ))
    .await
    .unwrap();

    // 3. Fake install snapshot request stream
    let total_chunks = 3;
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    // Generate valid chunks with seq 0..2
    tokio::spawn(async move {
        for seq in 0..total_chunks {
            let chunk = create_test_chunk(
                seq,
                &format!("chunk-{}", seq).into_bytes(),
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
}

/// Helper to create valid test chunk
fn create_test_chunk(
    seq: u32,
    data: &[u8],
    term: u64,
    leader_id: u64,
    total: u32,
) -> SnapshotChunk {
    SnapshotChunk {
        term,
        leader_id,
        seq,
        total,
        checksum: compute_checksum(data),
        metadata: Some(SnapshotMetadata {
            last_included: Some(LogId { index: 100, term }),
            checksum: vec![],
        }),
        data: data.to_vec(),
    }
}

fn create_test_stream(chunks: Vec<SnapshotChunk>) -> tonic::Streaming<SnapshotChunk> {
    // Convert chunks to encoded byte streams
    let byte_stream = stream::iter(chunks.into_iter().map(|chunk| {
        let mut buf = Vec::new();

        chunk
            .encode(&mut buf)
            .map_err(|e| Status::new(Code::Internal, format!("Encoding failed: {}", e)))?;

        // Add Tonic frame header
        let mut frame = BytesMut::new();
        frame.put_u8(0); // No compression
        debug!("buf.len()={}", buf.len());

        frame.put_u32(buf.len() as u32); // Message length
        frame.extend_from_slice(&buf);

        Ok(frame.freeze())
    }));

    let body = StreamBody::new(
        byte_stream
            .map_ok(Frame::data)
            .map_err(|e: Status| Status::new(Code::Internal, format!("Stream error: {}", e))),
    );
    tonic::Streaming::new_request(
        SnapshotChunkDecoder,
        body.boxed_unsync(),
        None,
        Some(1024 * 1024 * 1024),
    )
}

fn create_test_handler(temp_dir: &Path) -> DefaultStateMachineHandler<MockTypeConfig> {
    let state_machine = MockStateMachine::new();
    DefaultStateMachineHandler::new(
        1,
        0,
        1,
        Arc::new(state_machine),
        snapshot_config(temp_dir.to_path_buf()),
        MockSnapshotPolicy::new(),
    )
}

/// # Case 2: Successfully applies valid chunks
#[tokio::test]
async fn test_install_snapshot_chunk_case2() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
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
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );
    // 3. Fake install snapshot request stream
    let total_chunks = 1;
    // Generate valid chunks with seq 0..2
    let mut chunks: Vec<SnapshotChunk> = vec![];
    for seq in 0..total_chunks {
        chunks.push(create_test_chunk(
            seq,
            &format!("chunk-{}", seq).into_bytes(),
            3, // chunk term (higher than handler's current_term)
            1, // leader_id
            total_chunks,
        ));
    }

    let streaming_request = create_test_stream(chunks);
    let (sender, receiver) = MaybeCloneOneshot::new();
    let result = handler
        .install_snapshot_chunk(1, Box::new(streaming_request), sender)
        .await;
    assert!(result.is_ok());

    // Verify final response
    let response = receiver.await.unwrap().unwrap();
    assert!(response.success);
    assert_eq!(response.next_chunk, 0);
}

const TEST_TERM: u64 = 1;
const TEST_LEADER_ID: u64 = 1;

/// # Case 3: Rejects chunk with invalid checksum
#[tokio::test]
async fn test_install_snapshot_chunk_case3() {
    let temp_dir = tempdir().unwrap();
    let handler = create_test_handler(temp_dir.path());

    // Create chunk with invalid checksum
    let mut bad_chunk = create_test_chunk(0, b"bad data", TEST_TERM, TEST_LEADER_ID, 1);
    bad_chunk.checksum = vec![0xde, 0xad, 0xbe, 0xef]; // Corrupt checksum

    let (sender, receiver) = MaybeCloneOneshot::new();
    let stream = create_test_stream(vec![bad_chunk]);
    handler
        .install_snapshot_chunk(TEST_TERM, Box::new(stream), sender)
        .await
        .unwrap();

    let response = receiver.await.unwrap().unwrap();
    assert!(!response.success);
    assert_eq!(response.next_chunk, 0); // Expect retry same chunk
}

/// # Case 4: aborts_when_leader_changes_during_stream
#[tokio::test]
async fn test_install_snapshot_chunk_case4() {
    let temp_dir = tempdir().unwrap();
    let handler = create_test_handler(temp_dir.path());

    // First chunk with term 1, second with term 2
    let chunks = vec![
        create_test_chunk(0, b"chunk0", TEST_TERM, TEST_LEADER_ID, 2),
        create_test_chunk(1, b"chunk1", TEST_TERM + 1, TEST_LEADER_ID, 2),
    ];

    let (sender, receiver) = MaybeCloneOneshot::new();
    let stream = create_test_stream(chunks);
    handler
        .install_snapshot_chunk(TEST_TERM, Box::new(stream), sender)
        .await
        .unwrap();

    let status = receiver.await.unwrap().unwrap_err();
    assert_eq!(status.code(), tonic::Code::Aborted);
    assert_eq!(status.message(), "Leader changed");
}

/// # Case 5: handles_stream_errors_gracefully
#[tokio::test]
async fn test_install_snapshot_chunk_case5() {
    let temp_dir = tempdir().unwrap();
    let handler = create_test_handler(temp_dir.path());

    // Create stream that returns error after first chunk
    let chunks = vec![create_test_chunk(0, b"chunk0", TEST_TERM, TEST_LEADER_ID, 2)];
    let stream = create_test_stream(chunks);

    let (sender, receiver) = MaybeCloneOneshot::new();
    handler
        .install_snapshot_chunk(TEST_TERM, Box::new(stream), sender)
        .await
        .unwrap();

    // Should get error from receiver
    let status = receiver.await.unwrap().unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
}

/// # Case 6: rejects_chunks_with_missing_metadata
#[tokio::test]
async fn test_install_snapshot_chunk_case6() {
    let temp_dir = tempdir().unwrap();
    let handler = create_test_handler(temp_dir.path());

    // First chunk missing metadata
    let mut invalid_chunk = create_test_chunk(0, b"data", TEST_TERM, TEST_LEADER_ID, 1);
    invalid_chunk.metadata = None;

    let (sender, receiver) = MaybeCloneOneshot::new();
    let stream = create_test_stream(vec![invalid_chunk]);
    handler
        .install_snapshot_chunk(TEST_TERM, Box::new(stream), sender)
        .await
        .unwrap();

    let status = receiver.await.unwrap().unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
}

/// # Case 1: Basic creation flow
#[tokio::test]
async fn test_create_snapshot_case1() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut sm = MockStateMachine::new();

    // Mock state machine behavior
    let mut seq = Sequence::new();
    sm.expect_last_applied()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|| LogId { index: 5, term: 1 });
    sm.expect_generate_snapshot_data()
        .times(1)
        .withf(|path, last_included| {
            debug!(?path, ?last_included);
            // Create the directory structure correctly
            fs::create_dir_all(path).unwrap();
            //Simulate sled to create a subdirectory
            let db_path = path.join("state_machine");
            fs::create_dir(&db_path).unwrap();

            path.ends_with("temp-5-1") && last_included.index == 5 && last_included.term == 1
        })
        .returning(|_, _| Ok([0; 32]));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    // Execute snapshot creation
    let result = handler.create_snapshot().await;
    assert!(result.is_ok());

    // Verify file system changes
    let (metadata, final_path) = result.unwrap();
    assert!(final_path.exists());
    assert!(final_path
        .to_str()
        .unwrap()
        .contains(&format!("{}5-1", SNAPSHOT_DIR_PREFIX)));

    assert!(is_dir(&final_path).await.unwrap());
    assert_eq!(metadata.last_included, Some(LogId { term: 1, index: 5 }));
}

/// # Case 2: Test concurrent protection
#[tokio::test]
async fn test_create_snapshot_case2() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut sm = MockStateMachine::new();

    // Setup slow snapshot generation
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    sm.expect_last_applied().returning(|| LogId { term: 1, index: 1 });
    sm.expect_generate_snapshot_data().times(2).returning(move |path, _| {
        std::thread::sleep(Duration::from_millis(50));
        // Wait forever
        if let Err(_e) = rx.try_recv() {
            return Err(StorageError::Snapshot("test failure".into()).into());
        }

        debug!(?path, "generate_snapshot_data");

        // Create the directory structure correctly
        fs::create_dir_all(path.clone()).unwrap();
        //Simulate sled to create a subdirectory
        let db_path = path.join("state_machine");
        fs::create_dir(&db_path).unwrap();

        Ok([0; 32])
    });

    let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
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

    // Allow some time for execution
    tokio::time::sleep(Duration::from_millis(20)).await;

    let results = futures::future::join_all(vec![t1, t2]).await;
    println!("{:?}", &results);

    // Verify only one successful creation
    let success_count = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
    assert_eq!(success_count, 1);
    assert_eq!(count_snapshots(temp_dir.path()), 1);
}

/// # Case 3: Test cleanup old versions
#[tokio::test]
async fn test_create_snapshot_case3() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();

    let mut sm = MockStateMachine::new();
    let mut count = 0;
    sm.expect_last_applied().returning(move || {
        count += 1;
        LogId { term: 1, index: count }
    });
    sm.expect_generate_snapshot_data().returning(|path, _| {
        debug!(?path, "expect_generate_snapshot_data");
        let _new_db = init_sled_state_machine_db(path).expect("");

        Ok([0; 32])
    });
    let snapshot_dir = temp_dir.as_ref().to_path_buf();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        3, // Current version
        1,
        Arc::new(sm),
        snapshot_config(snapshot_dir.clone()),
        MockSnapshotPolicy::new(),
    );

    // Create new snapshot (version 4)
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();

    // Verify cleanup results
    let remaining: HashSet<u64> = get_snapshot_versions(snapshot_dir.as_path()).into_iter().collect();
    assert_eq!(remaining, [4, 3].into_iter().collect());
}

/// # Case 4: Test failure handling
#[tokio::test]
async fn test_create_snapshot_case4() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut sm = MockStateMachine::new();

    // Setup failing snapshot generation
    sm.expect_last_applied().returning(|| LogId { term: 1, index: 1 });
    sm.expect_generate_snapshot_data()
        .returning(|_, _| Err(StorageError::Snapshot("test failure".into()).into()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
        MockSnapshotPolicy::new(),
    );

    // Attempt snapshot creation
    let result = handler.create_snapshot().await;
    assert!(result.is_err());

    // Verify no files created
    assert_eq!(count_snapshots(temp_dir.path()), 0);
}

// Helper functions
fn count_snapshots(dir: &Path) -> usize {
    debug!(?dir, "count_snapshots");

    std::fs::read_dir(dir)
        .unwrap()
        .filter(|entry| {
            let name = entry.as_ref().unwrap().file_name();
            name.to_str().unwrap().starts_with(SNAPSHOT_DIR_PREFIX)
        })
        .count()
}

fn get_snapshot_versions(dir: &Path) -> Vec<u64> {
    debug!(?dir, "get_snapshot_versions");

    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_str().unwrap();
            debug!(%name, "get_snapshot_versions");
            name.split('-').nth(1).and_then(|v| v.parse().ok())
        })
        .collect()
}

/// # Case 1: Test normal deletion
#[tokio::test]
async fn test_cleanup_snapshot_case1() {
    enable_logger();

    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();

    create_test_dirs(&temp_dir, &[1, 2, 3]).await;

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
}

/// # Case 2: Test no old versions to be cleaned
#[tokio::test]
async fn test_cleanup_snapshot_case2() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    create_test_dirs(&temp_dir, &[3, 4]).await;

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
async fn test_cleanup_snapshot_case3() {
    enable_logger();

    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    // Create valid and invalid directories
    create_dir(&temp_dir, &format!("{}1-1", SNAPSHOT_DIR_PREFIX)).await;
    create_dir(&temp_dir, "invalid_format").await;
    create_dir(&temp_dir, &format!("{}bad-2-2", SNAPSHOT_DIR_PREFIX)).await;

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
    assert!(remaining.contains(&format!("{}bad-2-2", SNAPSHOT_DIR_PREFIX)));
    assert!(remaining.contains(&format!("{}1-1", SNAPSHOT_DIR_PREFIX)));
}

/// #Case 1: Reject stale term
#[tokio::test]
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
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(MockRaftLog::new()))
        .await
        .unwrap();

    assert!(!res.success);
    assert_eq!(res.term, 5);
}

/// # Case 2: Reject if not from current leader
#[tokio::test]
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
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(MockRaftLog::new()))
        .await
        .unwrap();

    assert!(!res.success);
}

// # Case 3: Reject if local state is behind
#[tokio::test]
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
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(MockRaftLog::new()))
        .await
        .unwrap();

    assert!(!res.success);
}

/// # Case 4: Reject on checksum mismatch
#[tokio::test]
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
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(MockRaftLog::new()))
        .await
        .unwrap();

    assert!(!res.success);
}

/// # Case 5: Successful purge
#[tokio::test]
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
    raft_log
        .expect_purge_logs_up_to()
        .returning(|_| Err(StorageError::DbError("expect_purge_logs_up_to failed".to_string()).into()));

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
        .handle_purge_request(5, Some(1), last_included, &req, &Arc::new(MockRaftLog::new()))
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
    sm.expect_last_included()
        .returning(move || (LogId { index, term }, Some(checksum)));
}

// Helper functions
async fn create_test_dirs(
    temp_dir: &TempDir,
    ids: &[u64],
) {
    for id in ids {
        create_dir(temp_dir, &format!("{}{}-1", SNAPSHOT_DIR_PREFIX, id,)).await;
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

fn snapshot_config(snapshots_dir: PathBuf) -> SnapshotConfig {
    SnapshotConfig {
        max_log_entries_before_snapshot: 1,
        snapshot_cool_down_since_last_check: Duration::from_secs(0),
        cleanup_retain_count: 2,
        snapshots_dir,
    }
}

fn mock_node_with_rpc_service(
    db_path: &str,
    listen_address: SocketAddr,
    is_leader: bool,
    shutdown_signal: watch::Receiver<()>,
    peers_meta_option: Option<Vec<NodeMeta>>,
) -> Arc<Node<MockTypeConfig>> {
    enable_logger();

    let mut node_config = node_config(db_path);
    if peers_meta_option.is_some() {
        node_config.cluster.initial_cluster = peers_meta_option.unwrap();
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
        .expect_handle_client_proposal_in_batch()
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
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
        election_handler
            .expect_broadcast_vote_requests()
            .returning(|_, _, _, _, _| {
                Err(Error::Consensus(ConsensusError::Election(ElectionError::HigherTerm(
                    100,
                ))))
            });
        election_handler
            .expect_handle_vote_request()
            .times(1)
            .returning(|_, _, _, _| {
                Err(Error::System(SystemError::Network(NetworkError::SingalSendFailed(
                    "".to_string(),
                ))))
            });
    }
    // let state_machine_handler = Arc::new(state_machine_handler);
    let mut mock_state_machine_handler = MockStateMachineHandler::new();
    mock_state_machine_handler.expect_install_snapshot_chunk().returning(
        move |_current_term, _stream_request, sender| {
            sender
                .send(Ok(SnapshotResponse {
                    term: 1,
                    success: true,
                    next_chunk: 0,
                }))
                .map_err(|e| StorageError::Snapshot(format!("Send snapshot error: {:?}", e)))?;

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

// Create a custom Decoder implementation
struct SnapshotChunkDecoder;
impl tonic::codec::Decoder for SnapshotChunkDecoder {
    type Item = SnapshotChunk;
    type Error = Status;
    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        debug!(?buf, "SnapshotChunkDecoder");

        match SnapshotChunk::decode(buf) {
            Ok(chunk) => Ok(Some(chunk)),
            Err(e) => Err(Status::new(Code::Internal, format!("Decode error: {}", e))),
        }
    }
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        tonic::codec::BufferSettings::new(4 * 1024 * 1024, 4 * 1024 * 1025)
    }
}
