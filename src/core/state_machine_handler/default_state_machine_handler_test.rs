use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crc32fast::Hasher;
use futures::Stream;
use mockall::predicate::{self};
use mockall::Predicate;
use mockall::Sequence;
use prost::Message;
use tempfile::TempDir;
use tokio_stream::StreamExt;
use tokio_stream::{self as stream};
use tracing::debug;

use super::DefaultStateMachineHandler;
use super::StateMachineHandler;
use crate::constants::SNAPSHOT_DIR_PREFIX;
use crate::file_io::is_dir;
use crate::proto::Entry;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotMetadata;
use crate::test_utils::enable_logger;
use crate::test_utils::MockTypeConfig;
use crate::MockRaftLog;
use crate::MockStateMachine;
use crate::SnapshotConfig;
use crate::StorageError;

// Case 1: normal update
#[test]
fn test_update_pending_case1() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case1")),
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
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case2")),
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
        0,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_update_pending_case3")),
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
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case1")),
    );
    assert_eq!(handler.pending_range(), None);
}

// Case 2: pending commit <= last_applied
#[test]
fn test_pending_range_case2() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case2")),
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
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_pending_range_case3")),
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
        10,
        1,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case1")),
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
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case2")),
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
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        snapshot_config(PathBuf::from("/tmp/test_apply_batch_case3")),
    );

    // Update pending commit
    handler.update_pending(21);

    assert!(handler.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(handler.last_applied(), 21);
}

/// Helper to compute CRC32 checksum for test data
#[allow(unused)]
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}

/// Helper to create valid test chunk
#[allow(unused)]
fn create_test_chunk(
    seq: u32,
    term: u64,
    leader_id: u64,
    total: u32,
) -> SnapshotChunk {
    let data = format!("chunk-{}", seq).into_bytes();
    SnapshotChunk {
        term,
        leader_id,
        seq,
        total,
        checksum: compute_checksum(&data),
        metadata: Some(SnapshotMetadata {
            last_included_index: 100,
            last_included_term: term,
        }),
        data,
    }
}
// fn echo_requests_iter(
//     term: u64,
//     leader_id: u64,
//     total: u32,
// ) -> Request<tonic::Streaming<SnapshotChunk>> {
//     let (tx, rx) = mpsc::channel::<Result<SnapshotChunk, Status>>(10); // Clear channel type

//     tokio::spawn(async move {
//         for i in 1..=total {
//             let data = format!("chunk-{}", i).into_bytes();
//             let chunk = SnapshotChunk {
//                 term,
//                 leader_id,
//                 seq: i,
//                 total,
//                 checksum: compute_checksum(&data),
//                 metadata: Some(SnapshotMetadata {
//                     last_included_index: 100,
//                     last_included_term: term,
//                 }),
//                 data,
//             };

//             if tx.send(Ok(chunk)).await.is_err() {
//                 break;
//             }
//         }
//     });

//     let stream = ReceiverStream::new(rx);
//     Request::new(tonic::Streaming::new(stream))
// }

fn mock_stream(
    term: u64,
    leader_id: u64,
    total: u32,
) -> impl Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> {
    let data = format!("chunk-{}", 1).into_bytes();
    let chunks = vec![SnapshotChunk {
        term,
        leader_id,
        seq: 1,
        total,
        checksum: compute_checksum(&data),
        metadata: Some(SnapshotMetadata {
            last_included_index: 100,
            last_included_term: term,
        }),
        data,
    }];

    // let codec = ProstCodec::default();

    stream::iter(chunks).map(move |chunk| {
        let mut buf = bytes::BytesMut::new();
        chunk
            .encode(&mut buf)
            .map_err(|e: prost::EncodeError| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(buf.freeze())
    })
}

fn echo_requests_iter2(
    term: u64,
    leader_id: u64,
    total: u32,
) -> impl Stream<Item = SnapshotChunk> {
    tokio_stream::iter(1..u32::MAX).map(move |i| {
        let data = format!("chunk-{}", i).into_bytes();
        SnapshotChunk {
            term,
            leader_id,
            seq: i,
            total,
            checksum: compute_checksum(&data),
            metadata: Some(SnapshotMetadata {
                last_included_index: 100,
                last_included_term: term,
            }),
            data,
        }
    })
}
// 1. Create a message stream generator
fn mock_chunks_stream(
    term: u64,
    leader_id: u64,
    total: u32,
) -> impl Stream<Item = SnapshotChunk> {
    tokio_stream::iter(1..=total).map(move |seq| {
        let data = format!("chunk-{}", seq).into_bytes();
        SnapshotChunk {
            term,
            leader_id,
            seq,
            total,
            checksum: compute_checksum(&data),
            metadata: Some(SnapshotMetadata {
                last_included_index: 100,
                last_included_term: term,
            }),
            data,
        }
    })
}

// fn encoded_stream(
//     term: u64,
//     leader_id: u64,
//     total: u32,
// ) -> impl Stream<Item = Result<Bytes, Status>> {
//     let codec = ProstCodec::<SnapshotChunk, Bytes>::default();
//     let mut encoder = codec.encoder();

//     mock_chunks_stream(term, leader_id, total).map(move |chunk| {
//         let mut buf = bytes::BytesMut::new();
//         encoder
//             .encode(chunk, &mut buf)
//             .map(|_| buf.freeze())
//             .map_err(|e| Status::internal(format!("Encoding error: {}", e)))
//     })
// }

/// Case1: Complete successful snapshot installation
#[tokio::test]
async fn test_install_snapshot_chunk_case1() {
    // let state_machine_mock = MockStateMachine::new();
    // let snapshot_dir = tempdir().unwrap().path().to_path_buf();
    // let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(0, 1,
    // Arc::new(state_machine_mock), snapshot_dir);

    // let total_chunks = 3;

    // let (sender, receiver) = oneshot::channel();

    // let encoded_stream = encoded_stream(3, 1, 3);
    // let body = MockBody(encoded_stream);
    // let codec = ProstCodec::<SnapshotChunk, Bytes>::default();

    // let request_stream = tonic::codec::Streaming::new_request(codec.decoder(),
    // BoxBody::new(body), None, None);

    // // let request = tonic::Request::new(request_stream);

    // let result = handler.install_snapshot_chunk(1, request_stream, sender.into()).await;

    // assert!(result.is_ok());
    // let response = receiver.await.unwrap().unwrap();
    // assert!(response.success);
    // assert_eq!(response.next_chunk, 0);
}
// struct MockBody<S>(S);

// impl<S, T> http_body::Body for MockBody<S>
// where
//     S: Stream<Item = Result<T, Status>> + Unpin + Send + 'static,
//     T: Into<Bytes>,
// {
//     type Data = Bytes;
//     type Error = Status;

//     fn poll_frame(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
//         match ready!(self.0.poll_next_unpin(cx)) {
//             Some(Ok(data)) => Poll::Ready(Some(Ok(http_body::Frame::data(data.into())))),
//             Some(Err(e)) => Poll::Ready(Some(Err(e))),
//             None => Poll::Ready(None),
//         }
//     }
// }

// #[tokio::test]
// async fn test_create_snapshot_case1() {
//     // Init Handler
//     let mut state_machine_mock = MockStateMachine::new();
//     state_machine_mock.expect_apply_chunk().times(10).returning(|_| Ok(()));
//     let mut raft_log_mock = MockRaftLog::new();
//     raft_log_mock.expect_get_entries_between().times(1).returning(move |_| {
//         let mut entries = vec![];
//         for i in 1..=10 {
//             entries.push(Entry {
//                 index: i,
//                 term: 1,
//                 command: vec![1; 8],
//             });
//         }
//         entries
//     });

//     let max_entries_per_chunk = 1;
//     let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
//         10,
//         max_entries_per_chunk,
//         Arc::new(state_machine_mock),
//         "/tmp/test_create_snapshot_case1",
//     );

//     handler.create_snapshot();
// }

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
        .returning(|| (5, 1));
    sm.expect_generate_snapshot_data()
        .times(1)
        .withf(|path, idx, term| {
            // Create the directory structure correctly
            fs::create_dir_all(path).unwrap();
            //Simulate sled to create a subdirectory
            let db_path = path.join("state_machine");
            fs::create_dir(&db_path).unwrap();

            path.ends_with("temp-1") && idx == &5 && term == &1
        })
        .returning(|_, _, _| Ok(()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
    );

    // Execute snapshot creation
    let result = handler.create_snapshot().await;
    assert!(result.is_ok());

    // Verify file system changes
    let final_path = result.unwrap();
    assert!(final_path.exists());
    assert!(final_path
        .to_str()
        .unwrap()
        .contains(&format!("{}1-5-1", SNAPSHOT_DIR_PREFIX)));

    assert!(is_dir(&final_path).await.unwrap());

    // Verify version update
    assert_eq!(handler.current_snapshot_version(), 1);
}

/// # Case 2: Test concurrent protection
#[tokio::test]
async fn test_create_snapshot_case2() {
    enable_logger();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut sm = MockStateMachine::new();

    // Setup slow snapshot generation
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    sm.expect_last_applied().returning(|| (1, 1));
    sm.expect_generate_snapshot_data()
        .times(2)
        .returning(move |path, _, _| {
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

            Ok(())
        });

    let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
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
    sm.expect_last_applied().returning(|| (9, 1));
    sm.expect_generate_snapshot_data().returning(|path, _, _| {
        // Create the directory structure correctly
        fs::create_dir_all(path.clone()).unwrap();
        //Simulate sled to create a subdirectory
        let db_path = path.join("state_machine");
        fs::create_dir(&db_path).unwrap();

        Ok(())
    });
    let snapshot_dir = temp_dir.as_ref().to_path_buf();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        3, // Current version
        1,
        Arc::new(sm),
        snapshot_config(snapshot_dir.clone()),
    );

    // Create new snapshot (version 4)
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();
    handler.create_snapshot().await.unwrap();

    // Verify cleanup results
    let remaining: HashSet<u64> = get_snapshot_versions(snapshot_dir.as_path()).into_iter().collect();
    assert_eq!(remaining, [4, 3, 2].into_iter().collect());
}

/// # Case 4: Test failure handling
#[tokio::test]
async fn test_create_snapshot_case4() {
    enable_logger();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut sm = MockStateMachine::new();

    // Setup failing snapshot generation
    sm.expect_last_applied().returning(|| (1, 1));
    sm.expect_generate_snapshot_data()
        .returning(|_, _, _| Err(StorageError::Snapshot("test failure".into()).into()));

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
    );

    // Attempt snapshot creation
    let result = handler.create_snapshot().await;
    assert!(result.is_err());

    // Verify no files created
    assert_eq!(count_snapshots(temp_dir.path()), 0);
    assert_eq!(handler.current_snapshot_version(), 0);
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

fn create_dummy_snapshot(
    dir: &tempfile::TempDir,
    version: u64,
    index: u64,
    term: u64,
) {
    let path = dir
        .path()
        .join(format!("{}{}-{}-{}", SNAPSHOT_DIR_PREFIX, version, index, term));
    std::fs::File::create(path).unwrap();
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
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
    );

    handler.cleanup_snapshot(2, &temp_dir.path().into()).await.unwrap();

    // Verify remaining snapshots
    let remaining = get_snapshot_versions(temp_dir.path());
    assert_eq!(remaining, vec![3]);
}

/// # Case 2: Test no old versions to be cleaned
#[tokio::test]
async fn test_cleanup_snapshot_case2() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    create_test_dirs(&temp_dir, &[3, 4]).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
    );

    handler.cleanup_snapshot(2, &temp_dir.path().into()).await.unwrap();

    // Verify no deletions
    let remaining = get_snapshot_versions(temp_dir.path());
    assert_eq!(remaining, vec![3, 4]);
}

/// # Case 3: Test invalid dirnames
#[tokio::test]
async fn test_cleanup_snapshot_case3() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    // Create valid and invalid directories
    create_dir(&temp_dir, &format!("{}1-100-1", SNAPSHOT_DIR_PREFIX)).await;
    create_dir(&temp_dir, "invalid_format").await;
    create_dir(&temp_dir, &format!("{}bad-200-2", SNAPSHOT_DIR_PREFIX)).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(sm),
        snapshot_config(temp_dir.path().to_path_buf()),
    );

    handler.cleanup_snapshot(2, &temp_dir.path().into()).await.unwrap();

    //Verify only valid version 1 is deleted
    let remaining = get_dir_names(temp_dir.path()).await;
    assert!(remaining.contains(&"invalid_format".into()));
    assert!(remaining.contains(&format!("{}bad-200-2", SNAPSHOT_DIR_PREFIX).into()));
    assert!(!remaining.contains(&format!("{}1-100-1", SNAPSHOT_DIR_PREFIX).into()));
}

// Helper functions
async fn create_test_dirs(
    temp_dir: &TempDir,
    versions: &[u64],
) {
    for v in versions {
        create_dir(temp_dir, &format!("{}{}-{}-1", SNAPSHOT_DIR_PREFIX, v, v * 100,)).await;
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

// Custom predicate for path comparison
fn path_eq(expected: PathBuf) -> impl Predicate<PathBuf> + 'static {
    predicate::function(move |x: &PathBuf| x == &expected)
}

fn snapshot_config(snapshots_dir: PathBuf) -> SnapshotConfig {
    SnapshotConfig {
        max_log_entries_before_snapshot: 1,
        cleanup_version_offset: 2,
        snapshots_dir,
    }
}
