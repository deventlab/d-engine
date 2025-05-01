use std::sync::Arc;

use bytes::Bytes;
use crc32fast::Hasher;
use futures::Stream;
use prost::Message;
use tokio_stream::StreamExt;
use tokio_stream::{self as stream};

use super::DefaultStateMachineHandler;
use super::StateMachineHandler;
use crate::proto::Entry;
use crate::proto::SnapshotChunk;
use crate::proto::SnapshotMetadata;
use crate::test_utils::MockTypeConfig;
use crate::MockRaftLog;
use crate::MockStateMachine;

// Case 1: normal update
#[test]
fn test_update_pending_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(0, 1, Arc::new(state_machine_mock), "/tmp/db");
    applier.update_pending(1);
    assert_eq!(applier.pending_commit(), 1);
    applier.update_pending(10);
    assert_eq!(applier.pending_commit(), 10);
}

// Case 2: new commit < existing commit
#[test]
fn test_update_pending_case2() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(0, 1, Arc::new(state_machine_mock), "/tmp/db");
    applier.update_pending(10);
    assert_eq!(applier.pending_commit(), 10);

    applier.update_pending(7);
    assert_eq!(applier.pending_commit(), 10);
}
// Case 3: multi thread update
#[tokio::test]
async fn test_update_pending_case3() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
        0,
        1,
        Arc::new(state_machine_mock),
        "/tmp/db",
    ));

    let mut tasks = vec![];
    for i in 1..=10 {
        let applier = applier.clone();
        tasks.push(tokio::spawn(async move {
            applier.update_pending(i);
        }));
    }
    futures::future::join_all(tasks).await;
    assert_eq!(applier.pending_commit(), 10);
}

// Case 1: pending commit is zero
#[test]
fn test_pending_range_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(10, 1, Arc::new(state_machine_mock), "/tmp/db");
    assert_eq!(applier.pending_range(), None);
}

// Case 2: pending commit <= last_applied
#[test]
fn test_pending_range_case2() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(10, 1, Arc::new(state_machine_mock), "/tmp/db");
    applier.update_pending(7);
    applier.update_pending(10);
    assert_eq!(applier.pending_range(), None);
}

// Case 3: pending commit > last_applied
#[test]
fn test_pending_range_case3() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(10, 1, Arc::new(state_machine_mock), "/tmp/db");
    applier.update_pending(7);
    applier.update_pending(10);
    applier.update_pending(11);
    assert_eq!(applier.pending_range(), Some(11..=11));
}

// Case1: No pending commit
#[tokio::test]
async fn test_apply_batch_case1() {
    // Init Applier
    let state_machine_mock = MockStateMachine::new();
    let mut raft_log_mock = MockRaftLog::new();
    raft_log_mock
        .expect_get_entries_between()
        .times(0)
        .returning(|_| vec![]);
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(10, 1, Arc::new(state_machine_mock), "/tmp/db");
    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
}

// Case2: There is pending commit
//
// Criteria:
// - last_applied should be updated
#[tokio::test]
async fn test_apply_batch_case2() {
    // Init Applier
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
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        "/tmp/db",
    );

    // Update pending commit
    applier.update_pending(11);

    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(applier.last_applied(), 11);
}

// Case 3: There is pending commit
//
// Criteria:
// - test the pending commit should be split into 10 chunks
#[tokio::test]
async fn test_apply_batch_case3() {
    // Init Applier
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
    let applier = DefaultStateMachineHandler::<MockTypeConfig>::new(
        10,
        max_entries_per_chunk,
        Arc::new(state_machine_mock),
        "/tmp/db",
    );

    // Update pending commit
    applier.update_pending(21);

    assert!(applier.apply_batch(Arc::new(raft_log_mock)).await.is_ok());
    assert_eq!(applier.last_applied(), 21);
}

/// Helper to compute CRC32 checksum for test data
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_be_bytes().to_vec()
}

/// Helper to create valid test chunk
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
