use super::DefaultStateMachineHandler;
use super::DefaultStateMachineWriter;
use super::StateMachineHandler;
use super::StateMachineWriterOps;
use super::broadcast_watch_events;
use super::new_reader_writer_pair;
use crate::Error;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::MockTypeConfig;
use crate::SnapshotError;
use crate::test_utils::create_test_compressed_snapshot;
use crate::test_utils::snapshot_config;
use bytes::Bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::batch_op::Op;
use d_engine_proto::client::write_command::{
    Batch, BatchOp as ProtoBatchOp, Insert, Operation, batch_op,
};
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use futures::StreamExt;
use mockall::Sequence;
use prost::Message;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tempfile::TempDir;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::debug;
use tracing_test::traced_test;

fn batch_entry(
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Entry {
    let write_cmd = WriteCommand {
        operation: Some(Operation::Batch(Batch {
            ops: vec![ProtoBatchOp {
                op: Some(Op::Insert(Insert {
                    key: Bytes::copy_from_slice(key),
                    value: Bytes::copy_from_slice(value),
                    ttl_secs: 0,
                })),
            }],
        })),
    };
    let mut buf = Vec::new();
    write_cmd.encode(&mut buf).unwrap();
    Entry {
        index,
        term: 1,
        payload: Some(EntryPayload {
            payload: Some(Payload::Command(Bytes::from(buf))),
        }),
    }
}

// Case 1: normal update
#[test]
fn test_update_pending_case1() {
    // Init Handler
    let state_machine_mock = MockStateMachine::new();
    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_update_pending_case1"),
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
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_update_pending_case2"),
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
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_update_pending_case3"),
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
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_pending_range_case1"),
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
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_pending_range_case2"),
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
        Arc::new(state_machine_mock),
        PathBuf::from("/tmp/test_pending_range_case3"),
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

    use d_engine_proto::common::EntryPayload;

    use super::*;

    fn noop_entry(index: u64) -> Entry {
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload::noop()),
        }
    }

    fn create_test_handler(
        path: &str,
        apply_chunk_error: bool,
        last_applied_index: Option<u64>,
    ) -> (
        DefaultStateMachineHandler<MockTypeConfig>,
        DefaultStateMachineWriter<MockTypeConfig>,
    ) {
        let mut state_machine = MockStateMachine::new();
        if apply_chunk_error {
            state_machine
                .expect_apply_chunk()
                .returning(|_| Err(Error::Fatal("Test error".to_string())));
        } else {
            state_machine.expect_apply_chunk().returning(|_| Ok(vec![]));
        }
        new_reader_writer_pair::<MockTypeConfig>(
            1,
            last_applied_index.unwrap_or(0),
            Arc::new(state_machine),
            PathBuf::from(path),
            snapshot_config(PathBuf::from(path)),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        )
    }

    #[tokio::test]
    async fn test_apply_chunk_updates_last_applied_case1() {
        let (handler, writer) = create_test_handler(
            "/tmp/test_apply_chunk_updates_last_applied_case1",
            false,
            None,
        );

        assert_eq!(handler.last_applied(), 0);

        let chunk = vec![noop_entry(90), noop_entry(100)];
        let result = writer.apply_chunk(chunk).await;

        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 100);
    }

    #[tokio::test]
    async fn test_apply_chunk_updates_last_applied_case2() {
        let (handler, writer) = create_test_handler(
            "/tmp/test_apply_chunk_updates_last_applied_case2",
            false,
            None,
        );

        assert_eq!(handler.last_applied(), 0);

        let chunk = vec![noop_entry(50), noop_entry(70)];
        let result = writer.apply_chunk(chunk).await;

        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 70);
    }

    #[tokio::test]
    async fn test_apply_chunk_handles_empty_chunk() {
        let (handler, writer) =
            create_test_handler("/tmp/test_apply_chunk_handles_empty_chunk", false, Some(2));

        let chunk = vec![noop_entry(1), noop_entry(2)];
        let result = writer.apply_chunk(chunk).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 2);

        // Empty chunk: last_applied must not regress
        let result = writer.apply_chunk(vec![]).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 2);
    }

    /// apply_chunk must NOT advance last_applied when the state machine returns an error.
    ///
    /// Raft at-most-once: an entry that fails to apply must not be acknowledged.
    /// last_applied advancing past a failed entry would cause the leader to believe
    /// it was committed into the SM, silently losing the write.
    #[tokio::test]
    async fn test_apply_chunk_with_state_machine_io_error() {
        let (handler, writer) = create_test_handler(
            "/tmp/test_apply_chunk_with_state_machine_io_error",
            true,
            None,
        );

        assert_eq!(handler.last_applied(), 0);

        let result = writer.apply_chunk(vec![noop_entry(50)]).await;
        assert!(result.is_err());
        assert_eq!(handler.last_applied(), 0);
    }

    /// A Command::Batch that fails must not advance last_applied and must not
    /// broadcast any watch events.
    ///
    /// Batch is an atomic operation: partial application is not allowed.
    /// Watch subscribers must not see events for keys that were never mutated.
    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn test_apply_chunk_batch_failure_is_atomic_no_side_effects() {
        // Build a proto-encoded Batch entry with one Insert op
        let cmd = WriteCommand {
            operation: Some(Operation::Batch(Batch {
                ops: vec![ProtoBatchOp {
                    op: Some(batch_op::Op::Insert(Insert {
                        key: Bytes::from_static(b"k1"),
                        value: Bytes::from_static(b"v1"),
                        ttl_secs: 0,
                    })),
                }],
            })),
        };
        let mut buf = Vec::new();
        cmd.encode(&mut buf).unwrap();
        let batch_entry = Entry {
            index: 5,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        };

        // Handler with watch channel so we can observe broadcast behaviour
        let (watch_tx, mut watch_rx) = tokio::sync::broadcast::channel(16);
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .returning(|_| Err(Error::Fatal("batch failed".to_string())));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            PathBuf::from("/tmp/test_batch_failure_atomic"),
            snapshot_config(PathBuf::from("/tmp/test_batch_failure_atomic")),
            MockSnapshotPolicy::new(),
            Some(watch_tx),
            Arc::new(AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![batch_entry]).await;

        assert!(result.is_err());
        assert_eq!(
            handler.last_applied(),
            0,
            "last_applied must not advance on batch failure"
        );
        assert!(
            watch_rx.try_recv().is_err(),
            "failed batch must not emit watch events"
        );
    }

    /// #436 (StateMachineWriter split) baseline: pins down that a *successful*
    /// apply_chunk broadcasts a watch event through the real integration path — not just
    /// via a direct `broadcast_watch_events()` call (see `broadcast_watch_events_tests`
    /// below, which tests that method in isolation) and not just the failure/no-event case
    /// above. `watch_event_tx`/`prev_kv_watcher_count` are moving to `DefaultStateMachineWriter`
    /// in that split — this must keep passing unchanged afterward.
    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn test_apply_chunk_broadcasts_watch_event_on_success() {
        let cmd = WriteCommand {
            operation: Some(Operation::Batch(Batch {
                ops: vec![ProtoBatchOp {
                    op: Some(batch_op::Op::Insert(Insert {
                        key: Bytes::from_static(b"k1"),
                        value: Bytes::from_static(b"v1"),
                        ttl_secs: 0,
                    })),
                }],
            })),
        };
        let mut buf = Vec::new();
        cmd.encode(&mut buf).unwrap();
        let entry = Entry {
            index: 7,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        };

        let (watch_tx, mut watch_rx) = tokio::sync::broadcast::channel(16);
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk().returning(|entries| {
            Ok(entries.iter().map(|e| crate::ApplyResult::success(e.index)).collect())
        });

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            PathBuf::from("/tmp/test_apply_chunk_broadcasts_watch_event_on_success"),
            snapshot_config(PathBuf::from(
                "/tmp/test_apply_chunk_broadcasts_watch_event_on_success",
            )),
            MockSnapshotPolicy::new(),
            Some(watch_tx),
            Arc::new(AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![entry]).await;

        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 7);
        let event = watch_rx.try_recv().expect("successful apply must emit a watch event");
        let _ = event; // exact WatchResponse shape already covered by broadcast_watch_events_tests
    }
}

/// #436 (StateMachineWriter split) baseline: pins down that `wait_applied()` is
/// actually woken by the *real* `apply_chunk()` path — every existing test in
/// `wait_applied_test.rs` only exercises `wait_applied()`'s own logic via the test-only
/// `test_simulate_apply()` bypass, never via a real apply. This must keep passing once
/// `last_applied`/the notify channel move into a shared `AppliedState` held by both
/// `DefaultStateMachineHandler` (reader) and `DefaultStateMachineWriter`.
#[cfg(test)]
mod apply_chunk_wakes_wait_applied_test {
    use std::sync::Arc;
    use std::time::Duration;

    use d_engine_proto::common::{Entry, EntryPayload};

    use super::StateMachineHandler;
    use super::StateMachineWriterOps;
    use crate::MockSnapshotPolicy;
    use crate::MockStateMachine;
    use crate::MockTypeConfig;
    use crate::test_utils::snapshot_config;

    fn noop_entry(index: u64) -> Entry {
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload::noop()),
        }
    }

    #[tokio::test]
    async fn test_apply_chunk_wakes_real_wait_applied_waiter() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk().returning(|_| Ok(vec![]));

        let (handler, writer) = super::new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_apply_chunk_wakes_real_wait_applied_waiter"),
            snapshot_config(std::path::PathBuf::from(
                "/tmp/test_apply_chunk_wakes_real_wait_applied_waiter",
            )),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);
        let applier = Arc::new(writer);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            applier.apply_chunk(vec![noop_entry(10)]).await.unwrap();
        });

        // Waiter is registered BEFORE the real apply_chunk runs — must be woken by the
        // real notify_tx.send() inside apply_chunk, not a test-only bypass.
        let result = handler.wait_applied(10, Duration::from_millis(200)).await;
        assert!(
            result.is_ok(),
            "real apply_chunk must wake a concurrent wait_applied waiter"
        );
        assert_eq!(handler.last_applied(), 10);
    }
}

/// Verifies the decode boundary: raw proto `Entry` → `ApplyEntry` transition happens inside
/// `DefaultStateMachineHandler::apply_chunk`, not inside the state machine.
///
/// The state machine mock's `apply_chunk` receives `&[ApplyEntry]` with fully decoded `Command`
/// variants. It never sees raw proto bytes.
#[cfg(test)]
mod decode_boundary_tests {
    use bytes::Bytes;
    use d_engine_proto::client::WriteCommand;
    use d_engine_proto::client::write_command::{Insert, Operation};
    use d_engine_proto::common::EntryPayload;
    use d_engine_proto::common::entry_payload::Payload;
    use prost::Message;

    use super::*;
    use crate::test_utils::snapshot_config;
    use crate::{
        ApplyResult, BatchOp, Command, MockSnapshotPolicy, MockStateMachine, MockTypeConfig,
    };

    fn noop_entry(index: u64) -> Entry {
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload::noop()),
        }
    }

    fn insert_entry(
        index: u64,
        key: &[u8],
        value: &[u8],
    ) -> Entry {
        let cmd = WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                ttl_secs: 0,
            })),
        };
        let mut buf = Vec::new();
        cmd.encode(&mut buf).unwrap();
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        }
    }

    fn config_entry(index: u64) -> Entry {
        use d_engine_proto::common::AddNode;
        use d_engine_proto::common::membership_change::Change;
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload::config(Change::AddNode(AddNode {
                node_id: 1,
                address: "127.0.0.1:4000".to_string(),
                status: 0,
            }))),
        }
    }

    /// SM receives `ApplyEntry` with `Command::Insert` — the handler decoded the proto bytes.
    #[tokio::test]
    async fn test_handler_decodes_insert_before_sm_receives_it() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .withf(|entries| {
                entries.len() == 1
                    && matches!(
                        &entries[0].command,
                        Command::Insert { key, .. } if key == &Bytes::from_static(b"mykey")
                    )
            })
            .returning(|chunk| Ok(vec![ApplyResult::success(chunk[0].index)]));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_decode_insert_boundary"),
            snapshot_config(std::path::PathBuf::from("/tmp/test_decode_insert_boundary")),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![insert_entry(5, b"mykey", b"myval")]).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 5);
    }

    /// Noop entries reach the SM as `Command::Noop` — index continuity is preserved.
    #[tokio::test]
    async fn test_handler_noop_reaches_sm_as_command_noop() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .withf(|entries| entries.len() == 1 && matches!(entries[0].command, Command::Noop))
            .returning(|chunk| Ok(vec![ApplyResult::success(chunk[0].index)]));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_decode_noop_boundary"),
            snapshot_config(std::path::PathBuf::from("/tmp/test_decode_noop_boundary")),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![noop_entry(3)]).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 3);
    }

    /// Config entries become Command::Noop — SM receives a Noop at the config index.
    /// This ensures sm.last_applied advances to the config index, so ReadIndex drain works.
    #[tokio::test]
    async fn test_handler_config_entry_becomes_noop_last_applied_advances() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .withf(|entries| entries.len() == 1 && matches!(entries[0].command, Command::Noop))
            .returning(|chunk| Ok(chunk.iter().map(|e| ApplyResult::success(e.index)).collect()));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_decode_config_boundary"),
            snapshot_config(std::path::PathBuf::from("/tmp/test_decode_config_boundary")),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![config_entry(7)]).await;
        assert!(result.is_ok());
        // last_applied must reach 7 — Config was at commit index 7.
        assert_eq!(handler.last_applied(), 7);
    }

    /// Mixed batch: noop + config + insert. Config becomes Noop; SM receives 3 entries in order.
    #[tokio::test]
    async fn test_handler_mixed_batch_config_becomes_noop_order_preserved() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .withf(|entries| {
                entries.len() == 3
                    && matches!(entries[0].command, Command::Noop)
                    && matches!(entries[1].command, Command::Noop) // config → noop
                    && matches!(&entries[2].command, Command::Insert { .. })
            })
            .returning(|chunk| Ok(chunk.iter().map(|e| ApplyResult::success(e.index)).collect()));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_decode_mixed_boundary"),
            snapshot_config(std::path::PathBuf::from("/tmp/test_decode_mixed_boundary")),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );

        // indices: 10=noop, 11=config(→noop), 12=insert — all 3 forwarded to SM
        let chunk = vec![
            noop_entry(10),
            config_entry(11),
            insert_entry(12, b"k", b"v"),
        ];
        let result = writer.apply_chunk(chunk).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 12);
    }

    /// Handler decodes a proto-encoded Batch entry and forwards Command::Batch
    /// with correct ops to the state machine — no raw bytes reach the SM.
    ///
    /// Mirrors test_handler_decodes_insert_before_sm_receives_it for the Batch path.
    #[tokio::test]
    async fn test_handler_decodes_batch_before_sm_receives_it() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk()
            .withf(|entries| {
                entries.len() == 1
                    && matches!(
                        &entries[0].command,
                        Command::Batch { ops } if ops.len() == 1
                            && matches!(&ops[0], BatchOp::Insert { key, .. } if key == &Bytes::from_static(b"mykey"))

                    )
            })
            .returning(|chunk| Ok(vec![ApplyResult::success(chunk[0].index)]));

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_handler_decodes_batch_before_sm_receives_it"),
            snapshot_config(std::path::PathBuf::from(
                "/tmp/test_handler_decodes_batch_before_sm_receives_it",
            )),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );

        let result = writer.apply_chunk(vec![batch_entry(5, b"mykey", b"myval")]).await;
        assert!(result.is_ok());
        assert_eq!(handler.last_applied(), 5);
    }
}

// fn listen_addr(port: u32) -> SocketAddr {
//     format!("127.0.0.1:{port}",).parse().unwrap()
// }

// /// Case1: Complete successful snapshot installation
// #[tokio::test]
// #[traced_test]
// async fn test_install_snapshot_case1() {
//     let port = MOCK_STATE_MACHINE_HANDLER_PORT_BASE + 1;
//     // 1. Simulate node with RPC server running in a new thread
//     let (graceful_tx, graceful_rx) = watch::channel(());
//     let node = mock_node_with_rpc_service(
//         "/tmp/test_install_snapshot_case1",
//         listen_addr(port),
//         false,
//         graceful_rx,
//         None,
//     );
//     node.set_ready(true);

//     // 3. Start the Raft main loop
//     let raft_handle = tokio::spawn(async move {
//         let mut raft = node.raft_core.lock().await;
//         let _ = time::timeout(Duration::from_millis(100), raft.run()).await;
//     });

//     // 2. Crate RPC client
//     let addr: SocketAddr = format!("[::]:{port}",).parse().unwrap();
//     let mut rpc_client = SnapshotServiceClient::connect(format!(
//         "grpc://localhost:{}",
//         addr.to_string().split(':').next_back().unwrap()
//     ))
//     .await
//     .unwrap();

//     // 3. Fake install snapshot request stream
//     let total_chunks = 3;
//     let (tx, rx) = tokio::sync::mpsc::channel(10);
//     // Generate valid chunks with seq 0..2
//     let h = tokio::spawn(async move {
//         for seq in 0..total_chunks {
//             let chunk = create_test_chunk(
//                 seq,
//                 &format!("chunk-{seq}",).into_bytes(),
//                 3, // chunk term (higher than handler's current_term)
//                 1, // leader_id
//                 total_chunks,
//             );

//             tx.send(chunk).await.expect("send failed");
//         }
//     });
//     // Convert mpsc receiver into tonic::Streaming
//     let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

//     // 4. Waiting to receive response
//     tokio::time::sleep(Duration::from_millis(20)).await;

//     let response = rpc_client.install_snapshot(request_stream).await.unwrap().into_inner();

//     assert!(response.success);
//     assert_eq!(response.term, 1); // Should reflect handler's current_term
//     assert_eq!(response.next_chunk, 0); // Indicates full success

//     // Release handler
//     graceful_tx.send(()).expect("shutdown successfully!");
//     raft_handle.await.expect("should succeed");
//     h.await.unwrap();
// }

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
        Arc::new(state_machine),
        temp_dir.to_path_buf(),
        config,
        MockSnapshotPolicy::new(),
    )
}

/// #436-adjacent: `prepare_snapshot_stream` is the path `follower_state.rs` uses
/// when the leader pushes a snapshot to a follower that's fallen behind. This
/// proves the received snapshot ends up as a real file in `snapshots_dir` at the
/// path `SnapshotPathManager::final_snapshot_path` would compute for it — not just
/// applied to the live state machine and forgotten.
///
/// Why this matters: if this node later needs to forward the same boundary to a
/// third, further-behind node (`prepare_transfer_meta`), it looks up the file at
/// exactly this path — nothing else in this flow would have put it there if
/// `finalize()`'s rename here didn't happen or got the path wrong.
#[tokio::test]
#[traced_test]
async fn test_prepare_snapshot_stream_persists_file_in_snapshots_dir() {
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().join("test_prepare_snapshot_stream_persists_file");
    create_dir_all(&temp_path).await.unwrap();

    let handler = create_test_handler(&temp_path, None);

    let (compressed_data, metadata) = create_test_compressed_snapshot().await;
    let last_included = metadata.last_included.expect("test fixture always sets this");

    // Single-chunk transfer: `first_chunk` is passed directly (below), so the
    // channel of *remaining* chunks stays empty — just close it right away.
    let first_chunk = SnapshotChunk {
        leader_term: TEST_TERM,
        leader_id: TEST_LEADER_ID,
        metadata: Some(metadata.clone()),
        seq: 0,
        total_chunks: 1,
        data: Bytes::from(compressed_data.clone()),
        chunk_checksum: Bytes::from(crc32fast::hash(&compressed_data).to_be_bytes().to_vec()),
    };

    let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(1);
    let (_tx, rx) = mpsc::channel(32);
    drop(_tx);

    let config = snapshot_config(temp_path.to_path_buf());
    let prepared = handler
        .prepare_snapshot_stream(first_chunk, rx, ack_tx, &config)
        .await
        .expect("prepare_snapshot_stream should succeed with a single valid chunk");

    let ack = ack_rx.recv().await.unwrap();
    assert_eq!(ack.status, ChunkStatus::Accepted as i32);
    assert_eq!(prepared.metadata.last_included, Some(last_included));

    let expected_final_path = temp_path.join(format!(
        "snapshot-{}-{}.tar.gz",
        last_included.index, last_included.term
    ));
    assert!(
        expected_final_path.exists(),
        "expected the received snapshot to be persisted at {:?} so a later \
         prepare_transfer_meta() call (forwarding to a 3rd, further-behind node) \
         can find it — it wasn't there",
        expected_final_path
    );
}

const TEST_TERM: u64 = 1;
const TEST_LEADER_ID: u32 = 1;

mod create_snapshot_tests {
    use d_engine_proto::common::NodeRole::Leader;

    use super::*;
    use crate::NewCommitData;

    /// Mirrors the pre-#436 single-call `create_snapshot()`: capture (via the writer,
    /// what `StateMachineWorker` does in production) then build (via the handler),
    /// guarded by the same try_begin/end pair `handle_create_snapshot` uses in
    /// `role_state.rs`. Kept as a test-only helper so the ~15 cases below don't each
    /// have to spell out the two-step flow.
    async fn create_snapshot(
        handler: &DefaultStateMachineHandler<MockTypeConfig>,
        writer: &DefaultStateMachineWriter<MockTypeConfig>,
    ) -> crate::Result<(SnapshotMetadata, PathBuf)> {
        handler.try_begin_local_snapshot_capture()?;
        let result = match writer.capture_local_snapshot().await {
            Ok(captured) => handler.build_local_snapshot(captured).await,
            Err(e) => Err(e),
        };
        handler.end_local_snapshot_capture();
        result
    }

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
        sm.expect_generate_snapshot_data()
            .times(1)
            .withf(|path, last_included| {
                debug!(?path, ?last_included);
                // Create the directory structure correctly
                fs::create_dir_all(path.clone()).unwrap();

                let db_path = path.join("state_machine");
                fs::create_dir(&db_path).unwrap();

                path.ends_with("temp-5-1") && last_included.index == 5 && last_included.term == 1
            })
            .returning(|_, _| Ok(Bytes::from(vec![0; 32])));

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.to_path_buf(),
            config.clone(),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        // Execute snapshot creation
        let result = create_snapshot(&handler, &writer).await;

        debug!(?result);

        assert!(result.is_ok());

        // Verify file system changes
        let (metadata, final_path) = result.unwrap();
        debug!(?final_path);
        assert!(final_path.is_file());
        assert!(final_path.extension().unwrap() == "gz");
        assert!(
            final_path
                .to_str()
                .unwrap()
                .contains(&format!("{}5-1.tar.gz", &config.snapshots_dir_prefix))
        );

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
                checksum: Bytes::from(vec![1; 8]),
            })
        });
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
                Ok(Bytes::copy_from_slice(&[0; 32]))
            } else {
                Err(SnapshotError::OperationFailed("Concurrency failure".into()).into())
            }
        });

        let mut snapshot_policy = MockSnapshotPolicy::new();
        snapshot_policy.expect_should_trigger().returning(|_| true);

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.to_path_buf(),
            config.clone(),
            snapshot_policy,
            None,
            Arc::new(AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);
        let writer = Arc::new(writer);
        tx.send(()).unwrap(); // Unblock the first task

        // Spawn concurrent snapshot creations
        let (h1, w1) = (handler.clone(), writer.clone());
        let t1 = tokio::spawn(async move { create_snapshot(&h1, &w1).await });

        let (h2, w2) = (handler.clone(), writer.clone());
        let t2 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            create_snapshot(&h2, &w2).await
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
        assert_eq!(count_snapshots(&temp_path, &config.snapshots_dir_prefix), 1);

        // Verify flag is reset regardless of task outcome
        let ctx = NewCommitData {
            role: Leader as i32,
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
        sm.expect_generate_snapshot_data().returning(|path, _| {
            debug!(?path, "expect_generate_snapshot_data");
            std::fs::create_dir_all(path).expect("Failed to create directory");

            Ok(Bytes::copy_from_slice(&[0; 32]))
        });
        let snapshot_dir = temp_path.to_path_buf();

        let mut config = snapshot_config(snapshot_dir.clone());
        config.retained_log_entries = 0;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            3, // Current version
            Arc::new(sm),
            snapshot_dir.clone(),
            config.clone(),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        // Create new snapshot (version 4)
        create_snapshot(&handler, &writer).await.unwrap();
        create_snapshot(&handler, &writer).await.unwrap();
        create_snapshot(&handler, &writer).await.unwrap();
        create_snapshot(&handler, &writer).await.unwrap();

        // Verify cleanup results
        let remaining: HashSet<u64> =
            get_snapshot_versions(snapshot_dir.as_path()).into_iter().collect();
        assert_eq!(remaining, [4, 3].into_iter().collect());

        // Verify files are compressed
        for version in &[3, 4] {
            let path =
                snapshot_dir.join(format!("{}{version}-1.tar.gz", config.snapshots_dir_prefix));
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
        sm.expect_generate_snapshot_data()
            .returning(|_, _| Err(SnapshotError::OperationFailed("test failure".into()).into()));

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 0;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.to_path_buf(),
            config.clone(),
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        // Attempt snapshot creation
        let result = create_snapshot(&handler, &writer).await;
        assert!(result.is_err());

        // Verify no files created
        assert_eq!(count_snapshots(&temp_path, &config.snapshots_dir_prefix), 0);
    }

    /// # Case 5: Test snapshot_in_progress flag is reset on success
    #[tokio::test]
    async fn test_create_snapshot_resets_flag_on_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_flag_reset_success");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId { term: 1, index: 5 });
        sm.expect_generate_snapshot_data().returning(|path, _| {
            fs::create_dir_all(path).unwrap();
            Ok(Bytes::from(vec![0; 32]))
        });

        let config = snapshot_config(temp_path.clone());
        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.clone(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);

        // Verify initial state
        assert!(!handler.snapshot_in_progress());

        let handler_clone = handler.clone();
        let result = create_snapshot(&handler_clone, &writer).await;
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
        sm.expect_generate_snapshot_data()
            .returning(|_, _| Err(SnapshotError::OperationFailed("test error".into()).into()));

        let config = snapshot_config(temp_path.clone());
        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.clone(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);

        // Verify initial state
        assert!(!handler.snapshot_in_progress());

        let handler_clone = handler.clone();
        let result = create_snapshot(&handler_clone, &writer).await;
        assert!(result.is_err());

        // Critical assertion: Flag must be reset even after failure
        assert!(!handler.snapshot_in_progress());
    }

    /// create_snapshot with retained_log_entries > 0 must still produce
    /// last_included == last_applied — retained_log_entries must NOT affect the snapshot label.
    ///
    /// # Purpose
    /// Regression guard for Bug #418: the old implementation subtracted
    /// retained_log_entries from last_applied to compute last_included, producing a
    /// snapshot whose label lied about how far the state had advanced.
    /// With retained=10 and last_applied=100, the old code emitted last_included=90
    /// while the snapshot file actually captured state through index 100.
    /// A follower installing that snapshot would re-apply entries 91~100, executing
    /// non-idempotent operations twice and corrupting cluster state.
    /// Log retention must be enforced at the purge layer (leader_state), not here.
    ///
    /// # Criteria
    /// - retained_log_entries = 10
    /// - last_applied = LogId { index: 100, term: 1 }
    ///
    /// # Expected
    /// - generate_snapshot_data is called with last_included = { index: 100, term: 1 }
    /// - metadata.last_included == Some(LogId { index: 100, term: 1 })  (NOT index 90)
    /// - re-apply gap = 0: follower installs at 100 and applies from 101, not 91
    #[tokio::test]
    async fn test_create_snapshot_respects_retained_log_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test_create_snapshot_respects_retained_log_entries");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId {
            term: 1,
            index: 100,
        });
        sm.expect_generate_snapshot_data()
            .times(1)
            .withf(|_path, last_included| {
                last_included.index == 100 && last_included.term == 1 // NOT 90
            })
            .returning(|path, _| {
                fs::create_dir_all(path).unwrap();
                Ok(Bytes::from(vec![0; 32]))
            });

        let mut config = snapshot_config(temp_path.clone());
        config.retained_log_entries = 10;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.clone(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);

        // Verify initial state
        assert!(!handler.snapshot_in_progress());

        let handler_clone = handler.clone();
        let result = create_snapshot(&handler_clone, &writer).await;
        assert!(result.is_ok());
        let (metadata, _) = result.unwrap();
        assert_eq!(
            metadata.last_included,
            Some(LogId {
                term: 1,
                index: 100
            })
        );
    }

    /// Snapshot last_included must carry the correct term from last_applied across a term boundary.
    ///
    /// # Purpose
    /// Regression guard for Bug #418: the old code used last_applied.term for
    /// last_included.term even when last_included.index fell in an earlier term.
    /// Example: term 1 wrote entries 1~90, term 2 wrote entries 91~100.
    /// With retained=60, old code computed last_included = { index: 40, term: 2 } —
    /// a LogId that never existed in the Raft log (entry 40 was in term 1, not term 2).
    /// With the fix, last_included is taken directly from last_applied, so both
    /// index and term are truthful and no LogId is fabricated.
    ///
    /// # Criteria
    /// - last_applied = LogId { index: 100, term: 2 }  (term 1: entries 1~90, term 2: 91~100)
    /// - retained_log_entries = 60
    ///
    /// # Expected
    /// - generate_snapshot_data is called with last_included = { index: 100, term: 2 }
    /// - metadata.last_included == Some(LogId { index: 100, term: 2 })
    /// - no forged LogId { index: 40, term: 2 } is produced
    #[tokio::test]
    async fn test_create_snapshot_last_included_equals_last_applied_across_term_boundary() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir
            .path()
            .join("test_create_snapshot_last_included_equals_last_applied_across_term_boundary");
        let mut sm = MockStateMachine::new();

        // term 1: entries 1~90, term 2: entries 91~100
        // last_applied is at the head of term 2
        sm.expect_last_applied().returning(|| LogId {
            term: 2,
            index: 100,
        });
        sm.expect_generate_snapshot_data()
            .times(1)
            .withf(|path, last_included| {
                fs::create_dir_all(path).unwrap();
                // Old code: index = 100 - 60 = 40, term = 2 (copied from last_applied.term)
                // → forged LogId { index: 40, term: 2 } — entry 40 was in term 1, not term 2
                // New code: last_included = last_applied = { index: 100, term: 2 } — truthful
                last_included.index == 100 && last_included.term == 2
            })
            .returning(|_, _| Ok(Bytes::from(vec![0; 32])));

        let mut config = snapshot_config(temp_path.to_path_buf());
        // retained=60: old code would subtract and land in term 1 territory (index 40)
        // while stamping term 2 — that LogId never existed
        config.retained_log_entries = 60;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.to_path_buf(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        let result = create_snapshot(&handler, &writer).await;
        assert!(result.is_ok());
        let (metadata, _) = result.unwrap();
        assert_eq!(
            metadata.last_included,
            Some(LogId {
                term: 2,
                index: 100
            })
        );
    }

    /// create_snapshot when last_applied.index is zero must not panic or produce an invalid snapshot.
    ///
    /// # Purpose
    /// Boundary: snapshot triggered before any entry has been applied.
    /// Old and new code both produce last_included.index = 0 here
    /// (0.saturating_sub(retained) = 0), but this test documents the boundary is
    /// safe and regression-free under both implementations.
    ///
    /// # Criteria
    /// - last_applied = LogId { index: 0, term: 0 }
    /// - retained_log_entries = 10
    ///
    /// # Expected
    /// - create_snapshot returns Ok (no panic)
    /// - metadata.last_included == Some(LogId { index: 0, term: 0 })
    #[tokio::test]
    async fn test_create_snapshot_when_last_applied_index_is_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path =
            temp_dir.path().join("test_create_snapshot_when_last_applied_index_is_zero");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId { term: 0, index: 0 });
        sm.expect_generate_snapshot_data()
            .times(1)
            .withf(|_, last_included| last_included.index == 0 && last_included.term == 0)
            .returning(|path, _| {
                fs::create_dir_all(path).unwrap();
                Ok(Bytes::from(vec![0; 32]))
            });

        let mut config = snapshot_config(temp_path.to_path_buf());
        config.retained_log_entries = 10;

        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            temp_path.to_path_buf(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        let result = create_snapshot(&handler, &writer).await;
        assert!(result.is_ok());
        let (metadata, _) = result.unwrap();
        assert_eq!(metadata.last_included, Some(LogId { term: 0, index: 0 }));
    }

    /// A crash mid-checkpoint must not leave the temp dir behind after create_snapshot() returns Err.
    #[tokio::test]
    async fn test_create_snapshot_removes_temp_dir_on_generate_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = temp_dir.path().join("snapshots");
        let mut sm = MockStateMachine::new();

        sm.expect_last_applied().returning(|| LogId { term: 1, index: 5 });
        sm.expect_generate_snapshot_data().returning(|path, _| {
            // Simulate a partial write before the failure (e.g. checkpoint killed mid-copy).
            fs::create_dir_all(&path).unwrap();
            Err(SnapshotError::OperationFailed("simulated crash".into()).into())
        });

        let config = snapshot_config(snapshot_dir.clone());
        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            snapshot_dir.clone(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        assert!(create_snapshot(&handler, &writer).await.is_err());
        assert!(
            !snapshot_dir.join("temp-5-1").exists(),
            "orphaned temp dir was not cleaned up"
        );
    }

    /// A stale temp dir left by a previous crash must not block the next snapshot at the same index.
    #[tokio::test]
    async fn test_create_snapshot_succeeds_despite_stale_temp_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = temp_dir.path().join("snapshots");
        let stale_temp_path = snapshot_dir.join("temp-5-1");
        fs::create_dir_all(stale_temp_path.join("sm.tmp")).unwrap();

        let mut sm = MockStateMachine::new();
        sm.expect_last_applied().returning(|| LogId { term: 1, index: 5 });
        sm.expect_generate_snapshot_data().returning(|path, _| {
            // Real engines (e.g. RocksDB checkpoint) fail with "File exists" if `path`
            // is already present — mirror that here instead of using the idempotent
            // `create_dir_all`, otherwise this test can't distinguish "cleaned up" from
            // "never checked".
            if path.exists() {
                return Err(SnapshotError::OperationFailed("mkdir: File exists".into()).into());
            }
            fs::create_dir_all(&path).unwrap();
            Ok(Bytes::from(vec![0; 32]))
        });

        let config = snapshot_config(snapshot_dir.clone());
        let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            snapshot_dir.clone(),
            config,
            MockSnapshotPolicy::new(),
            None,
            Arc::new(AtomicUsize::new(0)),
        );

        assert!(create_snapshot(&handler, &writer).await.is_ok());
    }
}

// Helper functions
fn count_snapshots(
    dir: &Path,
    snapshots_dir_prefix: &str,
) -> usize {
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
                .and_then(|name| name.starts_with(snapshots_dir_prefix).then_some(()))
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

    let config = snapshot_config(temp_dir.path().to_path_buf());
    create_test_files(&temp_dir, &[1, 2, 3], &config.snapshots_dir_prefix).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        Arc::new(sm),
        temp_dir.path().to_path_buf(),
        config.clone(),
        MockSnapshotPolicy::new(),
    );

    handler
        .cleanup_snapshot(2, temp_dir.path(), &config.snapshots_dir_prefix)
        .await
        .unwrap();

    // Verify remaining snapshots
    let mut remaining = get_snapshot_versions(temp_dir.path());
    remaining.sort();
    let mut expect = vec![2, 3];
    expect.sort();
    assert_eq!(remaining, expect);

    // Verify files are compressed
    for version in &[2, 3] {
        let path = temp_dir.path().join(format!(
            "{}{}-1.tar.gz",
            config.snapshots_dir_prefix, version
        ));
        assert!(path.is_file(), "Snapshot file not found: {path:?}",);
    }
}

/// # Case 2: Test no old versions to be cleaned
#[tokio::test]
#[traced_test]
async fn test_cleanup_snapshot_case2() {
    let temp_dir = TempDir::new().unwrap();
    let sm = MockStateMachine::new();
    let config = snapshot_config(temp_dir.path().to_path_buf());
    create_test_files(&temp_dir, &[3, 4], &config.snapshots_dir_prefix).await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        Arc::new(sm),
        temp_dir.path().to_path_buf(),
        config.clone(),
        MockSnapshotPolicy::new(),
    );

    handler
        .cleanup_snapshot(2, temp_dir.path(), &config.snapshots_dir_prefix)
        .await
        .unwrap();

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
    let config = snapshot_config(temp_dir.path().to_path_buf());
    // Create valid and invalid directories
    create_dir(&temp_dir, &format!("{}1-1", &config.snapshots_dir_prefix)).await;
    create_dir(&temp_dir, "invalid_format").await;
    create_dir(
        &temp_dir,
        &format!("{}bad-2-2", &config.snapshots_dir_prefix),
    )
    .await;

    let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
        1,
        0,
        Arc::new(sm),
        temp_dir.path().to_path_buf(),
        config.clone(),
        MockSnapshotPolicy::new(),
    );

    handler
        .cleanup_snapshot(2, temp_dir.path(), &config.snapshots_dir_prefix)
        .await
        .unwrap();

    //Verify only valid version 1 is deleted
    let remaining = get_dir_names(temp_dir.path()).await;
    debug!(?remaining);

    assert!(remaining.contains(&"invalid_format".into()));
    assert!(remaining.contains(&format!("{}bad-2-2", &config.snapshots_dir_prefix)));
    assert!(remaining.contains(&format!("{}1-1", &config.snapshots_dir_prefix)));
}

async fn create_test_files(
    temp_dir: &TempDir,
    ids: &[u64],
    snapshots_dir_prefix: &str,
) {
    for id in ids {
        let file_name = format!("{snapshots_dir_prefix}{id}-1.tar.gz",);
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
        checksum: Bytes::from(vec![1; 32]),
    };

    // #436: leader_term is a separate parameter, distinct from metadata's own
    // last_included.term (7) — proves the sender's current term is what ends up
    // on the wire, not the snapshotted data's (possibly stale) boundary term.
    let sender_current_term = 7;

    // Get snapshot stream
    let mut stream = handler
        .load_snapshot_data(metadata.clone(), sender_current_term)
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
    assert_eq!(
        chunk.leader_term, sender_current_term,
        "leader_term must come from the sender's current term, not metadata.last_included.term (1)"
    );
    assert_eq!(chunk.leader_id, 1);
    assert_eq!(chunk.seq, 0);
    assert_eq!(chunk.total_chunks, 1);
    assert_eq!(chunk.data, Bytes::from(content.to_vec()));
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
        checksum: Bytes::from(vec![2; 32]),
    };

    // Get and collect chunks
    let mut stream = handler.load_snapshot_data(metadata.clone(), 1).await.unwrap();
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
        checksum: Bytes::new(),
    };

    assert!(handler.load_snapshot_data(metadata, 1).await.is_err());
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
        checksum: Bytes::from(vec![5; 32]),
    };

    let mut stream = handler.load_snapshot_data(metadata, 1).await.unwrap();
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
        checksum: Bytes::from(vec![6; 32]),
    };

    let result = handler.load_snapshot_data(metadata, 1).await;
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
        checksum: Bytes::from(vec![7; 32]),
    };

    // Get and collect chunks
    let mut stream = handler.load_snapshot_data(metadata.clone(), 1).await.unwrap();
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
    sm.expect_generate_snapshot_data().returning(|path, _| {
        // Create the directory structure correctly
        fs::create_dir_all(path.clone()).unwrap();
        // Create test files in the temp directory
        let file1 = path.join("test1.txt");
        let file2 = path.join("test2.bin");

        std::fs::write(&file1, "This is a test file").unwrap();
        std::fs::write(&file2, vec![0u8; 1024]).unwrap();

        Ok(Bytes::from(vec![0; 32]))
    });

    let mut config = snapshot_config(temp_path.to_path_buf());
    config.retained_log_entries = 0;

    let (handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
        1,
        0,
        Arc::new(sm),
        temp_path.to_path_buf(),
        config,
        MockSnapshotPolicy::new(),
        None,
        Arc::new(AtomicUsize::new(0)),
    );

    // Create snapshot
    handler.try_begin_local_snapshot_capture().unwrap();
    let captured = writer.capture_local_snapshot().await.unwrap();
    let (_, snapshot_path) = handler.build_local_snapshot(captured).await.unwrap();
    handler.end_local_snapshot_capture();

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

#[cfg(feature = "watch")]
mod broadcast_watch_events_tests {
    use bytes::Bytes;
    use d_engine_proto::client::WatchEventType;

    use super::*;
    use crate::{ApplyEntry, ApplyResult, BatchOp, Command};

    /// Build an `ApplyEntry` directly from a `Command` — no proto encoding needed.
    /// `broadcast_watch_events` receives already-decoded entries; tests mirror that reality.
    fn apply_entry(
        index: u64,
        command: Command,
    ) -> ApplyEntry {
        ApplyEntry {
            index,
            term: 1,
            command,
        }
    }

    fn succeeded(index: u64) -> ApplyResult {
        ApplyResult {
            index,
            succeeded: true,
        }
    }

    fn failed(index: u64) -> ApplyResult {
        ApplyResult {
            index,
            succeeded: false,
        }
    }

    #[tokio::test]
    async fn test_broadcast_insert_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::Insert {
                key: Bytes::from_static(b"key1"),
                value: Bytes::from_static(b"value1"),
                ttl_secs: None,
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.key, Bytes::from_static(b"key1"));
        assert_eq!(event.value, Bytes::from_static(b"value1"));
        assert_eq!(event.event_type, WatchEventType::Put as i32);
    }

    #[tokio::test]
    async fn test_broadcast_delete_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::Delete {
                key: Bytes::from_static(b"key1"),
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.key, Bytes::from_static(b"key1"));
        assert_eq!(event.value, Bytes::new());
        assert_eq!(event.event_type, WatchEventType::Delete as i32);
    }

    // CAS succeeded → watcher receives Put event with new value
    #[tokio::test]
    async fn test_broadcast_cas_success() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::CompareAndSwap {
                key: Bytes::from_static(b"lock"),
                expected: Some(Bytes::from_static(b"owner1")),
                value: Bytes::from_static(b"owner2"),
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.key, Bytes::from_static(b"lock"));
        assert_eq!(event.value, Bytes::from_static(b"owner2"));
        assert_eq!(event.event_type, WatchEventType::Put as i32);
    }

    // CAS failed → no watch event emitted (key unchanged)
    #[tokio::test]
    async fn test_broadcast_cas_failure_no_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::CompareAndSwap {
                key: Bytes::from_static(b"lock"),
                expected: Some(Bytes::from_static(b"wrong_owner")),
                value: Bytes::from_static(b"owner2"),
            },
        );

        broadcast_watch_events(&[entry], &[failed(1)], &tx, None);

        assert!(
            rx.try_recv().is_err(),
            "Expected no watch event for failed CAS"
        );
    }

    // Multiple CAS: only succeeded ones emit events
    #[tokio::test]
    async fn test_broadcast_mixed_cas_success_and_failure() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entries = vec![
            apply_entry(
                1,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"k1"),
                    expected: Some(Bytes::from_static(b"v0")),
                    value: Bytes::from_static(b"v1"),
                },
            ),
            apply_entry(
                2,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"k2"),
                    expected: Some(Bytes::from_static(b"wrong")),
                    value: Bytes::from_static(b"v2"),
                },
            ),
            apply_entry(
                3,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"k3"),
                    expected: None,
                    value: Bytes::from_static(b"v3"),
                },
            ),
        ];
        let results = vec![succeeded(1), failed(2), succeeded(3)];

        broadcast_watch_events(&entries, &results, &tx, None);

        // k1 succeeded → Put event
        let event1 = rx.recv().await.unwrap();
        assert_eq!(event1.key, Bytes::from_static(b"k1"));
        assert_eq!(event1.value, Bytes::from_static(b"v1"));
        assert_eq!(event1.event_type, WatchEventType::Put as i32);

        // k3 succeeded → Put event (k2 failed, skipped)
        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.key, Bytes::from_static(b"k3"));
        assert_eq!(event2.value, Bytes::from_static(b"v3"));
        assert_eq!(event2.event_type, WatchEventType::Put as i32);

        assert!(rx.try_recv().is_err(), "Expected no further events");
    }

    // Insert + failed CAS + Delete → only 2 events (Insert and Delete)
    #[tokio::test]
    async fn test_broadcast_mixed_ops_with_failed_cas() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entries = vec![
            apply_entry(
                1,
                Command::Insert {
                    key: Bytes::from_static(b"k1"),
                    value: Bytes::from_static(b"v1"),
                    ttl_secs: None,
                },
            ),
            apply_entry(
                2,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"k2"),
                    expected: Some(Bytes::from_static(b"wrong")),
                    value: Bytes::from_static(b"v2"),
                },
            ),
            apply_entry(
                3,
                Command::Delete {
                    key: Bytes::from_static(b"k3"),
                },
            ),
        ];
        let results = vec![succeeded(1), failed(2), succeeded(3)];

        broadcast_watch_events(&entries, &results, &tx, None);

        let event1 = rx.recv().await.unwrap();
        assert_eq!(event1.key, Bytes::from_static(b"k1"));
        assert_eq!(event1.event_type, WatchEventType::Put as i32);

        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.key, Bytes::from_static(b"k3"));
        assert_eq!(event2.event_type, WatchEventType::Delete as i32);

        assert!(rx.try_recv().is_err(), "Expected no further events");
    }

    // results[i] aligns with chunk[i] by position, not by ApplyResult.index
    #[tokio::test]
    async fn test_broadcast_cas_results_index_alignment() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entries = vec![
            apply_entry(
                10,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"key-a"),
                    expected: Some(Bytes::from_static(b"old")),
                    value: Bytes::from_static(b"new-a"),
                },
            ),
            apply_entry(
                11,
                Command::CompareAndSwap {
                    key: Bytes::from_static(b"key-b"),
                    expected: Some(Bytes::from_static(b"old")),
                    value: Bytes::from_static(b"new-b"),
                },
            ),
        ];
        let results = vec![failed(10), succeeded(11)];

        broadcast_watch_events(&entries, &results, &tx, None);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.key, Bytes::from_static(b"key-b"));
        assert_eq!(event.value, Bytes::from_static(b"new-b"));
        assert_eq!(event.event_type, WatchEventType::Put as i32);

        assert!(rx.try_recv().is_err(), "key-a should not emit an event");
    }

    // Noop entries in chunk produce no watch events
    #[tokio::test]
    async fn test_broadcast_noop_produces_no_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(1, Command::Noop);
        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        assert!(rx.try_recv().is_err(), "Noop must not emit a watch event");
    }

    /// Command::Batch with multiple Insert ops must broadcast one Put event per key.
    ///
    /// # Purpose
    /// Batch is an atomic write of multiple ops. Each BatchOp::Insert must independently
    /// produce a watch event so subscribers watching individual keys are notified.
    /// This mirrors test_broadcast_insert_event but for the Batch command path.
    ///
    /// # Criteria
    /// - Command::Batch with 2 Insert ops: (k1 → v1), (k2 → v2)
    /// - apply succeeds for the batch entry
    ///
    /// # Expected
    /// - broadcast channel receives exactly 2 Put events
    /// - event 1: key=k1, value=v1, event_type=Put
    /// - event 2: key=k2, value=v2, event_type=Put
    /// - no extra events
    #[tokio::test]
    async fn test_broadcast_batch_insert_ops_each_produce_put_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::Batch {
                ops: vec![
                    BatchOp::Insert {
                        key: Bytes::from_static(b"k1"),
                        value: Bytes::from_static(b"v1"),
                    },
                    BatchOp::Insert {
                        key: Bytes::from_static(b"k2"),
                        value: Bytes::from_static(b"v2"),
                    },
                ],
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event1 = rx.recv().await.unwrap();
        assert_eq!(event1.key, Bytes::from_static(b"k1"));
        assert_eq!(event1.value, Bytes::from_static(b"v1"));
        assert_eq!(event1.event_type, WatchEventType::Put as i32);

        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.key, Bytes::from_static(b"k2"));
        assert_eq!(event2.value, Bytes::from_static(b"v2"));
        assert_eq!(event2.event_type, WatchEventType::Put as i32);

        assert!(rx.try_recv().is_err(), "Expected no further events");
    }

    /// Command::Batch with multiple Delete ops must broadcast one Delete event per key.
    ///
    /// # Purpose
    /// Mirrors test_broadcast_batch_insert_ops_each_produce_put_event for the Delete path.
    /// Each BatchOp::Delete must produce a Delete watch event with the correct key.
    ///
    /// # Criteria
    /// - Command::Batch with 2 Delete ops: keys k1, k2
    /// - apply succeeds for the batch entry
    ///
    /// # Expected
    /// - broadcast channel receives exactly 2 Delete events
    /// - event 1: key=k1, event_type=Delete
    /// - event 2: key=k2, event_type=Delete
    /// - no extra events
    #[tokio::test]
    async fn test_broadcast_batch_delete_ops_each_produce_delete_event() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::Batch {
                ops: vec![
                    BatchOp::Delete {
                        key: Bytes::from_static(b"k1"),
                    },
                    BatchOp::Delete {
                        key: Bytes::from_static(b"k2"),
                    },
                ],
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event1 = rx.recv().await.unwrap();
        assert_eq!(event1.key, Bytes::from_static(b"k1"));
        assert_eq!(event1.value, Bytes::new());
        assert_eq!(event1.event_type, WatchEventType::Delete as i32);

        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.key, Bytes::from_static(b"k2"));
        assert_eq!(event2.value, Bytes::new());
        assert_eq!(event2.event_type, WatchEventType::Delete as i32);

        assert!(rx.try_recv().is_err(), "Expected no further events");
    }

    /// Command::Batch with mixed Insert and Delete ops produces events in op order.
    ///
    /// # Purpose
    /// Op ordering within a batch must be preserved in the broadcast stream.
    /// Subscribers may rely on event order to reconstruct final state correctly
    /// (e.g., insert k1, then delete k1 must not arrive reversed).
    ///
    /// # Criteria
    /// - Command::Batch: [Insert(k1=v1), Delete(k2), Insert(k3=v3)]
    /// - apply succeeds for the batch entry
    ///
    /// # Expected
    /// - broadcast channel receives exactly 3 events in order:
    ///   Put(k1=v1), Delete(k2), Put(k3=v3)
    /// - no reordering, no missing events
    #[tokio::test]
    async fn test_broadcast_batch_mixed_ops_events_preserve_order() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(10);

        let entry = apply_entry(
            1,
            Command::Batch {
                ops: vec![
                    BatchOp::Insert {
                        key: Bytes::from_static(b"k1"),
                        value: Bytes::from_static(b"v1"),
                    },
                    BatchOp::Delete {
                        key: Bytes::from_static(b"k2"),
                    },
                    BatchOp::Insert {
                        key: Bytes::from_static(b"k3"),
                        value: Bytes::from_static(b"v3"),
                    },
                ],
            },
        );

        broadcast_watch_events(&[entry], &[succeeded(1)], &tx, None);

        let event1 = rx.recv().await.unwrap();
        assert_eq!(event1.key, Bytes::from_static(b"k1"));
        assert_eq!(event1.value, Bytes::from_static(b"v1"));
        assert_eq!(event1.event_type, WatchEventType::Put as i32);

        let event2 = rx.recv().await.unwrap();
        assert_eq!(event2.key, Bytes::from_static(b"k2"));
        assert_eq!(event2.value, Bytes::new());
        assert_eq!(event2.event_type, WatchEventType::Delete as i32);

        let event3 = rx.recv().await.unwrap();
        assert_eq!(event3.key, Bytes::from_static(b"k3"));
        assert_eq!(event3.value, Bytes::from_static(b"v3"));
        assert_eq!(event3.event_type, WatchEventType::Put as i32);

        assert!(rx.try_recv().is_err(), "Expected no further events");
    }
}

// =============================================================================
// #379: prev_kv — apply_chunk integration with prev_kv_watcher_count
// =============================================================================

#[cfg(feature = "watch")]
mod prev_kv_apply_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use d_engine_proto::client::{
        WriteCommand,
        write_command::{Insert, Operation},
    };
    use d_engine_proto::common::{Entry, EntryPayload, entry_payload::Payload};
    use prost::Message;

    use super::*;
    use crate::test_utils::snapshot_config;
    use crate::{ApplyResult, MockSnapshotPolicy, MockStateMachine, MockTypeConfig};

    fn insert_entry(
        index: u64,
        key: &[u8],
        value: &[u8],
    ) -> Entry {
        let write_cmd = WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                ttl_secs: 0,
            })),
        };
        let mut buf = Vec::new();
        write_cmd.encode(&mut buf).unwrap();
        Entry {
            index,
            term: 1,
            payload: Some(EntryPayload {
                payload: Some(Payload::Command(Bytes::from(buf))),
            }),
        }
    }

    /// #379 P5: When prev_kv_watcher_count == 0, apply_chunk must NOT call
    /// state_machine.get() at all.  The mock will panic if get() is called.
    ///
    /// This validates the performance optimization: zero extra RocksDB reads
    /// when no watcher has opted in to prev_kv.
    #[tokio::test]
    async fn test_apply_chunk_skips_get_when_no_prev_kv_watchers() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk().returning(|_| {
            Ok(vec![ApplyResult {
                index: 1,
                succeeded: true,
            }])
        });
        // DO NOT set up expect_get() → mockall panics if get() is called unexpectedly

        let (tx, _rx) = tokio::sync::broadcast::channel(10);
        let prev_kv_count = Arc::new(AtomicUsize::new(0)); // no prev_kv watchers

        let (_handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_skip_get_when_no_prev_kv"),
            snapshot_config(std::path::PathBuf::from(
                "/tmp/test_skip_get_when_no_prev_kv",
            )),
            MockSnapshotPolicy::new(),
            Some(tx),
            prev_kv_count,
        );

        let entry = insert_entry(1, b"k", b"v");
        writer.apply_chunk(vec![entry]).await.unwrap();
        // If get() were called, mockall would have panicked above.
    }

    /// #379 P6: When prev_kv_watcher_count > 0, apply_chunk reads the old value
    /// from the state machine and includes it in the broadcast WatchResponse.
    ///
    /// This validates the end-to-end prev_kv path: state machine read → broadcast event.
    #[tokio::test]
    async fn test_apply_chunk_reads_and_broadcasts_prev_value_when_count_nonzero() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk().returning(|_| {
            Ok(vec![ApplyResult {
                index: 1,
                succeeded: true,
            }])
        });
        // Expect exactly one get() call for the insert key
        sm.expect_get().times(1).returning(|_| Ok(Some(Bytes::from("old_val"))));

        let (tx, mut rx) = tokio::sync::broadcast::channel(10);
        let prev_kv_count = Arc::new(AtomicUsize::new(1)); // one prev_kv watcher active

        let (_handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_reads_prev_value_when_nonzero"),
            snapshot_config(std::path::PathBuf::from(
                "/tmp/test_reads_prev_value_when_nonzero",
            )),
            MockSnapshotPolicy::new(),
            Some(tx),
            prev_kv_count,
        );

        let entry = insert_entry(1, b"k", b"new_val");
        writer.apply_chunk(vec![entry]).await.unwrap();

        let event = rx.recv().await.expect("expected broadcast event");
        assert_eq!(
            event.prev_value,
            Bytes::from("old_val"),
            "broadcast event must carry the prev_value read before apply"
        );
        assert_eq!(event.value, Bytes::from("new_val"));
    }

    /// #379 P6b: After the last prev_kv watcher unregisters (count drops to 0),
    /// apply_chunk must NOT call state_machine.get() on the next apply.
    ///
    /// Tests the dynamic 1→0 transition: first apply with count=1 calls get()
    /// exactly once; then count is set to 0 and second apply must NOT call get().
    #[tokio::test]
    async fn test_apply_chunk_stops_reading_prev_value_after_count_drops_to_zero() {
        let mut sm = MockStateMachine::new();
        sm.expect_apply_chunk().times(2).returning(|_| {
            Ok(vec![ApplyResult {
                index: 1,
                succeeded: true,
            }])
        });
        // Expect exactly one get(): during the first apply (count=1).
        // The second apply (count=0) must not call get().
        // mockall verifies `times(1)` at drop — fails if called 0 or 2 times.
        sm.expect_get().times(1).returning(|_| Ok(Some(Bytes::from("old"))));

        let (tx, _rx) = tokio::sync::broadcast::channel(10);
        let prev_kv_count = Arc::new(AtomicUsize::new(1)); // one prev_kv watcher active

        let (_handler, writer) = new_reader_writer_pair::<MockTypeConfig>(
            1,
            0,
            Arc::new(sm),
            std::path::PathBuf::from("/tmp/test_stops_reading_after_count_zero"),
            snapshot_config(std::path::PathBuf::from(
                "/tmp/test_stops_reading_after_count_zero",
            )),
            MockSnapshotPolicy::new(),
            Some(tx),
            prev_kv_count.clone(),
        );

        // First apply: count=1 → get() is called
        let entry1 = insert_entry(1, b"k", b"v1");
        writer.apply_chunk(vec![entry1]).await.unwrap();

        // Simulate last prev_kv watcher unregistering
        prev_kv_count.store(0, Ordering::SeqCst);

        // Second apply: count=0 → get() must NOT be called
        let entry2 = insert_entry(2, b"k", b"v2");
        writer.apply_chunk(vec![entry2]).await.unwrap();
        // mockall verifies on drop: exactly 1 get() call was made
    }
}

mod wait_applied_tests {
    use super::*;
    use crate::test_utils::snapshot_config;
    use crate::{MockSnapshotPolicy, MockStateMachine, MockTypeConfig};

    /// wait_applied returns immediately when last_applied is already >= target index.
    ///
    /// # Purpose
    /// Raft ReadIndex fast path: if the SM has already applied up to or past the target
    /// commit index, the read may proceed without suspending. Blocking unnecessarily
    /// would stall linearizable reads and degrade throughput.
    ///
    /// # Criteria
    /// - handler initialized with last_applied = 10
    /// - wait_applied(target = 5) called  (5 <= 10, already satisfied)
    ///
    /// # Expected
    /// - returns Ok(()) immediately (< 10ms, no suspension)
    #[tokio::test]
    async fn test_wait_applied_returns_immediately_when_already_applied() {
        let handler = DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            10, // last_applied = 10
            Arc::new(MockStateMachine::new()),
            PathBuf::from("/tmp/test_wait_applied_fast_path"),
            snapshot_config(PathBuf::from("/tmp/test_wait_applied_fast_path")),
            MockSnapshotPolicy::new(),
        );

        let start = std::time::Instant::now();
        let result = handler.wait_applied(5, Duration::from_millis(100)).await;
        let elapsed = start.elapsed();

        assert!(
            result.is_ok(),
            "Should return Ok when target <= last_applied"
        );
        assert!(
            elapsed < Duration::from_millis(10),
            "Fast path must not suspend, actual: {elapsed:?}"
        );
    }

    /// wait_applied blocks until apply_chunk advances last_applied to the target index.
    ///
    /// # Purpose
    /// Raft ReadIndex blocking path: when the SM has not yet reached the commit index,
    /// the read must wait. A concurrent apply_chunk call advancing last_applied to the
    /// target must unblock the waiter at the correct index (not before, not after).
    ///
    /// # Criteria
    /// - handler starts with last_applied = 0
    /// - wait_applied(target = 5) is spawned concurrently
    /// - apply_chunk(entries 1..=5) is called from another task
    ///
    /// # Expected
    /// - wait_applied blocks until index 5 is applied
    /// - returns Ok(()) after unblocking
    /// - does not unblock at index 4 or earlier
    #[tokio::test]
    async fn test_wait_applied_blocks_until_target_index_reached() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            Arc::new(MockStateMachine::new()),
            PathBuf::from("/tmp/test_wait_applied_blocking"),
            snapshot_config(PathBuf::from("/tmp/test_wait_applied_blocking")),
            MockSnapshotPolicy::new(),
        ));

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();
        let h = handler.clone();

        tokio::spawn(async move {
            let _ = h.wait_applied(5, Duration::from_millis(500)).await;
            completed_clone.store(true, Ordering::SeqCst);
        });

        // Advance to 4 — waiter must remain blocked
        tokio::time::sleep(Duration::from_millis(5)).await;
        handler.test_simulate_apply(4);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(
            !completed.load(Ordering::SeqCst),
            "must not unblock at index 4"
        );

        // Advance to 5 — waiter must unblock
        handler.test_simulate_apply(5);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(completed.load(Ordering::SeqCst), "must unblock at index 5");
    }

    /// Multiple concurrent waiters at different indices all wake correctly and independently.
    ///
    /// # Purpose
    /// Prevents starvation and validates the broadcast-style notify under concurrent load.
    /// The ReadIndex path can have many in-flight linearizable reads simultaneously,
    /// each waiting on a different commit index. Each waiter must wake exactly at its
    /// own threshold — no earlier, no later.
    ///
    /// # Criteria
    /// - 3 concurrent waiters: wait_applied(3), wait_applied(5), wait_applied(7)
    /// - apply_chunk progresses entry by entry: 1, 2, 3, 4, 5, 6, 7
    ///
    /// # Expected
    /// - waiter at 3 unblocks after index 3, not before
    /// - waiter at 5 unblocks after index 5, not before
    /// - waiter at 7 unblocks after index 7, not before
    /// - all 3 return Ok(())
    #[tokio::test]
    async fn test_wait_applied_multiple_waiters_all_wake_correctly() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let handler = Arc::new(DefaultStateMachineHandler::<MockTypeConfig>::new(
            1,
            0,
            Arc::new(MockStateMachine::new()),
            PathBuf::from("/tmp/test_wait_applied_multiple_waiters"),
            snapshot_config(PathBuf::from("/tmp/test_wait_applied_multiple_waiters")),
            MockSnapshotPolicy::new(),
        ));

        let (c3, c5, c7) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );

        for (target, flag) in [(3u64, c3.clone()), (5, c5.clone()), (7, c7.clone())] {
            let h = handler.clone();
            tokio::spawn(async move {
                let _ = h.wait_applied(target, Duration::from_millis(500)).await;
                flag.store(true, Ordering::SeqCst);
            });
        }

        // Give spawned tasks a moment to enter their wait loops
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Advance to 2 — no waiter should unblock
        handler.test_simulate_apply(2);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !c3.load(Ordering::SeqCst),
            "waiter@3 must not unblock at index 2"
        );
        assert!(
            !c5.load(Ordering::SeqCst),
            "waiter@5 must not unblock at index 2"
        );
        assert!(
            !c7.load(Ordering::SeqCst),
            "waiter@7 must not unblock at index 2"
        );

        // Advance to 3 — only waiter@3 should unblock
        handler.test_simulate_apply(3);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            c3.load(Ordering::SeqCst),
            "waiter@3 must unblock at index 3"
        );
        assert!(
            !c5.load(Ordering::SeqCst),
            "waiter@5 must not unblock at index 3"
        );
        assert!(
            !c7.load(Ordering::SeqCst),
            "waiter@7 must not unblock at index 3"
        );

        // Advance to 5 — waiter@5 should unblock, waiter@7 still waiting
        handler.test_simulate_apply(5);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            c5.load(Ordering::SeqCst),
            "waiter@5 must unblock at index 5"
        );
        assert!(
            !c7.load(Ordering::SeqCst),
            "waiter@7 must not unblock at index 5"
        );

        // Advance to 7 — waiter@7 unblocks
        handler.test_simulate_apply(7);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            c7.load(Ordering::SeqCst),
            "waiter@7 must unblock at index 7"
        );
    }
}
