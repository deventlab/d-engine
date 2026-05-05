use std::path::PathBuf;
use std::time::Duration;

use crate::ApplyResult;
use d_engine_core::AppendResponseWithUpdates;
use d_engine_core::MockElectionCore;
use d_engine_core::MockMembership;
use d_engine_core::MockRaftLog;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockTypeConfig;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RoleEvent;
use d_engine_core::convert::safe_kv_bytes;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
use d_engine_proto::client::WatchMembershipRequest;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::raft_client_service_server::RaftClientService;
use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementService;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionService;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationService;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time;
use tonic::Code;
use tonic::Request;
use tracing_test::traced_test;

use crate::test_utils::MockBuilder;
use crate::test_utils::mock_node;

/// # Case: Test RPC services timeout
#[tokio::test]
#[traced_test]
async fn test_handle_service_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_handle_service_timeout", graceful_rx, None);

    // Vote request
    assert!(
        node.request_vote(Request::new(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await
        .is_err()
    );

    // Append Entries request
    assert!(
        node.append_entries(Request::new(AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 1
        }))
        .await
        .is_err()
    );

    // Update cluster conf request
    assert!(
        node.update_cluster_conf(Request::new(ClusterConfChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            change: None
        }))
        .await
        .is_err()
    );

    // Client Propose request
    assert!(
        node.handle_client_write(Request::new(ClientWriteRequest {
            command: Some(WriteCommand::default()),
            client_id: 1,
        }))
        .await
        .is_err()
    );

    // Metadata request
    assert!(node.get_cluster_metadata(Request::new(MetadataRequest {})).await.is_err());

    // Client read request
    assert!(
        node.handle_client_read(Request::new(ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            keys: vec![]
        }))
        .await
        .is_err()
    );
}

/// # Case: Test server is not ready
#[tokio::test]
#[traced_test]
async fn test_server_is_not_ready() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_server_is_not_ready", graceful_rx, None);
    // Force the server to not be ready (implementation-specific)
    node.set_rpc_ready(false);

    // Vote request
    let result = node
        .request_vote(Request::new(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Append Entries request
    let result = node
        .append_entries(Request::new(AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 1,
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Update cluster conf request
    let result = node
        .update_cluster_conf(Request::new(ClusterConfChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            change: None,
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Client Propose request
    let result = node
        .handle_client_write(Request::new(ClientWriteRequest {
            client_id: 1,
            command: Some(WriteCommand::default()),
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Metadata request
    let result = node.get_cluster_metadata(Request::new(MetadataRequest {})).await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Client read request
    let result = node
        .handle_client_read(Request::new(ClientReadRequest {
            client_id: 1,
            consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
            keys: vec![],
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);
}

/// # Case: Test handle rpc services successful
#[tokio::test]
#[traced_test]
async fn test_handle_rpc_services_successfully() {
    tokio::time::pause();
    let mut settings = RaftNodeConfig::new()
        .expect("Should succeed to init RaftNodeConfig.")
        .validate()
        .expect("Validate RaftNodeConfig successfully");
    settings.raft.general_raft_timeout_duration_in_ms = 200;
    settings.raft.batching.max_batch_size = 1;
    settings.cluster.db_root_dir = PathBuf::from(
        "/tmp/
    test_handle_rpc_services_successfully",
    );
    let mut membership = MockMembership::<MockTypeConfig>::new();

    membership.expect_voters().returning(Vec::new);
    membership.expect_members().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership
        .expect_update_cluster_conf_from_leader()
        .returning(|_, _, _, _, _| Ok(ClusterConfUpdateResponse::success(1, 1, 1)));
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|_current_leader_id| ClusterMembership {
            version: 1,
            nodes: vec![],
            current_leader_id: None,
        });

    // Dynamic log index: starts at 0, incremented by prepare_batch_requests per payload written.
    // Used so the test can read the correct log index when sending LogFlushed to trigger commit.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    let log_index = Arc::new(AtomicU64::new(0));
    let li_last = log_index.clone();
    let li_flush = log_index.clone();
    let li_prepare = log_index.clone();

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_entry_id()
        .returning(move || li_last.load(Ordering::Relaxed));
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| None);
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft_log.expect_last_log_id().returning(|| None);

    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    replication_handler
        .expect_check_append_entries_request_is_legal()
        .returning(|my_term, _, _| AppendEntriesResponse::success(1, my_term, None));
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(AppendResponseWithUpdates {
            response: AppendEntriesResponse::success(1, 1, Some(LogId { term: 1, index: 1 })),
            commit_index_update: Some(1),
        })
    });
    replication_handler
        .expect_prepare_batch_requests()
        .returning(move |payloads, _, _, _, _| {
            li_prepare.fetch_add(payloads.len() as u64, Ordering::Relaxed);
            Ok(d_engine_core::PrepareResult::default())
        });
    let mut election_handler = MockElectionCore::<MockTypeConfig>::new();
    election_handler
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    election_handler
        .expect_check_vote_request_is_legal()
        // Return false: the incoming vote request (term=1) is not more up-to-date than
        // the candidate's current term, so it must not cause a step-down.
        // Leader (term=2) receiving VoteRequest(term=1) directly rejects without calling
        // handle_vote_request — correct Raft protocol behavior.
        .returning(|_, _, _, _, _| false);
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    // Create role channel so the test can inject LogFlushed events directly into the raft loop.
    // Commit in single-voter mode is now async (driven by LogFlushed from BufferedRaftLog);
    // since this test uses MockRaftLog (no real batch_processor), we send LogFlushed manually.
    let (role_tx, role_rx) = mpsc::unbounded_channel::<RoleEvent>();
    let test_role_tx = role_tx.clone();
    let mut builder = MockBuilder::new(graceful_rx);
    builder.role_tx = Some(role_tx);
    builder.role_rx = Some(role_rx);
    let node = builder
        .with_raft_log(raft_log)
        .with_membership(membership)
        .with_replication_handler(replication_handler)
        .with_election_handler(election_handler)
        .with_node_config(settings)
        .build_node();
    node.set_rpc_ready(true);

    // Start Raft run thread
    let raft_lock = node.raft_core.clone();
    let raft_handle = tokio::spawn(async move {
        let mut raft = raft_lock.lock().await;
        let _ = time::timeout(Duration::from_millis(100), raft.run()).await;
    });

    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;

    let service_handler = tokio::spawn(async move {
        assert!(
            node.request_vote(Request::new(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .is_ok()
        );

        // Wait for raft loop to complete BecomeLeader (tick → broadcast_vote_requests → verify noop).
        tokio::time::sleep(Duration::from_millis(10)).await;

        // handle_client_write and handle_client_read must be sent while node is leader.
        // append_entries (term=1) will cause the candidate to step down, so write/read
        // are sent first before append_entries disrupts the leadership state.
        //
        // Client writes require commit + ApplyCompleted to resolve pending_write_apply.
        // Spawn a task to: (1) fire LogFlushed to advance commit_index, then
        // (2) simulate SM apply completion. Both are needed because with MockRaftLog
        // there is no real batch_processor sending LogFlushed events.
        tokio::spawn(async move {
            // Yield to let raft loop process the write cmd and store it in
            // pending_client_writes before we advance the commit index.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            // Advance commit_index via LogFlushed — moves entries from
            // pending_client_writes into pending_write_apply (drain_pending_client_writes).
            let flush_idx = li_flush.load(Ordering::Relaxed);
            let _ = test_role_tx.send(RoleEvent::LogFlushed {
                durable_index: flush_idx,
            });
            // Yield again so the raft loop processes LogFlushed before ApplyCompleted.
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            // ApplyCompleted is now sent via role_tx (P2) to avoid priority inversion.
            let _ = test_role_tx.send(RoleEvent::ApplyCompleted {
                last_index: 2,
                results: vec![ApplyResult {
                    index: 2,
                    succeeded: true,
                }],
            });
        });
        assert!(
            node.handle_client_write(Request::new(ClientWriteRequest {
                client_id: 1,
                command: Some(WriteCommand::delete(safe_kv_bytes(1))),
            }))
            .await
            .is_ok()
        );

        assert!(node.get_cluster_metadata(Request::new(MetadataRequest {})).await.is_ok());

        assert!(
            node.handle_client_read(Request::new(ClientReadRequest {
                client_id: 1,
                consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency as i32),
                keys: vec![],
            }))
            .await
            .is_ok()
        );

        assert!(
            node.append_entries(Request::new(AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit_index: 1,
            }))
            .await
            .is_ok()
        );

        assert!(
            node.update_cluster_conf(Request::new(ClusterConfChangeRequest {
                id: 1,
                term: 1,
                version: 1,
                change: None
            }))
            .await
            .is_ok()
        );
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (_, service_response) = tokio::join!(raft_handle, service_handler,);
    println!("{service_response:?}",);

    // Assert if the handle client propose result is ok.
    assert!(service_response.is_ok());
}

// =============================================================================
// WatchMembership unit tests
// =============================================================================

/// Node not ready → watch_membership must return UNAVAILABLE immediately.
///
/// Guards the readiness check path; without it, clients could subscribe to a
/// node that hasn't joined the cluster yet and receive stale/empty data.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_returns_unavailable_when_node_not_ready() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node("/tmp/test_watch_membership_not_ready", shutdown_rx, None);
    node.set_rpc_ready(false);

    let result = node
        .watch_membership(Request::new(WatchMembershipRequest { client_id: 1 }))
        .await;

    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);
}

/// Node ready with default (empty) snapshot → stream yields the current snapshot
/// immediately (via mark_changed), then the sentinel UNAVAILABLE when the sender
/// is dropped (mock_node drops the membership_tx).
///
/// This test validates the two-phase stream lifecycle:
/// 1. Initial state delivery on connect (mark_changed behavior)
/// 2. Sentinel error delivery when the server-side channel closes
#[tokio::test]
#[traced_test]
async fn test_watch_membership_yields_current_snapshot_then_sentinel_on_sender_drop() {
    use tokio_stream::StreamExt;

    let (_shutdown_tx, shutdown_rx) = watch::channel(());
    let node = mock_node(
        "/tmp/test_watch_membership_snapshot_then_sentinel",
        shutdown_rx,
        None,
    );
    node.set_rpc_ready(true);

    let response = node
        .watch_membership(Request::new(WatchMembershipRequest { client_id: 1 }))
        .await
        .expect("watch_membership should succeed when node is ready");

    let mut stream = response.into_inner();

    // First item: current snapshot (mark_changed causes immediate delivery).
    // mock_node initialises membership_rx with MembershipSnapshot::default().
    let first = stream.next().await.expect("stream must yield at least one item");
    assert!(first.is_ok(), "first item must be Ok(snapshot)");
    let snap = first.unwrap();
    assert!(snap.members.is_empty(), "default snapshot has no voters");
    assert!(snap.learners.is_empty(), "default snapshot has no learners");
    assert_eq!(snap.committed_index, 0, "default committed_index is 0");

    // Second item: sentinel error — mock_node drops the sender, so WatchStream ends
    // and the chained sentinel fires.
    let second = stream.next().await.expect("stream must yield sentinel");
    assert!(second.is_err(), "sentinel must be Err(UNAVAILABLE)");
    assert_eq!(second.unwrap_err().code(), Code::Unavailable);

    // Stream is exhausted after the sentinel.
    assert!(stream.next().await.is_none());
}
