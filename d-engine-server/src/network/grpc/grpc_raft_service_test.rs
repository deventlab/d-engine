use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::ApplyResult;
use d_engine_core::AppendResponseWithUpdates;
use d_engine_core::AppendResults;
use d_engine_core::MockElectionCore;
use d_engine_core::MockMembership;
use d_engine_core::MockReplicationCore;
use d_engine_core::MockTypeConfig;
use d_engine_core::RaftEvent;
use d_engine_core::RaftNodeConfig;
use d_engine_core::convert::safe_kv_bytes;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::ReadConsistencyPolicy;
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
        .expect_handle_raft_request_in_batch()
        .returning(|_, _, _, _, _| {
            Ok(AppendResults {
                // Must be true: leader requires quorum on noop entry to confirm leadership.
                // If false, verify_leadership_persistent() causes immediate step-down.
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::new(),
            })
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
    let node = MockBuilder::new(graceful_rx)
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
        // Client writes require ApplyCompleted to resolve pending_requests.
        // Spawn a task to simulate SM apply after the write is queued.
        let apply_tx = node.event_tx.clone();
        tokio::spawn(async move {
            // Yield multiple times to let raft loop process the write cmd
            // and queue the sender into pending_requests before we send ApplyCompleted.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            let _ = apply_tx
                .send(RaftEvent::ApplyCompleted {
                    last_index: 1,
                    results: vec![ApplyResult {
                        index: 1,
                        succeeded: true,
                    }],
                })
                .await;
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
