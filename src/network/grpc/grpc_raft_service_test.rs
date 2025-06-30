use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time;
use tonic::Code;
use tonic::Request;

use crate::convert::safe_kv;
use crate::proto::client::raft_client_service_server::RaftClientService;
use crate::proto::client::ClientReadRequest;
use crate::proto::client::ClientWriteRequest;
use crate::proto::client::WriteCommand;
use crate::proto::cluster::cluster_management_service_server::ClusterManagementService;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterConfUpdateResponse;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::MetadataRequest;
use crate::proto::common::LogId;
use crate::proto::election::raft_election_service_server::RaftElectionService;
use crate::proto::election::VoteRequest;
use crate::proto::replication::raft_replication_service_server::RaftReplicationService;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::test_utils::enable_logger;
use crate::test_utils::mock_node;
use crate::test_utils::MockBuilder;
use crate::test_utils::MockTypeConfig;
use crate::AppendResponseWithUpdates;
use crate::AppendResults;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockReplicationCore;
use crate::RaftNodeConfig;
use crate::StateUpdate;

/// # Case: Test RPC services timeout
#[tokio::test]
async fn test_handle_service_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_handle_service_timeout", graceful_rx, None);

    // Vote request
    assert!(node
        .request_vote(Request::new(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await
        .is_err());

    // Append Entries request
    assert!(node
        .append_entries(Request::new(AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 1
        }))
        .await
        .is_err());

    // Update cluster conf request
    assert!(node
        .update_cluster_conf(Request::new(ClusterConfChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            change: None
        }))
        .await
        .is_err());

    // Client Propose request
    assert!(node
        .handle_client_write(Request::new(ClientWriteRequest {
            commands: vec![],
            client_id: 1,
        }))
        .await
        .is_err());

    // Metadata request
    assert!(node
        .get_cluster_metadata(Request::new(MetadataRequest {}))
        .await
        .is_err());

    // Client read request
    assert!(node
        .handle_client_read(Request::new(ClientReadRequest {
            client_id: 1,
            linear: true,
            keys: vec![]
        }))
        .await
        .is_err());
}

/// # Case: Test server is not ready
#[tokio::test]
async fn test_server_is_not_ready() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_server_is_not_ready", graceful_rx, None);
    // Force the server to not be ready (implementation-specific)
    node.set_ready(false);

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
            commands: vec![],
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
            linear: true,
            keys: vec![],
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);
}

/// # Case: Test handle rpc services successful
#[tokio::test]
async fn test_handle_rpc_services_successfully() {
    tokio::time::pause();
    enable_logger();
    let mut settings = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");
    settings.raft.general_raft_timeout_duration_in_ms = 200;
    settings.raft.replication.rpc_append_entries_in_batch_threshold = 0;
    settings.cluster.db_root_dir = PathBuf::from(
        "/tmp/
    test_handle_rpc_services_successfully",
    );
    let mut membership = MockMembership::<MockTypeConfig>::new();
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    membership.expect_voters().returning(Vec::new);
    membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    membership
        .expect_update_cluster_conf_from_leader()
        .returning(|_, _, _, _, _| Ok(ClusterConfUpdateResponse::success(1, 1, 1)));
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership {
            version: 1,
            nodes: vec![],
        });
    membership.expect_current_leader_id().returning(|| None);
    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(move |_, _, _| {
            Ok(AppendResponseWithUpdates {
                response: AppendEntriesResponse::success(1, 1, Some(LogId { term: 1, index: 1 })),
                commit_index_update: Some(1),
            })
        });
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
        .expect_handle_vote_request()
        .times(1)
        .returning(|_, _, _, _| {
            Ok(StateUpdate {
                new_voted_for: None,
                term_update: None,
            })
        });
    election_handler
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    election_handler
        .expect_check_vote_request_is_legal()
        .returning(|_, _, _, _, _| true);
    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = MockBuilder::new(graceful_rx)
        .with_membership(membership)
        .with_replication_handler(replication_handler)
        .with_election_handler(election_handler)
        .wiht_node_config(settings)
        .build_node();
    node.set_ready(true);

    // Start Raft run thread
    let raft_lock = node.raft_core.clone();
    let raft_handle = tokio::spawn(async move {
        let mut raft = raft_lock.lock().await;
        let _ = time::timeout(Duration::from_millis(100), raft.run()).await;
    });

    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;

    let service_handler = tokio::spawn(async move {
        assert!(node
            .request_vote(Request::new(VoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .is_ok());

        assert!(node
            .append_entries(Request::new(AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit_index: 1,
            }))
            .await
            .is_ok());

        assert!(node
            .update_cluster_conf(Request::new(ClusterConfChangeRequest {
                id: 1,
                term: 1,
                version: 1,
                change: None
            }))
            .await
            .is_ok());

        assert!(node
            .handle_client_write(Request::new(ClientWriteRequest {
                client_id: 1,
                commands: vec![WriteCommand::delete(safe_kv(1))],
            }))
            .await
            .is_ok());

        assert!(node
            .get_cluster_metadata(Request::new(MetadataRequest {}))
            .await
            .is_ok());

        assert!(node
            .handle_client_read(Request::new(ClientReadRequest {
                client_id: 1,
                linear: false,
                keys: vec![],
            }))
            .await
            .is_ok());
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (_, service_response) = tokio::join!(raft_handle, service_handler,);
    println!("{service_response:?}",);

    // Assert if the handle client propose result is ok.
    assert!(service_response.is_ok());
}
