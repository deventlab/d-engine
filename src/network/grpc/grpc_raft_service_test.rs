use super::rpc_service::{rpc_service_server::RpcService, ClientProposeRequest};
use crate::{
    grpc::rpc_service::{
        AppendEntriesRequest, ClientCommand, ClientReadRequest, ClusteMembershipChangeRequest,
        ClusterMembership, MetadataRequest, VoteRequest,
    },
    test_utils::{enable_logger, mock_node, MockBuilder, MockTypeConfig},
    utils::util::kv,
    AppendResults, MockMembership, MockReplicationCore, Settings,
};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time,
};
use tonic::{Code, Request};

/// # Case: Test RPC services timeout
///
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
        .update_cluster_conf(Request::new(ClusteMembershipChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            cluster_membership: Some(ClusterMembership { nodes: vec![] })
        }))
        .await
        .is_err());

    // Client Propose request
    assert!(node
        .handle_client_propose(Request::new(ClientProposeRequest {
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
            commands: vec![]
        }))
        .await
        .is_err());
}

/// # Case: Test server is not ready
///
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
        .update_cluster_conf(Request::new(ClusteMembershipChangeRequest {
            id: 1,
            term: 1,
            version: 1,
            cluster_membership: Some(ClusterMembership { nodes: vec![] }),
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Client Propose request
    let result = node
        .handle_client_propose(Request::new(ClientProposeRequest {
            client_id: 1,
            commands: vec![],
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Metadata request
    let result = node
        .get_cluster_metadata(Request::new(MetadataRequest {}))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);

    // Client read request
    let result = node
        .handle_client_read(Request::new(ClientReadRequest {
            client_id: 1,
            linear: true,
            commands: vec![],
        }))
        .await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);
}

/// # Case: Test handle rpc services successful
///
#[tokio::test]
async fn test_handle_rpc_services_successfully() {
    enable_logger();
    let mut settings = Settings::new().expect("Should succeed to init Settings.");
    settings.raft_settings.general_raft_timeout_duration_in_ms = 200;
    settings.server_settings.db_root_dir = format!(
        "{}",
        "/tmp/
    test_handle_rpc_services_successfully"
    );
    let mut membership = MockMembership::<MockTypeConfig>::new();
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    membership.expect_voting_members().returning(|_| vec![]);
    membership
        .expect_update_cluster_conf_from_leader()
        .returning(|_, _| Ok(()));
    membership.expect_get_cluster_conf_version().returning(|| 1);
    membership
        .expect_retrieve_cluster_membership_config()
        .returning(|| ClusterMembership { nodes: vec![] });
    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    replication_handler
        .expect_handle_client_proposal_in_batch()
        .returning(|_, _, _, _, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: false,
                peer_updates: HashMap::new(),
            })
        });

    // Initializing Shutdown Signal
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = MockBuilder::new(graceful_rx)
        .with_membership(membership)
        .with_replication_handler(replication_handler)
        .with_settings(settings)
        .build_node();
    node.set_ready(true);

    // Start Raft run thread
    let raft_lock = node.raft_core.clone();
    let raft_handle = tokio::spawn(async move {
        let mut raft = raft_lock.lock().await;
        let _ = time::timeout(Duration::from_millis(500), raft.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

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
            .update_cluster_conf(Request::new(ClusteMembershipChangeRequest {
                id: 1,
                term: 1,
                version: 1,
                cluster_membership: Some(ClusterMembership { nodes: vec![] }),
            }))
            .await
            .is_ok());

        assert!(node
            .handle_client_propose(Request::new(ClientProposeRequest {
                client_id: 1,
                commands: vec![ClientCommand::get(kv(1))],
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
                commands: vec![],
            }))
            .await
            .is_ok());
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (_, service_response) = tokio::join!(raft_handle, service_handler,);

    // Assert if the handle client propose result is ok.
    assert!(service_response.is_ok());
}
