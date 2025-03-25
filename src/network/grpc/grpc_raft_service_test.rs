use super::rpc_service::{rpc_service_server::RpcService, ClientProposeRequest};
use crate::{
    grpc::rpc_service::{
        AppendEntriesRequest, ClientCommand, ClientReadRequest, ClientResponse,
        ClusteMembershipChangeRequest, ClusterMembership, MetadataRequest, VoteRequest,
    },
    test_utils::{enable_logger, mock_node, MockBuilder, MockTypeConfig},
    utils::util::kv,
    MockMembership, RaftEvent, Settings,
};
use log::debug;
use std::time::Duration;
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

/// # Case: Test successful handle vote request
///
#[tokio::test]
async fn test_handle_request_vote_case() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_handle_request_vote_case", graceful_rx, None);
    // Force the server to not be ready (implementation-specific)
    node.set_ready(true);

    // Vote request
    let result = node
        .request_vote(Request::new(VoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }))
        .await;
}

/// # Case: Test successful client propose
///
#[tokio::test]
async fn test_handle_client_propose_case() {
    let _ = tokio::time::pause();
    // let node = mock_node("/tmp/test_handle_client_propose_case3", None);
    enable_logger();
    let mut settings = Settings::new().expect("Should succeed to init Settings.");
    settings.server_settings.db_root_dir = format!(
        "{}",
        "/tmp/
    test_handle_client_propose_case3"
    );
    let mut membership = MockMembership::<MockTypeConfig>::new();
    membership.expect_mark_leader_id().returning(|_| Ok(()));
    membership.expect_voting_members().returning(|_| vec![]);
    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());
    let node = MockBuilder::new(graceful_rx)
        .with_membership(membership)
        .with_settings(settings)
        .build_node();
    node.set_ready(true);

    // Prepare client propose request
    let mut commands = Vec::new();
    for id in 1..=2 {
        commands.push(ClientCommand::insert(kv(id), kv(id)));
    }
    let request = Request::new(ClientProposeRequest {
        client_id: 1,
        commands,
    });
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<RaftEvent>();

    // Start Raft run thread
    let raft_lock = node.raft_core.clone();
    let raft_handle = tokio::spawn(async move {
        let mut raft = raft_lock.lock().await;
        raft.register_raft_event_listener(monitor_tx);
        let _ = time::timeout(Duration::from_millis(20), raft.run()).await;
    });

    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Handle client propose request and wait for propose result
    let handle1 = tokio::spawn(async move { node.handle_client_propose(request).await });

    // Raft run thread should send out the ClientPropose event
    let handle2 = tokio::spawn(async move {
        match monitor_rx.recv().await {
            Some(RaftEvent::ClientPropose(_, sender)) => {
                let response = ClientResponse::write_success();
                sender.send(Ok(response)).expect("should succeed");
            }
            Some(e) => {
                debug!("receive other event: {:?}", &e);
            }
            _ => {
                assert!(false)
            }
        }
    });

    let (_, r1, _) = tokio::join!(raft_handle, handle1, handle2);

    // Assert if the handle client propose result is ok.
    assert!(r1.is_ok());
}
