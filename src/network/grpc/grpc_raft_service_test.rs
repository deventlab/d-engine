use super::rpc_service::{rpc_service_server::RpcService, ClientProposeRequest};
use crate::{
    grpc::rpc_service::{ClientCommand, ClientResponse},
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

/// # Test client propose timeout
///
#[tokio::test]
async fn test_handle_client_propose_case1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_handle_client_propose_case1", graceful_rx, None);
    let request = Request::new(ClientProposeRequest {
        commands: vec![],
        client_id: 1,
    });
    let r = node.handle_client_propose(request).await;
    assert!(r.is_err());
}

/// # Test server is not ready
///
#[tokio::test]
async fn test_handle_client_propose_case2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let node = mock_node("/tmp/test_handle_client_propose_case2", graceful_rx, None);
    // Force the server to not be ready (implementation-specific)
    node.set_ready(false);

    let request = Request::new(ClientProposeRequest {
        client_id: 1,
        commands: vec![],
    });

    let result = node.handle_client_propose(request).await;
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().code(), Code::Unavailable);
}

/// # Test successful client propose
///
#[tokio::test]
async fn test_handle_client_propose_case3() {
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
    membership.expect_mark_leader_id().returning(|_| {});
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
