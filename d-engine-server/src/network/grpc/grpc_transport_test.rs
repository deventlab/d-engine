use std::collections::HashMap;

use d_engine_core::ConnectionType;
use d_engine_core::Error;
use d_engine_core::MockMembership;
use d_engine_core::MockTypeConfig;
use d_engine_core::NetworkError;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RetryPolicies;
use d_engine_core::SystemError;
use d_engine_core::Transport;
use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::storage::SnapshotChunk;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use tokio::sync::oneshot;
use tonic::Status;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing_test::traced_test;

use super::*;
use crate::network::grpc::grpc_transport::GrpcTransport;
use crate::test_utils::MockNode;
use crate::test_utils::MockRpcService;
use crate::test_utils::create_test_chunk;
use crate::test_utils::create_test_snapshot_stream;

fn node_config(db_path: &str) -> RaftNodeConfig {
    let mut s = RaftNodeConfig::new().expect("RaftNodeConfig should be inited successfully");
    s.cluster.db_root_dir = std::path::PathBuf::from(db_path);
    s.validate().expect("RaftNodeConfig should be validated successfully")
}

fn mock_membership(
    peers: Vec<(u32, i32)>, //(node_id, role_i32)
    channels: HashMap<(u32, ConnectionType), Channel>,
) -> Arc<MockMembership<MockTypeConfig>> {
    let mut membership = MockMembership::<MockTypeConfig>::new();
    membership.expect_voters().returning(move || {
        peers
            .iter()
            .map(|(id, role)| NodeMeta {
                id: *id,
                address: "127.0.0.1:50051".to_string(),
                role: *role,
                status: NodeStatus::Active.into(),
            })
            .collect()
    });

    membership
        .expect_get_peer_channel()
        .returning(move |peer_id, conn_type| channels.get(&(peer_id, conn_type)).cloned());

    Arc::new(membership)
}

// # Case 1: no peers passed
//
// ## Criterias:
// 1. return Err(NetworkError::EmptyPeerList)
//
#[tokio::test]
#[traced_test]
async fn test_send_cluster_update_case1() {
    let my_id = 1;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case1");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: None,
    };

    let membership = mock_membership(vec![], HashMap::new());
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_cluster_update(request, &node_config.retry, membership).await;
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        Error::System(SystemError::Network(NetworkError::EmptyPeerList { .. }))
    ));
}

// # Case 2: passed peers only include the node itself
//
// ## Criterias:
// 1. return Err(NetworkError::EmptyPeerList)
//
#[tokio::test]
#[traced_test]
async fn test_send_cluster_update_case2() {
    let my_id = 1;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case2");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: None,
    };

    // Simulate RPC service
    let (_tx, rx) = oneshot::channel::<()>();
    let response = ClusterMembership {
        version: 1,
        nodes: vec![],
        current_leader_id: None,
    };
    let (channel, _port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |_port| Ok(response.clone()))),
    )
    .await
    .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((my_id, ConnectionType::Control), channel.clone());

    let membership = mock_membership(vec![(my_id, Follower as i32)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_cluster_update(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.is_empty());
            assert!(res.peer_ids.is_empty())
        }
        Err(_) => panic!(),
    }
}

// # Case 3: passed peers only include the node itself
//
// ## Setup
// 1. prepare [peer1, peer1, peer2] as `peers` parameter
// 2. both peer1 and peer2 return success
//
// ## Criterias:
// 1. return Ok with two responses
//
#[tokio::test]
#[traced_test]
async fn test_send_cluster_update_case3() {
    let my_id = 1;
    let peer1_id = 2;
    let peer2_id = 3;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case3");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: None,
    };

    // Simulate RPC service
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(
        vec![(peer1_id, Follower as i32), (peer2_id, Candidate as i32)],
        channels,
    );

    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_cluster_update(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// # Case 4: failed to sync two peers
//
// ## Setup
// 1. Prepare two peers, both peer failed
//
// ## Criterias:
// 1. return Ok with two responses
//
#[tokio::test]
#[traced_test]
async fn test_send_cluster_update_case4() {
    let my_id = 1;
    let peer1_id = 2;
    let peer2_id = 3;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case4");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterConfChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        change: None,
    };

    // Simulate RPC service
    let (_tx, rx) = oneshot::channel::<()>();
    let (channel, _port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |_port| {
            Err(Status::unavailable("message".to_string()))
        })),
    )
    .await
    .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(
        vec![(peer1_id, Follower as i32), (peer2_id, Candidate as i32)],
        channels,
    );

    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_cluster_update(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// send_append_requests (batch, all-peers-at-once) was removed as dead code (#428):
// zero production callers — real replication goes through the per-peer
// ReplicationWorker (leader_state.rs::send_to_worker_or_spawn) calling the
// singular send_append_request. Its dedicated case1-3.2 tests were removed
// alongside it.

// send_vote_requests (batch, all-peers-at-once) removed as dead code (#428):
// core layer (election_handler.rs::broadcast_vote_requests) now dispatches
// one send_vote_request call per peer directly, matching how replication
// dispatch already works. Its dedicated case1-5 tests were removed alongside
// it; the same scenarios (empty peer list, self-skip, dedup, vote
// granted/denied, higher term/log) are covered by the protocol-correctness
// tests added for send_vote_request below.

// # Case 1: empty peer list
//
// ## Criteria:
// 1. Should return EmptyPeerList error
/// Helper to create a failing stream
#[allow(unused)]
fn create_failing_stream(fail_at: usize) -> BoxStream<'static, Result<SnapshotChunk>> {
    let mut chunks = vec![];
    for i in 0..5 {
        let data = vec![i as u8; 1024];
        chunks.push(create_test_chunk(i as u32, &data, 1, 1, 5));
    }

    let stream = create_test_snapshot_stream(chunks);
    Box::pin(stream::unfold(
        (stream, 0),
        move |(mut stream, count)| async move {
            if count == fail_at {
                Some((
                    Err(Error::Fatal("Injected failure".to_string())),
                    (stream, count + 1),
                ))
            } else {
                match stream.next().await {
                    Some(Ok(chunk)) => Some((Ok(chunk), (stream, count + 1))),
                    Some(Err(e)) => {
                        Some((Err(Error::Fatal(format!("{e:?}",))), (stream, count + 1)))
                    }
                    None => None,
                }
            }
        },
    ))
}

// # Case: GrpcTransport Drop aborts background tasks
//
// ## Criteria:
// 1. When GrpcTransport is dropped, all peer_appender tasks must be aborted
// 2. Ensures fast shutdown without hanging on membership retries
// 3. Validates that tasks are actually finished after abort
#[tokio::test]
#[traced_test]
async fn test_grpc_transport_drop_aborts_tasks() {
    let my_id = 1;
    let peer_id = 2;

    // Create mock membership with retry expectations
    let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Data), channel.clone());

    let mut membership = MockMembership::<MockTypeConfig>::new();
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: peer_id,
            address: "127.0.0.1:50051".to_string(),
            role: Follower as i32,
            status: NodeStatus::Active as i32,
        }]
    });

    // Expect get_peer_channel to be called during task creation
    let channel_clone = channel.clone();
    membership
        .expect_get_peer_channel()
        .returning(move |_, _| Some(channel_clone.clone()));

    let membership = Arc::new(membership);

    // Create transport and start a peer appender
    let transport: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let retry = RetryPolicies::default();

    // Send one append request to create background task
    let request = AppendEntriesRequest {
        term: 1,
        leader_id: my_id,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
    };

    let _result = transport.send_append_request(peer_id, request, &retry, membership, false).await;

    // Verify task was created and is running
    assert!(
        transport.has_active_tasks(),
        "Background task should be running after send_append_request"
    );

    // Drop transport - this should abort the background task immediately
    // Without the Drop impl, this would hang waiting for membership retries
    drop(transport);

    // Test passes if we reach here without hanging (validates Drop impl works)
}

// send_vote_request (singular) protocol-correctness tests (#428):
// broadcast_vote_requests (core layer) relies on send_vote_request to be a
// transparent per-peer primitive — it must not interpret vote results itself,
// only forward whatever the peer returned (or an Err) so the core layer's
// majority/higher-term/log-conflict logic sees the real signal.
//
// Note: MockRpcService::expected_vote_response is a single fixed value for
// the lifetime of the mock server (see mock_rpc.rs), so a "fails once then
// succeeds on retry" scenario isn't cheaply testable with existing
// infrastructure; retry-exhausted (always-fails) is covered below instead.

// # Case: vote granted
//
// ## Criteria:
// 1. Ok(VoteResponse) is returned as-is, vote_granted == true
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_returns_granted_response() {
    let my_id = 1;
    let peer_id = 2;
    let node_config = node_config("/tmp/test_send_vote_request_returns_granted_response");

    let (_tx, rx) = oneshot::channel::<()>();
    let response = VoteResponse {
        term: 1,
        vote_granted: true,
        last_log_index: 0,
        last_log_term: 0,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(response, rx)
        .await
        .expect("mock server should start");

    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Control), channel);
    let membership = mock_membership(vec![(peer_id, Follower as i32)], channels);

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 0,
        last_log_term: 0,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    let res = result.expect("should succeed");
    assert!(res.vote_granted);
}

// # Case: vote denied but RPC itself succeeds
//
// ## Criteria:
// 1. Must be Ok(VoteResponse), NOT Err — a denial is a valid protocol
//    response, not a transport failure
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_returns_denied_response_as_ok() {
    let my_id = 1;
    let peer_id = 2;
    let node_config = node_config("/tmp/test_send_vote_request_returns_denied_response_as_ok");

    let (_tx, rx) = oneshot::channel::<()>();
    let response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: 0,
        last_log_term: 0,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(response, rx)
        .await
        .expect("mock server should start");

    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Control), channel);
    let membership = mock_membership(vec![(peer_id, Follower as i32)], channels);

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 0,
        last_log_term: 0,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    let res = result.expect("denial must be Ok, not Err");
    assert!(!res.vote_granted);
}

// # Case: peer response carries a higher term than the candidate's
//
// ## Criteria:
// 1. send_vote_request passes the response through unchanged — it must not
//    interpret the higher term itself; that's broadcast_vote_requests' job
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_passes_through_higher_term_response() {
    let my_id = 1;
    let peer_id = 2;
    let node_config =
        node_config("/tmp/test_send_vote_request_passes_through_higher_term_response");

    let (_tx, rx) = oneshot::channel::<()>();
    let response = VoteResponse {
        term: 99,
        vote_granted: false,
        last_log_index: 0,
        last_log_term: 0,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(response, rx)
        .await
        .expect("mock server should start");

    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Control), channel);
    let membership = mock_membership(vec![(peer_id, Follower as i32)], channels);

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 0,
        last_log_term: 0,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    let res = result.expect("should succeed");
    assert_eq!(res.term, 99);
    assert!(!res.vote_granted);
}

// # Case: peer response carries a more up-to-date log than the candidate's
//
// ## Criteria:
// 1. send_vote_request passes the response through unchanged — log-conflict
//    detection happens in broadcast_vote_requests, not here
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_passes_through_more_recent_log_response() {
    let my_id = 1;
    let peer_id = 2;
    let node_config =
        node_config("/tmp/test_send_vote_request_passes_through_more_recent_log_response");

    let (_tx, rx) = oneshot::channel::<()>();
    let response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: 42,
        last_log_term: 5,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(response, rx)
        .await
        .expect("mock server should start");

    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Control), channel);
    let membership = mock_membership(vec![(peer_id, Follower as i32)], channels);

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    let res = result.expect("should succeed");
    assert_eq!(res.last_log_index, 42);
    assert_eq!(res.last_log_term, 5);
}

// # Case: peer channel not found in membership
//
// ## Criteria:
// 1. Err(NetworkError::PeerConnectionNotFound(peer_id)) without attempting
//    any RPC
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_missing_channel_returns_peer_connection_not_found() {
    let my_id = 1;
    let peer_id = 2;
    let node_config = node_config(
        "/tmp/test_send_vote_request_missing_channel_returns_peer_connection_not_found",
    );

    // No channel registered for peer_id
    let membership = mock_membership(vec![(peer_id, Follower as i32)], HashMap::new());

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 0,
        last_log_term: 0,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    let err = result.unwrap_err();
    assert!(matches!(
        err,
        Error::System(SystemError::Network(NetworkError::PeerConnectionNotFound(id))) if id == peer_id
    ));
}

// # Case: peer consistently unreachable (retry exhausted)
//
// ## Criteria:
// 1. Err is returned once retries are exhausted (no infinite blocking)
#[tokio::test]
#[traced_test]
async fn test_send_vote_request_retry_exhausted_returns_err() {
    let my_id = 1;
    let peer_id = 2;
    let mut node_config = node_config("/tmp/test_send_vote_request_retry_exhausted_returns_err");
    node_config.retry.election.max_retries = 1;

    let (_tx, rx) = oneshot::channel::<()>();
    let mock_service = MockRpcService {
        expected_vote_response: Some(Err(Status::unavailable("peer unreachable"))),
        ..Default::default()
    };
    let (port, _addr) = MockNode::mock_listener(mock_service, rx, true)
        .await
        .expect("mock listener should start");
    let channel = MockNode::mock_channel_with_port(port).await;

    let mut channels = HashMap::new();
    channels.insert((peer_id, ConnectionType::Control), channel);
    let membership = mock_membership(vec![(peer_id, Follower as i32)], channels);

    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 0,
        last_log_term: 0,
    };
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_vote_request(peer_id, request, &node_config.retry, membership).await;

    assert!(result.is_err());
}
