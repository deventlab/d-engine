use std::collections::HashMap;

use bytes::Bytes;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Status;
use tracing_test::traced_test;

use super::*;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::proto::cluster::ClusterConfChangeRequest;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::NodeMeta;
use crate::proto::common::LogId;
use crate::proto::common::NodeStatus;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::proto::storage::SnapshotChunk;
use crate::test_utils::crate_test_snapshot_stream;
use crate::test_utils::create_test_chunk;
use crate::test_utils::node_config;
use crate::test_utils::MockNode;
use crate::test_utils::MockTypeConfig;
use crate::ConnectionType;
use crate::Error;
use crate::MockMembership;
use crate::NetworkError;
use crate::RaftNodeConfig;
use crate::RetryPolicies;
use crate::SystemError;
use crate::Transport;
use crate::CANDIDATE;
use crate::FOLLOWER;
use crate::LEADER;
use crate::LEARNER;

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
    };
    let (channel, _port) = MockNode::simulate_mock_service_with_cluster_conf_reps(
        rx,
        Some(Box::new(move |_port| Ok(response.clone()))),
    )
    .await
    .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((my_id, ConnectionType::Control), channel.clone());

    let membership = mock_membership(vec![(my_id, FOLLOWER)], channels);
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
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);

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
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);

    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_cluster_update(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// Case 1: no followers or candidates found in cluster,
// Criterias: function should return false
//
#[tokio::test]
#[traced_test]
async fn test_send_append_requests_case1() {
    let my_id = 1;
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let membership = mock_membership(vec![], HashMap::new());
    match client
        .send_append_requests(vec![], &RetryPolicies::default(), membership, false)
        .await
    {
        Ok(_) => panic!(),
        Err(e) => assert!(matches!(
            e,
            Error::System(SystemError::Network(NetworkError::EmptyPeerList { .. }))
        )),
    }
}

// Case 2: passed peers only include the node itself
//
// ## Criterias:
// 1. return Ok with empty responses
#[tokio::test]
#[traced_test]
async fn test_send_append_requests_case2() {
    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_2_term = 1;
    let peer_2_match_index = 1;
    let response = AppendEntriesResponse::success(
        peer_2_id,
        peer_2_term,
        Some(LogId {
            term: peer_2_term,
            index: peer_2_match_index,
        }),
    );
    let (_tx, rx) = oneshot::channel::<()>();
    let (channel, _port) = MockNode::simulate_append_entries_mock_server(Ok(response), rx)
        .await
        .expect("should succeed");

    let request = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![(leader_id, request)];

    let mut channels = HashMap::new();
    channels.insert((leader_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(leader_id, LEADER)], channels);

    let node_config = node_config("/tmp/test_send_append_requests_case2");

    let my_id = 1;
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client
        .send_append_requests(
            requests_with_peer_address,
            &node_config.retry,
            membership,
            false,
        )
        .await
    {
        Ok(res) => {
            assert!(res.responses.is_empty());
            assert!(res.peer_ids.is_empty())
        }
        Err(_) => panic!(),
    }
}

// # Case 3.1: receive two peer's response
//
// ## Setup:
// 1. prepare two peers, peer2 success, while peer3 failed
//
// ## Criterias:
// 1. return Ok with two responses
//
#[tokio::test]
#[traced_test]
async fn test_send_append_requests_case3_1() {
    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_3_id = 3;
    let peer_2_term = leader_current_term;
    let peer_2_match_index = 10;
    let peer_3_term = leader_current_term;
    let peer_3_match_index = 1;

    let peer_2_response = AppendEntriesResponse::success(
        peer_2_id,
        peer_2_term,
        Some(LogId {
            term: peer_2_term,
            index: peer_2_match_index,
        }),
    );
    let peer_3_response = AppendEntriesResponse::conflict(
        peer_3_id,
        peer_3_term,
        Some(peer_3_term),
        Some(peer_3_match_index),
    );
    let (_tx2, rx2) = oneshot::channel::<()>();
    let (channel2, _port2) =
        MockNode::simulate_append_entries_mock_server(Ok(peer_2_response), rx2)
            .await
            .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let (channel3, _port3) =
        MockNode::simulate_append_entries_mock_server(Ok(peer_3_response), rx3)
            .await
            .expect("should succeed");

    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![(peer_2_id, peer_req.clone()), (peer_3_id, peer_req)];

    let mut channels = HashMap::new();
    channels.insert((peer_2_id, ConnectionType::Data), channel2.clone());
    channels.insert((peer_3_id, ConnectionType::Data), channel3.clone());
    let membership = mock_membership(vec![(leader_id, LEADER)], channels);

    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let my_id = 1;
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client
        .send_append_requests(
            requests_with_peer_address,
            &node_config.retry,
            membership,
            true,
        )
        .await
    {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// # Case 3.2: Send two peers, one of peers server status is not ready
//
// ## Setup:
// 1. prepare two peers, peer2 success, while peer3 failed
//
// ## Criterias:
// 1. peer2's match index and next index will be updated
// 2. peer3's match index and next index will not be updated
// 3. return Ok(true)
//
#[tokio::test]
#[traced_test]
async fn test_send_append_requests_case3_2() {
    //step1: setup
    let leader_id = 1;
    let leader_current_term = 1;
    let leader_commit_index = 1;
    let peer_2_id = 2;
    let peer_3_id = 3;
    let peer_2_term = leader_current_term;
    let peer_2_match_index = 10;

    let peer_2_response = AppendEntriesResponse::success(
        peer_2_id,
        peer_2_term,
        Some(LogId {
            term: peer_2_term,
            index: peer_2_match_index,
        }),
    );
    let peer_3_response = Err(Status::unavailable("Service is not ready"));
    let (_tx2, rx2) = oneshot::channel::<()>();
    let (channel2, _port2) =
        MockNode::simulate_append_entries_mock_server(Ok(peer_2_response), rx2)
            .await
            .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let (channel3, _port3) = MockNode::simulate_append_entries_mock_server(peer_3_response, rx3)
        .await
        .expect("should succeed");

    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![(peer_2_id, peer_req.clone()), (peer_3_id, peer_req)];

    let mut channels = HashMap::new();
    channels.insert((peer_2_id, ConnectionType::Data), channel2.clone());
    channels.insert((peer_3_id, ConnectionType::Data), channel3.clone());
    let membership = mock_membership(vec![(leader_id, LEADER)], channels);

    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let my_id = 1;
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client
        .send_append_requests(
            requests_with_peer_address,
            &node_config.retry,
            membership,
            true,
        )
        .await
    {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// # Case 1: no peers passed
//
// ## Criterias:
// 1. return Err(NetworkError::EmptyPeerList)
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case1() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };

    let membership = mock_membership(vec![], HashMap::new());
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_vote_requests(request, &node_config.retry, membership).await {
        Ok(_) => panic!(),
        Err(e) => assert!(matches!(
            e,
            Error::System(SystemError::Network(NetworkError::EmptyPeerList { .. }))
        )),
    }
}

// # Case 2: passed peers only include the node itself
//
// ## Criterias:
// 1. return Ok with empty responses
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case2() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();

    // This fake reponse doesn't affect the test result
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: true,
        last_log_index: 0,
        last_log_term: 0,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(vote_response, rx1)
        .await
        .expect("should succeed");
    let mut channels = HashMap::new();
    channels.insert((my_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(my_id, FOLLOWER)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_vote_requests(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.is_empty());
            assert!(res.peer_ids.is_empty())
        }
        Err(_) => panic!(),
    }
}

// # Case 3: passed peers with duplicates
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
async fn test_send_vote_requests_case3() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: true,
        last_log_index: 0,
        last_log_term: 0,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(vote_response, rx1)
        .await
        .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_vote_requests(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// # Case 4.1: two peers failed because they have elected the others
//
// ## Setup:
// 1. Prepare two peers, both peer failed
// 2. But both peers don't have any local log entry
//
// ## Criterias:
// 1. return Ok with two responses
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case4_1() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: 0,
        last_log_term: 0,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(vote_response, rx1)
        .await
        .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);

    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_vote_requests(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2)
        }
        Err(_) => panic!(),
    }
}

// # Case 4.2: vote response returns higher last_log_term
//
// ## Setup:
// 1. prepare two peers, both peer failed
// 2. Peer1 returns higher term of last log index,
//
// ## Criterias:
// 1. Ok(..)
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case4_2() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let my_last_log_term = 3;
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: 1,
        last_log_term: my_last_log_term + 1,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: my_last_log_term,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(vote_response, rx1)
        .await
        .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    assert!(client.send_vote_requests(request, &node_config.retry, membership).await.is_ok());
}

// # Case 4.3: vote response returns higher last_log_index
//
// ## Setup:
// 1. prepare two peers, both peer failed
// 2. Peer1 returns last log term is the same as candidate one, but last log index is higher than
//    candidate last log index,
//
// ## Criterias:
// 1. Ok(..)
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case4_3() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let my_last_log_index = 1;
    let my_last_log_term = 3;
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: my_last_log_index + 1,
        last_log_term: my_last_log_term,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: my_last_log_index,
        last_log_term: my_last_log_term,
    };
    let (channel, _port) = MockNode::simulate_send_votes_mock_server(vote_response, rx1)
        .await
        .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel.clone());
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    assert!(client.send_vote_requests(request, &node_config.retry, membership).await.is_ok());
}
// # Case 5: two peers passed
//
// ## Setup:
// 1. prepare two peers, one success while another failed
//
// ## Criterias:
// 1. return Ok(true)
//
#[tokio::test]
#[traced_test]
async fn test_send_vote_requests_case5() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let peer1_id = 2;
    let peer2_id = 3;
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let (_tx2, rx2) = oneshot::channel::<()>();
    let peer1_response = VoteResponse {
        term: 1,
        vote_granted: false,
        last_log_index: 0,
        last_log_term: 0,
    };
    let peer2_response = VoteResponse {
        term: 1,
        vote_granted: true,
        last_log_index: 0,
        last_log_term: 0,
    };
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let (channel1, _port1) = MockNode::simulate_send_votes_mock_server(peer1_response, rx1)
        .await
        .expect("should succeed");
    let (channel2, _port2) = MockNode::simulate_send_votes_mock_server(peer2_response, rx2)
        .await
        .expect("should succeed");

    let mut channels = HashMap::new();
    channels.insert((peer1_id, ConnectionType::Control), channel1.clone());
    channels.insert((peer2_id, ConnectionType::Control), channel2.clone());
    let membership = mock_membership(vec![(peer1_id, FOLLOWER), (peer2_id, CANDIDATE)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    match client.send_vote_requests(request, &node_config.retry, membership).await {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2);
        }
        Err(_) => panic!(),
    }
}

// # Case 1: empty peer list
//
// ## Criteria:
// 1. Should return EmptyPeerList error
#[tokio::test]
#[traced_test]
async fn test_purge_requests_case1_empty_peers() {
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(1);
    let req = PurgeLogRequest {
        term: 1,
        leader_id: 1,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: Bytes::new(),
        leader_commit: 5,
    };

    let membership = mock_membership(vec![], HashMap::new());
    let node_config = RaftNodeConfig::new().unwrap();
    match client.send_purge_requests(req, &node_config.retry, membership).await {
        Ok(_) => panic!("Should reject empty peer list"),
        Err(e) => assert!(
            matches!(
                e,
                Error::System(SystemError::Network(NetworkError::EmptyPeerList { .. }))
            ),
            "Unexpected error: {e:?}"
        ),
    }
}

// # Case 2: self-reference in peer list
//
// ## Criteria:
// 1. Should filter out self node
// 2. Return empty response collection
#[tokio::test]
#[traced_test]
async fn test_purge_requests_case2_self_reference() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: Bytes::new(),
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let purge_response = PurgeLogResponse {
        node_id: 2,
        term: 1,
        success: true,
        last_purged: Some(LogId { term: 1, index: 5 }),
    };

    let (channel, _port) =
        MockNode::simulate_purge_mock_server(purge_response, shutdown_rx).await.unwrap();

    let mut channels = HashMap::new();
    channels.insert((my_id, ConnectionType::Data), channel.clone());
    let membership = mock_membership(vec![(my_id, FOLLOWER)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_purge_requests(req, &node_config.retry, membership).await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert!(res.is_empty(), "Should filter self-reference");
            assert!(res.is_empty(), "Should have no responses");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

// # Case 3: duplicate peer entries
//
// ## Criteria:
// 1. Should deduplicate peer list
// 2. Process unique peers only
#[tokio::test]
#[traced_test]
async fn test_purge_requests_case3_duplicate_peers() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: Bytes::new(),
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let purge_response = PurgeLogResponse {
        node_id: 1,
        term: 1,
        success: true,
        last_purged: Some(LogId { term: 1, index: 5 }),
    };

    let (channel, _port) =
        MockNode::simulate_purge_mock_server(purge_response, shutdown_rx).await.unwrap();

    let mut channels = HashMap::new();
    channels.insert((2, ConnectionType::Data), channel.clone());
    channels.insert((3, ConnectionType::Data), channel.clone());
    let membership = mock_membership(vec![(2, LEARNER), (3, FOLLOWER)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_purge_requests(req, &node_config.retry, membership).await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should deduplicate peers");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

// # Case 4: mixed success and failure responses
//
// ## Criteria:
// 1. Should aggregate partial failures
// 2. Maintain response ordering
#[tokio::test]
#[traced_test]
async fn test_purge_requests_case4_mixed_responses() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: Bytes::new(),
        leader_commit: 5,
    };

    // Setup success responder
    let (success_tx, success_rx) = oneshot::channel();
    let (success_channel, _port) = MockNode::simulate_purge_mock_server(
        PurgeLogResponse {
            node_id: 2,
            term: 1,
            success: true,
            last_purged: Some(LogId { term: 1, index: 5 }),
        },
        success_rx,
    )
    .await
    .unwrap();

    // Setup failure responder
    let (failure_tx, failure_rx) = oneshot::channel();
    let (failure_channel, _port) = MockNode::simulate_purge_mock_server(
        PurgeLogResponse {
            node_id: 2,
            term: 1,
            success: false,
            last_purged: Some(LogId { term: 1, index: 0 }),
        },
        failure_rx,
    )
    .await
    .unwrap();

    let mut channels = HashMap::new();
    channels.insert((2, ConnectionType::Data), success_channel.clone());
    channels.insert((3, ConnectionType::Data), failure_channel.clone());
    let membership = mock_membership(vec![(2, FOLLOWER), (3, LEARNER)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_purge_requests(req, &node_config.retry, membership).await;

    success_tx.send(()).unwrap();
    failure_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should collect all responses");
            let successes =
                res.iter().filter_map(|r| r.as_ref().ok()).filter(|resp| resp.success).count();
            assert_eq!(successes, 1, "Should handle partial success");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

// # Case 5: full successful propagation
//
// ## Criteria:
// 1. Should process all peers
// 2. Return aggregated successes
#[tokio::test]
#[traced_test]
async fn test_purge_requests_case5_full_success() {
    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: Bytes::new(),
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (channel, _port) = MockNode::simulate_purge_mock_server(
        PurgeLogResponse {
            node_id: 2,
            term: 1,
            success: true,
            last_purged: Some(LogId { term: 1, index: 5 }),
        },
        shutdown_rx,
    )
    .await
    .unwrap();

    let mut channels = HashMap::new();
    channels.insert((2, ConnectionType::Data), channel.clone());
    channels.insert((3, ConnectionType::Data), channel.clone());
    let membership = mock_membership(vec![(2, FOLLOWER), (3, LEARNER)], channels);
    let client: GrpcTransport<MockTypeConfig> = GrpcTransport::new(my_id);
    let result = client.send_purge_requests(req, &node_config.retry, membership).await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should process all peers");
            assert!(
                res.iter().all(|r| r.is_ok()),
                "All responses should succeed"
            );
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

/// Helper to create a failing stream
#[allow(unused)]
fn create_failing_stream(fail_at: usize) -> BoxStream<'static, Result<SnapshotChunk>> {
    let mut chunks = vec![];
    for i in 0..5 {
        let data = vec![i as u8; 1024];
        chunks.push(create_test_chunk(i as u32, &data, 1, 1, 5));
    }

    let stream = crate_test_snapshot_stream(chunks);
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
