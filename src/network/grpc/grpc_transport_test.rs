use tokio::sync::oneshot;
use tonic::Status;

use super::*;
use crate::grpc::grpc_transport::GrpcTransport;
use crate::proto::cluster::ClusterMembership;
use crate::proto::cluster::ClusterMembershipChangeRequest;
use crate::proto::common::LogId;
use crate::proto::election::VoteRequest;
use crate::proto::election::VoteResponse;
use crate::proto::replication::AppendEntriesRequest;
use crate::proto::replication::AppendEntriesResponse;
use crate::proto::storage::PurgeLogRequest;
use crate::proto::storage::PurgeLogResponse;
use crate::test_utils::node_config;
use crate::test_utils::MockNode;
use crate::test_utils::MockRpcService;
use crate::test_utils::MOCK_PURGE_PORT_BASE;
use crate::test_utils::MOCK_RPC_CLIENT_PORT_BASE;
use crate::test_utils::{self};
use crate::ChannelWithAddress;
use crate::ChannelWithAddressAndRole;
use crate::Error;
use crate::NetworkError;
use crate::RaftNodeConfig;
use crate::RetryPolicies;
use crate::SystemError;
use crate::Transport;
use crate::CANDIDATE;
use crate::FOLLOWER;
use crate::LEARNER;

async fn simulate_append_entries_mock_server(
    port: u64,
    response: std::result::Result<AppendEntriesResponse, Status>,
    rx: oneshot::Receiver<()>,
) -> Result<ChannelWithAddress> {
    //prepare learner's channel address inside membership config
    let mock_service = MockRpcService {
        expected_append_entries_response: Some(response),
        ..Default::default()
    };
    let addr = match test_utils::MockNode::mock_listener(mock_service, port, rx, true).await {
        Ok(a) => a,
        Err(e) => {
            panic!("error: {:?}", e);
        }
    };
    Ok(test_utils::MockNode::mock_channel_with_address(addr.to_string(), port).await)
}

// # Case 1: no peers passed
//
// ## Criterias:
// 1. return Err(NetworkError::EmptyPeerList)
//
#[tokio::test]
async fn test_send_cluster_update_case1() {
    test_utils::enable_logger();

    let my_id = 1;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case1");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterMembershipChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        cluster_membership: None,
    };

    let client = GrpcTransport { my_id };
    let result = client.send_cluster_update(vec![], request, &node_config.retry).await;
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
async fn test_send_cluster_update_case2() {
    test_utils::enable_logger();

    let my_id = 1;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case2");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterMembershipChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        cluster_membership: None,
    };

    // Simulate RPC service
    let (_tx, rx) = oneshot::channel::<()>();
    let response = ClusterMembership { nodes: vec![] };
    let addr1 =
        MockNode::simulate_mock_service_with_cluster_conf_reps(MOCK_RPC_CLIENT_PORT_BASE + 50, Ok(response), rx)
            .await
            .expect("should succeed");
    let requests_with_peer_address = vec![ChannelWithAddressAndRole {
        id: my_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    }];

    let client = GrpcTransport { my_id };
    match client
        .send_cluster_update(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_cluster_update_case3() {
    test_utils::enable_logger();

    let my_id = 1;
    let peer1_id = 2;
    let peer2_id = 3;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case3");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterMembershipChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        cluster_membership: None,
    };

    // Simulate RPC service
    let (_tx, rx) = oneshot::channel::<()>();
    let response = ClusterMembership { nodes: vec![] };
    let addr1 =
        MockNode::simulate_mock_service_with_cluster_conf_reps(MOCK_RPC_CLIENT_PORT_BASE + 52, Ok(response), rx)
            .await
            .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];

    let client = GrpcTransport { my_id };
    match client
        .send_cluster_update(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_cluster_update_case4() {
    test_utils::enable_logger();

    let my_id = 1;
    let peer1_id = 2;
    let peer2_id = 3;
    let mut node_config = node_config("/tmp/test_send_cluster_update_case4");
    node_config.retry.membership.max_retries = 1;
    let request = ClusterMembershipChangeRequest {
        id: 1,
        term: 1,
        version: 1,
        cluster_membership: None,
    };

    // Simulate RPC service
    let (_tx, rx) = oneshot::channel::<()>();
    let addr1 = MockNode::simulate_mock_service_with_cluster_conf_reps(
        MOCK_RPC_CLIENT_PORT_BASE + 51,
        Err(Status::unavailable("message".to_string())),
        rx,
    )
    .await
    .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];

    let client = GrpcTransport { my_id };
    match client
        .send_cluster_update(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_append_requests_case1() {
    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client.send_append_requests(vec![], &RetryPolicies::default()).await {
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
async fn test_send_append_requests_case2() {
    test_utils::enable_logger();
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
    let addr = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 2, Ok(response), rx)
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
    let requests_with_peer_address = vec![(leader_id, addr, request)];

    let node_config = node_config("/tmp/test_send_append_requests_case2");

    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client
        .send_append_requests(requests_with_peer_address, &node_config.retry)
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
async fn test_send_append_requests_case3_1() {
    test_utils::enable_logger();

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
    let peer_3_response =
        AppendEntriesResponse::conflict(peer_3_id, peer_3_term, Some(peer_3_term), Some(peer_3_match_index));
    let (_tx2, rx2) = oneshot::channel::<()>();
    let addr2 = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 3, Ok(peer_2_response), rx2)
        .await
        .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let addr3 = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 4, Ok(peer_3_response), rx3)
        .await
        .expect("should succeed");

    let peer_2_address = addr2;
    let peer_3_address = addr3;
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![
        (peer_2_id, peer_2_address, peer_req.clone()),
        (peer_3_id, peer_3_address, peer_req),
    ];

    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client
        .send_append_requests(requests_with_peer_address, &node_config.retry)
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
async fn test_send_append_requests_case3_2() {
    test_utils::enable_logger();

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
    let addr2 = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 32, Ok(peer_2_response), rx2)
        .await
        .expect("should succeed");
    let (_tx3, rx3) = oneshot::channel::<()>();
    let addr3 = simulate_append_entries_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 34, peer_3_response, rx3)
        .await
        .expect("should succeed");

    let peer_2_address = addr2;
    let peer_3_address = addr3;
    let peer_req = AppendEntriesRequest {
        term: leader_current_term,
        leader_id,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index,
    };
    let requests_with_peer_address = vec![
        (peer_2_id, peer_2_address, peer_req.clone()),
        (peer_3_id, peer_3_address, peer_req),
    ];

    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

    let my_id = 1;
    let client = GrpcTransport { my_id };
    match client
        .send_append_requests(requests_with_peer_address, &node_config.retry)
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
async fn test_send_vote_requests_case1() {
    test_utils::enable_logger();

    let my_id = 1;
    let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");
    let request = VoteRequest {
        term: 1,
        candidate_id: my_id,
        last_log_index: 1,
        last_log_term: 1,
    };
    let client = GrpcTransport { my_id };
    match client.send_vote_requests(vec![], request, &node_config.retry).await {
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
async fn test_send_vote_requests_case2() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 10, vote_response, rx1)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![ChannelWithAddressAndRole {
        id: my_id,
        channel_with_address: addr1,
        role: FOLLOWER,
    }];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_vote_requests_case3() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 21, vote_response, rx1)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_vote_requests_case4_1() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 22, vote_response, rx1)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
    {
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
async fn test_send_vote_requests_case4_2() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 23, vote_response, rx1)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    assert!(client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
        .is_ok());
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
async fn test_send_vote_requests_case4_3() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 24, vote_response, rx1)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr1.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    assert!(client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
        .is_ok());
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
async fn test_send_vote_requests_case5() {
    test_utils::enable_logger();

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
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 25, peer1_response, rx1)
        .await
        .expect("should succeed");
    let addr2 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 26, peer2_response, rx2)
        .await
        .expect("should succeed");
    let requests_with_peer_address = vec![
        ChannelWithAddressAndRole {
            id: peer1_id,
            channel_with_address: addr1.clone(),
            role: FOLLOWER,
        },
        ChannelWithAddressAndRole {
            id: peer2_id,
            channel_with_address: addr2.clone(),
            role: CANDIDATE,
        },
    ];
    let client = GrpcTransport { my_id };
    match client
        .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
        .await
    {
        Ok(res) => {
            assert!(res.responses.len() == 2);
            assert!(res.peer_ids.len() == 2);
        }
        Err(_) => panic!(),
    }
}

// // # Case 6: High term found in vote response
// //
// // ## Setup:
// // 1. prepare two peers, one success while another failed with higher term
// //
// // ## Criterias:
// // 1. return Error
// //
// #[tokio::test]
// async fn test_send_vote_requests_case6() {
//     test_utils::enable_logger();

//     let my_id = 1;
//     let node_config = RaftNodeConfig::new().expect("Should succeed to init RaftNodeConfig.");

//     let peer1_id = 2;
//     let peer2_id = 3;
//     //prepare rpc service for getting peer address
//     let (_tx1, rx1) = oneshot::channel::<()>();
//     let (_tx2, rx2) = oneshot::channel::<()>();
//     let peer1_response = VoteResponse {
//         term: 1,
//         vote_granted: true,
//         last_log_index: 0,
//         last_log_term: 0,
//     };
//     let peer2_response = VoteResponse {
//         term: 100,
//         vote_granted: false,
//         last_log_index: 0,
//         last_log_term: 0,
//     };
//     let request = VoteRequest {
//         term: 1,
//         candidate_id: my_id,
//         last_log_index: 1,
//         last_log_term: 1,
//     };
//     let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 27,
// peer1_response, rx1)         .await
//         .expect("should succeed");
//     let addr2 = MockNode::simulate_send_votes_mock_server(MOCK_RPC_CLIENT_PORT_BASE + 28,
// peer2_response, rx2)         .await
//         .expect("should succeed");
//     let requests_with_peer_address = vec![
//         ChannelWithAddressAndRole {
//             id: peer1_id,
//             channel_with_address: addr1.clone(),
//             role: FOLLOWER,
//         },
//         ChannelWithAddressAndRole {
//             id: peer2_id,
//             channel_with_address: addr2.clone(),
//             role: CANDIDATE,
//         },
//     ];
//     let client = GrpcTransport { my_id };
//     match client
//         .send_vote_requests(requests_with_peer_address, request, &node_config.retry)
//         .await
//     {
//         Ok(_) => panic!(),
//         Err(e) => assert!(matches!(e, Error::HigherTermFoundError(_higher_term))),
//     }
// }

// # Case 1: empty peer list
//
// ## Criteria:
// 1. Should return EmptyPeerList error
#[tokio::test]
async fn test_purge_requests_case1_empty_peers() {
    test_utils::enable_logger();

    let client = GrpcTransport { my_id: 1 };
    let req = PurgeLogRequest {
        term: 1,
        leader_id: 1,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: vec![],
        leader_commit: 5,
    };

    let node_config = RaftNodeConfig::new().unwrap();
    match client.send_purge_requests(vec![], req, &node_config.retry).await {
        Ok(_) => panic!("Should reject empty peer list"),
        Err(e) => assert!(
            matches!(
                e,
                Error::System(SystemError::Network(NetworkError::EmptyPeerList { .. }))
            ),
            "Unexpected error: {:?}",
            e
        ),
    }
}

// # Case 2: self-reference in peer list
//
// ## Criteria:
// 1. Should filter out self node
// 2. Return empty response collection
#[tokio::test]
async fn test_purge_requests_case2_self_reference() {
    test_utils::enable_logger();

    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: vec![],
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let purge_response = PurgeLogResponse {
        node_id: 2,
        term: 1,
        success: true,
        last_purged: Some(LogId { term: 1, index: 5 }),
    };

    let addr = MockNode::simulate_purge_mock_server(MOCK_PURGE_PORT_BASE + 1, purge_response, shutdown_rx)
        .await
        .unwrap();

    let client = GrpcTransport { my_id };
    let result = client
        .send_purge_requests(
            vec![ChannelWithAddressAndRole {
                id: my_id,
                channel_with_address: addr,
                role: FOLLOWER,
            }],
            req,
            &node_config.retry,
        )
        .await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert!(res.is_empty(), "Should filter self-reference");
            assert!(res.is_empty(), "Should have no responses");
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// # Case 3: duplicate peer entries
//
// ## Criteria:
// 1. Should deduplicate peer list
// 2. Process unique peers only
#[tokio::test]
async fn test_purge_requests_case3_duplicate_peers() {
    test_utils::enable_logger();

    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: vec![],
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let purge_response = PurgeLogResponse {
        node_id: 1,
        term: 1,
        success: true,
        last_purged: Some(LogId { term: 1, index: 5 }),
    };

    let addr = MockNode::simulate_purge_mock_server(MOCK_PURGE_PORT_BASE + 2, purge_response, shutdown_rx)
        .await
        .unwrap();

    let client = GrpcTransport { my_id };
    let result = client
        .send_purge_requests(
            vec![
                ChannelWithAddressAndRole {
                    id: 2,
                    channel_with_address: addr.clone(),
                    role: FOLLOWER,
                },
                ChannelWithAddressAndRole {
                    id: 2, // Duplicate
                    channel_with_address: addr.clone(),
                    role: LEARNER,
                },
                ChannelWithAddressAndRole {
                    id: 3,
                    channel_with_address: addr.clone(),
                    role: FOLLOWER,
                },
            ],
            req,
            &node_config.retry,
        )
        .await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should deduplicate peers");
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// # Case 4: mixed success and failure responses
//
// ## Criteria:
// 1. Should aggregate partial failures
// 2. Maintain response ordering
#[tokio::test]
async fn test_purge_requests_case4_mixed_responses() {
    test_utils::enable_logger();

    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: vec![],
        leader_commit: 5,
    };

    // Setup success responder
    let (success_tx, success_rx) = oneshot::channel();
    let success_addr = MockNode::simulate_purge_mock_server(
        MOCK_PURGE_PORT_BASE + 3,
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
    let failure_addr = MockNode::simulate_purge_mock_server(
        MOCK_PURGE_PORT_BASE + 4,
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

    let client = GrpcTransport { my_id };
    let result = client
        .send_purge_requests(
            vec![
                ChannelWithAddressAndRole {
                    id: 2,
                    channel_with_address: success_addr,
                    role: FOLLOWER,
                },
                ChannelWithAddressAndRole {
                    id: 3,
                    channel_with_address: failure_addr,
                    role: LEARNER,
                },
            ],
            req,
            &node_config.retry,
        )
        .await;

    success_tx.send(()).unwrap();
    failure_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should collect all responses");
            let successes = res
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .filter(|resp| resp.success)
                .count();
            assert_eq!(successes, 1, "Should handle partial success");
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// # Case 5: full successful propagation
//
// ## Criteria:
// 1. Should process all peers
// 2. Return aggregated successes
#[tokio::test]
async fn test_purge_requests_case5_full_success() {
    test_utils::enable_logger();

    let my_id = 1;
    let node_config = RaftNodeConfig::new().unwrap();
    let req = PurgeLogRequest {
        term: 1,
        leader_id: my_id,
        last_included: Some(LogId { index: 5, term: 2 }),
        snapshot_checksum: vec![],
        leader_commit: 5,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let addr = MockNode::simulate_purge_mock_server(
        MOCK_PURGE_PORT_BASE + 5,
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

    let client = GrpcTransport { my_id };
    let result = client
        .send_purge_requests(
            vec![
                ChannelWithAddressAndRole {
                    id: 2,
                    channel_with_address: addr.clone(),
                    role: FOLLOWER,
                },
                ChannelWithAddressAndRole {
                    id: 3,
                    channel_with_address: addr.clone(),
                    role: LEARNER,
                },
            ],
            req,
            &node_config.retry,
        )
        .await;

    shutdown_tx.send(()).unwrap();

    match result {
        Ok(res) => {
            assert_eq!(res.len(), 2, "Should process all peers");
            assert!(res.iter().all(|r| r.is_ok()), "All responses should succeed");
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}
