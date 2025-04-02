use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio::time::{self};

use super::*;
use crate::candidate_state::CandidateState;
use crate::grpc::rpc_service::MetadataRequest;
use crate::grpc::rpc_service::VoteResponse;
use crate::is_candidate;
use crate::is_leader;
use crate::leader_state::LeaderState;
use crate::test_utils::mock_raft;
use crate::test_utils::MockNode;
use crate::test_utils::MOCK_RAFT_PORT_BASE;
use crate::ChannelWithAddressAndRole;
use crate::Error;
use crate::MaybeCloneOneshot;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockStateMachine;
use crate::MockStateStorage;
use crate::MockTransport;
use crate::RaftOneshot;

/// # Case 1: Tick has higher priority than role event
#[tokio::test]
async fn test_tick_priority_over_role_event() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_tick_priority_over_role_event", graceful_rx, None);

    // 2. Add state listeners
    let role_tx = raft.role_tx.clone();
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 3. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(3), raft.run()).await;
    });

    // 4. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await; // **key of test**  Ensure Tick is processed

    // 5. Send RoleEvent（role == Candidate）
    role_tx.send(RoleEvent::BecomeLeader).unwrap();

    // 6. Wait for Tick to trigger and process
    let first_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_candidate(first_state),
        "Tick should prioritize triggering the election"
    );

    // 7. Validate subsequent processing
    let second_state = monitor_rx.recv().await.unwrap();
    assert!(is_leader(second_state), "Then process the RoleEvent");

    raft_handle.await.expect("should succeed");
}

/// # Case 2: RoleEvent has higher priority than event_rx
#[tokio::test]
async fn test_role_event_priority_over_event_rx() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_role_event_priority_over_event_rx", graceful_rx, None);

    // 2. Add state listeners
    let raft_tx = raft.event_tx.clone();
    let role_tx = raft.role_tx.clone();
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx.clone());
    let (event_monitor_tx, mut event_monitor_rx) = mpsc::unbounded_channel::<RaftEvent>();
    raft.register_raft_event_listener(event_monitor_tx);

    // 3. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(3), raft.run()).await;
    });

    // 4. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await; // **key of test**  Ensure Tick is processed

    // 5. Send Events
    let (resp_tx, _resp_rx) = MaybeCloneOneshot::new();
    raft_tx
        .send(RaftEvent::ClusterConf(MetadataRequest {}, resp_tx))
        .await
        .unwrap();
    role_tx.send(RoleEvent::BecomeLeader).unwrap();

    // 6. Wait for Tick to trigger and process
    let first_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_candidate(first_state),
        "Tick should prioritize triggering the election"
    );

    // 7. Validate subsequent processing
    let second_state = monitor_rx.recv().await.unwrap();
    assert!(is_leader(second_state), "Then process the RoleEvent");
    let event = event_monitor_rx.recv().await.unwrap();
    assert_eq!(5, event.to_code()); // RecvHeartbeat code == 1

    raft_handle.await.expect("should succeed");
}

/// # Case 1: if I am Follower, now I should be upgraded to Candidate
///
/// ## Setup:
/// - prepare the node as follower
///
/// ## Criterias:
/// - should receive role change event with Candidate as new role
/// - term should no change
#[tokio::test]
async fn test_election_timeout_case1() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case1", graceful_rx, None);
    raft.ctx.election_handler = election_handler_mock;

    // 3. Add state listeners
    let (role_monitor_tx, mut role_monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(role_monitor_tx);
    let (event_monitor_tx, mut event_monitor_rx) = mpsc::unbounded_channel::<RaftEvent>();
    raft.register_raft_event_listener(event_monitor_tx);

    // 4. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(3), raft.run()).await;
    });

    // 5. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await; // **key of test**  Ensure Tick is processed

    // 6. Wait for Tick to trigger and process
    let first_state = role_monitor_rx.recv().await.unwrap();
    assert!(
        is_candidate(first_state),
        "Follower should be elected itself as Candidate"
    );
    match timeout(Duration::from_millis(5), event_monitor_rx.recv()).await {
        Ok(Some(_)) => assert!(false, "No event change event should happen"),
        Ok(None) => assert!(true, "Raft stops so event channel should be closed. "),
        Err(_) => assert!(true,),
    }

    // 7. Wait for thread finishes
    raft_handle.await.expect("should succeed");
}

/// # Case 2.1: if I am Candidate, now I should send vote requests to peers
///
/// ## Setup:
/// - prepare the node as candidate
/// - can_vote_myself = true
///
/// ## Criterias:
/// - broadcast_vote_requests should be invoked once
#[tokio::test]
async fn test_election_timeout_case2_1() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case2_1", graceful_rx, None);
    raft.ctx.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Candidate(CandidateState::new(1, raft.settings.clone())));

    // 4. Add state listeners
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 5. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(1), raft.run()).await;
    });

    // 6. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(1)).await; // **key of test**  Ensure Tick is processed

    // 7. Wait for Tick to trigger and process
    let first_state = monitor_rx.recv().await.unwrap();
    assert!(is_leader(first_state), "Candidate should be voted as Leader");

    // 8. Wait for thread finishes
    raft_handle.await.expect("should succeed");
}

/// # Case 2.2: if I am Candidate, now I should send vote requests to peers
///
/// ## Setup:
/// - prepare the node as candidate
/// - can_vote_myself = false
///
/// ## Criterias:
/// - broadcast_vote_requests should be invoked only one time
#[tokio::test]
async fn test_election_timeout_case2_2() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Err(Error::ElectionFailed("failed to receive majority votes.".to_string())));

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case2_2", graceful_rx, None);
    raft.ctx.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Candidate(CandidateState::new(1, raft.settings.clone())));

    // 4. Add state listeners
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 5. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(1), raft.run()).await;
    });

    // 6. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(1)).await; // **key of test**  Ensure Tick is processed

    // 7. Wait for Tick to trigger and process
    match timeout(Duration::from_millis(5), monitor_rx.recv()).await {
        Ok(Some(_)) => assert!(false, "No event change event should happen"),
        Ok(None) => assert!(true, "Raft stops so event channel should be closed. "),
        Err(_) => assert!(true,),
    }

    // 8. Wait for thread finishes
    raft_handle.await.expect("should succeed");
}

/// # Case 3: if I am Leader, nothing should happens
///
/// ## Setup:
/// - prepare the node as leader
///
/// ## Criterias:
/// - broadcast_vote_requests should be called zero times
/// - no role change event should be received
#[tokio::test]
async fn test_election_timeout_case3() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(0)
        .returning(|_, _, _, _, _| Ok(()));

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case2", graceful_rx, None);
    raft.ctx.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Leader(LeaderState::new(1, raft.settings.clone())));

    // 4. Add state listeners
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 5. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(1), raft.run()).await;
    });

    // 6. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(1)).await; // **key of test**  Ensure Tick is processed

    // 7. Wait for Tick to trigger and process
    match timeout(Duration::from_millis(5), monitor_rx.recv()).await {
        Ok(Some(_)) => assert!(false, "No role change event should happen"),
        Ok(None) => assert!(true, "Raft stops so role event channel should be closed. "),
        Err(_) => assert!(true,),
    }

    // 8. Wait for thread finishes
    raft_handle.await.expect("should succeed");
}

/// # Case 4: if I am follower, test until it voted as Leader
///
/// ## Setup:
/// - prepare the node as leader
///
/// ## Criterias:
/// - broadcast_vote_requests should be called zero times
/// - no role change event should be received
#[tokio::test]
async fn test_election_timeout_case4() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case4", graceful_rx, None);
    raft.ctx.election_handler = election_handler_mock;
    let peer1_id = 2;
    let peer2_id = 3;

    // 3. prepare mock service response
    //prepare rpc service for getting peer address
    let (_tx1, rx1) = oneshot::channel::<()>();
    let vote_response = VoteResponse {
        term: 1,
        vote_granted: false,
    };
    let addr1 = MockNode::simulate_send_votes_mock_server(MOCK_RAFT_PORT_BASE + 1, vote_response, rx1)
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

    // 4. Mock Raft Context
    let mut mock_membership = MockMembership::new();
    mock_membership
        .expect_voting_members()
        .returning(move |_| requests_with_peer_address.clone());
    mock_membership.expect_mark_leader_id().returning(|_| Ok(()));
    raft.ctx.set_membership(Arc::new(mock_membership));

    let mut mock_transport = MockTransport::new();
    mock_transport.expect_send_vote_requests().returning(|_, _, _| Ok(true));
    raft.ctx.set_transport(Arc::new(mock_transport));

    // 5. Add state listeners
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 6. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(10), raft.run()).await;
    });

    // 7. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(5)).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    // 8. Validate if node become Leader
    let state = monitor_rx.recv().await.unwrap();
    assert!(is_candidate(state), "Not Candidate");

    let state = monitor_rx.recv().await.unwrap();
    assert!(is_leader(state), "Not Leader");

    raft_handle.await.expect("should succeed");
}

/// # Case 1.1: Leader can not switch to Learner
#[tokio::test]
async fn test_handle_role_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.2: Leader can not switch to candidate
#[tokio::test]
async fn test_handle_role_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_2", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.3: Leader can not switch to Leader
#[tokio::test]
async fn test_handle_role_event_case1_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_3", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.4: Leader can not switch to Follower
#[tokio::test]
async fn test_handle_role_event_case1_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_4", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 2.1: Candidate can switch to Leader
#[tokio::test]
async fn test_handle_role_event_case2_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_1", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 2.2: Candidate can switch to Follower
#[tokio::test]
async fn test_handle_role_event_case2_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_2", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 2.3: Candidate can switch to Learner
#[tokio::test]
async fn test_handle_role_event_case2_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_3", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 2.4: Candidate can not switch to Candidate
#[tokio::test]
async fn test_handle_role_event_case2_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_4", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));
}

/// # Case 3.1: Follower can not switch to Leader
#[tokio::test]
async fn test_handle_role_event_case3_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_1", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}
/// # Case 3.2: Follower can switch to Candidate
#[tokio::test]
async fn test_handle_role_event_case3_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_2", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
    let old_term = raft.role.current_term();

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));
}

/// # Case 3.3: Follower can switch to Learner
#[tokio::test]
async fn test_handle_role_event_case3_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_3", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 3.4: Follower can not switch to Follower
#[tokio::test]
async fn test_handle_role_event_case3_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_4", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 4.1: Learner can not switch to Leader
#[tokio::test]
async fn test_handle_role_event_case4_1() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_1", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 4.2: Learner can not switch to Candidate
#[tokio::test]
async fn test_handle_role_event_event_case4_2() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_2", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 4.3: Learner can switch to Follower
#[tokio::test]
async fn test_handle_role_event_event_case4_3() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_3", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 4.4: Learner can not switch to Learner
#[tokio::test]
async fn test_handle_role_event_event_case4_4() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_4", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// Case 1.1: as Follower
#[tokio::test]
async fn test_handle_role_event_state_update_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_state_update1_1", graceful_rx, None);

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex { new_commit_index })
        .await
        .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap(), new_commit_index);
}

/// Case 1.2: as Candidate
#[tokio::test]
async fn test_handle_role_event_state_update_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_state_update1_2", graceful_rx, None);

    // Prepare node as Candidate
    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex { new_commit_index })
        .await
        .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap(), new_commit_index);
}

/// Case 1.3: as Leader
#[tokio::test]
async fn test_handle_role_event_state_update_case1_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_state_update1_3", graceful_rx, None);

    // Prepare node as Leader
    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader)
        .await
        .expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex { new_commit_index })
        .await
        .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap(), new_commit_index);
}

/// Case 1.4: as Learner
#[tokio::test]
async fn test_handle_role_event_state_update_case1_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_state_update1_4", graceful_rx, None);

    // Prepare node as Candidate
    raft.handle_role_event(RoleEvent::BecomeLearner)
        .await
        .expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex { new_commit_index })
        .await
        .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap(), new_commit_index);
}

/// # Test before if raft is shutdown
///
/// ## Validation criterias:
/// 1. raft loop should exist
/// 2. HardState should be persisted
/// 3. Raft Log should be flushed
/// 4. State Machine should be flushed
#[tokio::test]
async fn test_raft_shutdown() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_raft_shutdown", graceful_rx, None);
    let mut state_storage = MockStateStorage::new();
    let mut raft_log = MockRaftLog::new();
    let mut state_machine = MockStateMachine::new();

    state_storage.expect_save_hard_state().times(1).returning(|_| Ok(()));
    raft_log.expect_flush().times(1).returning(|| Ok(()));
    state_machine.expect_flush().times(1).returning(|| Ok(()));
    raft.ctx.raft_log = Arc::new(raft_log);
    raft.ctx.state_machine = Arc::new(state_machine);
    raft.ctx.state_storage = Box::new(state_storage);

    // 2. Start the Raft main loop
    let raft_handle = tokio::spawn(async move { raft.run().await });

    // 3. Send shutdown signal
    graceful_tx.send(()).expect("send signal success");
    // 4. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    if let Ok(Ok(())) = raft_handle.await {
        assert!(true);
    } else {
        assert!(false);
    }
}
