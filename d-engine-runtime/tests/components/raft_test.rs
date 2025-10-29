use d_engine_core::MockReplicationCore;
use d_engine_core::RaftEvent;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time;
use tokio::time::timeout;
use tracing_test::traced_test;

use d_engine_core::AppendResults;
use d_engine_core::ConsensusError;
use d_engine_core::ElectionError;
use d_engine_core::Error;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::MockElectionCore;
use d_engine_core::MockMembership;
use d_engine_core::MockRaftLog;
use d_engine_core::MockTransport;
use d_engine_core::MockTypeConfig;
use d_engine_core::NewCommitData;
use d_engine_core::PeerUpdate;
use d_engine_core::RaftOneshot;
use d_engine_core::RaftRole;
use d_engine_core::RoleEvent;
use d_engine_core::TestEvent;
use d_engine_core::VoteResult;
use d_engine_core::candidate_state::CandidateState;
use d_engine_core::cluster::is_candidate;
use d_engine_core::cluster::is_follower;
use d_engine_core::cluster::is_leader;
use d_engine_core::cluster::is_learner;
use d_engine_core::leader_state::LeaderState;
use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::election::VoteResponse;
use d_engine_runtime::mock_raft;

/// # Case 1: Tick has higher priority than role event
#[tokio::test]
#[traced_test]
async fn test_tick_priority_over_role_event() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_tick_priority_over_role_event", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;
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
#[traced_test]
async fn test_role_event_priority_over_event_rx() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_role_event_priority_over_event_rx",
        graceful_rx,
        None,
    );
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    // 2. Add state listeners
    let raft_tx = raft.event_tx.clone();
    let role_tx = raft.role_tx.clone();
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx.clone());
    let (event_monitor_tx, mut event_monitor_rx) = mpsc::unbounded_channel::<TestEvent>();
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
    raft_tx.send(RaftEvent::ClusterConf(MetadataRequest {}, resp_tx)).await.unwrap();
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
    assert!(matches!(event, TestEvent::ClusterConf(_)));

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
#[traced_test]
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
    raft.ctx.handlers.election_handler = election_handler_mock;
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.handlers.replication_handler = replication_core;
    raft.ctx.storage.raft_log = Arc::new(raft_log);

    // 3. Add state listeners
    let (role_monitor_tx, mut role_monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(role_monitor_tx);
    let (event_monitor_tx, mut event_monitor_rx) = mpsc::unbounded_channel::<TestEvent>();
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
    if let Ok(Some(_)) = timeout(Duration::from_millis(5), event_monitor_rx.recv()).await {
        panic!("No event change event should happen");
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
#[traced_test]
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
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;
    raft.ctx.handlers.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Candidate(Box::new(CandidateState::new(
        1,
        raft.ctx.node_config.clone(),
    ))));

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
    assert!(
        is_leader(first_state),
        "Candidate should be voted as Leader"
    );

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
#[traced_test]
async fn test_election_timeout_case2_2() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .times(1)
        .returning(|_, _, _, _, _| {
            Err(Error::Consensus(ConsensusError::Election(
                ElectionError::QuorumFailure {
                    required: 3,
                    succeed: 1,
                },
            )))
        });

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case2_2", graceful_rx, None);
    raft.ctx.handlers.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Candidate(Box::new(CandidateState::new(
        1,
        raft.ctx.node_config.clone(),
    ))));

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
    if let Ok(Some(_)) = timeout(Duration::from_millis(5), monitor_rx.recv()).await {
        panic!("No event change event should happen");
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
#[traced_test]
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
    raft.ctx.handlers.election_handler = election_handler_mock;

    // 3. Prepare the node as Candidate
    raft.set_role(RaftRole::Leader(Box::new(LeaderState::new(
        1,
        raft.ctx.node_config.clone(),
    ))));

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
    if let Ok(Some(_)) = timeout(Duration::from_millis(5), monitor_rx.recv()).await {
        panic!("No role change event should happen");
    }

    // 8. Wait for thread finishes
    raft_handle.await.expect("should succeed");
}

/// # Case 4: if I am follower, test until I am voted as Leader
///
/// ## Setup:
/// - prepare the node as leader
///
/// ## Criterias:
/// - broadcast_vote_requests should be called zero times
/// - no role change event should be received
#[tokio::test]
#[traced_test]
async fn test_election_timeout_case4() {
    tokio::time::pause();

    // 1. Mock Election Handler, assume broadcast_vote_requests successfully.
    let mut election_handler_mock = MockElectionCore::new();
    election_handler_mock
        .expect_broadcast_vote_requests()
        .returning(|_, _, _, _, _| Ok(()));
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();

    // 2. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_election_timeout_case4", graceful_rx, None);
    raft.ctx.handlers.election_handler = election_handler_mock;
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;
    let peer1_id = 2;
    let peer2_id = 3;

    // 4. Mock Raft Context
    let mut mock_membership = MockMembership::new();
    mock_membership.expect_get_zombie_candidates().returning(Vec::new);
    mock_membership.expect_voters().returning(move || {
        vec![
            NodeMeta {
                id: peer1_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55002".to_string(),
                role: Candidate.into(),
                status: NodeStatus::Active.into(),
            },
        ]
    });
    mock_membership.expect_mark_leader_id().returning(|_| Ok(()));
    mock_membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
    raft.ctx.set_membership(Arc::new(mock_membership));

    let mut mock_transport = MockTransport::new();
    mock_transport.expect_send_vote_requests().returning(|_, _, _| {
        Ok(VoteResult {
            peer_ids: vec![2].into_iter().collect(),
            responses: vec![Ok(VoteResponse {
                term: 1,
                vote_granted: true,
                last_log_index: 1,
                last_log_term: 1,
            })],
        })
    });
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
#[traced_test]
async fn test_handle_role_event_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;
    assert!(raft.handle_role_event(RoleEvent::BecomeFollower(None)).await.is_err());
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeLearner).await.is_err());
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.2: Leader can not switch to candidate
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_2", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeCandidate).await.is_err());
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.3: Leader can not switch to Leader
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case1_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_3", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeLeader).await.is_err());
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 1.4: Leader can  switch to Follower
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case1_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case1_4", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeFollower(None)).await.is_ok());
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 2.1: Candidate can switch to Leader
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case2_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_1", graceful_rx, None);
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));
}

/// # Case 2.2: Candidate can switch to Follower
#[tokio::test]
#[traced_test]
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
#[traced_test]
async fn test_handle_role_event_case2_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_3", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 2.4: Candidate can not switch to Candidate
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case2_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case2_4", graceful_rx, None);

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeCandidate).await.is_err());
    assert!(is_candidate(raft.role.as_i32()));
}

/// # Case 3.1: Follower can not switch to Leader
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case3_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_1", graceful_rx, None);
    assert!(is_follower(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeLeader).await.is_err());
    assert!(is_follower(raft.role.as_i32()));
}
/// # Case 3.2: Follower can switch to Candidate
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case3_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_2", graceful_rx, None);
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));
}

/// # Case 3.3: Follower can switch to Learner
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case3_3() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_3", graceful_rx, None);
    assert!(is_follower(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 3.4: Follower can not switch to Follower
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case3_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case3_4", graceful_rx, None);
    assert!(is_follower(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeFollower(None)).await.is_err());
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 4.1: Learner can not switch to Leader
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_case4_1() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_1", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeLeader).await.is_err());
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 4.2: Learner can not switch to Candidate
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_event_case4_2() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_2", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeCandidate).await.is_err());
    assert!(is_learner(raft.role.as_i32()));
}

/// # Case 4.3: Learner can switch to Follower
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_event_case4_3() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_3", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeFollower(None))
        .await
        .expect("should succeed");
    assert!(is_follower(raft.role.as_i32()));
}

/// # Case 4.4: Learner can not switch to Learner
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_event_case4_4() {
    // 1. Create a Raft instance
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_handle_role_event_case4_4", graceful_rx, None);
    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    assert!(raft.handle_role_event(RoleEvent::BecomeLearner).await.is_err());
    assert!(is_learner(raft.role.as_i32()));
}

/// Case 1.1: as Follower
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update1_1",
        graceful_rx,
        None,
    );

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: Leader.into(),
        current_term: 1,
    }))
    .await
    .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap().new_commit_index, new_commit_index);
}

/// Case 1.2: as Candidate
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update1_2",
        graceful_rx,
        None,
    );

    // Prepare node as Candidate
    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: Leader.into(),
        current_term: 1,
    }))
    .await
    .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap().new_commit_index, new_commit_index);
}

/// Case 1.3.1: as Leader,
///
/// Test Criterias:
/// 1. Test Candidate could become Leader
/// 2. Test commit index listener
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_3_1() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update1_3_1",
        graceful_rx,
        None,
    );
    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    // Prepare node as Leader
    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: Leader.into(),
        current_term: 1,
    }))
    .await
    .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap().new_commit_index, new_commit_index);
}
/// Case 1.3.2: as Leader,
///
/// Test Criterias:
/// 1. peer next index and match index should be updated
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_3_2() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update_case1_3_2",
        graceful_rx,
        None,
    );

    // Prepare Peers
    let mut membership = MockMembership::new();
    membership
        .expect_get_peers_id_with_condition()
        .returning(|_| vec![2, 3])
        .times(1);
    membership.expect_voters().returning(move || {
        vec![NodeMeta {
            id: 2,
            address: "http://127.0.0.1:55001".to_string(),
            role: Follower.into(),
            status: NodeStatus::Active.into(),
        }]
    });

    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    //Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .returning(move |_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(11),
                            next_index: 12,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(11),
                            next_index: 12,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    // Prepare none empty raft_logs
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_entry_id().returning(|| 11).times(1);
    raft_log.expect_flush().returning(|| Ok(()));
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(11));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft.ctx.membership = Arc::new(membership);
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // Prepare node as Leader
    raft.handle_role_event(RoleEvent::BecomeCandidate)
        .await
        .expect("should succeed");
    assert!(is_candidate(raft.role.as_i32()));

    raft.handle_role_event(RoleEvent::BecomeLeader).await.expect("should succeed");
    assert!(is_leader(raft.role.as_i32()));

    // Validate if peer's next index and match index been initialized
    assert_eq!(raft.role.next_index(2), Some(12));
    assert_eq!(raft.role.next_index(3), Some(12));
    assert_eq!(raft.role.match_index(2), Some(11));
    assert_eq!(raft.role.match_index(3), Some(11));
}

/// Case 1.4: as Learner
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_4() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update1_4",
        graceful_rx,
        None,
    );

    // Prepare node as Candidate
    raft.handle_role_event(RoleEvent::BecomeLearner).await.expect("should succeed");
    assert!(is_learner(raft.role.as_i32()));

    let new_commit_index = 11;
    let (tx, mut rx) = mpsc::unbounded_channel();
    raft.register_new_commit_listener(tx);
    raft.handle_role_event(RoleEvent::NotifyNewCommitIndex(NewCommitData {
        new_commit_index,
        role: Leader.into(),
        current_term: 1,
    }))
    .await
    .expect("should succeed");
    assert_eq!(rx.recv().await.unwrap().new_commit_index, new_commit_index);
}

fn prepare_succeed_majority_confirmation() -> (MockRaftLog, MockReplicationCore<MockTypeConfig>) {
    // Initialize the mock object
    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    let mut raft_log = MockRaftLog::new();

    //Configure mock behavior
    replication_handler
        .expect_handle_raft_request_in_batch()
        .returning(move |_, _, _, _| {
            Ok(AppendResults {
                commit_quorum_achieved: true,
                learner_progress: HashMap::new(),
                peer_updates: HashMap::from([
                    (
                        2,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                    (
                        3,
                        PeerUpdate {
                            match_index: Some(5),
                            next_index: 6,
                            success: true,
                        },
                    ),
                ]),
            })
        });

    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_flush().return_once(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));

    (raft_log, replication_handler)
}

/// # Case 1.5.1: as Leader, try to verify leadership in new term failed
/// due to **technical failures in the verification process**, not quorum rejection.
///
/// ## Validation criterias:
/// 1. should monitor BecomeFollower event been send out.
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_5_1() {
    tokio::time::pause();
    // 1. Create a Raft instance with mocks
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update_case1_5_1",
        graceful_rx,
        None,
    );
    let mut replication_handler = MockReplicationCore::<MockTypeConfig>::new();
    replication_handler
        .expect_handle_raft_request_in_batch()
        .returning(move |_, _, _, _| Err(Error::Fatal("".to_string())));

    let mut raft_log = MockRaftLog::new();
    raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));
    raft_log.expect_last_entry_id().return_const(1_u64);
    raft_log.expect_flush().return_once(|| Ok(()));
    raft_log.expect_load_hard_state().returning(|| Ok(None));
    raft_log.expect_save_hard_state().returning(|_| Ok(()));
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_handler;

    // 2. Add state listeners
    let role_tx = raft.role_tx.clone();
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 3. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(20), raft.run()).await;
    });

    // 4. Send RoleEvent（role == Candidate）
    role_tx.send(RoleEvent::BecomeLeader).unwrap();

    // 5. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::time::sleep(Duration::from_millis(10)).await; // **key of test**  Ensure Tick is processed

    // 6. Wait for Tick to trigger and process
    let role_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_candidate(role_state),
        "Tick should prioritize triggering the election"
    );
    let role_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_leader(role_state),
        "Tick should prioritize triggering the election"
    );

    // 7. Wait for step back as Follower
    let role_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_follower(role_state),
        "Tick should prioritize triggering the election"
    );
    raft_handle.await.expect("should succeed");
}

/// # Case 1.5.2: as Leader, verify leadership in new term successfully
///
/// ## Validation criterias:
/// 1. should no BecomeFollower event been sent out.
#[tokio::test]
#[traced_test]
async fn test_handle_role_event_state_update_case1_5_2() {
    tokio::time::pause();
    // 1. Create a Raft instance with mocks
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft(
        "/tmp/test_handle_role_event_state_update_case1_5_2",
        graceful_rx,
        None,
    );

    let (raft_log, replication_core) = prepare_succeed_majority_confirmation();
    raft.ctx.storage.raft_log = Arc::new(raft_log);
    raft.ctx.handlers.replication_handler = replication_core;

    // 2. Add state listeners
    let role_tx = raft.role_tx.clone();
    let (monitor_tx, mut monitor_rx) = mpsc::unbounded_channel::<i32>();
    raft.register_role_transition_listener(monitor_tx);

    // 3. Start the Raft main loop
    let raft_handle = tokio::spawn(async move {
        let _ = time::timeout(Duration::from_millis(100), raft.run()).await;
    });

    // 4. Send RoleEvent（role == Candidate）
    role_tx.send(RoleEvent::BecomeLeader).unwrap();

    // 5. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    // 6. Wait for Tick to trigger and process
    let role_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_candidate(role_state),
        "Tick should prioritize triggering the election"
    );
    let role_state = monitor_rx.recv().await.unwrap();
    assert!(
        is_leader(role_state),
        "Tick should prioritize triggering the election"
    );

    // 7. No role changes
    assert!(monitor_rx.try_recv().is_err());
    raft_handle.await.expect("should succeed");
}

/// # Test before if raft is shutdown
///
/// ## Validation criterias:
/// 1. raft loop should exist
/// 2. HardState should be persisted
/// 3. Raft Log should be flushed
/// 4. State Machine should be flushed
#[tokio::test]
#[traced_test]
async fn test_raft_drop() {
    tokio::time::pause();

    // 1. Create a Raft instance
    let (graceful_tx, graceful_rx) = watch::channel(());
    let mut raft = mock_raft("/tmp/test_raft_drop", graceful_rx, None);
    let mut raft_log = MockRaftLog::new();

    raft_log.expect_save_hard_state().times(1).returning(|_| Ok(()));
    raft.ctx.storage.raft_log = Arc::new(raft_log);

    // 2. Start the Raft main loop
    let raft_handle = tokio::spawn(async move { raft.run().await });

    // 3. Send shutdown signal
    graceful_tx.send(()).expect("send signal success");
    // 4. Time advancement control (step-by-step trigger Tick)
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::sleep(Duration::from_millis(2)).await;

    let result = raft_handle.await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "Expected Ok(Ok(())), but got {result:?}"
    );
}
