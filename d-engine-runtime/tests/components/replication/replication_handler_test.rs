use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use prost::Message;
use tokio::sync::watch;
use tracing::debug;
use tracing_test::traced_test;

use super::ReplicationCore;
use super::ReplicationData;
use super::ReplicationHandler;
use crate::AppendResult;
use crate::ConsensusError;
use crate::Error;
use crate::LeaderStateSnapshot;
use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockTransport;
use crate::NetworkError;
use crate::PeerUpdate;
use crate::RaftLog;
use crate::ReplicationError;
use crate::StateSnapshot;
use crate::StorageError;
use crate::SystemError;
use crate::client_command_to_entry_payloads;
use crate::convert::safe_kv_bytes;
use crate::test_utils::MockTypeConfig;
use crate::test_utils::generate_insert_commands;
use crate::test_utils::mock_raft_context;
use crate::test_utils::setup_raft_components;
use crate::test_utils::simulate_insert_command;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Insert;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Leader;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::common::entry_payload::Payload;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::ConflictResult;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;

/// # Case 1: The peer3's next_index is equal to
///     the end of the leader's old log,
///     and only the new log is sent
///
/// ## Validate criterias
/// 1. only new_entries returned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case1() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case1",
        None,
        false,
    );
    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));
}

/// # Case 2: Peer3 needs old log + new log
///     (not exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate one entry in local raft log(log-1)
/// 2. Peer 3 next_index is 1
///
/// ## Validate criterias
/// 1. both log-1 and new_entries are returned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case2() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case2",
        None,
        false,
    );

    // Simulate one entry in local raft log
    let raft_log = context.raft_log;
    simulate_insert_command(&raft_log, vec![1], 1).await;

    let my_id = 1;
    let peer3_id = 3;
    let next_index = raft_log.pre_allocate_raft_logs_next_index();
    let new_entries = vec![Entry {
        index: next_index,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];

    let leader_last_index_before_inserting_new_entries = raft_log.last_entry_id();
    let max_entries = 100;
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &raft_log,
    );
    let last_log_entry = raft_log.last_entry().unwrap();
    let mut merged_entries = vec![last_log_entry];
    merged_entries.extend(new_entries);
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == merged_entries));
}

/// # Case 3: new_entries is empty and Peer3 has latencies
///     (not exceeding the max limit)
///
/// ## Prepration setup
/// 1. Simulate one entry in local raft log(log-1)
/// 2. Peer 3 next_index is 1
///
/// ## Validate criterias
/// 1. only log-1 is returned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case3() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case3",
        None,
        false,
    );

    // Simulate one entry in local raft log
    let raft_log = context.raft_log;
    simulate_insert_command(&raft_log, vec![1], 1).await;

    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![];

    let leader_last_index_before_inserting_new_entries = raft_log.last_entry_id();
    let max_entries = 100;
    let peer_next_indices =
        HashMap::from([(peer3_id, leader_last_index_before_inserting_new_entries)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &raft_log,
    );
    let last_log_entry = raft_log.last_entry().unwrap();
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == vec![last_log_entry]));
}

/// # Case 4.1: Peer3 needs old log + new log - max_legacy_entries_per_peer = 2
///     (while exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate entries in local raft log(log-1, log-2, log-3)
/// 2. Peer 3 next_index is 1
/// 3. max_legacy_entries_per_peer = 2
///
/// ## Validate criterias
/// 1. both log-1,log-2 and new_entries are returned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case4_1() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_1",
        None,
        false,
    );

    let my_id = 1;
    let peer3_id = 3;
    let peer3_next_id = 1;
    // Simulate one entry in local raft log
    let max_legacy_entries_per_peer = 2;
    let leader_last_index_before_inserting_new_entries = 3;
    let raft_log = context.raft_log;
    simulate_insert_command(
        &raft_log,
        (1..=leader_last_index_before_inserting_new_entries).collect(),
        1,
    )
    .await;

    let new_entries = vec![Entry {
        index: 3,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];
    let peer_next_indices = HashMap::from([(peer3_id, peer3_next_id)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &raft_log,
    );
    let mut lagency_entries = raft_log
        .get_entries_range(peer3_next_id..=(peer3_next_id + max_legacy_entries_per_peer - 1))
        .expect("Successfully retrieved entries");
    lagency_entries.extend(new_entries);
    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == lagency_entries));
}

/// # Case 4.2: Peer3 needs old log + new log - max_legacy_entries_per_peer = 0
///     (while exceeding the max limit)
///     and returning latencies and the new log
///
/// ## Prepration setup
/// 1. Simulate entriesin local raft log(log-1, log-2, log-3)
/// 2. Peer 3 next_index is 1
/// 3. max_legacy_entries_per_peer = 0
///
/// ## Validate criterias
/// 1. only new_entries are returned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case4_2() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case4_2",
        None,
        false,
    );

    let my_id = 1;
    let peer3_id = 3;
    let peer3_next_id = 1;
    // Simulate one entry in local raft log
    let max_legacy_entries_per_peer = 0;
    let leader_last_index_before_inserting_new_entries = 3;
    let raft_log = context.raft_log;
    simulate_insert_command(
        &raft_log,
        (1..=leader_last_index_before_inserting_new_entries).collect(),
        1,
    )
    .await;

    let new_entries = vec![Entry {
        index: 3,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];
    let peer_next_indices = HashMap::from([(peer3_id, peer3_next_id)]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_legacy_entries_per_peer,
        &peer_next_indices,
        &raft_log,
    );

    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));
}

/// # Case 5: returned entries should not has leader ones
///
/// ## Validate criterias
/// 1. No leader ones should be retruned
#[tokio::test]
#[traced_test]
async fn test_retrieve_to_be_synced_logs_for_peers_case5() {
    let context = setup_raft_components(
        "/tmp/test_retrieve_to_be_synced_logs_for_peers_case5",
        None,
        false,
    );
    let my_id = 1;
    let peer3_id = 3;
    let new_entries = vec![Entry {
        index: 1,
        term: 1,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }];
    let leader_last_index_before_inserting_new_entries = 10;
    let max_entries = 100;
    let peer_next_indices = HashMap::from([
        (my_id, 1),
        (peer3_id, leader_last_index_before_inserting_new_entries),
    ]);
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    let r = handler.retrieve_to_be_synced_logs_for_peers(
        new_entries.clone(),
        leader_last_index_before_inserting_new_entries,
        max_entries,
        &peer_next_indices,
        &context.raft_log,
    );

    assert!(r.get(&peer3_id).is_some_and(|entries| *entries == new_entries));

    assert!(r.get(&my_id).is_none());
}

/// # Case 1: Test with empty commands
///
/// ## Validation criterias:
/// 1. fun returns Ok(vec![])
/// 2. no update on local raft log
#[tokio::test]
#[traced_test]
async fn test_generate_new_entries_case1() {
    let context = setup_raft_components("/tmp/test_generate_new_entries_case1", None, false);
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);
    let last_id = context.raft_log.last_entry_id();
    let commands = vec![];
    let current_term = 1;
    let r = handler.generate_new_entries(commands, current_term, &context.raft_log).await;
    assert_eq!(r.unwrap(), vec![]);
    assert_eq!(context.raft_log.last_entry_id(), last_id);
}

/// # Case 2: Test with one command
///
/// ## Validation criterias:
/// 1. fun returns Ok(vec![log-1])
/// 2. update on local raft log with one extra entry
#[tokio::test]
#[traced_test]
async fn test_generate_new_entries_case2() {
    let context = setup_raft_components("/tmp/test_generate_new_entries_case2", None, false);
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);
    let last_id = context.raft_log.last_entry_id();
    debug!("last_id: {}", last_id);
    let commands = vec![WriteCommand::delete(safe_kv_bytes(1))];
    let current_term = 1;
    assert_eq!(
        handler
            .generate_new_entries(
                client_command_to_entry_payloads(commands),
                current_term,
                &context.raft_log
            )
            .await
            .unwrap()
            .len(),
        1
    );

    assert_eq!(context.raft_log.last_entry_id(), last_id + 1);
}

/// # Case: Test retrieve expected items for peer 2
///
/// ## Scenario Setup
/// 1. entries_per_peer: peer-2: log-3 peer-3: log-1, log-2, log-3
///
/// ## Validation criterias:
/// 1. retrieved entries' length is 2
#[tokio::test]
#[traced_test]
async fn test_build_append_request_case() {
    let context = setup_raft_components("/tmp/test_build_append_request_case", None, false);
    let my_id = 1;
    let peer2_id = 2;
    let peer2_next_index = 3;
    let peer3_id = 3;
    let peer3_next_index = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);
    // Prepare entries to be replicated for each peer
    let entries_per_peer: DashMap<u32, Vec<Entry>> = DashMap::new();
    entries_per_peer.insert(
        peer2_id,
        vec![Entry {
            index: 3,
            term: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
        }],
    );
    entries_per_peer.insert(
        peer3_id,
        vec![
            Entry {
                index: 1,
                term: 1,
                payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
            },
            Entry {
                index: 2,
                term: 1,
                payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
            },
            Entry {
                index: 3,
                term: 1,
                payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
            },
        ],
    );

    let data = ReplicationData {
        leader_last_index_before: 3,
        current_term: 1,
        commit_index: 1,
        peer_next_indices: HashMap::from([
            (peer2_id, peer2_next_index),
            (peer3_id, peer3_next_index),
        ]),
    };

    let (_id, to_be_replicated_request) =
        handler.build_append_request(&context.raft_log, peer2_id, &entries_per_peer, &data);
    assert_eq!(to_be_replicated_request.entries.len(), 1);
}

fn mk_log(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
    }
}
#[test]
fn test_valid_request() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    let entry_term = 10;
    let prev_log_index = 5;
    let prev_log_term = entry_term;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: entry_term,
            index: 1,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index,
        prev_log_term,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response =
        handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_success());

    if let append_entries_response::Result::Success(SuccessResult {
        last_match: Some(last_match),
    }) = response.result.unwrap()
    {
        assert_eq!(last_match.index, prev_log_index);
        assert_eq!(last_match.term, prev_log_term);
    }
}

#[test]
fn test_stale_term() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    raft_log.expect_last_entry().returning(|| None);

    let request = AppendEntriesRequest {
        term: my_term - 1,
        prev_log_index: 5,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    assert!(
        handler
            .check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log))
            .is_higher_term()
    );
}

/// # Case 1: follower local raft log length > prev_log_index
#[test]
fn test_mismatched_prev_term_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 1;
    let entry_term = 10;
    let local_last_log_id = 2;
    let prev_log_index = local_last_log_id - 1;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: my_term,
            index: local_last_log_id,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: 1,
        prev_log_index,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response =
        handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_conflict());
    if let append_entries_response::Result::Conflict(ConflictResult {
        conflict_term,
        conflict_index,
    }) = response.result.unwrap()
    {
        assert_eq!(conflict_term.unwrap(), entry_term);
        assert_eq!(conflict_index.unwrap(), prev_log_index.saturating_sub(1));
    }
}

/// # Case 2: follower local raft log length > prev_log_index
#[test]
fn test_mismatched_prev_term_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 1;
    let entry_term = 10;
    let local_last_log_id = 2;
    let prev_log_index = local_last_log_id + 1;
    raft_log.expect_last_log_id().return_once(move || {
        Some(LogId {
            term: my_term,
            index: local_last_log_id,
        })
    });
    // entry_term != prev_log_term
    raft_log.expect_entry_term().return_once(move |_| Some(entry_term));

    let request = AppendEntriesRequest {
        term: 1,
        prev_log_index,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response =
        handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_conflict());
    if let append_entries_response::Result::Conflict(ConflictResult {
        conflict_term,
        conflict_index,
    }) = response.result.unwrap()
    {
        assert_eq!(conflict_term.unwrap(), entry_term);
        assert_eq!(conflict_index.unwrap(), local_last_log_id + 1);
    }
}

#[test]
fn test_virtual_log_handling() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    raft_log.expect_last_log_id().returning(|| Some(LogId { term: 1, index: 5 }));

    let my_term = 2;
    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit_index: 5,
        leader_id: 2,
    };

    assert!(
        handler
            .check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log))
            .is_success()
    );
}
#[test]
fn test_virtual_log_with_non_empty_log() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    raft_log.expect_last_log_id().returning(|| Some(LogId { term: 1, index: 5 }));

    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![Entry {
            term: 2,
            index: 1,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![1]))),
        }],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response =
        handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    // Should accept the request and handle the actual conflict later
    assert!(response.is_success());
}

/// # Case 1: conflict_with_term_and_index
#[test]
fn test_handle_conflict_response_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: Some(3),
        conflict_index: Some(5),
    };
    let last_index_for_term = 2;
    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_index_for_term()
        .returning(move |_| Some(last_index_for_term));

    let update = handler
        .handle_conflict_response(2, conflict_result, &Arc::new(raft_log), 5)
        .unwrap();

    // Temporary logic: next_index = 5 - 1 = 4
    assert_eq!(update.next_index, last_index_for_term + 1);
    assert_eq!(update.match_index, None);
}

/// # Case 2: conflict_with_index_only
#[test]
fn test_handle_conflict_response_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: Some(5),
    };
    let current_next_index = 11;
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler
        .handle_conflict_response(2, conflict_result, &raft_log, current_next_index)
        .unwrap();
    assert_eq!(update.next_index, 5);
    assert_eq!(update.match_index, None);
}

/// # Case 3: conflict_with_no_info
#[test]
fn test_handle_conflict_response_case3() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: None,
    };
    let raft_log = Arc::new(MockRaftLog::new());
    let current_next_index = 1;

    let update = handler
        .handle_conflict_response(2, conflict_result, &raft_log, current_next_index)
        .unwrap();
    assert_eq!(
        update.next_index,
        current_next_index.saturating_sub(1).max(1)
    );
    assert_eq!(update.match_index, None);
}

/// # Case 4: conflict_with_index_zero
#[test]
fn test_handle_conflict_response_case4() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: Some(0), // Illegal but needs to be defended
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log, 0).unwrap();

    // next_index is forced to be >= 1
    assert_eq!(update.next_index, 1);
    assert_eq!(update.match_index, None);
}

/// # Case 1: test_higher_responder_term_triggers_step_down
#[test]
fn test_handle_success_response_case1() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 5; // Follower term > leader term (4)
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let result = handler.handle_success_response(2, responder_term, success_result, 4);
    assert!(matches!(
        result,
        Err(Error::Consensus(ConsensusError::Replication(
            ReplicationError::HigherTerm(5)
        )))
    ));
}

/// # Case 2: test_valid_success_response_updates_indices
#[test]
fn test_handle_success_response_case2() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.match_index, Some(10));
    assert_eq!(update.next_index, 11);
}

/// # Case 3: test_lower_responder_term_ignored
#[test]
fn test_handle_success_response_case3() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 5).unwrap();
    assert_eq!(update.match_index, Some(10)); // Update normally, do not trigger step down
}

/// # Case 4: test_empty_follower_log_handling
#[test]
fn test_handle_success_response_case4() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult { last_match: None };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.match_index, Some(0)); // Synchronize from index 0
    assert_eq!(update.next_index, 1);
}

/// # Case 5: test_zero_index_handling
#[test]
fn test_handle_success_response_case5() {
    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 0 }), // Legal scenario (initial state)
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.next_index, 1); // Ensure next_index is at least 1
}

#[test]
fn test_client_command_to_entry_payloads_case1() {
    // Create test commands
    let commands = vec![
        WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::from(b"key1".to_vec()),
                value: Bytes::from(b"value1".to_vec()),
            })),
        },
        WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::from(b"key2".to_vec()),
                value: Bytes::from(b"value2".to_vec()),
            })),
        },
    ];

    // Convert commands to entry payloads
    let payloads = client_command_to_entry_payloads(commands);

    // Verify the conversion
    assert_eq!(payloads.len(), 2, "Should produce one payload per command");

    // Check first payload
    if let Some(Payload::Command(bytes)) = &payloads[0].payload {
        let decoded = WriteCommand::decode(bytes.as_ref()).unwrap();
        assert!(matches!(
            decoded.operation,
            Some(Operation::Insert(Insert { key, value }))
            if key == b"key1".as_ref() && value == b"value1".as_ref()
        ));
    } else {
        panic!("First payload should be Command variant");
    }

    // Check second payload
    if let Some(Payload::Command(bytes)) = &payloads[1].payload {
        let decoded = WriteCommand::decode(bytes.as_ref()).unwrap();
        assert!(matches!(
            decoded.operation,
            Some(Operation::Insert(Insert { key, value }))
            if key == b"key2".as_ref() && value == b"value2".as_ref()
        ));
    } else {
        panic!("Second payload should be Command variant");
    }
}

#[test]
fn test_test_client_command_to_entry_payloads_case2_empty_input() {
    // Test with empty command vector
    let payloads = client_command_to_entry_payloads(vec![]);
    assert!(
        payloads.is_empty(),
        "Should return empty vector for empty input"
    );
}

#[cfg(test)]
mod handle_raft_request_in_batch_test {
    use tracing::debug;

    use super::*;
    use crate::convert::safe_kv_bytes;
    use crate::test_utils::MockBuilder;
    use crate::test_utils::node_config;

    /// # Case 1: No peers found
    /// ## Validation Criteria
    /// 1. Return Error::AppendEntriesNoPeerFound
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case1() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case1",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Prepare fun parameters
        let commands = Vec::new();
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let e = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::Replication(ReplicationError::NoPeerFound {
                leader_id: _
            }))
        ));
    }

    /// # Case 2.1: Successful Client Proposal Replication - one voter
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case2_1_one_voter() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case2_1",
            graceful_rx,
            None,
        );

        let my_id = 1;
        let peer2_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Prepare fun parameters
        let commands = Vec::new();
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 1);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::success(
                    peer2_id,
                    1,
                    Some(LogId { term: 1, index: 3 }),
                ))],
            })
        });
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let result = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await
            .unwrap();

        assert!(result.commit_quorum_achieved);
        assert_eq!(
            result.peer_updates.get(&peer2_id),
            Some(&PeerUpdate {
                match_index: Some(3),
                next_index: 4,
                success: true
            })
        );
    }

    /// # Case 2.2: empty peer error
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case2_empty_peer_error() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case2_2",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Prepare fun parameters
        let commands = Vec::new();
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // Prepare AppendResults
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 1);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().returning(move |_, _, _, _| {
            Err(NetworkError::EmptyPeerList {
                request_type: "send_vote_requests",
            }
            .into())
        });
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(Vec::new);
        context.membership = Arc::new(membership);

        let e = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await
            .unwrap_err();

        debug!(?e);
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::Replication(ReplicationError::NoPeerFound {
                leader_id: _
            }))
        ));
    }

    /// # Case3: Ignore success responses from stale terms
    /// ## Validation Criteria
    /// - Responses with term < leader's current term are ignored
    /// - Success counter remains unchanged, no peer updates
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case3() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case3",
            graceful_rx,
            None,
        );

        let my_id = 1;
        let peer2_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        let commands = vec![];
        let state_snapshot = StateSnapshot {
            current_term: 2, // Leader's term is 2
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from_iter(vec![(peer2_id, 3)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // Response with term=1 (stale)
        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::success(
                    peer2_id,
                    1,
                    Some(LogId { term: 1, index: 3 }),
                ))],
            })
        });

        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(3_u64);
        raft_log.expect_entry_term().return_const(Some(1_u64));
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);

        let result = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await;

        assert!(result.is_ok());
        let append_result = result.unwrap();
        assert!(!append_result.commit_quorum_achieved); // successes=1 (leader only)
        assert!(append_result.peer_updates.is_empty()); // No updates due to stale term
    }

    /// # Case4: Higher term response triggers leader step down
    /// ## Validation Criteria
    /// - HigherTerm response with term > leader's term returns error
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case4() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case4",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        let commands = vec![];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // HigherTerm response with term=2
        let higher_term = 2;
        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::higher_term(
                    peer2_id,
                    higher_term,
                ))],
            })
        });

        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(1_u64);

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);
        let result = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await;

        assert!(matches!(
            result,
            Err(Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(term)))) if term == higher_term
        ));
    }

    /// # Case 5: Test prepare_peer_entries ensures new commands appear only once in AppendEntries.
    ///
    /// ## Validation Criteria
    /// - For each peer, entries in AppendEntries start exactly at their `next_index`.
    /// - New commands are only included in the first replication attempt.
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case5() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case5",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport
            .expect_send_append_requests()
            .withf(move |requests, _, _, _| {
                // Send the request to the channel for subsequent assertions
                let _ = tx.send(requests.clone());
                true // Return true to indicate that the parameters match successfully
            })
            .return_once(|_, _, _, _| {
                Ok(AppendResult {
                    peer_ids: vec![].into_iter().collect(),
                    responses: vec![],
                })
            }); // Returns an empty response without affecting the test logic

        // ----------------------
        //Call the function to be tested
        // ----------------------
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                role: Follower.into(),
                address: "".to_string(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        context.membership = Arc::new(membership);

        let _ = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await;

        // ----------------------
        // Get the captured request from the channel and verify
        // ----------------------
        let captured_requests = rx.recv().unwrap();

        // Verify Peer2's request
        let peer2_request =
            captured_requests.iter().find(|(peer_id, _)| *peer_id == peer2_id).unwrap();
        let peer2_entries = &peer2_request.1.entries;
        assert_eq!(peer2_entries.len(), 2);
    }

    /// # Case7: Leader resolves log conflicts across divergent followers
    /// Validates next_index updates per Raft's conflict resolution rules (§5.3)
    ///
    /// ## Scenario Setup
    /// Leader log: [1(1), 2(1), 3(1), 4(4), 5(4), 6(5), 7(5), 8(6), 9(6), 10(6)]
    /// Followers with varying log states:
    /// - follower_a: log1-9 (match index 9)
    /// - follower_b: log1-4 (match index 4)
    /// - follower_c: log1-10 (match index 10)
    /// - follower_d: log1-12 (higher term)
    /// - follower_e: log1-7 (term 4)
    /// - follower_f: log1-11 (term 3)
    ///
    /// ## Validation Criteria
    /// Verify next_index updates match Raft's conflict resolution rules:
    /// - follower_a: next_index=10 (no conflict)
    /// - follower_b: next_index=5 (missing entries)
    /// - follower_c: next_index=11 (caught up)
    /// - follower_d: next_index=11 (term mismatch)
    /// - follower_e: next_index=6 (term regression)
    /// - follower_f: next_index=4 (deep conflict)
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case6() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case6",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Prepare client commands (new entries to replicate)
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(300), safe_kv_bytes(300)), /* Will create log
                                                                           * index 11 */
        ];

        // Initialize leader state
        let state_snapshot = StateSnapshot {
            current_term: 6,
            voted_for: None,
            commit_index: 10,
            role: Leader.into(),
        };

        // Initial next_index values from test case description
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from_iter(vec![
                (2, 10), // follower_a
                (3, 5),  // follower_b
                (4, 11), // follower_c
                (5, 11), // follower_d
                (6, 6),  // follower_e
                (7, 4),  // follower_f
            ]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // Configure mock Raft log - leader has logs 1-10
        let last_index_for_term = 2;
        let mut raft_log = MockRaftLog::new();
        raft_log
            .expect_last_index_for_term()
            .returning(move |_| Some(last_index_for_term));
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_last_entry_id().return_const(10_u64);
        raft_log.expect_get_entries_range().returning(|range| {
            // Simulate log entries for conflict resolution
            Ok(match range.start() {
                5..=10 => vec![
                    mk_log(5, 5),
                    mk_log(5, 6),
                    mk_log(6, 7),
                    mk_log(6, 8),
                    mk_log(6, 9),
                    mk_log(6, 10),
                ],
                4 => vec![mk_log(4, 4), mk_log(4, 5)],
                _ => vec![],
            })
        });
        raft_log.expect_insert_batch().returning(|_| Ok(()));
        raft_log.expect_entry_term().returning(|prev_index| match prev_index {
            10 => Some(6),
            9 => Some(6),
            8 => Some(6),
            7 => Some(5),
            6 => Some(5),
            5 => Some(4),
            4 => Some(4),
            _ => Some(0),
        });

        // Configure mock transport responses
        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |requests, _, _, _| {
            Ok(AppendResult {
                peer_ids: requests.iter().map(|(id, _)| *id).collect(),
                responses: vec![
                    // follower_a (id=2) - success
                    Ok(AppendEntriesResponse::success(
                        2,
                        6,
                        Some(LogId { term: 6, index: 10 }),
                    )),
                    // follower_b (id=3) - conflict at index 5 (term 4)
                    Ok(AppendEntriesResponse::conflict(3, 6, Some(4), Some(5))),
                    // follower_c (id=4) - success (already up-to-date)
                    Ok(AppendEntriesResponse::success(
                        4,
                        6,
                        Some(LogId { term: 6, index: 10 }),
                    )),
                    // follower_d (id=5) - higher term (7)
                    Ok(AppendEntriesResponse::higher_term(5, 7)),
                    // follower_e (id=6) - conflict at index 6 (term 4)
                    Ok(AppendEntriesResponse::conflict(6, 6, Some(4), Some(6))),
                    // follower_f (id=7) - conflict at index 4 (term 2)
                    Ok(AppendEntriesResponse::conflict(7, 6, Some(2), Some(4))),
                ],
            })
        });

        // Setup replication members
        let futures: Vec<_> = (2..=7)
            .map(|id| async move {
                NodeMeta {
                    id: id as u32,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                }
            })
            .collect();

        let members = futures::future::join_all(futures).await;
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || members.clone());
        context.membership = Arc::new(membership);
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        // Execute test
        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await;

        // Verify results
        match result {
            Ok(append_result) => {
                // Check quorum (should fail due to follower_d's higher term)
                assert!(!append_result.commit_quorum_achieved);

                // Verify peer updates
                let updates = &append_result.peer_updates;

                // follower_a (success)
                assert_eq!(
                    updates[&2],
                    PeerUpdate {
                        match_index: Some(10),
                        next_index: 11,
                        success: true
                    }
                );

                // follower_b (conflict at term 4 index 5)
                assert_eq!(
                    updates[&3],
                    PeerUpdate {
                        match_index: None,
                        next_index: last_index_for_term + 1,
                        success: false
                    }
                );

                // follower_c (success)
                assert_eq!(
                    updates[&4],
                    PeerUpdate {
                        match_index: Some(10),
                        next_index: 11,
                        success: true
                    }
                );

                // follower_d (higher term) - no update (error handled)
                assert!(!updates.contains_key(&5));

                // follower_e (conflict at term 4 index 6)
                assert_eq!(
                    updates[&6],
                    PeerUpdate {
                        match_index: None,
                        next_index: last_index_for_term + 1,
                        success: false
                    }
                );

                // follower_f (conflict at term 2 index 4)
                assert_eq!(
                    updates[&7],
                    PeerUpdate {
                        match_index: None,
                        next_index: last_index_for_term + 1,
                        success: false
                    }
                );
            }
            Err(e) => {
                // Verify higher term error from follower_d
                assert!(matches!(
                    e,
                    Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(7)))
                ));
            }
        }
    }

    /// # Case 7: Partial node timeouts
    /// ## Scenario
    /// - 5 voting nodes (leader + 4 followers)
    /// - 1 follower responds successfully
    /// - 3 follower times out
    /// ## Validation Criteria
    /// 1. Returns Ok(AppendResults)
    /// 2. commit_quorum_achieved = false (1 success + leader < majority of 3)
    /// 3. peer_updates contains only the successful peer
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case7() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case7",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let peer3_id = 3;
        let peer4_id = 4;
        let peer5_id = 5;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6), (peer3_id, 6), (peer4_id, 6), (peer5_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id, peer3_id, peer4_id, peer5_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        peer2_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                    Err(NetworkError::Timeout {
                        node_id: peer3_id,
                        duration: Duration::from_millis(200),
                    }
                    .into()), // Simulate timeout
                    Err(NetworkError::Timeout {
                        node_id: peer4_id,
                        duration: Duration::from_millis(200),
                    }
                    .into()), // Simulate timeout
                    Err(NetworkError::Timeout {
                        node_id: peer5_id,
                        duration: Duration::from_millis(200),
                    }
                    .into()), // Simulate timeout
                ],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer3_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer4_id,
                    address: "http://127.0.0.1:55003".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer5_id,
                    address: "http://127.0.0.1:55004".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
            ]
        });
        context.membership = Arc::new(membership);

        // ----------------------
        // Execute test
        // ----------------------
        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(
            !result.commit_quorum_achieved,
            "Should not achieve quorum with 1/2 followers responding"
        );
        assert_eq!(
            result.peer_updates.len(),
            1,
            "Should only update successful peer"
        );
        assert!(
            result.peer_updates.contains_key(&peer2_id),
            "Should contain successful peer"
        );
        assert!(
            !result.peer_updates.contains_key(&peer3_id),
            "Should not contain timed out peer"
        );

        let update = result.peer_updates.get(&peer2_id).unwrap();
        assert_eq!(update.match_index, Some(6));
        assert_eq!(update.next_index, 7);
        assert!(update.success);
    }

    /// # Case 8: All nodes timeout
    /// ## Scenario
    /// - 3 voting nodes (leader + 2 followers)
    /// - Both followers time out
    /// ## Validation Criteria
    /// 1. Returns Ok(AppendResults)
    /// 2. commit_quorum_achieved = false (only leader)
    /// 3. peer_updates is empty
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case8() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case8",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let peer3_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6), (peer3_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id, peer3_id].into_iter().collect(),
                responses: vec![
                    Err(NetworkError::Timeout {
                        node_id: peer2_id,
                        duration: Duration::from_millis(200),
                    }
                    .into()),
                    Err(NetworkError::Timeout {
                        node_id: peer3_id,
                        duration: Duration::from_millis(200),
                    }
                    .into()),
                ],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer3_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
            ]
        });
        context.membership = Arc::new(membership);

        // ----------------------
        // Execute test
        // ----------------------
        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(
            !result.commit_quorum_achieved,
            "Should not achieve quorum with 1/2 followers responding"
        );
        assert!(result.peer_updates.is_empty());
    }

    /// # Case 9: Exactly majority quorum
    /// ## Scenario
    /// - 3 voting nodes (leader + 2 followers)
    /// - 1 follower responds successfully
    /// - 1 follower conflicts
    /// ## Validation Criteria
    /// 1. Returns Ok(AppendResults)
    /// 2. commit_quorum_achieved = true (1 success + leader = majority of 3)
    /// 3. peer_updates contains both peers
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case9() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case9",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let peer3_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let last_index_for_term = 2;
        let mut raft_log = MockRaftLog::new();
        raft_log
            .expect_last_index_for_term()
            .returning(move |_| Some(last_index_for_term));
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6), (peer3_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id, peer3_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        peer2_id,
                        1,
                        Some(LogId { term: 6, index: 10 }),
                    )),
                    Ok(AppendEntriesResponse::conflict(
                        peer3_id,
                        6,
                        Some(4),
                        Some(5),
                    )),
                ],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer3_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
            ]
        });
        context.membership = Arc::new(membership);

        // ----------------------
        // Execute test
        // ----------------------
        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(result.commit_quorum_achieved);
        assert_eq!(result.peer_updates.len(), 2);
    }

    /// # Case 10: Log generation failure
    /// ## Validation Criteria
    /// 1. Returns Err
    /// 2. Error type is LogGenerationError
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case10() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case10",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let peer3_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();

        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        // Force log generation error
        raft_log.expect_last_entry_id().return_const(0_u64);
        raft_log
            .expect_insert_batch()
            .returning(|_| Err(StorageError::DbError("dberror".to_string()).into()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6), (peer3_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id, peer3_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        peer2_id,
                        1,
                        Some(LogId { term: 6, index: 10 }),
                    )),
                    Ok(AppendEntriesResponse::conflict(
                        peer3_id,
                        6,
                        Some(4),
                        Some(5),
                    )),
                ],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: peer2_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: peer3_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
            ]
        });
        context.membership = Arc::new(membership);

        // ----------------------
        // Execute test
        // ----------------------
        let e = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap_err();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(matches!(
            e,
            Error::System(SystemError::Storage(StorageError::DbError(_)))
        ));
    }

    /// # Case 11: Single node cluster
    /// ## Validation Criteria
    /// 1. commit_quorum_achieved = true (only leader)
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case11() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case11",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let peer3_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6), (peer3_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id, peer3_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        peer2_id,
                        1,
                        Some(LogId { term: 6, index: 10 }),
                    )),
                    Ok(AppendEntriesResponse::conflict(
                        peer3_id,
                        6,
                        Some(4),
                        Some(5),
                    )),
                ],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        // ----------------------
        // Execute test
        // ----------------------
        let e = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap_err();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::Replication(ReplicationError::NoPeerFound {
                leader_id: _
            }))
        ));
    }

    /// # Case 12: Term change during replication
    /// ## Validation Criteria
    /// 1. Returns HigherTerm error
    /// 2. Error contains correct term value
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case12() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case12",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let peer2_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // ----------------------
        // Initialization state
        // ----------------------
        //Leader's current term and initial log
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);

        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // New commands submitted by the client generate logs with index=6~7
        let commands = vec![
            WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100)),
            WriteCommand::insert(safe_kv_bytes(200), safe_kv_bytes(200)),
        ];

        // Leader status snapshot
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(peer2_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // ----------------------
        // Configure MockTransport to capture requests and verify parameters
        // ----------------------
        // let (tx, rx) = std::sync::mpsc::channel(); // used to pass captured requests
        let mut transport = MockTransport::new();

        // Use `with` to capture request parameters
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![peer2_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::higher_term(peer2_id, 5))],
            })
        });

        // ----------------------
        //Call the function to be tested
        // ----------------------
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: peer2_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);

        // ----------------------
        // Execute test
        // ----------------------
        let e = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap_err();

        // ----------------------
        // Verify results
        // ----------------------
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::Replication(ReplicationError::HigherTerm(5)))
        ));
    }

    /// # Case: Learner progress is tracked in AppendResults
    /// - Only Active nodes are counted for quorum
    /// - Learner node receives replication and its progress is tracked
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case12_learner_progress() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case12_learner_progress",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let voter_id = 2;
        let learner_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: one voter, one learner
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: voter_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: learner_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Joining.into(),
                },
            ]
        });
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: voter_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![voter_id, learner_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        voter_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        learner_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                ],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(voter_id, 6), (learner_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // Only voter is counted for quorum
        assert!(result.commit_quorum_achieved);
        // Voter is in peer_updates
        assert!(result.peer_updates.contains_key(&voter_id));
        // Learner is not in peer_updates, but in learner_progress
        debug!(?result, ?learner_id);
        assert!(result.peer_updates.contains_key(&learner_id));
        assert_eq!(result.learner_progress.get(&learner_id), Some(&Some(6)));
    }

    /// # Case: Syncing node receives replication but is not counted for quorum
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case14_syncing() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case14_syncing",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let voter_id = 2;
        let pending_id = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: one voter, one pending active
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: voter_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                },
                NodeMeta {
                    id: pending_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Syncing.into(),
                },
            ]
        });
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: voter_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active.into(),
            }]
        });
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![voter_id, pending_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        voter_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        pending_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                ],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(voter_id, 6), (pending_id, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // Only voter is counted for quorum
        assert!(result.commit_quorum_achieved);
        // Voter is in peer_updates
        assert!(result.peer_updates.contains_key(&voter_id));
        // Syncing is not in peer_updates, nor in learner_progress
        assert!(result.peer_updates.contains_key(&pending_id));
        assert!(result.learner_progress.contains_key(&pending_id));
    }

    /// # Case: All peers are learners, no quorum possible, but learner_progress is updated
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case15_all_learners() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case15_all_learners",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let learner1 = 2;
        let learner2 = 3;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: only learners
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: learner1,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Joining.into(),
                },
                NodeMeta {
                    id: learner2,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Joining.into(),
                },
            ]
        });
        membership.expect_voters().returning(Vec::new);
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![learner1, learner2].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        learner1,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        learner2,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                ],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(learner1, 6), (learner2, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        debug!(?result);

        // # Quorum:
        // - If there are no voters (not even the leader), quorum is not possible.
        // - If the leader is the only voter, quorum is always achieved.
        // - If all nodes are learners, quorum is not achieved.
        assert!(
            result.commit_quorum_achieved,
            "No voters, quorum should not be achieved"
        );
        // No peer_updates for learners
        assert!(!result.peer_updates.is_empty());
        // Both learners in learner_progress
        assert_eq!(result.learner_progress.get(&learner1), Some(&Some(6)));
        assert_eq!(result.learner_progress.get(&learner2), Some(&Some(6)));
    }

    /// # Case: Single node cluster (leader only, no other peers)
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case16_single_node() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case16_single_node",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Membership: no other peers
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_voters().returning(Vec::new);
        let context = {
            let mut c = context;
            c.membership = Arc::new(membership);
            c
        };

        let commands = vec![];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        // Should return error: NoPeerFound
        let e = handler
            .handle_raft_request_in_batch(commands, state_snapshot, leader_state_snapshot, &context)
            .await
            .unwrap_err();
        assert!(matches!(
            e,
            Error::Consensus(ConsensusError::Replication(ReplicationError::NoPeerFound {
                leader_id: _
            }))
        ));
    }

    /// # Case: Learner progress tracking with mixed node types
    #[tokio::test]
    async fn test_learner_progress_with_mixed_nodes() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_learner_progress_with_mixed_nodes",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let voter_id = 2;
        let joining_id = 3;
        let pending_id = 4;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: 1 voter, 1 joining, 1 syncing
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: voter_id,
                    address: "http://127.0.0.1:55001".to_string(),
                    role: Follower.into(),
                    status: NodeStatus::Active as i32,
                },
                NodeMeta {
                    id: joining_id,
                    address: "http://127.0.0.1:55002".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Joining as i32,
                },
                NodeMeta {
                    id: pending_id,
                    address: "http://127.0.0.1:55003".to_string(),
                    role: Learner.into(),
                    status: NodeStatus::Syncing as i32,
                },
            ]
        });
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: voter_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Follower.into(),
                status: NodeStatus::Active as i32,
            }]
        });
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(10_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![voter_id, joining_id, pending_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        voter_id,
                        1,
                        Some(LogId { term: 1, index: 10 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        joining_id,
                        1,
                        Some(LogId { term: 1, index: 10 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        pending_id,
                        1,
                        Some(LogId { term: 1, index: 10 }),
                    )),
                ],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 10,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(voter_id, 11), (joining_id, 11), (pending_id, 11)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // Verify results
        assert!(result.commit_quorum_achieved); // Voter success + leader = 2/2
        assert_eq!(result.peer_updates.len(), 3); // Only voter
        assert!(result.peer_updates.contains_key(&voter_id));
        assert_eq!(result.learner_progress.get(&joining_id), Some(&Some(10)));
        assert_eq!(result.learner_progress.get(&pending_id), Some(&Some(10)));
    }

    /// # Case: Quorum calculation with mixed node types
    #[tokio::test]
    async fn test_quorum_calculation_mixed_nodes() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_quorum_calculation_mixed_nodes",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: 2 voters, 1 joining, 1 syncing
        let pending_id = 2;
        let voter_id = 3;
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![
                NodeMeta {
                    id: voter_id,
                    role: Follower.into(),
                    status: NodeStatus::Active.into(),
                    ..Default::default()
                },
                NodeMeta {
                    id: pending_id,
                    status: NodeStatus::Joining as i32,
                    role: Learner.into(),
                    ..Default::default()
                },
            ]
        });
        membership.expect_voters().returning(move || {
            vec![NodeMeta {
                id: voter_id,
                role: Follower.into(),
                status: NodeStatus::Active.into(),
                ..Default::default()
            }]
        });
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().return_const(5_u64);
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![voter_id, pending_id].into_iter().collect(),
                responses: vec![
                    Ok(AppendEntriesResponse::success(
                        voter_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                    Ok(AppendEntriesResponse::success(
                        pending_id,
                        1,
                        Some(LogId { term: 1, index: 6 }),
                    )),
                ],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 5,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(2, 6), (3, 6)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // 2/3 required for majority (leader + 1 voter)
        assert!(result.commit_quorum_achieved);
    }

    /// # Case: Learner catch-up threshold logic
    #[tokio::test]
    async fn test_learner_catchup_threshold() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        let mut node_config = node_config("/tmp/test_learner_catchup_threshold");
        node_config.raft.replication.rpc_append_entries_in_batch_threshold = 1;
        // Reduce timeout for test
        node_config.retry.auto_discovery.timeout_ms = 10;
        // Set learner catch-up threshold
        node_config.raft.learner_catchup_threshold = 5;

        let mut context =
            MockBuilder::new(graceful_rx).with_node_config(node_config).build_context();
        let my_id = 1;
        let learner_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        let leader_commit_index = 100;

        // Membership: one learner
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: learner_id,
                status: NodeStatus::Joining as i32,
                ..Default::default()
            }]
        });
        membership.expect_voters().returning(Vec::new);
        context.membership = Arc::new(membership);

        // Mock raft log and transport
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_last_entry_id().return_const(leader_commit_index);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));

        // Simulate learner's match_index is just below the threshold
        let learner_match_index =
            leader_commit_index - context.node_config.raft.learner_catchup_threshold + 1;

        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![learner_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::success(
                    learner_id,
                    1,
                    Some(LogId {
                        term: 1,
                        index: learner_match_index,
                    }),
                ))],
            })
        });

        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: leader_commit_index,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(learner_id, learner_match_index + 1)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // The learner's progress should be updated
        assert_eq!(
            result.learner_progress.get(&learner_id),
            Some(&Some(learner_match_index))
        );
    }

    /// # Case: 0 voters with conflict response
    #[tokio::test]
    async fn test_handle_raft_request_in_batch_case19_zero_voters_conflict() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let mut context = mock_raft_context(
            "/tmp/test_handle_raft_request_in_batch_case19_zero_voters_conflict",
            graceful_rx,
            None,
        );
        let my_id = 1;
        let learner_id = 2;
        let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

        // Setup membership: only learners (0 voters)
        let mut membership = MockMembership::new();
        membership.expect_replication_peers().returning(move || {
            vec![NodeMeta {
                id: learner_id,
                address: "http://127.0.0.1:55001".to_string(),
                role: Learner.into(),
                status: NodeStatus::Syncing.into(),
            }]
        });
        membership.expect_voters().returning(Vec::new);
        context.membership = Arc::new(membership);

        // Mock responses - conflict
        let mut transport = MockTransport::new();
        transport.expect_send_append_requests().return_once(move |_, _, _, _| {
            Ok(AppendResult {
                peer_ids: vec![learner_id].into_iter().collect(),
                responses: vec![Ok(AppendEntriesResponse::conflict(
                    learner_id,
                    1,
                    Some(4),
                    Some(5),
                ))],
            })
        });

        // Mock raft log and transport
        let last_index_for_term = 2;
        let mut raft_log = MockRaftLog::new();
        raft_log
            .expect_last_index_for_term()
            .returning(move |_| Some(last_index_for_term));
        raft_log.expect_pre_allocate_id_range().returning(|_| 1..=2);
        raft_log.expect_last_entry_id().return_const(1_u64);
        raft_log.expect_get_entries_range().returning(|_| Ok(vec![]));
        raft_log.expect_entry_term().returning(|_| None);
        raft_log.expect_insert_batch().returning(|_| Ok(()));
        context.storage.raft_log = Arc::new(raft_log);
        context.transport = Arc::new(transport);

        let commands = vec![WriteCommand::insert(safe_kv_bytes(100), safe_kv_bytes(100))];
        let state_snapshot = StateSnapshot {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
            role: Leader.into(),
        };
        let leader_state_snapshot = LeaderStateSnapshot {
            next_index: HashMap::from([(learner_id, 1 + 1)]),
            match_index: HashMap::new(),
            noop_log_id: None,
        };

        let result = handler
            .handle_raft_request_in_batch(
                client_command_to_entry_payloads(commands),
                state_snapshot,
                leader_state_snapshot,
                &context,
            )
            .await
            .unwrap();

        // Verify results
        assert!(
            result.commit_quorum_achieved,
            "Single node, should still achieve quorum even with 0 voters"
        );
        assert_eq!(result.peer_updates.len(), 1, "Should update learner");
        assert_eq!(
            result.peer_updates.get(&learner_id).unwrap(),
            &PeerUpdate {
                match_index: None, // 5-1
                next_index: last_index_for_term + 1,
                success: false
            }
        );
    }
}
