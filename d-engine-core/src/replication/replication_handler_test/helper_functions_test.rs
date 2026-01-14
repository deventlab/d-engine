//! Helper function tests for ReplicationHandler
//!
//! Tests verify internal helper functions used by ReplicationHandler:
//! - generate_new_entries: Convert commands to log entries
//! - build_append_request: Build AppendEntriesRequest for peers

use std::collections::HashMap;
use std::sync::Arc;

use crate::MockRaftLog;
use crate::MockTypeConfig;
use crate::ReplicationCore;
use crate::ReplicationData;
use crate::ReplicationHandler;
use crate::client_command_to_entry_payloads;
use crate::test_utils::mock_insert_log_entries;
use crate::test_utils::setup_mock_replication_test_context;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::Entry;
use dashmap::DashMap;

/// Generate new entries with empty commands should return empty vector.
///
/// # Scenario
/// - Input: Empty command list
/// - Expected: No entries generated, raft log unchanged
///
/// # Test Purpose
/// Validates that empty command batches are handled gracefully
/// without generating unnecessary log entries.
#[tokio::test]
async fn test_generate_new_entries_with_empty_commands() {
    // Arrange: Setup test context
    let context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Mock raft log to return last_entry_id
    let raft_log = context.raft_log.clone();

    // Arrange: Empty commands
    let commands = vec![];
    let current_term = 1;

    // Act: Generate new entries
    let result = handler.generate_new_entries(commands, current_term, &raft_log).await;

    // Assert: No entries generated
    assert!(result.is_ok(), "should succeed with empty commands");
    assert_eq!(
        result.unwrap(),
        vec![],
        "empty commands should generate no entries"
    );
}

/// Generate new entries with one command should create one log entry.
///
/// # Scenario
/// - Input: One delete command
/// - Expected: One log entry generated
///
/// # Test Purpose
/// Validates that commands are correctly converted to log entries
/// with proper index and term assignment.
#[tokio::test]
async fn test_generate_new_entries_with_single_command() {
    // Arrange: Setup test context
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Mock raft log
    let last_id_before = 5;
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    raft_log_mut.expect_last_entry_id().returning(move || last_id_before);
    raft_log_mut
        .expect_pre_allocate_id_range()
        .returning(move |count| (last_id_before + 1)..=(last_id_before + count));
    raft_log_mut.expect_insert_batch().returning(|_| Ok(()));

    // Arrange: One command
    let commands = client_command_to_entry_payloads(vec![WriteCommand::delete(
        crate::convert::safe_kv_bytes(1),
    )]);
    let current_term = 1;

    // Act: Generate new entries
    let result = handler.generate_new_entries(commands, current_term, &context.raft_log).await;

    // Assert: One entry generated
    assert!(result.is_ok(), "should succeed with one command");
    let entries = result.unwrap();
    assert_eq!(entries.len(), 1, "one command should generate one entry");
    assert_eq!(
        entries[0].index,
        last_id_before + 1,
        "entry index should be last_id + 1"
    );
    assert_eq!(
        entries[0].term, current_term,
        "entry term should match current_term"
    );
}

/// Build append request should create correct request for each peer.
///
/// # Scenario
/// - Peer2: next_index=3, needs 1 entry (index 3)
/// - Peer3: next_index=1, needs 3 entries (index 1-3)
/// - Expected: peer2 request has 1 entry, peer3 request has 3 entries
///
/// # Test Purpose
/// Validates that AppendEntriesRequest is correctly built based on
/// peer's next_index and available entries.
#[tokio::test]
async fn test_build_append_request_for_different_peers() {
    // Arrange: Setup test context
    let mut context = setup_mock_replication_test_context(1);
    let my_id = 1;
    let peer2_id = 2;
    let peer2_next_index = 3;
    let peer3_id = 3;
    let peer3_next_index = 1;
    let handler = ReplicationHandler::<MockTypeConfig>::new(my_id);

    // Arrange: Prepare entries for each peer
    let entries_per_peer: DashMap<u32, Vec<Entry>> = DashMap::new();
    entries_per_peer.insert(peer2_id, mock_insert_log_entries(vec![300], 1, 3));
    entries_per_peer.insert(peer3_id, mock_insert_log_entries(vec![100, 200, 300], 1, 1));

    // Arrange: Mock raft log to return prev_log_term
    let raft_log_mut = Arc::get_mut(&mut context.raft_log).unwrap();
    raft_log_mut
        .expect_entry_term()
        .returning(|index| if index == 2 { Some(1) } else { Some(0) });

    // Arrange: Replication data
    let data = ReplicationData {
        leader_last_index_before: 3,
        current_term: 1,
        commit_index: 1,
        peer_next_indices: HashMap::from([
            (peer2_id, peer2_next_index),
            (peer3_id, peer3_next_index),
        ]),
    };

    // Act: Build request for peer2
    let (_id, peer2_request) =
        handler.build_append_request(&context.raft_log, peer2_id, &entries_per_peer, &data);

    // Assert: Peer2 request should have 1 entry
    assert_eq!(
        peer2_request.entries.len(),
        1,
        "peer2 with next_index=3 should receive 1 entry"
    );
    assert_eq!(
        peer2_request.entries[0].index, 3,
        "peer2 should receive entry at index 3"
    );
    assert_eq!(
        peer2_request.prev_log_index, 2,
        "peer2's prev_log_index should be 2 (next_index - 1)"
    );

    // Act: Build request for peer3
    let (_id, peer3_request) =
        handler.build_append_request(&context.raft_log, peer3_id, &entries_per_peer, &data);

    // Assert: Peer3 request should have 3 entries
    assert_eq!(
        peer3_request.entries.len(),
        3,
        "peer3 with next_index=1 should receive 3 entries"
    );
    assert_eq!(
        peer3_request.prev_log_index, 0,
        "peer3's prev_log_index should be 0 (next_index - 1)"
    );
}

// ============================================================================
// Request Validation Tests
// ============================================================================

/// Valid append entries request should return success response.
///
/// # Test Purpose
/// Validates that legitimate AppendEntriesRequest is accepted when
/// prev_log matches follower's log.
#[test]
fn test_append_request_validation_with_valid_request() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::AppendEntriesRequest;
    use d_engine_proto::server::replication::SuccessResult;
    use d_engine_proto::server::replication::append_entries_response;

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

/// Stale term request should be rejected.
///
/// # Test Purpose
/// Validates that requests with term lower than current term are rejected.
#[test]
fn test_append_request_validation_rejects_stale_term() {
    use d_engine_proto::server::replication::AppendEntriesRequest;

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

/// Mismatched prev_term (follower log longer) should return conflict.
///
/// # Test Purpose
/// Validates conflict detection when follower's log length > prev_log_index
/// but term doesn't match.
#[test]
fn test_append_request_validation_conflict_when_log_longer() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::AppendEntriesRequest;
    use d_engine_proto::server::replication::ConflictResult;
    use d_engine_proto::server::replication::append_entries_response;

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

/// Mismatched prev_term (follower log shorter) should return conflict.
///
/// # Test Purpose
/// Validates conflict detection when follower's log length < prev_log_index.
#[test]
fn test_append_request_validation_conflict_when_log_shorter() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::AppendEntriesRequest;
    use d_engine_proto::server::replication::ConflictResult;
    use d_engine_proto::server::replication::append_entries_response;

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

/// Virtual log (prev_log_index=0) should be accepted.
///
/// # Test Purpose
/// Validates that requests with prev_log_index=0 (virtual log entry)
/// are always accepted.
#[test]
fn test_append_request_validation_accepts_virtual_log() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::AppendEntriesRequest;

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

/// Virtual log with non-empty entries should be accepted.
///
/// # Test Purpose
/// Validates that virtual log requests with new entries are accepted,
/// conflicts will be handled later during entry application.
#[test]
fn test_append_request_validation_virtual_log_with_entries() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::AppendEntriesRequest;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let mut raft_log = MockRaftLog::new();
    let my_term = 2;
    raft_log.expect_last_log_id().returning(|| Some(LogId { term: 1, index: 5 }));

    let request = AppendEntriesRequest {
        term: my_term,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![d_engine_proto::common::Entry {
            term: 2,
            index: 1,
            payload: Some(d_engine_proto::common::EntryPayload::command(
                crate::test_utils::generate_insert_commands(vec![1]),
            )),
        }],
        leader_commit_index: 5,
        leader_id: 2,
    };

    let response =
        handler.check_append_entries_request_is_legal(my_term, &request, &Arc::new(raft_log));
    assert!(response.is_success());
}

// ============================================================================
// Conflict Response Handling Tests
// ============================================================================

/// Conflict with term and index should use last_index_for_term.
///
/// # Test Purpose
/// Validates optimized backtracking using last_index_for_term when
/// conflict includes both term and index information.
#[test]
fn test_conflict_response_with_term_and_index() {
    use d_engine_proto::server::replication::ConflictResult;

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

    assert_eq!(update.next_index, last_index_for_term + 1);
    assert_eq!(update.match_index, None);
}

/// Conflict with index only should use conflict_index.
///
/// # Test Purpose
/// Validates fallback to conflict_index when no term information available.
#[test]
fn test_conflict_response_with_index_only() {
    use d_engine_proto::server::replication::ConflictResult;

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

/// Conflict with no info should decrement next_index.
///
/// # Test Purpose
/// Validates conservative fallback of decrementing next_index by 1
/// when no conflict information is available.
#[test]
fn test_conflict_response_with_no_info() {
    use d_engine_proto::server::replication::ConflictResult;

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

/// Conflict with index zero should be clamped to 1.
///
/// # Test Purpose
/// Validates defensive handling of illegal conflict_index=0,
/// ensuring next_index is always >= 1.
#[test]
fn test_conflict_response_clamps_zero_index() {
    use d_engine_proto::server::replication::ConflictResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let conflict_result = ConflictResult {
        conflict_term: None,
        conflict_index: Some(0),
    };
    let raft_log = Arc::new(MockRaftLog::new());

    let update = handler.handle_conflict_response(2, conflict_result, &raft_log, 0).unwrap();

    assert_eq!(update.next_index, 1);
    assert_eq!(update.match_index, None);
}

// ============================================================================
// Success Response Handling Tests
// ============================================================================

/// Higher responder term should trigger step down.
///
/// # Test Purpose
/// Validates that receiving response with higher term triggers
/// HigherTerm error for leader step-down.
#[test]
fn test_success_response_triggers_step_down_on_higher_term() {
    use crate::ConsensusError;
    use crate::Error;
    use crate::ReplicationError;
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::SuccessResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 5;
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

/// Valid success response should update indices.
///
/// # Test Purpose
/// Validates that successful replication updates match_index
/// and advances next_index.
#[test]
fn test_success_response_updates_peer_indices() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::SuccessResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.match_index, Some(10));
    assert_eq!(update.next_index, 11);
}

/// Lower responder term should still update indices.
///
/// # Test Purpose
/// Validates that responses with lower (but not stale) term
/// are still processed normally.
#[test]
fn test_success_response_accepts_lower_term() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::SuccessResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 10 }),
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 5).unwrap();
    assert_eq!(update.match_index, Some(10));
}

/// Empty follower log (last_match=None) should sync from index 0.
///
/// # Test Purpose
/// Validates handling of empty follower logs by setting
/// match_index=0 and next_index=1.
#[test]
fn test_success_response_handles_empty_follower_log() {
    use d_engine_proto::server::replication::SuccessResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult { last_match: None };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.match_index, Some(0));
    assert_eq!(update.next_index, 1);
}

/// Zero index match should advance next_index to 1.
///
/// # Test Purpose
/// Validates that match at virtual log (index=0) correctly
/// sets next_index=1 for first real entry.
#[test]
fn test_success_response_handles_zero_index_match() {
    use d_engine_proto::common::LogId;
    use d_engine_proto::server::replication::SuccessResult;

    let handler = ReplicationHandler::<MockTypeConfig>::new(1);
    let responder_term = 3;
    let success_result = SuccessResult {
        last_match: Some(LogId { term: 3, index: 0 }),
    };
    let update = handler.handle_success_response(2, responder_term, success_result, 3).unwrap();
    assert_eq!(update.next_index, 1);
}

// ============================================================================
// Command Conversion Tests
// ============================================================================

/// Multiple commands should convert to multiple payloads.
///
/// # Test Purpose
/// Validates that WriteCommands are correctly serialized into
/// EntryPayload format for log entries.
#[test]
fn test_command_conversion_with_multiple_commands() {
    use bytes::Bytes;
    use d_engine_proto::client::WriteCommand;
    use d_engine_proto::client::write_command::Insert;
    use d_engine_proto::client::write_command::Operation;
    use d_engine_proto::common::entry_payload::Payload;
    use prost::Message;

    let commands = vec![
        WriteCommand {
            operation: Some(Operation::Insert(Insert {
                ttl_secs: 0,
                key: Bytes::from(b"key1".to_vec()),
                value: Bytes::from(b"value1".to_vec()),
            })),
        },
        WriteCommand {
            operation: Some(Operation::Insert(Insert {
                ttl_secs: 0,
                key: Bytes::from(b"key2".to_vec()),
                value: Bytes::from(b"value2".to_vec()),
            })),
        },
    ];

    let payloads = client_command_to_entry_payloads(commands);

    assert_eq!(payloads.len(), 2);

    if let Some(Payload::Command(bytes)) = &payloads[0].payload {
        let decoded = WriteCommand::decode(bytes.as_ref()).unwrap();
        assert!(matches!(
            decoded.operation,
            Some(Operation::Insert(Insert { key, value, ttl_secs: _ }))
            if key == b"key1".as_ref() && value == b"value1".as_ref()
        ));
    } else {
        panic!("First payload should be Command variant");
    }

    if let Some(Payload::Command(bytes)) = &payloads[1].payload {
        let decoded = WriteCommand::decode(bytes.as_ref()).unwrap();
        assert!(matches!(
            decoded.operation,
            Some(Operation::Insert(Insert { key, value, ttl_secs: _ }))
            if key == b"key2".as_ref() && value == b"value2".as_ref()
        ));
    } else {
        panic!("Second payload should be Command variant");
    }
}

/// Empty command list should return empty payloads.
///
/// # Test Purpose
/// Validates handling of empty command batches.
#[test]
fn test_command_conversion_with_empty_input() {
    let payloads = client_command_to_entry_payloads(vec![]);
    assert!(payloads.is_empty());
}
