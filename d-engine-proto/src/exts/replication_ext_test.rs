use crate::common::LogId;
use crate::server::replication::AppendEntriesResponse;

#[test]
fn test_append_entries_response_success() {
    let log_id = LogId { term: 5, index: 10 };
    let response = AppendEntriesResponse::success(1, 5, Some(log_id));

    assert_eq!(response.node_id, 1);
    assert_eq!(response.term, 5);
    assert!(response.is_success());
    assert!(!response.is_conflict());
    assert!(!response.is_higher_term());

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::Success(success)) => {
            assert_eq!(success.last_match, Some(log_id));
        }
        _ => panic!("Expected success result"),
    }
}

#[test]
fn test_append_entries_response_success_without_log_id() {
    let response = AppendEntriesResponse::success(2, 3, None);

    assert_eq!(response.node_id, 2);
    assert_eq!(response.term, 3);
    assert!(response.is_success());
    assert!(!response.is_conflict());
    assert!(!response.is_higher_term());

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::Success(success)) => {
            assert_eq!(success.last_match, None);
        }
        _ => panic!("Expected success result"),
    }
}

#[test]
fn test_append_entries_response_conflict() {
    let response = AppendEntriesResponse::conflict(3, 6, Some(4), Some(8));

    assert_eq!(response.node_id, 3);
    assert_eq!(response.term, 6);
    assert!(!response.is_success());
    assert!(response.is_conflict());
    assert!(!response.is_higher_term());

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::Conflict(conflict)) => {
            assert_eq!(conflict.conflict_term, Some(4));
            assert_eq!(conflict.conflict_index, Some(8));
        }
        _ => panic!("Expected conflict result"),
    }
}

#[test]
fn test_append_entries_response_conflict_without_details() {
    let response = AppendEntriesResponse::conflict(4, 7, None, None);

    assert_eq!(response.node_id, 4);
    assert_eq!(response.term, 7);
    assert!(!response.is_success());
    assert!(response.is_conflict());
    assert!(!response.is_higher_term());

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::Conflict(conflict)) => {
            assert_eq!(conflict.conflict_term, None);
            assert_eq!(conflict.conflict_index, None);
        }
        _ => panic!("Expected conflict result"),
    }
}

#[test]
fn test_append_entries_response_higher_term() {
    let response = AppendEntriesResponse::higher_term(5, 10);

    assert_eq!(response.node_id, 5);
    assert_eq!(response.term, 10);
    assert!(!response.is_success());
    assert!(!response.is_conflict());
    assert!(response.is_higher_term());

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::HigherTerm(term)) => {
            assert_eq!(*term, 10);
        }
        _ => panic!("Expected higher term result"),
    }
}

#[test]
fn test_append_entries_response_mutually_exclusive() {
    // Test success response
    let success = AppendEntriesResponse::success(1, 1, None);
    assert!(success.is_success());
    assert!(!success.is_conflict());
    assert!(!success.is_higher_term());

    // Test conflict response
    let conflict = AppendEntriesResponse::conflict(2, 2, None, None);
    assert!(!conflict.is_success());
    assert!(conflict.is_conflict());
    assert!(!conflict.is_higher_term());

    // Test higher term response
    let higher = AppendEntriesResponse::higher_term(3, 3);
    assert!(!higher.is_success());
    assert!(!higher.is_conflict());
    assert!(higher.is_higher_term());
}

#[test]
fn test_append_entries_response_large_indices() {
    let log_id = LogId {
        term: u64::MAX,
        index: u64::MAX,
    };
    let response = AppendEntriesResponse::success(u32::MAX, u64::MAX, Some(log_id));

    assert_eq!(response.node_id, u32::MAX);
    assert_eq!(response.term, u64::MAX);

    match &response.result {
        Some(crate::server::replication::append_entries_response::Result::Success(success)) => {
            assert_eq!(success.last_match.unwrap().term, u64::MAX);
            assert_eq!(success.last_match.unwrap().index, u64::MAX);
        }
        _ => panic!("Expected success result"),
    }
}

#[test]
fn test_append_entries_response_zero_values() {
    let response = AppendEntriesResponse::success(0, 0, None);

    assert_eq!(response.node_id, 0);
    assert_eq!(response.term, 0);
    assert!(response.is_success());
}
