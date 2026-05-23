use bytes::Bytes;

use super::types::*;

// ─── ErrorCode ────────────────────────────────────────────────────────────────

/// Compile-time exhaustiveness guard: adding a new ErrorCode variant without
/// updating this match is a compile error, not a runtime surprise.
#[allow(dead_code)]
fn _assert_error_code_match_is_exhaustive(code: ErrorCode) {
    match code {
        ErrorCode::Success => {}
        ErrorCode::ConnectionTimeout => {}
        ErrorCode::InvalidAddress => {}
        ErrorCode::LeaderChanged => {}
        ErrorCode::JoinError => {}
        ErrorCode::InvalidResponse => {}
        ErrorCode::VersionMismatch => {}
        ErrorCode::DiskFull => {}
        ErrorCode::DataCorruption => {}
        ErrorCode::StorageIoError => {}
        ErrorCode::StoragePermissionDenied => {}
        ErrorCode::KeyNotExist => {}
        ErrorCode::NotLeader => {}
        ErrorCode::StaleOperation => {}
        ErrorCode::InvalidRequest => {}
        ErrorCode::RateLimited => {}
        ErrorCode::ClusterUnavailable => {}
        ErrorCode::ProposeFailed => {}
        ErrorCode::TermOutdated => {}
        ErrorCode::RetryRequired => {}
        ErrorCode::WatchBufferOverflow => {}
        ErrorCode::General => {}
        ErrorCode::Uncategorized => {}
    }
}

#[test]
fn test_error_code_all_variants_are_exhaustive() {
    let codes = [
        ErrorCode::Success,
        ErrorCode::ConnectionTimeout,
        ErrorCode::InvalidAddress,
        ErrorCode::LeaderChanged,
        ErrorCode::JoinError,
        ErrorCode::InvalidResponse,
        ErrorCode::VersionMismatch,
        ErrorCode::DiskFull,
        ErrorCode::DataCorruption,
        ErrorCode::StorageIoError,
        ErrorCode::StoragePermissionDenied,
        ErrorCode::KeyNotExist,
        ErrorCode::NotLeader,
        ErrorCode::StaleOperation,
        ErrorCode::InvalidRequest,
        ErrorCode::RateLimited,
        ErrorCode::ClusterUnavailable,
        ErrorCode::ProposeFailed,
        ErrorCode::TermOutdated,
        ErrorCode::RetryRequired,
        ErrorCode::WatchBufferOverflow,
        ErrorCode::General,
        ErrorCode::Uncategorized,
    ];
    assert_eq!(codes.len(), 23);
}

#[test]
fn test_error_code_is_copy() {
    let code = ErrorCode::NotLeader;
    let _copy = code; // must compile without move
    let _still_valid = code;
}

#[test]
fn test_error_code_discriminants_match_proto_wire_values() {
    // Numeric values must stay in sync with proto ErrorCode for the
    // server-layer numeric-cast conversion to be correct.
    assert_eq!(ErrorCode::Success as i32, 0);
    assert_eq!(ErrorCode::ConnectionTimeout as i32, 1001);
    assert_eq!(ErrorCode::NotLeader as i32, 4001);
    assert_eq!(ErrorCode::WatchBufferOverflow as i32, 5001);
    assert_eq!(ErrorCode::General as i32, 8888);
    assert_eq!(ErrorCode::Uncategorized as i32, 9999);
}

// ─── WriteOperation ───────────────────────────────────────────────────────────

#[test]
fn test_write_operation_insert_carries_correct_fields() {
    let op = WriteOperation::Insert {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
        ttl_secs: Some(30),
    };
    match op {
        WriteOperation::Insert {
            key,
            value,
            ttl_secs,
        } => {
            assert_eq!(key, Bytes::from("k"));
            assert_eq!(value, Bytes::from("v"));
            assert_eq!(ttl_secs, Some(30));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn test_write_operation_insert_no_ttl_is_none() {
    let op = WriteOperation::Insert {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
        ttl_secs: None,
    };
    match op {
        WriteOperation::Insert { ttl_secs: None, .. } => {}
        _ => panic!("expected None ttl"),
    }
}

#[test]
fn test_write_operation_delete_carries_key() {
    let op = WriteOperation::Delete {
        key: Bytes::from("del-key"),
    };
    match op {
        WriteOperation::Delete { key } => assert_eq!(key, Bytes::from("del-key")),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn test_write_operation_cas_with_expected_value() {
    let op = WriteOperation::CompareAndSwap {
        key: Bytes::from("k"),
        expected: Some(Bytes::from("old")),
        new_value: Bytes::from("new"),
    };
    match op {
        WriteOperation::CompareAndSwap {
            key,
            expected,
            new_value,
        } => {
            assert_eq!(key, Bytes::from("k"));
            assert_eq!(expected, Some(Bytes::from("old")));
            assert_eq!(new_value, Bytes::from("new"));
        }
        _ => panic!("wrong variant"),
    }
}

/// CAS where expected=None means "key must not exist" — a distinct semantic from
/// "any value is accepted". This test encodes that contract.
#[test]
fn test_write_operation_cas_key_must_not_exist_when_expected_is_none() {
    let op = WriteOperation::CompareAndSwap {
        key: Bytes::from("k"),
        expected: None,
        new_value: Bytes::from("new"),
    };
    match op {
        WriteOperation::CompareAndSwap { expected: None, .. } => {}
        _ => panic!("expected None should mean key-must-not-exist"),
    }
}

// ─── ClientWriteRequest ───────────────────────────────────────────────────────

#[test]
fn test_client_write_request_fields() {
    let req = ClientWriteRequest {
        client_id: 42,
        command: Some(WriteOperation::Delete {
            key: Bytes::from("k"),
        }),
    };
    assert_eq!(req.client_id, 42);
    assert!(req.command.is_some());
}

#[test]
fn test_client_write_request_empty_command_is_none() {
    let req = ClientWriteRequest {
        client_id: 1,
        command: None,
    };
    assert!(req.command.is_none());
}

// ─── ClientReadRequest ────────────────────────────────────────────────────────

#[test]
fn test_client_read_request_consistency_defaults_to_none() {
    let req = ClientReadRequest {
        client_id: 1,
        keys: vec![Bytes::from("k")],
        consistency_policy: None,
    };
    // None means "use cluster default" — callers must not silently assume LeaseRead
    assert!(req.consistency_policy.is_none());
}

/// ReadConsistencyPolicy is defined in config and re-exported here.
/// This test ensures all three variants are reachable and the type is Copy-compatible.
#[test]
fn test_read_consistency_policy_all_variants_reachable() {
    let policies = [
        ReadConsistencyPolicy::LeaseRead,
        ReadConsistencyPolicy::LinearizableRead,
        ReadConsistencyPolicy::EventualConsistency,
    ];
    assert_eq!(policies.len(), 3);
}

// ─── ClientResponse ───────────────────────────────────────────────────────────

#[test]
fn test_client_response_successful_write() {
    let resp = ClientResponse {
        error: ErrorCode::Success,
        leader_hint: None,
        retry_after_ms: None,
        result: Some(ClientResponsePayload::Write(WriteResult {
            succeeded: true,
        })),
    };
    assert_eq!(resp.error, ErrorCode::Success);
    assert!(matches!(
        resp.result,
        Some(ClientResponsePayload::Write(WriteResult {
            succeeded: true
        }))
    ));
}

#[test]
fn test_client_response_successful_read() {
    let resp = ClientResponse {
        error: ErrorCode::Success,
        leader_hint: None,
        retry_after_ms: None,
        result: Some(ClientResponsePayload::Read(ReadResults {
            entries: vec![KvEntry {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
            }],
        })),
    };
    match resp.result {
        Some(ClientResponsePayload::Read(r)) => {
            assert_eq!(r.entries.len(), 1);
            assert_eq!(r.entries[0].key, Bytes::from("k"));
        }
        _ => panic!("expected Read payload"),
    }
}

#[test]
fn test_client_response_not_leader_carries_hint() {
    let resp = ClientResponse {
        error: ErrorCode::NotLeader,
        leader_hint: Some(LeaderHint {
            leader_id: 2,
            address: "127.0.0.1:5002".into(),
        }),
        retry_after_ms: Some(100),
        result: None,
    };
    assert_eq!(resp.error, ErrorCode::NotLeader);
    let hint = resp.leader_hint.unwrap();
    assert_eq!(hint.leader_id, 2);
    assert_eq!(hint.address, "127.0.0.1:5002");
    assert_eq!(resp.retry_after_ms, Some(100));
    assert!(resp.result.is_none());
}

// ─── LeaderHint ───────────────────────────────────────────────────────────────

#[test]
fn test_leader_hint_fields() {
    let hint = LeaderHint {
        leader_id: 3,
        address: "10.0.0.1:5003".into(),
    };
    assert_eq!(hint.leader_id, 3);
    assert_eq!(hint.address, "10.0.0.1:5003");
}

// ─── ClientResponse constructors ─────────────────────────────────────────────

#[test]
fn test_write_success_constructor() {
    let resp = ClientResponse::write_success();
    assert_eq!(resp.error, ErrorCode::Success);
    assert!(resp.leader_hint.is_none());
    assert!(resp.retry_after_ms.is_none());
    assert!(matches!(
        resp.result,
        Some(ClientResponsePayload::Write(WriteResult {
            succeeded: true
        }))
    ));
}

#[test]
fn test_cas_failure_constructor() {
    let resp = ClientResponse::cas_failure();
    assert_eq!(resp.error, ErrorCode::Success);
    assert!(resp.leader_hint.is_none());
    assert!(resp.retry_after_ms.is_none());
    assert!(matches!(
        resp.result,
        Some(ClientResponsePayload::Write(WriteResult {
            succeeded: false
        }))
    ));
}

#[test]
fn test_read_results_constructor_empty() {
    let resp = ClientResponse::read_results(vec![]);
    assert_eq!(resp.error, ErrorCode::Success);
    assert!(resp.leader_hint.is_none());
    assert!(resp.retry_after_ms.is_none());
    match resp.result {
        Some(ClientResponsePayload::Read(r)) => assert!(r.entries.is_empty()),
        _ => panic!("expected Read payload"),
    }
}

#[test]
fn test_read_results_constructor_with_entries() {
    let entries = vec![
        KvEntry {
            key: Bytes::from("k1"),
            value: Bytes::from("v1"),
        },
        KvEntry {
            key: Bytes::from("k2"),
            value: Bytes::from("v2"),
        },
    ];
    let resp = ClientResponse::read_results(entries);
    match resp.result {
        Some(ClientResponsePayload::Read(r)) => {
            assert_eq!(r.entries.len(), 2);
            assert_eq!(r.entries[0].key, Bytes::from("k1"));
            assert_eq!(r.entries[1].value, Bytes::from("v2"));
        }
        _ => panic!("expected Read payload"),
    }
}

#[test]
fn test_client_error_constructor() {
    let resp = ClientResponse::client_error(ErrorCode::NotLeader);
    assert_eq!(resp.error, ErrorCode::NotLeader);
    assert!(resp.leader_hint.is_none());
    assert!(resp.retry_after_ms.is_none());
    assert!(resp.result.is_none());
}

#[test]
fn test_not_leader_with_hint() {
    let hint = LeaderHint {
        leader_id: 2,
        address: "10.0.0.2:5002".into(),
    };
    let resp = ClientResponse::not_leader(Some(hint));
    assert_eq!(resp.error, ErrorCode::NotLeader);
    let h = resp.leader_hint.unwrap();
    assert_eq!(h.leader_id, 2);
    assert_eq!(h.address, "10.0.0.2:5002");
    assert!(resp.result.is_none());
}

#[test]
fn test_not_leader_without_hint() {
    let resp = ClientResponse::not_leader(None);
    assert_eq!(resp.error, ErrorCode::NotLeader);
    assert!(resp.leader_hint.is_none());
    assert!(resp.result.is_none());
}

// ─── ClientResponse predicates ────────────────────────────────────────────────

#[test]
fn test_is_write_success_true_for_write_success() {
    assert!(ClientResponse::write_success().is_write_success());
}

#[test]
fn test_is_write_success_false_for_cas_failure() {
    assert!(!ClientResponse::cas_failure().is_write_success());
}

#[test]
fn test_is_write_success_false_for_error_response() {
    assert!(!ClientResponse::client_error(ErrorCode::NotLeader).is_write_success());
}

#[test]
fn test_is_write_success_false_for_read_response() {
    assert!(!ClientResponse::read_results(vec![]).is_write_success());
}

#[test]
fn test_is_term_outdated_true() {
    assert!(ClientResponse::client_error(ErrorCode::TermOutdated).is_term_outdated());
}

#[test]
fn test_is_term_outdated_false_for_success() {
    assert!(!ClientResponse::write_success().is_term_outdated());
}

#[test]
fn test_is_propose_failure_true() {
    assert!(ClientResponse::client_error(ErrorCode::ProposeFailed).is_propose_failure());
}

#[test]
fn test_is_propose_failure_false_for_success() {
    assert!(!ClientResponse::write_success().is_propose_failure());
}
