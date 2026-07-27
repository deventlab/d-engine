use bytes::Bytes;
use d_engine_core::client::{
    ClientReadRequest, ClientResponse, ClientResponsePayload, ClientWriteRequest, ErrorCode,
    KvEntry, LeaderHint, ReadResults, WriteResult,
};
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_core::types::ScanResults;
use d_engine_proto::client as proto_client;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::{Insert, Operation};
use d_engine_proto::error::ErrorCode as ProtoErrorCode;
use prost::Message;

use crate::proto_convert::{
    core_error_to_proto, proto_error_to_core, to_core_read_req, to_core_response,
    to_core_write_req, to_proto_response,
};
// ─── to_core_write_req ────────────────────────────────────────────────────────

#[test]
fn test_to_core_write_req_insert_roundtrip() {
    let proto_req = proto_client::ClientWriteRequest {
        client_id: 7,
        command: Some(proto_client::WriteCommand {
            operation: Some(Operation::Insert(Insert {
                key: Bytes::from("key"),
                value: Bytes::from("val"),
                ttl_secs: 0,
            })),
        }),
    };
    let core_req = to_core_write_req(proto_req);
    assert_eq!(core_req.client_id, 7);
    let bytes = core_req.command.expect("command must be Some");
    let decoded = WriteCommand::decode(bytes).expect("must decode to WriteCommand");
    assert!(
        matches!(
            decoded.operation,
            Some(Operation::Insert(Insert { ttl_secs: 0, .. }))
        ),
        "decoded command must be an Insert operation"
    );
}

#[test]
fn test_to_core_write_req_empty_command_becomes_none() {
    let proto_req = proto_client::ClientWriteRequest {
        client_id: 1,
        command: None,
    };
    let core_req: ClientWriteRequest = to_core_write_req(proto_req);
    assert!(core_req.command.is_none());
}

// ─── to_core_read_req ─────────────────────────────────────────────────────────

#[test]
fn test_to_core_read_req_keys_moved() {
    let keys = vec![Bytes::from("k1"), Bytes::from("k2")];
    let proto_req = proto_client::ClientReadRequest {
        client_id: 3,
        keys: keys.clone(),
        consistency_policy: None,
    };
    let core_req: ClientReadRequest = to_core_read_req(proto_req);
    assert_eq!(core_req.client_id, 3);
    assert_eq!(core_req.keys, keys);
}

#[test]
fn test_to_core_read_req_no_policy_becomes_none() {
    let proto_req = proto_client::ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: None,
    };
    let core_req: ClientReadRequest = to_core_read_req(proto_req);
    // None → use cluster default; must NOT silently assume LeaseRead
    assert!(core_req.consistency_policy.is_none());
}

#[test]
fn test_to_core_read_req_lease_read_policy_converts() {
    let proto_req = proto_client::ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(proto_client::ReadConsistencyPolicy::LeaseRead as i32),
    };
    let core_req: ClientReadRequest = to_core_read_req(proto_req);
    assert_eq!(
        core_req.consistency_policy,
        Some(ReadConsistencyPolicy::LeaseRead)
    );
}

#[test]
fn test_to_core_read_req_linearizable_policy_converts() {
    let proto_req = proto_client::ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(proto_client::ReadConsistencyPolicy::LinearizableRead as i32),
    };
    let core_req: ClientReadRequest = to_core_read_req(proto_req);
    assert_eq!(
        core_req.consistency_policy,
        Some(ReadConsistencyPolicy::LinearizableRead)
    );
}

// ─── ErrorCode bidirectional ──────────────────────────────────────────────────

/// All 23 core::ErrorCode variants must survive a core→proto→core roundtrip.
/// This test catches any variant added to one side without updating the other.
#[test]
fn test_error_code_core_to_proto_to_core_roundtrip() {
    let all_core = [
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

    for code in all_core {
        let proto = core_error_to_proto(code);
        let back = proto_error_to_core(proto);
        assert_eq!(back, code, "Roundtrip failed for {code:?}");
    }
}

// ─── to_proto_response ────────────────────────────────────────────────────────

#[test]
fn test_to_proto_response_success_write() {
    let core_resp = ClientResponse {
        error: ErrorCode::Success,
        leader_hint: None,
        retry_after_ms: None,
        result: Some(ClientResponsePayload::Write(WriteResult {
            succeeded: true,
        })),
    };
    let proto_resp = to_proto_response(core_resp);

    assert_eq!(proto_resp.error, ProtoErrorCode::Success as i32);
    assert!(proto_resp.metadata.is_none());
    assert!(matches!(
        proto_resp.success_result,
        Some(proto_client::client_response::SuccessResult::WriteResult(
            proto_client::WriteResult { succeeded: true }
        ))
    ));
}

#[test]
fn test_to_proto_response_success_read() {
    let core_resp = ClientResponse {
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
    let proto_resp = to_proto_response(core_resp);

    assert_eq!(proto_resp.error, ProtoErrorCode::Success as i32);
    match proto_resp.success_result {
        Some(proto_client::client_response::SuccessResult::ReadData(rd)) => {
            assert_eq!(rd.results.len(), 1);
            assert_eq!(rd.results[0].key, Bytes::from("k"));
            assert_eq!(rd.results[0].value, Bytes::from("v"));
        }
        _ => panic!("expected ReadData"),
    }
}

#[test]
fn test_to_proto_response_success_scan() {
    let core_resp = ClientResponse {
        error: ErrorCode::Success,
        leader_hint: None,
        retry_after_ms: None,
        result: Some(ClientResponsePayload::Scan(ScanResults {
            entries: vec![(Bytes::from("k1"), Bytes::from("v1"))],
            revision: 42,
        })),
    };
    let proto_resp = to_proto_response(core_resp);

    assert_eq!(proto_resp.error, ProtoErrorCode::Success as i32);
    match proto_resp.success_result {
        Some(proto_client::client_response::SuccessResult::ScanData(sd)) => {
            assert_eq!(sd.entries.len(), 1);
            assert_eq!(sd.entries[0].key, Bytes::from("k1"));
            assert_eq!(sd.entries[0].value, Bytes::from("v1"));
            assert_eq!(sd.revision, 42);
        }
        _ => panic!("expected ScanData"),
    }
}

/// revision must survive even when there are no matching entries — it is not
/// derived from entries.len(), it's the Raft applied index at scan time.
#[test]
fn test_to_proto_response_empty_scan_keeps_revision() {
    let core_resp = ClientResponse {
        error: ErrorCode::Success,
        leader_hint: None,
        retry_after_ms: None,
        result: Some(ClientResponsePayload::Scan(ScanResults {
            entries: vec![],
            revision: 9,
        })),
    };
    let proto_resp = to_proto_response(core_resp);

    match proto_resp.success_result {
        Some(proto_client::client_response::SuccessResult::ScanData(sd)) => {
            assert!(sd.entries.is_empty());
            assert_eq!(sd.revision, 9);
        }
        _ => panic!("expected ScanData"),
    }
}

#[test]
fn test_to_proto_response_not_leader_with_hint() {
    let core_resp = ClientResponse {
        error: ErrorCode::NotLeader,
        leader_hint: Some(LeaderHint {
            leader_id: 2,
            address: "10.0.0.2:5002".into(),
        }),
        retry_after_ms: Some(100),
        result: None,
    };
    let proto_resp = to_proto_response(core_resp);

    assert_eq!(proto_resp.error, ProtoErrorCode::NotLeader as i32);
    let meta = proto_resp.metadata.unwrap();
    assert_eq!(meta.retry_after_ms, Some(100));
    assert_eq!(meta.leader_id.as_deref(), Some("2"));
    assert_eq!(meta.leader_address.as_deref(), Some("10.0.0.2:5002"));
    assert!(proto_resp.success_result.is_none());
}

#[test]
fn test_to_proto_response_error_no_hint_has_no_metadata() {
    let core_resp = ClientResponse {
        error: ErrorCode::ClusterUnavailable,
        leader_hint: None,
        retry_after_ms: None,
        result: None,
    };
    let proto_resp = to_proto_response(core_resp);
    assert!(proto_resp.metadata.is_none());
}

#[test]
fn test_to_proto_response_retry_without_hint_has_metadata() {
    let core_resp = ClientResponse {
        error: ErrorCode::RateLimited,
        leader_hint: None,
        retry_after_ms: Some(500),
        result: None,
    };
    let proto_resp = to_proto_response(core_resp);
    let meta = proto_resp.metadata.unwrap();
    assert_eq!(meta.retry_after_ms, Some(500));
    assert!(meta.leader_id.is_none());
}

// ─── to_core_response ─────────────────────────────────────────────────────────

#[test]
fn test_to_core_response_success_write() {
    let proto_resp = proto_client::ClientResponse {
        error: ProtoErrorCode::Success as i32,
        metadata: None,
        success_result: Some(proto_client::client_response::SuccessResult::WriteResult(
            proto_client::WriteResult { succeeded: true },
        )),
    };
    let core_resp = to_core_response(proto_resp);
    assert_eq!(core_resp.error, ErrorCode::Success);
    assert!(core_resp.leader_hint.is_none());
    assert!(matches!(
        core_resp.result,
        Some(ClientResponsePayload::Write(WriteResult {
            succeeded: true
        }))
    ));
}

#[test]
fn test_to_core_response_not_leader_with_metadata() {
    use d_engine_proto::error::ErrorMetadata;

    let proto_resp = proto_client::ClientResponse {
        error: ProtoErrorCode::NotLeader as i32,
        metadata: Some(ErrorMetadata {
            retry_after_ms: Some(200),
            leader_id: Some("3".into()),
            leader_address: Some("10.0.0.3:5003".into()),
            debug_message: None,
        }),
        success_result: None,
    };
    let core_resp = to_core_response(proto_resp);
    assert_eq!(core_resp.error, ErrorCode::NotLeader);
    assert_eq!(core_resp.retry_after_ms, Some(200));
    let hint = core_resp.leader_hint.unwrap();
    assert_eq!(hint.leader_id, 3);
    assert_eq!(hint.address, "10.0.0.3:5003");
}

#[test]
fn test_to_core_response_success_scan() {
    let proto_resp = proto_client::ClientResponse {
        error: ProtoErrorCode::Success as i32,
        metadata: None,
        success_result: Some(proto_client::client_response::SuccessResult::ScanData(
            proto_client::ScanResults {
                entries: vec![proto_client::KvEntry {
                    key: Bytes::from("k1"),
                    value: Bytes::from("v1"),
                }],
                revision: 42,
            },
        )),
    };
    let core_resp = to_core_response(proto_resp);
    assert_eq!(core_resp.error, ErrorCode::Success);
    match core_resp.result {
        Some(ClientResponsePayload::Scan(sr)) => {
            assert_eq!(sr.entries, vec![(Bytes::from("k1"), Bytes::from("v1"))]);
            assert_eq!(sr.revision, 42);
        }
        _ => panic!("expected Scan payload"),
    }
}

/// revision must survive even when there are no matching entries — it is not
/// derived from entries.len(), it's the Raft applied index at scan time.
#[test]
fn test_to_core_response_empty_scan_keeps_revision() {
    let proto_resp = proto_client::ClientResponse {
        error: ProtoErrorCode::Success as i32,
        metadata: None,
        success_result: Some(proto_client::client_response::SuccessResult::ScanData(
            proto_client::ScanResults {
                entries: vec![],
                revision: 9,
            },
        )),
    };
    let core_resp = to_core_response(proto_resp);
    match core_resp.result {
        Some(ClientResponsePayload::Scan(sr)) => {
            assert!(sr.entries.is_empty());
            assert_eq!(sr.revision, 9);
        }
        _ => panic!("expected Scan payload"),
    }
}

/// Unparseable leader_id string → LeaderHint is None (not a panic).
#[test]
fn test_to_core_response_bad_leader_id_yields_no_hint() {
    use d_engine_proto::error::ErrorMetadata;

    let proto_resp = proto_client::ClientResponse {
        error: ProtoErrorCode::NotLeader as i32,
        metadata: Some(ErrorMetadata {
            retry_after_ms: None,
            leader_id: Some("not-a-number".into()),
            leader_address: Some("10.0.0.3:5003".into()),
            debug_message: None,
        }),
        success_result: None,
    };
    let core_resp = to_core_response(proto_resp);
    // Graceful degradation: bad leader_id string → no hint, no panic
    assert!(core_resp.leader_hint.is_none());
}
