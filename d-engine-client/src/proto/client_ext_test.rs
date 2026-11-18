use bytes::Bytes;
use d_engine_core::client::ErrorCode;
use d_engine_core::client::KvEntry;
use d_engine_core::client::LeaderHint;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::ReadResults;
use d_engine_proto::client::WriteResult;
use d_engine_proto::client::client_response::SuccessResult;
use d_engine_proto::error::ErrorMetadata;

use super::*;
use crate::ClientApiError;

// --- into_write_result tests ---

#[test]
fn test_into_write_result_succeeded() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::WriteResult(WriteResult { succeeded: true })),
    };

    let result = response.into_write_result();
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[test]
fn test_into_write_result_failed_cas() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::WriteResult(WriteResult { succeeded: false })),
    };

    let result = response.into_write_result();
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[test]
fn test_into_write_result_wrong_variant_read_data() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::ReadData(ReadResults { results: vec![] })),
    };

    let result = response.into_write_result();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected WriteResult"));
        assert!(message.contains("found ReadData"));
    } else {
        panic!("Expected Protocol error");
    }
}

#[test]
fn test_into_write_result_none_variant() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: None,
    };

    let result = response.into_write_result();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected WriteResult"));
        assert!(message.contains("found None"));
    } else {
        panic!("Expected Protocol error");
    }
}

#[test]
fn test_into_write_result_with_error_code() {
    let response = ClientResponse {
        error: ErrorCode::NotLeader as i32,
        metadata: None,
        success_result: Some(SuccessResult::WriteResult(WriteResult { succeeded: true })),
    };

    let result = response.into_write_result();
    assert!(result.is_err());

    // Should fail at validate_error() before checking success_result.
    // NotLeader carries a leader_hint, so it must be ClientApiError::Network, not Business.
    if let Err(ClientApiError::Network { code, .. }) = result {
        assert_eq!(code, ErrorCode::NotLeader);
    } else {
        panic!("Expected Network error with NotLeader code");
    }
}

#[test]
fn test_validate_error_not_leader_carries_leader_hint() {
    let response = ClientResponse {
        error: ErrorCode::NotLeader as i32,
        metadata: Some(ErrorMetadata {
            retry_after_ms: Some(100),
            leader_id: Some("2".to_string()),
            leader_address: Some("127.0.0.1:9082".to_string()),
            debug_message: None,
        }),
        success_result: None,
    };

    let result = response.validate_error();
    assert!(result.is_err());

    match result {
        Err(ClientApiError::Network {
            code,
            leader_hint,
            retry_after_ms,
            ..
        }) => {
            assert_eq!(code, ErrorCode::NotLeader);
            assert_eq!(
                leader_hint,
                Some(LeaderHint {
                    leader_id: 2,
                    address: "127.0.0.1:9082".to_string(),
                })
            );
            assert_eq!(retry_after_ms, Some(100));
        }
        other => panic!("expected Network error with leader_hint, got {other:?}"),
    }
}

// --- into_read_results tests ---

#[test]
fn test_into_read_results_success() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::ReadData(ReadResults {
            results: vec![ClientResult {
                key: Bytes::from(vec![1, 2, 3]),
                value: Bytes::from(vec![4, 5, 6]),
            }],
        })),
    };

    // Type annotation is the spec: into_read_results must return KvEntry, not proto ClientResult
    let data: Vec<Option<KvEntry>> = response.into_read_results().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        Some(KvEntry {
            key: Bytes::from(vec![1, 2, 3]),
            value: Bytes::from(vec![4, 5, 6]),
        })
    );
}

#[test]
fn test_into_read_results_multiple_entries_are_kv_entries() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::ReadData(ReadResults {
            results: vec![
                ClientResult {
                    key: Bytes::from("k1"),
                    value: Bytes::from("v1"),
                },
                ClientResult {
                    key: Bytes::from("k2"),
                    value: Bytes::from("v2"),
                },
            ],
        })),
    };

    let data: Vec<Option<KvEntry>> = response.into_read_results().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(
        data[0],
        Some(KvEntry {
            key: Bytes::from("k1"),
            value: Bytes::from("v1")
        })
    );
    assert_eq!(
        data[1],
        Some(KvEntry {
            key: Bytes::from("k2"),
            value: Bytes::from("v2")
        })
    );
}

#[test]
fn test_into_read_results_wrong_variant_succeeded() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::WriteResult(WriteResult { succeeded: true })),
    };

    let result = response.into_read_results();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected ReadData"));
        assert!(message.contains("found WriteResult"));
    } else {
        panic!("Expected Protocol error");
    }
}

#[test]
fn test_into_read_results_none_variant() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: None,
    };

    let result = response.into_read_results();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected ReadData"));
        assert!(message.contains("found None"));
    } else {
        panic!("Expected Protocol error");
    }
}

#[test]
fn test_into_read_results_with_error_code() {
    let response = ClientResponse {
        error: ErrorCode::NotLeader as i32,
        metadata: None,
        success_result: Some(SuccessResult::ReadData(ReadResults { results: vec![] })),
    };

    let result = response.into_read_results();
    assert!(result.is_err());

    // Should fail at validate_error() before checking success_result.
    // NotLeader carries a leader_hint, so it must be ClientApiError::Network, not Business.
    if let Err(ClientApiError::Network { code, .. }) = result {
        assert_eq!(code, ErrorCode::NotLeader);
    } else {
        panic!("Expected Network error with NotLeader code");
    }
}

// --- into_scan_results tests ---
//
// Mirrors into_write_result/into_read_results above — into_scan_results had
// zero direct tests before (only exercised indirectly via grpc_client_test.rs
// success-path tests), unlike its siblings which each have a full set here.

#[test]
fn test_into_scan_results_success() {
    use d_engine_proto::client::KvEntry as ProtoKvEntry;
    use d_engine_proto::client::ScanResults as ProtoScanResults;

    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::ScanData(ProtoScanResults {
            entries: vec![ProtoKvEntry {
                key: Bytes::from("/services/node1"),
                value: Bytes::from("10.0.0.1"),
            }],
            revision: 7,
        })),
    };

    let scan = response.into_scan_results().unwrap();
    assert_eq!(scan.revision, 7);
    assert_eq!(
        scan.entries,
        vec![(Bytes::from("/services/node1"), Bytes::from("10.0.0.1"))]
    );
}

#[test]
fn test_into_scan_results_wrong_variant_write_result() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: Some(SuccessResult::WriteResult(WriteResult { succeeded: true })),
    };

    let result = response.into_scan_results();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected ScanData"));
        assert!(message.contains("found WriteResult"));
    } else {
        panic!("Expected Protocol error");
    }
}

#[test]
fn test_into_scan_results_none_variant() {
    let response = ClientResponse {
        error: ErrorCode::Success as i32,
        metadata: None,
        success_result: None,
    };

    let result = response.into_scan_results();
    assert!(result.is_err());

    if let Err(ClientApiError::Protocol { code, message, .. }) = result {
        assert_eq!(code, ErrorCode::InvalidResponse);
        assert!(message.contains("expected ScanData"));
        assert!(message.contains("found None"));
    } else {
        panic!("Expected Protocol error");
    }
}

/// #425: scan must fail at validate_error() before ever touching success_result,
/// and NotLeader must map to Network (leader_hint-capable), not Business —
/// same contract already proven for write/read, now proven for scan too.
#[test]
fn test_into_scan_results_with_error_code() {
    let response = ClientResponse {
        error: ErrorCode::NotLeader as i32,
        metadata: None,
        success_result: None,
    };

    let result = response.into_scan_results();
    assert!(result.is_err());

    if let Err(ClientApiError::Network { code, .. }) = result {
        assert_eq!(code, ErrorCode::NotLeader);
    } else {
        panic!("Expected Network error with NotLeader code");
    }
}
