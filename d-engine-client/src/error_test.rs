use d_engine_proto::error::ErrorCode;
use tonic::Code;
use tonic::Status;

use super::*;

#[test]
fn test_error_code_conversion() {
    let error = ClientApiError::from(ErrorCode::NotLeader);
    assert_eq!(error.code(), ErrorCode::NotLeader);
    assert_eq!(error.message(), "Not leader");
}

#[test]
fn test_network_error_retry_logic() {
    let error = ClientApiError::Network {
        code: ErrorCode::ConnectionTimeout,
        message: "timeout".to_string(),
        retry_after_ms: Some(3000),
        leader_hint: None,
    };

    match error {
        ClientApiError::Network { retry_after_ms, .. } => {
            assert_eq!(retry_after_ms, Some(3000));
        }
        _ => panic!("Expected Network error"),
    }
}

#[test]
fn test_business_error_with_action() {
    let error = ClientApiError::from(ErrorCode::NotLeader);

    match error {
        ClientApiError::Business {
            required_action, ..
        } => {
            assert_eq!(required_action, Some("redirect to leader".to_string()));
        }
        _ => panic!("Expected Business error"),
    }
}

#[test]
fn test_status_to_error_conversion() {
    let status = Status::unavailable("cluster down");
    let error = ClientApiError::from(status);

    assert_eq!(error.code(), ErrorCode::ClusterUnavailable);
    assert!(error.message().contains("cluster down"));
}

#[test]
fn test_leader_hint_parsing() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        "x-raft-leader",
        r#"{"leader_id": 1, "address": "127.0.0.1:9081"}"#.parse().unwrap(),
    );

    let status = Status::with_metadata(Code::FailedPrecondition, "not leader", metadata);
    let error = ClientApiError::from(status);

    match error {
        ClientApiError::Network { leader_hint, .. } => {
            let hint = leader_hint.expect("should have leader hint");
            assert_eq!(hint.leader_id, 1);
            assert_eq!(hint.address, "127.0.0.1:9081");
        }
        _ => panic!("Expected Network error with leader hint"),
    }
}

#[test]
fn test_general_client_error() {
    let error = ClientApiError::general_client_error("custom error".to_string());
    assert_eq!(error.code(), ErrorCode::General);
    assert_eq!(error.message(), "custom error");
}
