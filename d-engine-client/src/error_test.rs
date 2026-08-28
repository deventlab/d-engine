use d_engine_core::client::ErrorCode;
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
fn test_not_leader_error_is_network_variant() {
    // NotLeader is redirectable (carries a leader_hint when available), so it
    // must be ClientApiError::Network, not Business — Business has no
    // leader_hint field. The bare `From<ErrorCode>` conversion has no
    // metadata to populate leader_hint from; that only happens in
    // `ClientResponseExt::validate_error()`, which has `self.metadata`.
    let error = ClientApiError::from(ErrorCode::NotLeader);

    match error {
        ClientApiError::Network {
            code, leader_hint, ..
        } => {
            assert_eq!(code, ErrorCode::NotLeader);
            assert_eq!(leader_hint, None);
        }
        _ => panic!("Expected Network error"),
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
fn test_general_client_error() {
    let error = ClientApiError::general_client_error("custom error".to_string());
    assert_eq!(error.code(), ErrorCode::General);
    assert_eq!(error.message(), "custom error");
}
