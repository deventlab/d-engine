use bytes::Bytes;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientResult;
use d_engine_proto::client::ReadResults;
use d_engine_proto::client::WriteResult;
use d_engine_proto::client::client_response::SuccessResult;
use d_engine_proto::error::ErrorCode;

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

    // Should fail at validate_error() before checking success_result
    if let Err(ClientApiError::Business { code, .. }) = result {
        assert_eq!(code, ErrorCode::NotLeader);
    } else {
        panic!("Expected Business error with NotLeader code");
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

    let result = response.into_read_results();
    assert!(result.is_ok());
    let data = result.unwrap();
    assert_eq!(data.len(), 1);
    assert!(data[0].is_some());
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

    // Should fail at validate_error() before checking success_result
    if let Err(ClientApiError::Business { code, .. }) = result {
        assert_eq!(code, ErrorCode::NotLeader);
    } else {
        panic!("Expected Business error with NotLeader code");
    }
}
