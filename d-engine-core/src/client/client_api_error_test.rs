//! Tests for ClientApiError

#[cfg(test)]
mod client_api_error_tests {
    use crate::client::client_api_error::{ClientApiError, ClientApiResult};
    use d_engine_proto::error::ErrorCode;

    /// Test: NotLeader error construction and code mapping
    #[test]
    fn test_not_leader_error() {
        let error: ClientApiError = ErrorCode::NotLeader.into();

        assert_eq!(error.code(), ErrorCode::NotLeader);
        assert!(error.message().contains("Not leader"));
    }

    /// Test: Various error code conversions
    #[test]
    fn test_error_code_conversions() {
        let test_cases = vec![
            (ErrorCode::NotLeader, "Not leader"),
            (ErrorCode::ConnectionTimeout, "Connection timeout"),
            (ErrorCode::InvalidRequest, "Invalid request"),
            (ErrorCode::StorageIoError, "Storage I/O error"),
            (ErrorCode::ClusterUnavailable, "Cluster unavailable"),
        ];

        for (code, expected_msg_part) in test_cases {
            let error: ClientApiError = code.into();
            assert_eq!(error.code(), code);
            assert!(
                error.message().to_lowercase().contains(&expected_msg_part.to_lowercase()),
                "Error message '{}' should contain '{}'",
                error.message(),
                expected_msg_part
            );
        }
    }

    /// Test: Error display formatting
    #[test]
    fn test_error_display_formatting() {
        let error: ClientApiError = ErrorCode::NotLeader.into();
        let display = format!("{error}",);

        // Display should include error code and message
        assert!(!display.is_empty());
        assert!(display.contains("NotLeader") || display.contains("Not leader"));
    }

    /// Test: Result type usage
    #[test]
    fn test_result_type() {
        // Test Ok case
        let ok_result: ClientApiResult<String> = Ok("success".to_string());
        assert!(ok_result.is_ok());
        assert_eq!(ok_result.as_ref().unwrap(), "success");

        // Test Err case
        let err_result: ClientApiResult<String> = Err(ErrorCode::ConnectionTimeout.into());
        assert!(err_result.is_err());
        assert_eq!(
            err_result.as_ref().unwrap_err().code(),
            ErrorCode::ConnectionTimeout
        );
    }

    /// Test: Error message content
    #[test]
    fn test_error_messages() {
        let error: ClientApiError = ErrorCode::NotLeader.into();
        assert!(!error.message().is_empty());

        let error: ClientApiError = ErrorCode::ClusterUnavailable.into();
        assert!(error.message().to_lowercase().contains("unavailable"));
    }

    /// Test: Network error variants
    #[test]
    fn test_network_errors() {
        let error: ClientApiError = ErrorCode::ConnectionTimeout.into();
        assert_eq!(error.code(), ErrorCode::ConnectionTimeout);
        assert!(matches!(error, ClientApiError::Network { .. }));
    }

    /// Test: Business logic error variants
    #[test]
    fn test_business_errors() {
        let error: ClientApiError = ErrorCode::NotLeader.into();
        assert_eq!(error.code(), ErrorCode::NotLeader);
        assert!(matches!(error, ClientApiError::Business { .. }));

        let error: ClientApiError = ErrorCode::InvalidRequest.into();
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        assert!(matches!(error, ClientApiError::Business { .. }));
    }

    /// Test: Storage error variants
    #[test]
    fn test_storage_errors() {
        let error: ClientApiError = ErrorCode::StorageIoError.into();
        assert_eq!(error.code(), ErrorCode::StorageIoError);
        assert!(matches!(error, ClientApiError::Storage { .. }));

        let error: ClientApiError = ErrorCode::DiskFull.into();
        assert_eq!(error.code(), ErrorCode::DiskFull);
        assert!(matches!(error, ClientApiError::Storage { .. }));
    }

    /// Test: Protocol error variants
    #[test]
    fn test_protocol_errors() {
        let error: ClientApiError = ErrorCode::InvalidResponse.into();
        assert_eq!(error.code(), ErrorCode::InvalidResponse);
        assert!(matches!(error, ClientApiError::Protocol { .. }));
    }

    /// Test: Error comparison by code
    #[test]
    fn test_error_code_equality() {
        let error1: ClientApiError = ErrorCode::NotLeader.into();
        let error2: ClientApiError = ErrorCode::NotLeader.into();
        let error3: ClientApiError = ErrorCode::ConnectionTimeout.into();

        assert_eq!(error1.code(), error2.code());
        assert_ne!(error1.code(), error3.code());
    }

    /// Test: General error helper
    #[test]
    fn test_general_error_helper() {
        let error = ClientApiError::general_client_error("Custom error message".to_string());
        assert_eq!(error.code(), ErrorCode::General);
        assert_eq!(error.message(), "Custom error message");
    }
}
