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

    // ── From<Status> conversions ──────────────────────────────────────────────

    /// Code::Unavailable maps to Business { ClusterUnavailable }.
    #[test]
    fn test_from_status_unavailable_maps_to_cluster_unavailable() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::Unavailable, "cluster down");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::ClusterUnavailable);
        assert!(matches!(err, ClientApiError::Business { .. }));
    }

    /// Code::Cancelled maps to Network { ConnectionTimeout }.
    #[test]
    fn test_from_status_cancelled_maps_to_connection_timeout() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::Cancelled, "cancelled");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::ConnectionTimeout);
        assert!(matches!(err, ClientApiError::Network { .. }));
    }

    /// Code::InvalidArgument maps to Business { InvalidRequest }.
    #[test]
    fn test_from_status_invalid_argument_maps_to_invalid_request() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::InvalidArgument, "bad arg");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::InvalidRequest);
    }

    /// Code::PermissionDenied maps to Business { NotLeader }.
    #[test]
    fn test_from_status_permission_denied_maps_to_not_leader() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::PermissionDenied, "not leader");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::NotLeader);
    }

    /// Unhandled gRPC codes produce Business { Uncategorized }.
    #[test]
    fn test_from_status_unhandled_code_maps_to_uncategorized() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::DataLoss, "data loss");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::Uncategorized);
    }

    /// Code::FailedPrecondition without leader metadata maps to Business { StaleOperation }.
    #[test]
    fn test_from_status_failed_precondition_without_leader_maps_to_stale() {
        use tonic::Code;
        use tonic::Status;
        let s = Status::new(Code::FailedPrecondition, "stale");
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::StaleOperation);
        assert!(matches!(err, ClientApiError::Business { .. }));
    }

    /// Code::FailedPrecondition with valid x-raft-leader metadata maps to
    /// Network { LeaderChanged } and populates the leader_hint field.
    #[test]
    fn test_from_status_failed_precondition_with_leader_metadata_maps_to_leader_changed() {
        use tonic::Code;
        use tonic::Status;
        use tonic::metadata::MetadataValue;
        let mut s = Status::new(Code::FailedPrecondition, "leader changed");
        s.metadata_mut().insert(
            "x-raft-leader",
            MetadataValue::from_static(r#"{"leader_id":"2","address":"127.0.0.1:8081"}"#),
        );
        let err: ClientApiError = s.into();
        assert_eq!(err.code(), ErrorCode::LeaderChanged);
        if let ClientApiError::Network { leader_hint, .. } = err {
            let hint = leader_hint.expect("leader_hint must be populated");
            assert_eq!(hint.leader_id, 2);
            assert_eq!(hint.address, "127.0.0.1:8081");
        } else {
            panic!("expected Network variant");
        }
    }

    /// parse_leader_from_metadata returns None for malformed metadata values,
    /// causing FailedPrecondition to fall back to Business { StaleOperation }.
    #[test]
    fn test_from_status_failed_precondition_with_malformed_leader_metadata_falls_back_to_stale() {
        use tonic::Code;
        use tonic::Status;
        use tonic::metadata::MetadataValue;
        let mut s = Status::new(Code::FailedPrecondition, "fp");
        s.metadata_mut().insert(
            "x-raft-leader",
            MetadataValue::from_static("not-valid-json"),
        );
        let err: ClientApiError = s.into();
        // parse_leader_from_metadata must fail to extract a valid LeaderHint → StaleOperation.
        assert_eq!(err.code(), ErrorCode::StaleOperation);
    }
}
