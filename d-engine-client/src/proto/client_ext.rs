use d_engine_proto::{
    client::{ClientResponse, ClientResult, client_response::SuccessResult},
    error::ErrorCode,
};
use tracing::error;

use crate::ClientApiError;

pub trait ClientResponseExt {
    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    #[allow(dead_code)]
    fn into_write_result(self) -> std::result::Result<bool, ClientApiError>;

    /// Convert response to read results
    ///
    /// # Returns
    /// Vector of optional key-value pairs wrapped in Result
    fn into_read_results(self) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError>;

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    fn validate_error(&self) -> std::result::Result<(), ClientApiError>;
}

impl ClientResponseExt for ClientResponse {
    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    fn into_write_result(self) -> std::result::Result<bool, ClientApiError> {
        self.validate_error()?;
        Ok(match self.success_result {
            Some(SuccessResult::WriteAck(success)) => success,
            _ => false,
        })
    }

    /// Convert response to read results
    ///
    /// # Returns
    /// Vector of optional key-value pairs wrapped in Result
    fn into_read_results(self) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
        self.validate_error()?;
        match &self.success_result {
            Some(SuccessResult::ReadData(data)) => data
                .results
                .clone()
                .into_iter()
                .map(|item| {
                    Ok(Some(ClientResult {
                        key: item.key,
                        value: item.value,
                    }))
                })
                .collect(),
            _ => {
                let found = match &self.success_result {
                    Some(SuccessResult::WriteAck(_)) => "WriteAck",
                    None => "None",
                    _ => "Unknown",
                };
                error!(
                    "Unexpected response type for read operation: expected ReadData, found {}",
                    found
                );
                Err(ClientApiError::Protocol {
                    code: d_engine_proto::error::ErrorCode::InvalidResponse,
                    message: format!("Unexpected response type: expected ReadData, found {found}",),
                    supported_versions: None,
                })
            }
        }
    }

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    fn validate_error(&self) -> std::result::Result<(), ClientApiError> {
        match ErrorCode::try_from(self.error).unwrap_or(ErrorCode::Uncategorized) {
            ErrorCode::Success => Ok(()),
            e => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use d_engine_proto::client::{ReadResults, client_response::SuccessResult};

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
    fn test_into_read_results_wrong_variant_write_ack() {
        let response = ClientResponse {
            error: ErrorCode::Success as i32,
            metadata: None,
            success_result: Some(SuccessResult::WriteAck(true)),
        };

        let result = response.into_read_results();
        assert!(result.is_err());

        if let Err(ClientApiError::Protocol { code, message, .. }) = result {
            assert_eq!(code, ErrorCode::InvalidResponse);
            assert!(message.contains("expected ReadData"));
            assert!(message.contains("found WriteAck"));
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
}
