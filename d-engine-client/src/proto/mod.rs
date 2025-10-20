impl ClientResponse {
    /// Build success response for write operations
    ///
    /// # Returns
    /// Response with NoError code and write confirmation
    pub fn write_success() -> Self {
        Self {
            error: ErrorCode::Success as i32,
            success_result: Some(SuccessResult::WriteAck(true)),
            metadata: None,
        }
    }

    /// Check if the write operation was successful
    ///
    /// # Returns
    /// - `true` if the response indicates a successful write operation
    /// - `false` if the response indicates a failed write operation or is not a write response
    pub fn is_write_success(&self) -> bool {
        self.error == ErrorCode::Success as i32
            && matches!(self.success_result, Some(SuccessResult::WriteAck(true)))
    }

    /// Build success response for read operations
    ///
    /// # Parameters
    /// - `results`: Vector of retrieved key-value pairs
    pub fn read_results(results: Vec<ClientResult>) -> Self {
        Self {
            error: ErrorCode::Success as i32,
            success_result: Some(SuccessResult::ReadData(ReadResults { results })),
            metadata: None,
        }
    }

    /// Build generic error response for any operation type
    ///
    /// # Parameters
    /// - `error_code`: Predefined client request error code
    pub fn client_error(error_code: ErrorCode) -> Self {
        Self {
            error: error_code as i32,
            success_result: None,
            metadata: None,
        }
    }

    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    pub fn into_write_result(self) -> std::result::Result<bool, ClientApiError> {
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
    pub fn into_read_results(
        self
    ) -> std::result::Result<Vec<Option<ClientResult>>, ClientApiError> {
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
                error!("Invalid response type for read operation");
                unreachable!()
            }
        }
    }

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    pub fn validate_error(&self) -> std::result::Result<(), ClientApiError> {
        match ErrorCode::try_from(self.error).unwrap_or(ErrorCode::Uncategorized) {
            ErrorCode::Success => Ok(()),
            e => Err(e.into()),
        }
    }

    /// Check if this response indicates the leader's term is outdated
    pub fn is_term_outdated(&self) -> bool {
        ErrorCode::try_from(self.error).map(|e| e.is_term_outdated()).unwrap_or(false)
    }

    /// Check if this response indicates a quorum timeout or failure to receive majority responses
    pub fn is_quorum_timeout_or_failure(&self) -> bool {
        ErrorCode::try_from(self.error)
            .map(|e| e.is_quorum_timeout_or_failure())
            .unwrap_or(false)
    }

    /// Check if this response indicates a failure to receive majority responses
    pub fn is_propose_failure(&self) -> bool {
        ErrorCode::try_from(self.error).map(|e| e.is_propose_failure()).unwrap_or(false)
    }

    /// Check if this response indicates a a retry required
    pub fn is_retry_required(&self) -> bool {
        ErrorCode::try_from(self.error).map(|e| e.is_retry_required()).unwrap_or(false)
    }
}
