pub trait ClientResponseExt {
    /// Convert response to boolean write result
    ///
    /// # Returns
    /// - `Ok(true)` on successful write
    /// - `Err` with converted error code on failure
    fn into_write_result(self) -> std::result::Result<bool, ClientApiError>;

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
                error!("Invalid response type for read operation");
                unreachable!()
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
                error!("Invalid response type for read operation");
                unreachable!()
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
