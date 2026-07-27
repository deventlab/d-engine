//! Extends the generated proto `ClientResponse` with typed conversions.
//!
//! `ClientResponse` carries its payload as an untyped `oneof` (write/read/scan)
//! plus a numeric error code. This module unwraps that wire shape into the
//! typed result each RPC caller actually wants (`bool`, `Vec<KvEntry>`,
//! `ScanResults`), checking the error code first. Used by `GrpcClient` and the
//! mock RPC service to go from raw proto response to `ClientApiResult<T>`.

use d_engine_core::client::ErrorCode;
use d_engine_core::client::KvEntry;
use d_engine_core::client::LeaderHint;
use d_engine_core::types::ScanResults;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::client_response::SuccessResult;
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
    fn into_read_results(self) -> std::result::Result<Vec<Option<KvEntry>>, ClientApiError>;

    /// Convert response to prefix-scan results
    ///
    /// # Returns
    /// Matching key-value entries plus the revision anchor
    fn into_scan_results(self) -> std::result::Result<ScanResults, ClientApiError>;

    /// Validate error code in response header
    ///
    /// # Internal Logic
    /// Converts numeric error code to enum variant
    fn validate_error(&self) -> std::result::Result<(), ClientApiError>;

    /// Returns true if this is a successful write with `succeeded = true`.
    #[allow(dead_code)]
    fn is_write_success(&self) -> bool;
}

impl ClientResponseExt for ClientResponse {
    /// Convert response to boolean result
    ///
    /// # Returns
    /// - `Ok(true)` for successful Put/Delete, or successful CAS
    /// - `Ok(false)` for failed CAS
    /// - `Err` with error code on failure
    fn into_write_result(self) -> std::result::Result<bool, ClientApiError> {
        self.validate_error()?;
        match self.success_result {
            Some(SuccessResult::WriteResult(result)) => Ok(result.succeeded),
            other => {
                let found = match &other {
                    Some(SuccessResult::ReadData(_)) => "ReadData",
                    Some(_) => "Unknown",
                    None => "None",
                };
                error!(
                    "Unexpected response type for write operation: expected WriteResult, found {found}"
                );
                Err(ClientApiError::Protocol {
                    code: ErrorCode::InvalidResponse,
                    message: format!(
                        "Unexpected response type: expected WriteResult, found {found}"
                    ),
                    supported_versions: None,
                })
            }
        }
    }

    /// Convert response to read results
    ///
    /// # Returns
    /// Vector of optional key-value pairs wrapped in Result
    fn into_read_results(self) -> std::result::Result<Vec<Option<KvEntry>>, ClientApiError> {
        self.validate_error()?;
        match &self.success_result {
            Some(SuccessResult::ReadData(data)) => data
                .results
                .clone()
                .into_iter()
                .map(|item| {
                    Ok(Some(KvEntry {
                        key: item.key,
                        value: item.value,
                    }))
                })
                .collect(),
            _ => {
                let found = match &self.success_result {
                    Some(SuccessResult::WriteResult(_)) => "WriteResult",
                    None => "None",
                    _ => "Unknown",
                };
                error!(
                    "Unexpected response type for read operation: expected ReadData, found {}",
                    found
                );
                Err(ClientApiError::Protocol {
                    code: ErrorCode::InvalidResponse,
                    message: format!("Unexpected response type: expected ReadData, found {found}",),
                    supported_versions: None,
                })
            }
        }
    }

    /// Convert response to prefix-scan results
    ///
    /// # Returns
    /// Matching key-value entries plus the revision anchor
    fn into_scan_results(self) -> std::result::Result<ScanResults, ClientApiError> {
        self.validate_error()?;
        match self.success_result {
            Some(SuccessResult::ScanData(data)) => Ok(ScanResults {
                entries: data.entries.into_iter().map(|e| (e.key, e.value)).collect(),
                revision: data.revision,
            }),
            other => {
                let found = match &other {
                    Some(SuccessResult::WriteResult(_)) => "WriteResult",
                    Some(SuccessResult::ReadData(_)) => "ReadData",
                    None => "None",
                    _ => "Unknown",
                };
                error!(
                    "Unexpected response type for scan operation: expected ScanData, found {found}"
                );
                Err(ClientApiError::Protocol {
                    code: ErrorCode::InvalidResponse,
                    message: format!("Unexpected response type: expected ScanData, found {found}"),
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
        let code = ErrorCode::try_from(self.error).unwrap_or(ErrorCode::Uncategorized);
        if code == ErrorCode::Success {
            return Ok(());
        }

        // NotLeader carries redirect info in `metadata`, which the generic
        // `From<ErrorCode>` conversion has no way to see — extract it here,
        // where `self.metadata` is actually in scope.
        if code == ErrorCode::NotLeader {
            let leader_hint = self.metadata.as_ref().and_then(|m| {
                let leader_id = m.leader_id.as_ref()?.parse::<u32>().ok()?;
                let address = m.leader_address.clone()?;
                Some(LeaderHint { leader_id, address })
            });
            let retry_after_ms = self.metadata.as_ref().and_then(|m| m.retry_after_ms);
            return Err(ClientApiError::Network {
                code,
                message: "Not leader".to_string(),
                retry_after_ms: retry_after_ms.or(Some(100)),
                leader_hint,
            });
        }

        Err(code.into())
    }

    fn is_write_success(&self) -> bool {
        self.error == ErrorCode::Success as i32
            && matches!(&self.success_result, Some(SuccessResult::WriteResult(w)) if w.succeeded)
    }
}
