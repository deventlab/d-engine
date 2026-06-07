//! Conversions between proto-generated types and d-engine-core native types.
//!
//! # Why free functions instead of From impls
//!
//! Both proto types (d-engine-proto) and core types (d-engine-core) are external
//! to this crate.  Rust's orphan rule forbids `impl From<ExternalA> for ExternalB`
//! in a third crate.  Free functions sidestep this without any ergonomics or
//! performance cost — callers are explicit about the conversion, and `#[inline]`
//! still works across crate boundaries.
//!
//! # Performance
//!
//! - All functions are `#[inline]`
//! - `Bytes` fields are moved (refcount unchanged, zero allocation)
//! - `ErrorCode` conversion uses exhaustive match — LLVM optimises to identity;
//!   a missing variant is a compile error (correctness guarantee)

use d_engine_core::client::{
    ClientReadRequest, ClientResponse, ClientResponsePayload, ClientWriteRequest, ErrorCode,
    KvEntry, LeaderHint, ReadResults, WriteOperation, WriteResult,
};
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_proto::client::write_command::Operation;
use d_engine_proto::client::{
    self as proto_client, ClientResult, ReadResults as ProtoReadResults,
    WriteResult as ProtoWriteResult,
};
use d_engine_proto::error::{ErrorCode as ProtoErrorCode, ErrorMetadata};

// ─── proto → core ─────────────────────────────────────────────────────────────

/// Convert a proto `WriteCommand` to a core `WriteOperation`.
///
/// Panics if `wc.operation` is `None`.  Callers must validate the nested
/// operation field before calling — the gRPC handler does this at the network
/// boundary via an explicit `invalid_argument` check.
#[inline]
pub(crate) fn write_command_to_op(wc: proto_client::WriteCommand) -> WriteOperation {
    match wc.operation {
        Some(Operation::Insert(i)) => WriteOperation::Insert {
            key: i.key,
            value: i.value,
            // Proto convention: 0 == no expiration → None in core
            ttl_secs: if i.ttl_secs == 0 {
                None
            } else {
                Some(i.ttl_secs)
            },
        },
        Some(Operation::Delete(d)) => WriteOperation::Delete { key: d.key },
        Some(Operation::CompareAndSwap(c)) => WriteOperation::CompareAndSwap {
            key: c.key,
            expected: c.expected_value,
            new_value: c.new_value,
        },
        None => unreachable!("WriteCommand must have an operation"),
    }
}

#[inline]
pub(crate) fn to_core_write_req(req: proto_client::ClientWriteRequest) -> ClientWriteRequest {
    ClientWriteRequest {
        client_id: req.client_id,
        command: req.command.map(write_command_to_op),
    }
}

#[inline]
pub(crate) fn to_core_read_req(req: proto_client::ClientReadRequest) -> ClientReadRequest {
    ClientReadRequest {
        client_id: req.client_id,
        keys: req.keys, // Vec<Bytes> moved, O(1)
        consistency_policy: req
            .consistency_policy
            .and_then(|i| proto_client::ReadConsistencyPolicy::try_from(i).ok())
            .map(ReadConsistencyPolicy::from),
    }
}

#[allow(dead_code)]
#[inline]
pub(crate) fn proto_error_to_core(e: ProtoErrorCode) -> ErrorCode {
    match e {
        ProtoErrorCode::Success => ErrorCode::Success,
        ProtoErrorCode::ConnectionTimeout => ErrorCode::ConnectionTimeout,
        ProtoErrorCode::InvalidAddress => ErrorCode::InvalidAddress,
        ProtoErrorCode::LeaderChanged => ErrorCode::LeaderChanged,
        ProtoErrorCode::JoinError => ErrorCode::JoinError,
        ProtoErrorCode::InvalidResponse => ErrorCode::InvalidResponse,
        ProtoErrorCode::VersionMismatch => ErrorCode::VersionMismatch,
        ProtoErrorCode::DiskFull => ErrorCode::DiskFull,
        ProtoErrorCode::DataCorruption => ErrorCode::DataCorruption,
        ProtoErrorCode::StorageIoError => ErrorCode::StorageIoError,
        ProtoErrorCode::StoragePermissionDenied => ErrorCode::StoragePermissionDenied,
        ProtoErrorCode::KeyNotExist => ErrorCode::KeyNotExist,
        ProtoErrorCode::NotLeader => ErrorCode::NotLeader,
        ProtoErrorCode::StaleOperation => ErrorCode::StaleOperation,
        ProtoErrorCode::InvalidRequest => ErrorCode::InvalidRequest,
        ProtoErrorCode::RateLimited => ErrorCode::RateLimited,
        ProtoErrorCode::ClusterUnavailable => ErrorCode::ClusterUnavailable,
        ProtoErrorCode::ProposeFailed => ErrorCode::ProposeFailed,
        ProtoErrorCode::TermOutdated => ErrorCode::TermOutdated,
        ProtoErrorCode::RetryRequired => ErrorCode::RetryRequired,
        ProtoErrorCode::WatchBufferOverflow => ErrorCode::WatchBufferOverflow,
        ProtoErrorCode::General => ErrorCode::General,
        ProtoErrorCode::Uncategorized => ErrorCode::Uncategorized,
    }
}

#[allow(dead_code)]
#[inline]
pub(crate) fn to_core_response(r: proto_client::ClientResponse) -> ClientResponse {
    let error = ProtoErrorCode::try_from(r.error)
        .map(proto_error_to_core)
        .unwrap_or(ErrorCode::Uncategorized);

    let (leader_hint, retry_after_ms) = match r.metadata {
        Some(meta) => {
            let hint = parse_leader_hint(&meta);
            (hint, meta.retry_after_ms)
        }
        None => (None, None),
    };

    let result = r.success_result.map(|sr| match sr {
        proto_client::client_response::SuccessResult::WriteResult(w) => {
            ClientResponsePayload::Write(WriteResult {
                succeeded: w.succeeded,
            })
        }
        proto_client::client_response::SuccessResult::ReadData(rd) => {
            ClientResponsePayload::Read(ReadResults {
                entries: rd
                    .results
                    .into_iter()
                    .map(|e| KvEntry {
                        key: e.key,
                        value: e.value,
                    })
                    .collect(),
            })
        }
    });

    ClientResponse {
        error,
        leader_hint,
        retry_after_ms,
        result,
    }
}

// ─── core → proto ─────────────────────────────────────────────────────────────

#[inline]
pub(crate) fn core_error_to_proto(e: ErrorCode) -> ProtoErrorCode {
    match e {
        ErrorCode::Success => ProtoErrorCode::Success,
        ErrorCode::ConnectionTimeout => ProtoErrorCode::ConnectionTimeout,
        ErrorCode::InvalidAddress => ProtoErrorCode::InvalidAddress,
        ErrorCode::LeaderChanged => ProtoErrorCode::LeaderChanged,
        ErrorCode::JoinError => ProtoErrorCode::JoinError,
        ErrorCode::InvalidResponse => ProtoErrorCode::InvalidResponse,
        ErrorCode::VersionMismatch => ProtoErrorCode::VersionMismatch,
        ErrorCode::DiskFull => ProtoErrorCode::DiskFull,
        ErrorCode::DataCorruption => ProtoErrorCode::DataCorruption,
        ErrorCode::StorageIoError => ProtoErrorCode::StorageIoError,
        ErrorCode::StoragePermissionDenied => ProtoErrorCode::StoragePermissionDenied,
        ErrorCode::KeyNotExist => ProtoErrorCode::KeyNotExist,
        ErrorCode::NotLeader => ProtoErrorCode::NotLeader,
        ErrorCode::StaleOperation => ProtoErrorCode::StaleOperation,
        ErrorCode::InvalidRequest => ProtoErrorCode::InvalidRequest,
        ErrorCode::RateLimited => ProtoErrorCode::RateLimited,
        ErrorCode::ClusterUnavailable => ProtoErrorCode::ClusterUnavailable,
        ErrorCode::ProposeFailed => ProtoErrorCode::ProposeFailed,
        ErrorCode::TermOutdated => ProtoErrorCode::TermOutdated,
        ErrorCode::RetryRequired => ProtoErrorCode::RetryRequired,
        ErrorCode::WatchBufferOverflow => ProtoErrorCode::WatchBufferOverflow,
        ErrorCode::General => ProtoErrorCode::General,
        ErrorCode::Uncategorized => ProtoErrorCode::Uncategorized,
    }
}

#[inline]
pub(crate) fn to_proto_response(r: ClientResponse) -> proto_client::ClientResponse {
    let error_code = core_error_to_proto(r.error);
    let metadata = build_error_metadata(r.leader_hint, r.retry_after_ms);
    let success_result = r.result.map(|payload| match payload {
        ClientResponsePayload::Write(w) => {
            proto_client::client_response::SuccessResult::WriteResult(ProtoWriteResult {
                succeeded: w.succeeded,
            })
        }
        ClientResponsePayload::Read(rd) => {
            proto_client::client_response::SuccessResult::ReadData(ProtoReadResults {
                results: rd
                    .entries
                    .into_iter()
                    .map(|e| ClientResult {
                        key: e.key,
                        value: e.value,
                    })
                    .collect(),
            })
        }
    });
    proto_client::ClientResponse {
        error: error_code as i32,
        metadata,
        success_result,
    }
}

/// Build a fast-path read response from a batch of keys and their optional values.
///
/// Keys with `None` values are omitted from results (key not found).
/// The result slice is positionally ordered and corresponds 1:1 to `keys`.
pub(crate) fn fast_path_batch_read_response(
    keys: &[bytes::Bytes],
    values: Vec<Option<bytes::Bytes>>,
) -> proto_client::ClientResponse {
    let results = keys
        .iter()
        .zip(values)
        .filter_map(|(key, value)| {
            value.map(|v| ClientResult {
                key: key.clone(),
                value: v,
            })
        })
        .collect();
    proto_client::ClientResponse {
        error: 0, // ErrorCode::Success
        metadata: None,
        success_result: Some(proto_client::client_response::SuccessResult::ReadData(
            ProtoReadResults { results },
        )),
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

#[inline]
fn build_error_metadata(
    leader_hint: Option<LeaderHint>,
    retry_after_ms: Option<u64>,
) -> Option<ErrorMetadata> {
    if leader_hint.is_none() && retry_after_ms.is_none() {
        return None;
    }
    Some(ErrorMetadata {
        retry_after_ms,
        leader_id: leader_hint.as_ref().map(|h| h.leader_id.to_string()),
        leader_address: leader_hint.map(|h| h.address),
        debug_message: None,
    })
}

#[allow(dead_code)]
#[inline]
fn parse_leader_hint(meta: &ErrorMetadata) -> Option<LeaderHint> {
    let leader_id = meta.leader_id.as_ref()?.parse::<u32>().ok()?;
    let address = meta.leader_address.clone()?;
    Some(LeaderHint { leader_id, address })
}
