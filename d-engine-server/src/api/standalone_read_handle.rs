//! StandaloneReadHandle — read routing for standalone (gRPC) mode.
//!
//! Routes reads through the ReadActor channel for Eventual/LeaseRead fast path,
//! falling back to `cmd_tx` (Raft readIndex) for LinearizableRead or on error.
//!
//! For embedded mode, use `EmbeddedReadHandle` which calls the SM directly
//! without any channel overhead.

use std::time::Duration;

use bytes::Bytes;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftOneshot;
use d_engine_core::client::{
    ClientApiError, ClientApiResult, ClientReadRequest, ClientResponsePayload, ErrorCode,
    LeaderHint, ReadResults,
};
use d_engine_core::config::ReadConsistencyPolicy;
use tokio::sync::{mpsc, oneshot};

use crate::read_actor::{ReadActorError, ReadCmd};

// ── Error helpers ─────────────────────────────────────────────────────────────

pub(crate) fn channel_closed_error() -> ClientApiError {
    ClientApiError::Network {
        code: ErrorCode::ConnectionTimeout,
        message: "Channel closed, node may be shutting down".to_string(),
        retry_after_ms: None,
        leader_hint: None,
    }
}

pub(crate) fn timeout_error(duration: Duration) -> ClientApiError {
    ClientApiError::Network {
        code: ErrorCode::ConnectionTimeout,
        message: format!("Operation timed out after {duration:?}"),
        retry_after_ms: Some(1000),
        leader_hint: None,
    }
}

pub(crate) fn server_error(msg: String) -> ClientApiError {
    ClientApiError::Business {
        code: ErrorCode::Uncategorized,
        message: msg,
        required_action: None,
    }
}

pub(crate) fn not_leader_error(
    leader_id: Option<String>,
    leader_address: Option<String>,
    retry_after_ms: Option<u64>,
) -> ClientApiError {
    let message = match (&leader_address, &leader_id) {
        (Some(addr), _) => format!("Not leader, try leader at: {addr}"),
        (None, Some(id)) => format!("Not leader, leader_id: {id}"),
        (None, None) => "Not leader".to_string(),
    };
    let leader_hint = match (&leader_id, &leader_address) {
        (Some(id_str), Some(addr)) => id_str.parse::<u32>().ok().map(|id| LeaderHint {
            leader_id: id,
            address: addr.clone(),
        }),
        _ => None,
    };
    ClientApiError::Network {
        code: ErrorCode::NotLeader,
        message,
        retry_after_ms: retry_after_ms.or(Some(100)),
        leader_hint,
    }
}

pub(crate) fn map_error_response(
    error: ErrorCode,
    leader_hint: Option<LeaderHint>,
    retry_after_ms: Option<u64>,
) -> ClientApiError {
    match error {
        ErrorCode::NotLeader => {
            let (leader_id, leader_address) = if let Some(hint) = leader_hint {
                (Some(hint.leader_id.to_string()), Some(hint.address))
            } else {
                (None, None)
            };
            not_leader_error(leader_id, leader_address, retry_after_ms)
        }
        _ => server_error(format!("Error code: {error:?}")),
    }
}

/// Unwrap a `ClientResponsePayload` as `ReadResults`.
pub(crate) fn extract_read_payload(
    result: Option<ClientResponsePayload>
) -> ClientApiResult<ReadResults> {
    match result {
        Some(ClientResponsePayload::Read(r)) => Ok(r),
        Some(ClientResponsePayload::Write(_)) => Err(ClientApiError::Protocol {
            code: ErrorCode::InvalidResponse,
            message: "expected ReadData payload, got WriteResult".to_string(),
            supported_versions: None,
        }),
        None => Err(ClientApiError::Protocol {
            code: ErrorCode::InvalidResponse,
            message: "expected ReadData payload, got None".to_string(),
            supported_versions: None,
        }),
    }
}

// ── StandaloneReadHandle ──────────────────────────────────────────────────────

/// Routes read requests to the ReadActor fast path or `cmd_tx` for standalone gRPC mode.
///
/// Cloneable — the gRPC `Node` handler holds a clone per request.
/// The underlying channels are shared (`mpsc::Sender` is cheaply cloneable).
#[derive(Clone)]
pub(crate) struct StandaloneReadHandle {
    /// ReadActor channel. `None` if node was started without a ReadActor.
    pub(crate) read_tx: Option<mpsc::Sender<ReadCmd>>,
    /// Raft command channel for the cmd_tx fallback path.
    pub(crate) cmd_tx: mpsc::Sender<d_engine_core::ClientCmd>,
}

impl StandaloneReadHandle {
    /// Create a `StandaloneReadHandle` wiring the fast-path sender and the Raft cmd channel.
    pub(crate) fn new(
        read_tx: Option<mpsc::Sender<ReadCmd>>,
        cmd_tx: mpsc::Sender<d_engine_core::ClientCmd>,
    ) -> Self {
        Self { read_tx, cmd_tx }
    }

    /// Route a single-key read. Convenience wrapper around [`Self::get_batch`].
    ///
    /// # Routing rules
    /// | Policy         | Path                       | On failure            |
    /// |----------------|----------------------------|-----------------------|
    /// | Eventual/Lease | ReadActor (fast path)      | fall through to cmd_tx|
    /// | Linearizable   | cmd_tx always              | —                     |
    /// | SmError        | direct return              | no fallback           |
    ///
    /// Note: Not called internally; exposed as public API for callers to use.
    #[allow(dead_code)]
    pub async fn get(
        &self,
        key: &[u8],
        consistency: ReadConsistencyPolicy,
        client_id: u32,
        timeout: Duration,
    ) -> ClientApiResult<Option<Bytes>> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut results = self.get_batch(&[key_bytes], consistency, client_id, timeout).await?;
        Ok(results.pop().flatten())
    }

    /// Route a multi-key read atomically.
    ///
    /// For `Eventual` / `LeaseRead`, all keys are sent to ReadActor in a single
    /// `ReadCmd` and read from one consistent snapshot.  On fallback or
    /// `Linearizable`, the full key list is forwarded to `cmd_tx` as a single
    /// `ClientCmd::Read`.
    ///
    /// The returned `Vec` is the same length as `keys` and positionally ordered.
    pub async fn get_batch(
        &self,
        keys: &[Bytes],
        consistency: ReadConsistencyPolicy,
        client_id: u32,
        timeout: Duration,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        // Fast path: Eventual and LeaseRead bypass cmd_tx via ReadActor.
        if let Some(read_tx) = &self.read_tx
            && matches!(
                consistency,
                ReadConsistencyPolicy::EventualConsistency | ReadConsistencyPolicy::LeaseRead
            )
        {
            let (reply_tx, reply_rx) = oneshot::channel();
            if read_tx
                .send(ReadCmd {
                    keys: keys.to_vec(),
                    consistency: consistency.clone(),
                    reply: reply_tx,
                })
                .await
                .is_ok()
            {
                match tokio::time::timeout(timeout, reply_rx).await {
                    Ok(Ok(Ok(values))) => return Ok(values),
                    Ok(Ok(Err(ReadActorError::SmError(e)))) => return Err(server_error(e)),
                    // LeaseInvalid / SmStopped → fall through to cmd_tx
                    Ok(Ok(Err(ReadActorError::LeaseInvalid | ReadActorError::SmStopped))) => {}
                    // ReadActor exited without replying → fall through
                    Ok(Err(_)) => {}
                    // ReadActor stalled beyond timeout → fall through to cmd_tx
                    Err(_timeout) => {}
                }
            }
            // send() failed (channel closed) → fall through to cmd_tx
        }

        // cmd_tx path (Raft readIndex — always correct, Linearizable always arrives here)
        let request = ClientReadRequest {
            client_id,
            keys: keys.to_vec(),
            consistency_policy: Some(consistency),
        };
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.cmd_tx
            .send(d_engine_core::ClientCmd::Read(request, resp_tx))
            .await
            .map_err(|_| channel_closed_error())?;

        let result = tokio::time::timeout(timeout, resp_rx)
            .await
            .map_err(|_| timeout_error(timeout))?
            .map_err(|_| channel_closed_error())?;

        let response =
            result.map_err(|status| server_error(format!("RPC error: {}", status.message())))?;

        if response.error != ErrorCode::Success {
            return Err(map_error_response(
                response.error,
                response.leader_hint,
                response.retry_after_ms,
            ));
        }

        let read_results = extract_read_payload(response.result)?;
        let entry_map: std::collections::HashMap<Bytes, Bytes> =
            read_results.entries.into_iter().map(|e| (e.key, e.value)).collect();
        Ok(keys.iter().map(|k| entry_map.get(k).cloned()).collect())
    }
}

#[cfg(test)]
#[path = "standalone_read_handle_test.rs"]
mod tests;
