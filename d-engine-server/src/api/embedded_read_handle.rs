//! EmbeddedReadHandle — zero-channel read path for embedded mode.
//!
//! Replaces `ReadHandle` (which routes through the ReadActor channel) for
//! `EmbeddedClient`.  Eventual and LeaseRead call `SM::get_multi()` directly —
//! no oneshot allocation, no task context switch, no channel round-trip.
//!
//! # Routing
//!
//! ```text
//! Eventual / LeaseRead (valid)  →  sm.get_multi()       (direct, zero channel)
//! Eventual / LeaseRead (error)  →  cmd_tx fallback       (SM stopped or error)
//! LeaseRead (invalid lease)     →  cmd_tx fallback
//! LinearizableRead              →  cmd_tx always         (Raft readIndex)
//! ```
//!
//! # Shutdown safety
//!
//! After `EmbeddedEngine::stop()` calls `sm.close_db()`, `sm.get_multi()` returns
//! `NotServing`.  The handle falls through to `cmd_tx` which is also closed at
//! that point, so callers receive a `channel_closed_error` — the same behaviour
//! as any other shutdown scenario.

use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftOneshot;
use d_engine_core::ReadLease;
use d_engine_core::StateMachine;
use d_engine_core::TypeConfig;
use d_engine_core::client::{ClientApiResult, ClientReadRequest, ErrorCode};
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_core::now_ms;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::standalone_read_handle::{
    channel_closed_error, extract_read_payload, map_error_response, server_error, timeout_error,
};

// ── EmbeddedReadHandle ────────────────────────────────────────────────────────

/// Direct-SM read handle for embedded mode.
///
/// Cloneable — `EmbeddedClient` and its clones each hold their own instance.
/// Cloning is cheap: all fields are `Arc` or `mpsc::Sender` (reference-counted).
pub(crate) struct EmbeddedReadHandle<T: TypeConfig> {
    sm: Arc<T::SM>,
    lease: Arc<ReadLease>,
    pub(crate) cmd_tx: mpsc::Sender<d_engine_core::ClientCmd>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: TypeConfig> Clone for EmbeddedReadHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sm: Arc::clone(&self.sm),
            lease: Arc::clone(&self.lease),
            cmd_tx: self.cmd_tx.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: TypeConfig> EmbeddedReadHandle<T> {
    pub(crate) fn new(
        sm: Arc<T::SM>,
        lease: Arc<ReadLease>,
        cmd_tx: mpsc::Sender<d_engine_core::ClientCmd>,
    ) -> Self {
        Self {
            sm,
            lease,
            cmd_tx,
            _phantom: PhantomData,
        }
    }

    /// Single-key read.  Convenience wrapper around [`Self::get_batch`].
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

    /// Multi-key read with consistency routing.
    ///
    /// For `Eventual` and `LeaseRead` (when the lease is valid), all keys are
    /// served from a single `SM::get_multi()` snapshot — no Raft round-trip.
    /// On any SM error, or when the lease is invalid, the request falls through
    /// to `cmd_tx` (Raft readIndex protocol).
    pub async fn get_batch(
        &self,
        keys: &[Bytes],
        consistency: ReadConsistencyPolicy,
        client_id: u32,
        timeout: Duration,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
        match consistency {
            ReadConsistencyPolicy::EventualConsistency => {
                if let Ok(values) = self.sm.get_multi(keys) {
                    return Ok(values);
                }
                // SM stopped or hard error → fall through to cmd_tx
            }
            ReadConsistencyPolicy::LeaseRead => {
                if self.lease.is_valid(now_ms())
                    && let Ok(values) = self.sm.get_multi(keys)
                {
                    return Ok(values);

                    // SM stopped or hard error → fall through
                }
                // invalid lease → fall through to cmd_tx
            }
            ReadConsistencyPolicy::LinearizableRead => {
                // always cmd_tx — Raft readIndex protocol
            }
        }

        self.cmd_tx_path(keys, consistency, client_id, timeout).await
    }

    // ── cmd_tx fallback ───────────────────────────────────────────────────────

    async fn cmd_tx_path(
        &self,
        keys: &[Bytes],
        consistency: ReadConsistencyPolicy,
        client_id: u32,
        timeout: Duration,
    ) -> ClientApiResult<Vec<Option<Bytes>>> {
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
#[path = "embedded_read_handle_test.rs"]
mod tests;
