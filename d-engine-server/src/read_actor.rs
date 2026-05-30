//! ReadActor — dedicated read task for Eventual and LeaseRead fast path.
//!
//! Lives in `d-engine-server`, not `d-engine-core`, because it is a
//! performance optimisation component, not a Raft protocol component.
//! Removing it does not affect correctness of consensus, election, or
//! log replication.  (Contrast: TiKV's LocalReader lives in tikv/src/server/,
//! not in raft-rs.)
//!
//! # Routing contract
//!
//! ```text
//! Eventual / LeaseRead  ──►  ReadActor  ──►  SM.get_multi()   (no Raft loop)
//! Linearizable          ──►  cmd_tx     ──►  Raft loop         (readIndex)
//! ```
//!
//! # Lifecycle guarantee
//!
//! The caller drops `read_tx` to signal shutdown.  `run_read_actor` exits when
//! `read_rx` is drained and closed, at which point `Arc<SM>` is dropped and the
//! RocksDB LOCK file is released — before `stop()` signals the Raft loop.

use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::ReadLease;
use d_engine_core::StateMachine;
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_core::now_ms;
use tokio::sync::{mpsc, oneshot};

// ── ReadCmd ────────────────────────────────────────────────────────────────────

/// A read request dispatched from `ReadHandle` to `ReadActor`.
///
/// Only `EventualConsistency` and `LeaseRead` are valid; `LinearizableRead`
/// must go through `cmd_tx` (Raft readIndex protocol).
///
/// # Ordering guarantee
///
/// The reply `Vec` is guaranteed to be the same length as `keys` and
/// positionally corresponds to the input key order.
pub(crate) struct ReadCmd {
    pub keys: Vec<Bytes>,
    pub consistency: ReadConsistencyPolicy,
    pub reply: oneshot::Sender<Result<Vec<Option<Bytes>>, ReadActorError>>,
}

// ── ReadActorError ─────────────────────────────────────────────────────────────

/// Error returned by the ReadActor to the caller.
///
/// `LeaseInvalid` and `SmStopped` are recoverable: fall back to `cmd_tx`.
/// `SmError` is a hard failure; do NOT retry via cmd_tx.
#[derive(Debug)]
pub(crate) enum ReadActorError {
    /// Lease has been revoked or expired. Retry via cmd_tx.
    LeaseInvalid,
    /// State machine is not running (node is stopping). Retry via cmd_tx.
    SmStopped,
    /// SM returned a hard error.
    SmError(String),
}

// ── run_read_actor ─────────────────────────────────────────────────────────────

/// Run the ReadActor loop until `read_rx` is closed.
///
/// Designed to be spawned as an independent task:
/// ```ignore
/// tokio::spawn(run_read_actor(read_rx, lease, sm, max_drain));
/// ```
///
/// `max_drain` caps how many commands are drained per wake-up (mirrors
/// `batching.max_batch_size` in the Raft loop).  After the first `recv().await`
/// unblocks, the actor drains up to `max_drain` pending commands with
/// `try_recv()` before yielding — amortising the task-switch cost across
/// request batches.
pub(crate) async fn run_read_actor<SM>(
    mut read_rx: mpsc::Receiver<ReadCmd>,
    lease: Arc<ReadLease>,
    sm: Arc<SM>,
    max_drain: usize,
) where
    SM: StateMachine,
{
    while let Some(cmd) = read_rx.recv().await {
        let reply = serve_read(&cmd, &lease, &sm);
        let _ = cmd.reply.send(reply);

        // Drain commands that arrived while processing, without going back to
        // sleep — same pattern as Raft::drain_client_cmds().
        let mut count = 1;
        while count < max_drain {
            match read_rx.try_recv() {
                Ok(cmd) => {
                    let reply = serve_read(&cmd, &lease, &sm);
                    let _ = cmd.reply.send(reply);
                    count += 1;
                }
                Err(_) => break,
            }
        }
    }
    // read_rx closed → Arc<SM> drops here, releasing RocksDB LOCK
}

fn serve_read<SM>(
    cmd: &ReadCmd,
    lease: &Arc<ReadLease>,
    sm: &Arc<SM>,
) -> Result<Vec<Option<Bytes>>, ReadActorError>
where
    SM: StateMachine,
{
    match cmd.consistency {
        ReadConsistencyPolicy::EventualConsistency => {
            if !sm.is_running() {
                return Err(ReadActorError::SmStopped);
            }
        }
        ReadConsistencyPolicy::LeaseRead => {
            if !lease.is_valid(now_ms()) || !sm.is_running() {
                return Err(ReadActorError::LeaseInvalid);
            }
        }
        // LinearizableRead must not reach ReadActor — guard defensively
        _ => return Err(ReadActorError::LeaseInvalid),
    }

    sm.get_multi(&cmd.keys).map_err(|e| ReadActorError::SmError(e.to_string()))
}
