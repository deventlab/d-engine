//! ReadActor — dedicated read task for Eventual and LeaseRead fast path.
//!
//! `run_read_actor` is the TiKV LocalReader equivalent: it owns `Arc<SM>` as sole
//! holder in the read path, checks `ReadLease` with a single atomic load, and serves
//! reads without entering the Raft loop.
//!
//! # Lifecycle guarantee
//!
//! The caller drops `read_tx` to signal shutdown. `run_read_actor` exits when
//! `read_rx` is drained and closed, at which point `Arc<SM>` is dropped and the
//! RocksDB LOCK is released — before `stop()` signals the Raft loop.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::ReadLease;
use crate::StateMachine;
use crate::config::ReadConsistencyPolicy;
use crate::now_ms;

/// A single read request sent from `EmbeddedClient` to the `ReadActor`.
///
/// Only `EventualConsistency` and `LeaseRead` are valid here.
/// `LinearizableRead` must go through `cmd_tx` (Raft readIndex protocol).
pub struct ReadCmd {
    pub key: Bytes,
    pub consistency: ReadConsistencyPolicy,
    pub reply: oneshot::Sender<Result<Option<Bytes>, ReadActorError>>,
}

/// Error returned by the ReadActor to the caller.
///
/// `LeaseInvalid` and `SmStopped` are recoverable: the caller should
/// fall back to `cmd_tx` for a Raft-backed read.
#[derive(Debug)]
pub enum ReadActorError {
    /// Lease has been revoked or expired. Retry via cmd_tx.
    LeaseInvalid,
    /// State machine is not running (node is stopping). Retry via cmd_tx.
    SmStopped,
    /// SM returned an error.
    SmError(String),
}

/// Run the ReadActor loop until `read_rx` is closed.
///
/// Designed to be spawned as an independent task:
/// ```ignore
/// let handle = tokio::spawn(run_read_actor(read_rx, lease, sm, max_drain));
/// ```
///
/// `max_drain` caps how many commands are drained per wake-up (mirrors
/// `batching.max_batch_size` in the Raft loop). After the first `recv().await`
/// unblocks, the actor drains up to `max_drain` pending commands with
/// `try_recv()` before yielding back to the executor — amortizing the
/// task-switch cost across batches of requests.
///
/// When the caller drops `read_tx`, the loop exits and `Arc<SM>` is released.
pub async fn run_read_actor<SM>(
    mut read_rx: mpsc::Receiver<ReadCmd>,
    lease: Arc<ReadLease>,
    sm: Arc<SM>,
    max_drain: usize,
) where
    SM: StateMachine,
{
    while let Some(cmd) = read_rx.recv().await {
        // Process the command that woke us up.
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
    // read_rx closed → sm drops here if this is the last Arc<SM> reference
}

fn serve_read<SM: StateMachine>(
    cmd: &ReadCmd,
    lease: &Arc<ReadLease>,
    sm: &Arc<SM>,
) -> Result<Option<Bytes>, ReadActorError> {
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

    sm.get(cmd.key.as_ref()).map_err(|e| ReadActorError::SmError(e.to_string()))
}
