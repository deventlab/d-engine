use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::warn;
use tracing::{debug, error, info};

use crate::Result;
use crate::StateMachineCommand;
use crate::StateMachineWriterOps;
use crate::TypeConfig;
use crate::alias::SMWOF;
use crate::{InternalEvent, SnapshotApplyResult};
use crate::{PreparedSnapshot, SnapshotError};

/// State Machine Worker
///
/// Decouples state machine apply operations from CommitHandler.
/// Runs in independent task, allowing apply_chunk I/O to not block commit processing.
///
/// Sole authority for ordering and outcome of state-machine mutations and snapshot
/// lifecycle decisions (ApplyEntries, InstallSnapshot, CaptureLocalSnapshot — see
/// `StateMachineCommand`). ApplyEntries/InstallSnapshot run inline here; the slow
/// CaptureLocalSnapshot export runs in a supervised background task (#436) that
/// reports back into this same queue (`LocalSnapshotReady`) instead of deciding or
/// replying on its own. Nothing else may call `apply_chunk`/
/// `apply_snapshot_from_file`/`capture_local_snapshot` directly.
///
/// Holds its own clone of `sm_apply_tx` for that self-report, so the channel never
/// closes on its own — dropping every external `StateMachineCommandSender` does NOT
/// stop this Worker. Shutdown must go through `shutdown_signal`.
///
/// InstallSnapshot always waits for the capture/install lock inline — while it
/// waits, subsequent queued commands (including ApplyEntries) wait behind it too.
/// Only CaptureLocalSnapshot is guaranteed not to block the queue.
///
/// On shutdown:
/// - Drains whatever's currently queued in sm_apply_rx (non-blocking) before exiting
/// - Completes all pending work before shutdown
/// - This ensures no data loss and all results are propagated to Leader
pub struct StateMachineWorker<T: TypeConfig> {
    /// Executes the actual mutations.
    state_machine_writer: Arc<SMWOF<T>>,
    /// Own sender — lets a finished capture report back into this queue (#436).
    sm_apply_tx: mpsc::UnboundedSender<StateMachineCommand>,
    /// Incoming command queue.
    sm_apply_rx: mpsc::UnboundedReceiver<StateMachineCommand>,
    /// Reports apply/fatal results back to the Raft loop.
    internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    /// Coordinates CaptureLocalSnapshot vs InstallSnapshot only (#436).
    capture_install_lock: Arc<tokio::sync::Mutex<()>>,
    /// Bumped on each successful install; lets a capture detect it was superseded.
    install_epoch: Arc<AtomicU64>,
    /// Signals graceful shutdown.
    shutdown_signal: watch::Receiver<()>,
    /// For log lines only.
    node_id: u32,
}

impl<T: TypeConfig> StateMachineWorker<T> {
    pub fn new(
        node_id: u32,
        state_machine_writer: Arc<SMWOF<T>>,
        sm_apply_tx: mpsc::UnboundedSender<StateMachineCommand>,
        sm_apply_rx: mpsc::UnboundedReceiver<StateMachineCommand>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            state_machine_writer,
            sm_apply_tx,
            sm_apply_rx,
            internal_event_tx,
            capture_install_lock: Arc::new(tokio::sync::Mutex::new(())),
            install_epoch: Arc::new(AtomicU64::new(0)),
            shutdown_signal,
            node_id,
        }
    }

    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    pub async fn run(self) -> Result<()> {
        debug!("[Node-{}] SM Worker started", self.node_id);
        let node_id = self.node_id;
        let state_machine_writer = self.state_machine_writer;
        let sm_apply_tx = self.sm_apply_tx;
        let internal_event_tx = self.internal_event_tx;
        let mut sm_apply_rx = self.sm_apply_rx;
        let mut shutdown_signal = self.shutdown_signal.clone();
        let mut shutdown = false;
        let capture_install_lock = self.capture_install_lock;
        let install_epoch = self.install_epoch;

        // Tracks the in-flight capture task so shutdown can wait for it (#436).
        let mut in_flight_capture: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            tokio::select! {
                // Shutdown signal received: drain remaining entries before exit
                _ = shutdown_signal.changed() => {
                    info!("[Node-{}] SM Worker shutdown signal received", node_id);
                    shutdown = true;
                }

                // Normal apply processing
                command = sm_apply_rx.recv() => {
                    match command {
                        Some(command) => {
                            Self::handle_command(&node_id, &state_machine_writer, &sm_apply_tx, &internal_event_tx, &capture_install_lock, &install_epoch, &mut in_flight_capture, command).await?;
                                                   }
                        None => {
                            debug!("[Node-{}] SM Worker: apply channel closed", node_id);
                            return Ok(());
                        }
                    }
                }
            }

            // Exit loop after shutdown signal to start graceful drain
            if shutdown {
                break;
            }
        }

        // Graceful drain: process remaining entries after shutdown signal
        debug!("[Node-{}] SM Worker draining pending applies", node_id);
        while let Ok(command) = sm_apply_rx.try_recv() {
            Self::handle_command(
                &node_id,
                &state_machine_writer,
                &sm_apply_tx,
                &internal_event_tx,
                &capture_install_lock,
                &install_epoch,
                &mut in_flight_capture,
                command,
            )
            .await?;
        }

        // Wait for a still-running capture rather than leaving it orphaned — see
        // call_with_timing_guard's doc comment on why storage ops can't just be
        // dropped mid-flight.
        if let Some(handle) = in_flight_capture
            && let Err(e) = handle.await
        {
            error!(
                "[Node-{}] in-flight capture task panicked during shutdown: {:?}",
                node_id, e
            );
        }

        info!("[Node-{}] SM Worker shutdown complete", node_id);
        Ok(())
    }

    /// Single dispatch point for all `StateMachineCommand` variants — the
    /// only place allowed to touch the state machine's destructive APIs.
    async fn handle_command(
        node_id: &u32,
        state_machine_writer: &Arc<SMWOF<T>>,
        sm_apply_tx: &mpsc::UnboundedSender<StateMachineCommand>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        capture_install_lock: &Arc<tokio::sync::Mutex<()>>,
        install_epoch: &Arc<AtomicU64>,
        in_flight_capture: &mut Option<tokio::task::JoinHandle<()>>,
        command: StateMachineCommand,
    ) -> Result<()> {
        match command {
            StateMachineCommand::ApplyEntries { entries } => {
                Self::apply_and_notify(node_id, state_machine_writer, internal_event_tx, entries)
                    .await
            }
            StateMachineCommand::InstallSnapshot { snapshot, response } => {
                // temp_dir must outlive the install; cleaned up when this scope ends.
                let PreparedSnapshot { metadata, temp_dir } = snapshot;
                let dir = temp_dir.path().to_path_buf();
                // Leader-authoritative: always waits for the lock, never skips.
                let _guard = capture_install_lock.lock().await;
                let result = call_with_timing_guard(
                    "install_prepared_snapshot",
                    Duration::from_secs(5),
                    state_machine_writer.install_prepared_snapshot(metadata, dir),
                )
                .await;
                if matches!(result, Ok(SnapshotApplyResult::Applied { .. })) {
                    install_epoch.fetch_add(1, Ordering::Release);
                }
                let _ = response.send(result);
                Ok(())
            }
            StateMachineCommand::CaptureLocalSnapshot { response } => {
                // Spawned so the slow export never blocks this loop (#436).
                let writer = state_machine_writer.clone();
                let lock = capture_install_lock.clone();
                let epoch = install_epoch.clone();
                let self_tx = sm_apply_tx.clone();

                let handle = tokio::spawn(async move {
                    let Ok(_guard) = lock.try_lock() else {
                        let _ = response.send(Err(SnapshotError::CaptureSkipped.into()));
                        return;
                    };
                    let epoch_before = epoch.load(Ordering::Acquire);
                    let captured = call_with_timing_guard(
                        "capture_local_snapshot",
                        Duration::from_secs(5),
                        writer.capture_local_snapshot(),
                    )
                    .await;
                    drop(_guard);
                    // Fresh/stale decided on Worker's own loop, not here (#436).
                    let _ = self_tx.send(StateMachineCommand::LocalSnapshotReady {
                        captured,
                        epoch_before,
                        response,
                    });
                });
                // Keep the older handle if it's still running — that's the one worth waiting for.
                if in_flight_capture.as_ref().is_none_or(|h| h.is_finished()) {
                    *in_flight_capture = Some(handle);
                }
                Ok(())
            }
            StateMachineCommand::LocalSnapshotReady {
                captured,
                epoch_before,
                response,
            } => {
                // Runs on Worker's loop, not the spawned task above — the only way
                // this stays FIFO-ordered with InstallSnapshot (#436).
                let result = match captured {
                    Err(e) => Err(e),
                    Ok(captured) => {
                        // Leader install wins unconditionally — no boundary comparison (#436).
                        if install_epoch.load(Ordering::Acquire) != epoch_before {
                            Err(SnapshotError::CaptureSuperseded.into())
                        } else {
                            Ok(captured)
                        }
                    }
                };
                let _ = response.send(result);
                Ok(())
            }
        }
    }

    async fn apply_and_notify(
        node_id: &u32,
        state_machine_writer: &Arc<SMWOF<T>>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        entries: Vec<d_engine_proto::common::Entry>,
    ) -> Result<()> {
        // Apply all entries at once (no chunking)
        match call_with_timing_guard(
            "apply_chunk",
            Duration::from_secs(1),
            state_machine_writer.apply_chunk(entries),
        )
        .await
        {
            Ok(results) => {
                if let Some(last) = results.last() {
                    debug!(
                        "[Node-{}] SM apply completed: last_index={}",
                        node_id, last.index
                    );

                    // Send via internal_event_tx (P2, unbounded) to prevent priority inversion:
                    // ApplyCompleted is internal (driven by commit) and must not be
                    // starved by external RPCs on event_tx (P4, bounded).
                    if let Err(e) = internal_event_tx.send(InternalEvent::ApplyCompleted {
                        last_index: last.index,
                        results,
                    }) {
                        error!(
                            "[Node-{}] Failed to send ApplyCompleted event: {:?}",
                            node_id, e
                        );
                        return Err(crate::Error::Fatal(format!(
                            "ApplyCompleted event send failed: {e:?}",
                        )));
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!("[Node-{}] SM apply failed: {:?}", node_id, e);

                // Send FatalError via internal_event_tx (unbounded) — must not block waiting for
                // space on the bounded event_tx when the node is already in fatal state.
                if let Err(send_err) = internal_event_tx.send(InternalEvent::FatalError {
                    source: "StateMachine".to_string(),
                    error: format!("{e:?}"),
                }) {
                    error!(
                        "[Node-{}] Failed to send FatalError event: {:?}",
                        node_id, send_err
                    );
                }

                Err(e)
            }
        }
    }
}

/// Runs `call` to completion, warning (not cancelling) if it takes longer than
/// `threshold`. Never drops the underlying future — a `tokio::time::timeout` here
/// would risk aborting a half-done destructive storage operation (e.g. a WriteBatch
/// commit or a column-family drop/rebuild) mid-flight, leaving it in an unknown
/// state. This only makes slow calls observable; it does not bound them — a
/// third-party `StateMachine` implementation is not guaranteed to be fast.
pub(super) async fn call_with_timing_guard<F, T>(
    label: &'static str,
    threshold: Duration,
    call: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(call);
    tokio::select! {
        result = &mut call => result,
        _ = tokio::time::sleep(threshold) => {
            warn!(label, ?threshold, "state machine call exceeded threshold, still waiting for completion");
            call.await
        }
    }
}
