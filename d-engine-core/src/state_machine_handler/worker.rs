use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info};

use crate::RaftEvent;
use crate::Result;
use crate::StateMachineHandler;
use crate::TypeConfig;
use crate::alias::SMHOF;

/// State Machine Worker
///
/// Decouples state machine apply operations from CommitHandler.
/// Runs in independent task, allowing apply_chunk I/O to not block commit processing.
///
/// On shutdown:
/// - Drains remaining entries from sm_apply_rx until channel closes
/// - Completes all pending applies before shutdown
/// - This ensures no data loss and all results are propagated to Leader
pub struct StateMachineWorker<T: TypeConfig> {
    state_machine_handler: Arc<SMHOF<T>>,
    sm_apply_rx: mpsc::UnboundedReceiver<Vec<d_engine_proto::common::Entry>>,
    event_tx: mpsc::Sender<RaftEvent>,
    shutdown_signal: watch::Receiver<()>,
    node_id: u32,
}

impl<T: TypeConfig> StateMachineWorker<T> {
    pub fn new(
        node_id: u32,
        state_machine_handler: Arc<SMHOF<T>>,
        sm_apply_rx: mpsc::UnboundedReceiver<Vec<d_engine_proto::common::Entry>>,
        event_tx: mpsc::Sender<RaftEvent>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            state_machine_handler,
            sm_apply_rx,
            event_tx,
            shutdown_signal,
            node_id,
        }
    }

    pub async fn run(self) -> Result<()> {
        debug!("[Node-{}] SM Worker started", self.node_id);
        let node_id = self.node_id;
        let state_machine_handler = self.state_machine_handler;
        let event_tx = self.event_tx;
        let mut sm_apply_rx = self.sm_apply_rx;
        let mut shutdown_signal = self.shutdown_signal.clone();
        let mut shutdown = false;

        loop {
            tokio::select! {
                // Shutdown signal received: drain remaining entries before exit
                _ = shutdown_signal.changed() => {
                    info!("[Node-{}] SM Worker shutdown signal received", node_id);
                    shutdown = true;
                }

                // Normal apply processing
                entries = sm_apply_rx.recv() => {
                    match entries {
                        Some(entries) => {
                            Self::apply_and_notify(&node_id, &state_machine_handler, &event_tx, entries).await?;
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
        while let Ok(entries) = sm_apply_rx.try_recv() {
            Self::apply_and_notify(&node_id, &state_machine_handler, &event_tx, entries).await?;
        }

        info!("[Node-{}] SM Worker shutdown complete", node_id);
        Ok(())
    }

    async fn apply_and_notify(
        node_id: &u32,
        state_machine_handler: &Arc<SMHOF<T>>,
        event_tx: &mpsc::Sender<RaftEvent>,
        entries: Vec<d_engine_proto::common::Entry>,
    ) -> Result<()> {
        // Apply all entries at once (no chunking)
        match state_machine_handler.apply_chunk(entries).await {
            Ok(results) => {
                if let Some(last) = results.last() {
                    debug!(
                        "[Node-{}] SM apply completed: last_index={}",
                        node_id, last.index
                    );

                    // let send_start = std::time::Instant::now();
                    if let Err(e) = event_tx
                        .send(RaftEvent::ApplyCompleted {
                            last_index: last.index,
                            results,
                        })
                        .await
                    {
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

                // Classify error and send FatalError event if needed
                if Self::is_fatal_error(&e) {
                    let error_msg = format!("{e:?}");
                    if let Err(send_err) = event_tx
                        .send(RaftEvent::FatalError {
                            source: "StateMachine".to_string(),
                            error: error_msg,
                        })
                        .await
                    {
                        error!(
                            "[Node-{}] Failed to send FatalError event: {:?}",
                            node_id, send_err
                        );
                    }
                }

                Err(e)
            }
        }
    }

    /// Check if error should trigger node shutdown.
    /// Conservative: all SM errors treated as fatal (future: distinguish error types).
    fn is_fatal_error(_error: &crate::Error) -> bool {
        true // All errors are fatal - SM apply failures indicate unrecoverable state
    }
}
