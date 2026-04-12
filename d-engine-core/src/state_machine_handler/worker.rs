use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info};

use crate::Result;
use crate::RoleEvent;
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
    role_tx: mpsc::UnboundedSender<RoleEvent>,
    shutdown_signal: watch::Receiver<()>,
    node_id: u32,
}

impl<T: TypeConfig> StateMachineWorker<T> {
    pub fn new(
        node_id: u32,
        state_machine_handler: Arc<SMHOF<T>>,
        sm_apply_rx: mpsc::UnboundedReceiver<Vec<d_engine_proto::common::Entry>>,
        role_tx: mpsc::UnboundedSender<RoleEvent>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            state_machine_handler,
            sm_apply_rx,
            role_tx,
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
        let state_machine_handler = self.state_machine_handler;
        let role_tx = self.role_tx;
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
                            Self::apply_and_notify(&node_id, &state_machine_handler, &role_tx, entries).await?;
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
            Self::apply_and_notify(&node_id, &state_machine_handler, &role_tx, entries).await?;
        }

        info!("[Node-{}] SM Worker shutdown complete", node_id);
        Ok(())
    }

    async fn apply_and_notify(
        node_id: &u32,
        state_machine_handler: &Arc<SMHOF<T>>,
        role_tx: &mpsc::UnboundedSender<RoleEvent>,
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

                    // Send via role_tx (P2, unbounded) to prevent priority inversion:
                    // ApplyCompleted is internal (driven by commit) and must not be
                    // starved by external RPCs on event_tx (P4, bounded).
                    if let Err(e) = role_tx.send(RoleEvent::ApplyCompleted {
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

                // Send FatalError via role_tx (unbounded) — must not block waiting for
                // space on the bounded event_tx when the node is already in fatal state.
                if let Err(send_err) = role_tx.send(RoleEvent::FatalError {
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
