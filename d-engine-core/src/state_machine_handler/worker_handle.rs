//! Owns the life of the state machine worker: built unstarted, started once, stopped through
//! its own queue so everything queued before the stop request is applied first.
//!
//! The handle's sender is private and only ever sends [`StateMachineCommand::Shutdown`]: whoever
//! owns the handle can start and stop the worker but cannot feed it.

use std::fmt;
use std::io;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::error;
use tracing::info;

use super::StateMachineCommand;
use super::StateMachineWorker;
use crate::TypeConfig;

/// Why [`SmWorkerHandle::stop_and_join`] could not confirm a clean stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopError {
    /// The worker did not finish within the given time. Its thread keeps running and a later
    /// call may wait for it again.
    TimedOut(Duration),
    /// The worker thread panicked.
    Panicked,
    /// The worker ended with an error (a failed apply, or a failed `close`).
    Failed(String),
}

impl fmt::Display for StopError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::TimedOut(t) => write!(f, "state machine worker did not stop within {t:?}"),
            Self::Panicked => write!(f, "state machine worker thread panicked"),
            Self::Failed(e) => write!(f, "state machine worker failed: {e}"),
        }
    }
}

impl std::error::Error for StopError {}

enum State<T: TypeConfig> {
    Pending(StateMachineWorker<T>),
    Running {
        join: std::thread::JoinHandle<crate::Result<()>>,
        done: watch::Receiver<()>,
    },
    Stopped,
}

impl<T: TypeConfig> State<T> {
    fn name(&self) -> &'static str {
        match self {
            Self::Pending(_) => "Pending",
            Self::Running { .. } => "Running",
            Self::Stopped => "Stopped",
        }
    }
}

pub struct SmWorkerHandle<T: TypeConfig> {
    /// Only ever used to send `Shutdown`.
    tx: mpsc::UnboundedSender<StateMachineCommand>,
    state: Mutex<State<T>>,
}

impl<T: TypeConfig> SmWorkerHandle<T> {
    /// Wraps an unstarted worker. `tx` must be a sender of the queue the worker reads from.
    pub fn new(
        worker: StateMachineWorker<T>,
        tx: mpsc::UnboundedSender<StateMachineCommand>,
    ) -> Self {
        Self {
            tx,
            state: Mutex::new(State::Pending(worker)),
        }
    }

    /// Starts the worker on a dedicated thread (`sm-apply-<node_id>`) that drives it on the
    /// current Tokio runtime. Only the first call has an effect.
    ///
    /// # Errors
    /// No Tokio runtime is available, or the OS refused to create the thread.
    pub fn start(&self) -> io::Result<()> {
        let mut state = self.lock();
        let State::Pending(_) = &*state else {
            return Ok(());
        };
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| io::Error::other(format!("no Tokio runtime to run the worker: {e}")))?;
        let State::Pending(worker) = std::mem::replace(&mut *state, State::Stopped) else {
            unreachable!("checked Pending above while holding the lock");
        };

        let node_id = worker.node_id();
        // Dropped when the thread ends, however it ends (return or panic): the closed channel
        // is the "done" signal, `stop_and_join` waits for it.
        let (done_tx, done_rx) = watch::channel(());
        let join =
            std::thread::Builder::new().name(format!("sm-apply-{node_id}")).spawn(move || {
                let _done = done_tx;
                runtime.block_on(async move {
                    let result = worker.run().await;
                    match &result {
                        Ok(()) => info!("[Node-{node_id}] state machine worker exit"),
                        Err(e) => {
                            error!("[Node-{node_id}] state machine worker exit with error: {e:?}")
                        }
                    }
                    result
                })
            })?;
        *state = State::Running {
            join,
            done: done_rx,
        };
        Ok(())
    }

    /// Asks the worker to stop after everything already queued, and waits up to `timeout` for
    /// its thread to end. A worker that was never started is simply discarded. Safe to call more
    /// than once; after a clean stop it returns `Ok` immediately.
    pub async fn stop_and_join(
        &self,
        timeout: Duration,
    ) -> Result<(), StopError> {
        // Fails only if the worker is already gone, which is the goal anyway.
        let _ = self.tx.send(StateMachineCommand::Shutdown);

        let mut done = {
            let mut state = self.lock();
            match &*state {
                State::Running { done, .. } => done.clone(),
                State::Pending(_) | State::Stopped => {
                    if let State::Pending(worker) = std::mem::replace(&mut *state, State::Stopped) {
                        // never started: close what it would have closed
                        return worker.close().map_err(|e| StopError::Failed(format!("{e:?}")));
                    }
                    return Ok(());
                }
            }
        };

        if tokio::time::timeout(timeout, done.changed()).await.is_err() {
            return Err(StopError::TimedOut(timeout));
        }

        match std::mem::replace(&mut *self.lock(), State::Stopped) {
            State::Running { join, .. } => join
                .join()
                .map_err(|_| StopError::Panicked)?
                .map_err(|e| StopError::Failed(format!("{e:?}"))),
            State::Pending(_) | State::Stopped => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // The state only moves between plain variants, so a poisoned lock is still consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: TypeConfig> Drop for SmWorkerHandle<T> {
    /// Last resort for an owner that is dropped without calling `stop_and_join` (task aborted,
    /// panic): ask the worker to stop. Does not wait.
    fn drop(&mut self) {
        let _ = self.tx.send(StateMachineCommand::Shutdown);
        if let State::Pending(worker) = std::mem::replace(
            self.state.get_mut().unwrap_or_else(PoisonError::into_inner),
            State::Stopped,
        ) && let Err(e) = worker.close()
        {
            error!("state machine close failed for a worker that never started: {e:?}");
        }
    }
}

impl<T: TypeConfig> fmt::Debug for SmWorkerHandle<T> {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("SmWorkerHandle").field("state", &self.lock().name()).finish()
    }
}
