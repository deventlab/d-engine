use crate::Error;
use crate::InternalEvent;
use crate::LogStore;
use crate::Result;
use d_engine_proto::common::LogId;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::error;

/// Schedules physical fsync calls — batches concurrent requests into one
/// flush() at a time. The final content check is `RaftLogCore::
/// try_advance_durable_index`; this only orders the pending mark term-first.
pub(super) struct FsyncWorker<L: LogStore> {
    node_id: u32,
    inflight: AtomicBool,
    /// Highest `(term, index)` awaiting fsync. Term-first: a newer term's mark
    /// wins over an older term's higher index, so a stale pre-truncation submit
    /// cannot swallow the valid post-truncation one.
    pending_max: Mutex<LogId>,
    pending_replies: Mutex<Vec<oneshot::Sender<Result<()>>>>,

    /// Bumped on truncation/reset. A round whose start predates the bump errs
    /// its queued flush() replies instead of reporting a superseded result.
    generation: AtomicU64,

    log_store: Arc<L>,
    poisoned: Arc<AtomicBool>,
    internal_event_tx: Option<mpsc::UnboundedSender<InternalEvent>>,

    // How far the *previous* completed fsync round reached
    last_synced_index: AtomicU64,
}

impl<L: LogStore> FsyncWorker<L> {
    pub(super) fn new(
        node_id: u32,
        log_store: Arc<L>,
        poisoned: Arc<AtomicBool>,
        internal_event_tx: Option<mpsc::UnboundedSender<InternalEvent>>,
    ) -> Self {
        Self {
            node_id,
            inflight: AtomicBool::new(false),
            pending_max: Mutex::new(LogId::default()),
            pending_replies: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
            log_store,
            poisoned,
            internal_event_tx,
            last_synced_index: AtomicU64::new(0),
        }
    }

    /// Called from the IO thread on every wakeup. Records new work and, if no
    /// fsync task is currently running, kicks one off. Never spawns a second
    /// concurrent task — additional calls while one is in flight just update
    /// the pending state for it to pick up next round.
    pub(super) fn submit(
        self: &Arc<Self>,
        mark: LogId,
        replies: Vec<oneshot::Sender<Result<()>>>,
    ) {
        if mark.index > 0 {
            let mut p = self.pending_max.lock();
            if (mark.term, mark.index) > (p.term, p.index) {
                *p = mark;
            }
        }
        if !replies.is_empty() {
            self.pending_replies.lock().extend(replies);
        }

        if self
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            metrics::counter!("core.raft.fsync.coalesced_submit").increment(1);
            return; // Already running — it will pick up what we just recorded.
        }
        metrics::counter!("core.raft.fsync.fresh_round").increment(1);

        metrics::gauge!("core.raft.fsync.inflight").set(1.0);

        let worker = Arc::clone(self);
        tokio::task::spawn_blocking(move || worker.run_until_caught_up());
    }

    /// Runs on the blocking pool. Keeps fsyncing and re-checking for newly
    /// accumulated work until there's nothing left, then clears `inflight`.
    pub(super) fn run_until_caught_up(&self) {
        loop {
            let gen_at_start = self.generation.load(Ordering::Acquire);

            let mark = std::mem::take(&mut *self.pending_max.lock());
            let replies = std::mem::take(&mut *self.pending_replies.lock());

            if self.is_poisoned() {
                for reply in replies {
                    let _ = reply.send(Err(Error::Fatal("raft log storage is poisoned".into())));
                }
                self.inflight.store(false, Ordering::Release);
                metrics::gauge!("core.raft.fsync.inflight").set(0.0);
                return;
            }

            if mark.index == 0 && replies.is_empty() {
                self.inflight.store(false, Ordering::Release);
                metrics::gauge!("core.raft.fsync.inflight").set(0.0);
                // Re-check: something may have slipped in between the swap
                // above and clearing `inflight`. If so, re-arm.
                if (self.pending_max.lock().index > 0 || !self.pending_replies.lock().is_empty())
                    && self
                        .inflight
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    metrics::gauge!("core.raft.fsync.inflight").set(1.0);
                    continue;
                }
                return;
            }

            if mark.index > 0 {
                let prev = self.last_synced_index.load(Ordering::Acquire);
                let batch_size = mark.index.saturating_sub(prev);
                metrics::histogram!("core.raft.fsync.batch_entries").record(batch_size as f64);
            }

            let result = if self.log_store.is_write_durable() {
                Ok(())
            } else {
                let t0 = std::time::Instant::now();
                let r = self.log_store.flush();
                let elapsed = t0.elapsed();
                metrics::histogram!("core.raft.fsync.duration_ms")
                    .record(elapsed.as_secs_f64() * 1_000.0);
                metrics::counter!("core.raft.fsync.busy_nanos_total")
                    .increment(elapsed.as_nanos() as u64);
                r
            };

            // Skip replying if this round is already known stale.
            if self.generation.load(Ordering::Acquire) != gen_at_start {
                for reply in replies {
                    let _ = reply.send(Err(crate::Error::Fatal(
                        "stale fsync generation, superseded by reset".into(),
                    )));
                }
                continue; // do NOT call advance_durable_and_notify
            }

            match &result {
                Ok(()) => {
                    self.last_synced_index.store(mark.index, Ordering::Release);
                    self.notify_fsync_completed(mark);
                }
                Err(e) => {
                    // One fsync failure = fatal, no threshold, no retry-and-hope.
                    // Durability state is now unknown, this node
                    // must stop promising any further persistence.
                    self.mark_poisoned_and_notify(format!("fsync failed: {e:?}")); // mirrors advance_durable_and_notify's pattern
                    error!(
                        ?self.node_id,
                        "WAL fsync failed at index {}: {:?} — node entering fatal state",
                        mark.index, e
                    );
                }
            }

            for reply in replies {
                let _ = reply.send(match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(Error::Fatal(format!("WAL fsync failed: {:?}", e))),
                });
            }
        }
    }

    /// Called from reset_internal() before clearing in-memory state.
    /// Bumps generation to fence the in-flight physical flush (if any),
    /// AND drains anything already queued but not yet picked up by a
    /// flush round — that queued data was submitted before reset and
    /// must not be silently adopted by the next round.
    pub(super) fn fence_reset(&self) {
        *self.pending_max.lock() = LogId::default();
        let stale = std::mem::take(&mut *self.pending_replies.lock());
        for reply in stale {
            let _ = reply.send(Err(Error::Fatal(
                "stale fsync generation, superseded by reset".into(),
            )));
        }
        self.bump_generation();
    }

    pub(super) fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn notify_fsync_completed(
        &self,
        mark: LogId,
    ) {
        if let Some(tx) = &self.internal_event_tx {
            let _ = tx.send(InternalEvent::FsyncCompleted {
                mark,
                sent_at: Instant::now(),
            });
        }
    }

    fn mark_poisoned_and_notify(
        &self,
        error: String,
    ) {
        self.poisoned.store(true, Ordering::Release);
        if let Some(tx) = &self.internal_event_tx
            && tx
                .send(InternalEvent::FatalError {
                    source: "RaftLog".into(),
                    error,
                })
                .is_err()
        {
            error!(
                ?self.node_id,
                "FatalError delivery failed (channel closed) — node may be poisoned \
                 but still running; durability is no longer guaranteed"
            );
        }
    }
}

// TODO: FsyncWorker needs its own dedicated test module — the old
// fsync_coordinator_test.rs tests FsyncCoordinator/BufferedRaftLog directly
// and can't be shared via #[path] with this struct.
