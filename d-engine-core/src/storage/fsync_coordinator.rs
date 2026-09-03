use crate::BufferedRaftLog;
use crate::Error;
use crate::LogStore;
use crate::RaftLog;
use crate::Result;
use crate::TypeConfig;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::oneshot;
use tracing::error;

/// Schedules physical fsync calls — batches concurrent requests into one
/// flush() at a time. Does not judge whether results are still valid; see
/// BufferedRaftLog::apply_durable_report.
pub(super) struct FsyncCoordinator {
    inflight: AtomicBool,
    pending_max: AtomicU64,
    pending_replies: Mutex<Vec<oneshot::Sender<Result<()>>>>,

    // Lets a stale round skip its reply early. Optional — not required for correctness.
    generation: AtomicU64,
}

impl FsyncCoordinator {
    pub(super) fn new() -> Self {
        Self {
            inflight: AtomicBool::new(false),
            pending_max: AtomicU64::new(0),
            pending_replies: Mutex::new(Vec::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Called from the IO thread on every wakeup. Records new work and, if no
    /// fsync task is currently running, kicks one off. Never spawns a second
    /// concurrent task — additional calls while one is in flight just update
    /// the pending state for it to pick up next round.
    pub(super) fn submit(
        self: &Arc<Self>,
        this: &Arc<BufferedRaftLog<impl TypeConfig>>,
        max_index: u64,
        replies: Vec<oneshot::Sender<Result<()>>>,
    ) {
        if max_index > 0 {
            self.pending_max.fetch_max(max_index, Ordering::AcqRel);
        }
        if !replies.is_empty() {
            self.pending_replies.lock().unwrap().extend(replies);
        }

        if self
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return; // Already running — it will pick up what we just recorded.
        }

        metrics::gauge!("core.raft.fsync.inflight").set(1.0);

        let coord = Arc::clone(self);
        let this = Arc::clone(this);
        tokio::task::spawn_blocking(move || coord.run_until_caught_up(&this));
    }

    /// Runs on the blocking pool. Keeps fsyncing and re-checking for newly
    /// accumulated work until there's nothing left, then clears `inflight`.
    pub(super) fn run_until_caught_up(
        &self,
        this: &Arc<BufferedRaftLog<impl TypeConfig>>,
    ) {
        loop {
            let gen_at_start = self.generation.load(Ordering::Acquire);

            let max_index = self.pending_max.swap(0, Ordering::AcqRel);
            let max_term = this.entry_term(max_index).unwrap_or(0);
            let replies = std::mem::take(&mut *self.pending_replies.lock().unwrap());

            if this.is_poisoned() {
                for reply in replies {
                    let _ = reply.send(Err(Error::Fatal("raft log storage is poisoned".into())));
                }
                self.inflight.store(false, Ordering::Release);
                metrics::gauge!("core.raft.fsync.inflight").set(0.0);
                return;
            }

            if max_index == 0 && replies.is_empty() {
                self.inflight.store(false, Ordering::Release);
                metrics::gauge!("core.raft.fsync.inflight").set(0.0);
                // Re-check: something may have slipped in between the swap
                // above and clearing `inflight`. If so, re-arm.
                if (self.pending_max.load(Ordering::Acquire) > 0
                    || !self.pending_replies.lock().unwrap().is_empty())
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

            if max_index > 0 {
                let batch_size =
                    max_index.saturating_sub(this.durable_index.load(Ordering::Acquire));
                metrics::histogram!("core.raft.fsync.batch_entries").record(batch_size as f64);
            }

            let result = if this.log_store.is_write_durable() {
                Ok(())
            } else {
                let t0 = std::time::Instant::now();
                let r = this.log_store.flush();
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
                Ok(()) => this.notify_fsync_completed(max_index, max_term),
                Err(e) => {
                    // One fsync failure = fatal, no threshold, no retry-and-hope.
                    // Durability state is now unknown, this node
                    // must stop promising any further persistence.
                    this.mark_poisoned_and_notify(format!("fsync failed: {e:?}")); // mirrors advance_durable_and_notify's pattern
                    error!(
                        "WAL fsync failed at index {}: {:?} — node entering fatal state",
                        max_index, e
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

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Called from reset_internal() before clearing in-memory state.
    /// Bumps generation to fence the in-flight physical flush (if any),
    /// AND drains anything already queued but not yet picked up by a
    /// flush round — that queued data was submitted before reset and
    /// must not be silently adopted by the next round.
    pub(super) fn fence_reset(&self) {
        self.pending_max.store(0, Ordering::Release);
        let stale = std::mem::take(&mut *self.pending_replies.lock().unwrap());
        for reply in stale {
            let _ = reply.send(Err(Error::Fatal(
                "stale fsync generation, superseded by reset".into(),
            )));
        }
        self.bump_generation();
    }

    pub(super) fn fence_truncation(
        &self,
        new_max: u64,
    ) {
        self.pending_max.fetch_min(new_max, Ordering::AcqRel);
        self.bump_generation();
    }
}

#[cfg(test)]
#[path = "fsync_coordinator_test.rs"]
mod tests;
