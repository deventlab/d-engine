//! Snapshot policy based on Raft log size.
//! Triggers a snapshot when the number of log entries exceeds a configured threshold.
//!
//! Uses adaptive cooldown: the interval between evaluations shrinks linearly as
//! the log lag approaches the snapshot threshold, ensuring timely triggers without
//! excessive polling when the log is small.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tracing::trace;
use tracing::warn;

use super::SnapshotContext;
use super::SnapshotPolicy;
use crate::time::timestamp_millis;

#[derive(Debug)]
pub struct LogSizePolicy {
    threshold: AtomicU64,
    last_checked: AtomicU64, // Stored as milliseconds
    last_lag: AtomicU64,     // Lag from previous evaluation, drives adaptive cooldown
    base_cooldown_ms: u64,
    is_checking: AtomicBool, // CAS lock for concurrent checks
}

impl SnapshotPolicy for LogSizePolicy {
    #[inline]
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        if ctx.current_term < ctx.last_included.term {
            return false;
        }

        // Adaptive cooldown — Relaxed is sufficient since the CAS on
        // is_checking provides actual mutual exclusion.
        let now = timestamp_millis();
        let last = self.last_checked.load(Ordering::Relaxed);

        if now.saturating_sub(last) < self.effective_cooldown_ms() {
            return false;
        }

        // CAS lock to prevent concurrent checks
        if self
            .is_checking
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }

        self.last_checked.store(now, Ordering::Relaxed);

        let lag = self.calculate_lag(ctx);
        let threshold = self.threshold.load(Ordering::Relaxed);
        self.last_lag.store(lag, Ordering::Relaxed);

        if threshold > 0 && lag >= threshold.saturating_mul(10) {
            warn!(
                lag,
                threshold,
                "Log lag exceeds 10x snapshot threshold — snapshots may not be keeping up"
            );
        }

        let should_trigger = lag >= threshold;

        self.is_checking.store(false, Ordering::Release);

        should_trigger
    }

    #[allow(unused)]
    /// For sized based policy, no need to use this function.
    fn mark_snapshot_created(&mut self) {}
}

impl LogSizePolicy {
    pub fn new(
        threshold: u64,
        cooldown: Duration,
    ) -> Self {
        LogSizePolicy {
            threshold: AtomicU64::new(threshold),
            last_checked: AtomicU64::new(0),
            last_lag: AtomicU64::new(0),
            base_cooldown_ms: cooldown.as_millis() as u64,
            is_checking: AtomicBool::new(false),
        }
    }

    /// Adaptive cooldown based on how close the lag is to the threshold.
    ///
    /// Below threshold: linearly reduces cooldown as lag approaches threshold,
    /// so we check more frequently and detect the crossing sooner.
    /// At or above threshold: reverts to base cooldown to space out re-triggers.
    #[inline]
    pub(crate) fn effective_cooldown_ms(&self) -> u64 {
        let lag = self.last_lag.load(Ordering::Relaxed);
        let threshold = self.threshold.load(Ordering::Relaxed);

        if threshold == 0 || lag >= threshold {
            return self.base_cooldown_ms;
        }

        let remaining = threshold - lag;
        self.base_cooldown_ms.saturating_mul(remaining) / threshold
    }

    #[inline]
    pub(crate) fn calculate_lag(
        &self,
        ctx: &SnapshotContext,
    ) -> u64 {
        let lag = ctx.last_applied.index.saturating_sub(ctx.last_included.index);
        trace!("calculate_lag: {}", lag);
        lag
    }

    #[allow(unused)]
    pub(crate) fn update_threshold(
        &self,
        new_val: u64,
    ) {
        self.threshold.store(new_val, Ordering::Relaxed);
    }
}

#[cfg(test)]
impl LogSizePolicy {
    pub(super) fn set_last_lag(
        &self,
        lag: u64,
    ) {
        self.last_lag.store(lag, Ordering::Relaxed);
    }
}
