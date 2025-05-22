//! Snapshot policy based on Raft log size.
//! Triggers a snapshot when the number of log entries exceeds a configured threshold.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::SnapshotContext;
use super::SnapshotPolicy;
use crate::cluster::is_leader;
use crate::time::timestamp_millis;

#[derive(Debug)]
pub struct LogSizePolicy {
    threshold: AtomicU64,    // e.g. 5000 log entries
    last_checked: AtomicU64, // Stored as milliseconds
    cooldown_ms: u64,
    is_checking: AtomicBool, // CAS lock for concurrent checks
}

impl SnapshotPolicy for LogSizePolicy {
    #[inline]
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        if !is_leader(ctx.role) {
            return false; // Only the Leader actively triggers
        }

        if ctx.current_term < ctx.last_included.term {
            return false;
        }

        // Cooldown check using atomic operations
        let now = timestamp_millis();
        let last = self.last_checked.load(Ordering::Acquire);

        if now - last < self.cooldown_ms {
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

        let should_trigger = self.calculate_lag(ctx) >= self.threshold.load(Ordering::Relaxed);
        if should_trigger {
            self.last_checked.store(now, Ordering::Release);
        }

        self.is_checking.store(false, Ordering::Release);

        should_trigger
    }
}

impl LogSizePolicy {
    pub(crate) fn new(
        threshold: u64,
        cooldown: Duration,
    ) -> Self {
        LogSizePolicy {
            threshold: AtomicU64::new(threshold),
            last_checked: AtomicU64::new(0),
            cooldown_ms: cooldown.as_millis() as u64,
            is_checking: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(crate) fn calculate_lag(
        &self,
        ctx: &SnapshotContext,
    ) -> u64 {
        ctx.last_applied.index.saturating_sub(ctx.last_included.index)
    }

    pub(crate) fn update_threshold(
        &self,
        new_val: u64,
    ) {
        self.threshold.store(new_val, Ordering::Relaxed);
    }
}
