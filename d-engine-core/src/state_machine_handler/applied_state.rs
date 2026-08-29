use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

/// The single authoritative source of "how far has this node applied" — shared between
/// `DefaultStateMachineHandler` (reads it) and `DefaultStateMachineWriter` (advances it).
/// `AtomicU64` is the authoritative current value (cheap, lock-free reads on the hot
/// `last_applied()` path); `watch::Sender` is purely a notification mechanism for
/// `wait_applied()` — the two are not duplicated state, just two access modes onto one value.
#[derive(Debug)]
pub(crate) struct AppliedState {
    index: AtomicU64,
    notify_tx: watch::Sender<u64>,
}

impl AppliedState {
    /// Returns the shared state plus the paired `Receiver` — construct once, hand the
    /// `Receiver` to the reader side, keep the `Arc<Self>` on both sides.
    pub fn new(initial: u64) -> (Arc<Self>, watch::Receiver<u64>) {
        let (notify_tx, notify_rx) = watch::channel(initial);
        (
            Arc::new(Self {
                index: AtomicU64::new(initial),
                notify_tx,
            }),
            notify_rx,
        )
    }

    pub fn load(&self) -> u64 {
        self.index.load(Ordering::Acquire)
    }

    /// Only advances — never regresses, even if called with a stale/out-of-order value.
    pub fn advance(
        &self,
        next: u64,
    ) {
        let current = self.index.load(Ordering::Acquire);
        if next > current {
            self.index.store(next, Ordering::Release);
            if let Err(e) = self.notify_tx.send(next) {
                debug_assert!(false, "apply notify send failed: {e:?}");
            }
        }
    }
}
