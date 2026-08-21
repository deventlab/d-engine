//! Test-only synchronization point for outbound snapshot push transfers.
//!
//! Freezes a push transfer to a given peer right after it has completed all
//! setup and is about to send its first chunk, so integration tests can
//! observe a deterministic "transfer started but not completed" window
//! instead of racing wall-clock timing or bandwidth throttling. Registration
//! is keyed by peer id and has no effect unless a gate was installed for that
//! peer. Not compiled into production builds.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

#[derive(Clone)]
pub struct SnapshotTransferGate {
    started: Arc<Notify>,
    started_flag: Arc<AtomicBool>,
    release: Arc<Notify>,
    completed: Arc<AtomicBool>,
}

impl SnapshotTransferGate {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            started_flag: Arc::new(AtomicBool::new(false)),
            release: Arc::new(Notify::new()),
            completed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Waits until the transfer has entered the push loop and is holding for `release()`.
    pub async fn wait_started(&self) {
        if self.started_flag.load(Ordering::Acquire) {
            return;
        }
        self.started.notified().await;
    }

    /// True once the transfer has entered the push loop (before the first chunk is sent).
    pub fn is_started(&self) -> bool {
        self.started_flag.load(Ordering::Acquire)
    }

    /// True once the transfer has finished, regardless of success, failure, or cancellation.
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Lets a paused transfer proceed. Safe to call more than once.
    pub fn release(&self) {
        self.release.notify_one();
    }

    pub(crate) fn mark_started(&self) {
        self.started_flag.store(true, Ordering::Release);
        self.started.notify_one();
    }

    pub(crate) async fn wait_release(&self) {
        self.release.notified().await;
    }

    pub(crate) fn mark_completed(&self) {
        self.completed.store(true, Ordering::Release);
    }
}

fn registry() -> &'static Mutex<HashMap<u32, SnapshotTransferGate>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u32, SnapshotTransferGate>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Removes the gate registration for `peer_id` when dropped, and releases
/// the gate unconditionally as a safety net — e.g. if the test panics after
/// the transfer started but before it explicitly called `release()`, a
/// blocked transfer task must not be left hanging forever (which would in
/// turn hang engine shutdown).
#[must_use]
pub struct SnapshotTransferGateGuard {
    peer_id: u32,
    gate: SnapshotTransferGate,
}

impl Drop for SnapshotTransferGateGuard {
    fn drop(&mut self) {
        registry().lock().unwrap().remove(&self.peer_id);
        self.gate.release();
    }
}

/// Registers a test gate for outbound snapshot pushes to `peer_id`. Drop the
/// returned guard to unregister (e.g. at the end of the test).
pub fn install_snapshot_transfer_gate(
    peer_id: u32
) -> (SnapshotTransferGate, SnapshotTransferGateGuard) {
    let gate = SnapshotTransferGate::new();
    registry().lock().unwrap().insert(peer_id, gate.clone());
    let guard = SnapshotTransferGateGuard {
        peer_id,
        gate: gate.clone(),
    };
    (gate, guard)
}

/// Looks up the gate registered for `peer_id`, if any. The registry lock is
/// released before returning, so callers never hold it across an `.await`.
pub(crate) fn lookup_gate(peer_id: u32) -> Option<SnapshotTransferGate> {
    registry().lock().unwrap().get(&peer_id).cloned()
}
