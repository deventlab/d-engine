use std::collections::BTreeSet;

/// A point-in-time snapshot of committed cluster membership.
///
/// Delivered via [`EmbeddedEngine::watch_membership`] whenever a `ConfChange`
/// entry commits.  The snapshot reflects the membership state **after** the
/// change has been applied, so `borrow()` always returns a consistent view.
///
/// ## Idempotency
///
/// `committed_index` is the Raft log index of the `ConfChange` entry that
/// triggered this snapshot.  It is strictly monotonically increasing across
/// snapshots and can be used as an idempotency key:
///
/// ```ignore
/// if snapshot.committed_index <= self.last_applied {
///     return; // already handled
/// }
/// self.last_applied = snapshot.committed_index;
/// scheduler.rebalance(&snapshot);
/// ```
///
/// ## Diff computation
///
/// No diff fields are included.  Because `watch::channel` is lossy (only the
/// latest value is retained), a diff embedded in the snapshot could be stale
/// if the receiver is slow and skips an intermediate change.  Callers that
/// need a diff should compute it against their own previous snapshot:
///
/// ```ignore
/// let prev = std::mem::replace(&mut self.prev_snapshot, snapshot.clone());
/// let joined: BTreeSet<_> = snapshot.members.difference(&prev.members).copied().collect();
/// let left:   BTreeSet<_> = prev.members.difference(&snapshot.members).copied().collect();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MembershipSnapshot {
    /// Current voting members (Follower / Leader role, Active status).
    pub members: BTreeSet<u32>,

    /// Current non-voting learners (Learner role, Promotable or ReadOnly status).
    pub learners: BTreeSet<u32>,

    /// Raft log index of the ConfChange entry that produced this snapshot.
    /// Monotonically increasing; use as an idempotency key.
    pub committed_index: u64,
}
