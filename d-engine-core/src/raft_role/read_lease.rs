use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// Process-local monotonic epoch. Initialized on first call to now_ms().
// Using Instant (not SystemTime) guarantees NTP-safe monotonic time.
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Monotonic milliseconds since first call. Safe to use for deadline arithmetic.
#[must_use]
pub fn now_ms() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Initializes the monotonic clock epoch. Call once at engine startup to avoid
/// first-call jitter on the hot path.
pub fn init_clock() {
    let _ = now_ms();
}

/// Lock-free Raft read lease shared between the Raft loop (writer) and EmbeddedClient (reader).
///
/// Packs term (16 bits) and deadline_ms (48 bits) into a single AtomicU64 so
/// that readers always observe a consistent (term, deadline) pair from one atomic
/// load — eliminating the ABA race that two separate AtomicU64 fields would have.
///
/// Layout: `[63..48] term | [47..0] deadline_ms`
///
/// - `deadline_ms = 0` → no valid lease (sentinel)
/// - `deadline_ms > now_ms()` → lease is still live
/// - `term` wraps at 16 bits (max 65535); Raft clusters rarely exceed a few thousand terms.
///
/// # Writers (Raft loop only)
/// - `renew(term, deadline_ms)` — called on every quorum ACK
/// - `invalidate(new_term)` — called when leader steps down
///
/// # Readers (EmbeddedClient hot path)
/// - `is_valid_for_leader(term, now_ms)` — validates term + deadline (~4 ns on L1 hit)
#[derive(Debug)]
pub struct ReadLease {
    packed: AtomicU64,
}

impl ReadLease {
    const DEADLINE_MASK: u64 = (1u64 << 48) - 1;
    const TERM_SHIFT: u32 = 48;

    pub fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }

    #[inline]
    fn pack(
        term: u64,
        deadline_ms: u64,
    ) -> u64 {
        assert!(
            deadline_ms <= Self::DEADLINE_MASK,
            "deadline_ms overflows 48 bits: {deadline_ms}"
        );
        ((term & 0xFFFF) << Self::TERM_SHIFT) | (deadline_ms & Self::DEADLINE_MASK)
    }

    #[inline]
    fn unpack(v: u64) -> (u64, u64) {
        (v >> Self::TERM_SHIFT, v & Self::DEADLINE_MASK)
    }

    /// Renew the lease. Called by the Raft loop after every quorum ACK.
    /// `deadline_ms` should be `now_ms() + lease_duration_ms`.
    #[inline]
    pub fn renew(
        &self,
        term: u64,
        deadline_ms: u64,
    ) {
        self.packed.store(Self::pack(term, deadline_ms), Ordering::Release);
    }

    /// Invalidate the lease. Called when leader steps down.
    /// Sets `deadline_ms = 0`; subsequent `is_valid_for_leader` calls return false.
    #[inline]
    pub fn invalidate(
        &self,
        new_term: u64,
    ) {
        self.packed.store(Self::pack(new_term, 0), Ordering::Release);
    }

    /// Revoke the lease immediately. Single atomic store(0, Release).
    ///
    /// Called by the Raft loop on ANY term change (leader step-down, new election).
    /// Sets packed = 0 → deadline = 0 → `is_valid()` returns false until next `renew()`.
    ///
    /// Safety invariant: every term-change path in the Raft loop MUST call this.
    /// Missing a call causes silent stale reads (LeaseRead correctness violation).
    #[inline]
    pub fn revoke(&self) {
        self.packed.store(0, Ordering::Release);
    }

    /// ReadActor hot path: single atomic load, no external term required.
    ///
    /// Returns `true` iff `deadline_ms > now_ms`. Safety relies on `revoke()` being
    /// called on every term change — see `revoke()` doc for the invariant.
    #[must_use]
    #[inline]
    pub fn is_valid(
        &self,
        now_ms: u64,
    ) -> bool {
        let packed = self.packed.load(Ordering::Acquire);
        (packed & Self::DEADLINE_MASK) > now_ms
    }

    /// Check both term and deadline in one atomic load (~4 ns on L1 hit).
    ///
    /// Used by both `LeaderState` (internal) and `EmbeddedClient` (fast path).
    /// `current_term` is masked to 16 bits before comparison, matching the storage layout.
    #[must_use]
    #[inline]
    pub fn is_valid_for_leader(
        &self,
        current_term: u64,
        now_ms: u64,
    ) -> bool {
        let (term, deadline) = Self::unpack(self.packed.load(Ordering::Acquire));
        term == (current_term & 0xFFFF) && deadline > now_ms
    }
}

impl Default for ReadLease {
    fn default() -> Self {
        Self::new()
    }
}

// ReadLease is Send + Sync because AtomicU64 is Send + Sync.
// The compiler derives this automatically.

#[cfg(test)]
#[path = "read_lease_test.rs"]
mod tests;
