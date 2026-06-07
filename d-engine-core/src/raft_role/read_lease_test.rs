use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use super::{ReadLease, now_ms};

// ── now_ms ────────────────────────────────────────────────────────────────────

#[test]
fn test_now_ms_is_monotonically_nondecreasing() {
    let a = now_ms();
    let b = now_ms();
    assert!(b >= a, "now_ms must never go backwards: a={a}, b={b}");
}

#[test]
fn test_now_ms_advances_over_time() {
    let before = now_ms();
    thread::sleep(std::time::Duration::from_millis(5));
    let after = now_ms();
    assert!(
        after > before,
        "now_ms must advance over 5ms: before={before}, after={after}"
    );
}

// ── new / default ─────────────────────────────────────────────────────────────

#[test]
fn test_new_starts_invalid() {
    let lease = ReadLease::new();
    // deadline=0 → always false regardless of term
    assert!(
        !lease.is_valid_for_leader(1, now_ms()),
        "fresh ReadLease must be invalid (deadline=0)"
    );
}

#[test]
fn test_new_is_invalid_for_any_term() {
    let lease = ReadLease::new();
    assert!(!lease.is_valid_for_leader(1, now_ms()));
    assert!(!lease.is_valid_for_leader(u16::MAX as u64, now_ms()));
}

// ── renew ─────────────────────────────────────────────────────────────────────

#[test]
fn test_renew_makes_lease_valid_for_matching_term() {
    let lease = ReadLease::new();
    let deadline = now_ms() + 5_000;
    lease.renew(1, deadline);
    assert!(lease.is_valid_for_leader(1, now_ms()));
}

#[test]
fn test_renew_invalid_for_leader_with_wrong_term() {
    let lease = ReadLease::new();
    let deadline = now_ms() + 5_000;
    lease.renew(42, deadline);
    assert!(!lease.is_valid_for_leader(41, now_ms()));
    assert!(!lease.is_valid_for_leader(43, now_ms()));
}

#[test]
fn test_renew_with_past_deadline_is_invalid() {
    let lease = ReadLease::new();
    let past = now_ms().saturating_sub(1_000);
    lease.renew(1, past);
    assert!(!lease.is_valid_for_leader(1, now_ms()));
}

#[test]
fn test_renew_zero_deadline_is_invalid() {
    let lease = ReadLease::new();
    lease.renew(1, 0);
    assert!(!lease.is_valid_for_leader(1, now_ms()));
}

// ── invalidate ────────────────────────────────────────────────────────────────

#[test]
fn test_invalidate_clears_valid_lease() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    assert!(
        lease.is_valid_for_leader(1, now_ms()),
        "pre-condition: lease must be valid"
    );
    lease.invalidate(2);
    assert!(
        !lease.is_valid_for_leader(1, now_ms()),
        "lease must be invalid after invalidate"
    );
}

#[test]
fn test_invalidate_updates_term() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    lease.invalidate(2);
    // deadline=0 after invalidate → false for any term
    assert!(!lease.is_valid_for_leader(1, now_ms()));
    assert!(!lease.is_valid_for_leader(2, now_ms()));
}

#[test]
fn test_invalidate_on_fresh_lease_stays_invalid() {
    let lease = ReadLease::new();
    lease.invalidate(5);
    assert!(!lease.is_valid_for_leader(5, now_ms()));
}

// ── renew → invalidate → renew cycle ─────────────────────────────────────────

#[test]
fn test_renew_after_invalidate_makes_lease_valid_again() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    lease.invalidate(2);
    assert!(
        !lease.is_valid_for_leader(1, now_ms()),
        "must be invalid after invalidate"
    );
    lease.renew(3, now_ms() + 5_000);
    assert!(
        lease.is_valid_for_leader(3, now_ms()),
        "must be valid after second renew"
    );
    assert!(
        !lease.is_valid_for_leader(1, now_ms()),
        "old term must not match"
    );
}

// ── term packing (16-bit truncation) ─────────────────────────────────────────

#[test]
fn test_term_truncation_wraps_at_16_bits() {
    let lease = ReadLease::new();
    let term_17bit: u64 = 0x1_0001; // bit 16 set — truncated to 0x0001 in 16-bit slot
    lease.renew(term_17bit, now_ms() + 5_000);
    // stored term = term_17bit & 0xFFFF = 1
    // both argument and stored value are masked to 16 bits, so 0x10001 → 1 on both sides
    assert!(
        lease.is_valid_for_leader(1, now_ms()),
        "low 16 bits of 0x10001 == 1"
    );
    assert!(
        lease.is_valid_for_leader(term_17bit, now_ms()),
        "0x10001 & 0xFFFF == 1 — same as passing 1"
    );
    assert!(
        !lease.is_valid_for_leader(2, now_ms()),
        "term 2 must not match stored term 1"
    );
}

// ── revoke ────────────────────────────────────────────────────────────────────

#[test]
fn test_revoke_clears_valid_lease() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    assert!(
        lease.is_valid_for_leader(1, now_ms()),
        "pre-condition: lease must be valid before revoke"
    );
    lease.revoke();
    assert!(
        !lease.is_valid_for_leader(1, now_ms()),
        "lease must be invalid after revoke"
    );
}

#[test]
fn test_revoke_on_fresh_lease_stays_invalid() {
    let lease = ReadLease::new();
    lease.revoke();
    assert!(!lease.is_valid_for_leader(0, now_ms()));
    assert!(!lease.is_valid_for_leader(1, now_ms()));
}

#[test]
fn test_revoke_sets_packed_to_zero() {
    let lease = ReadLease::new();
    lease.renew(42, now_ms() + 10_000);
    lease.revoke();
    // After revoke, neither term=42 nor any other term should be valid.
    assert!(!lease.is_valid_for_leader(42, now_ms()));
    assert!(!lease.is_valid_for_leader(0, now_ms()));
}

// ── is_valid ──────────────────────────────────────────────────────────────────

#[test]
fn test_is_valid_returns_true_when_deadline_in_future() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    assert!(
        lease.is_valid(now_ms()),
        "is_valid must return true when deadline is in the future"
    );
}

#[test]
fn test_is_valid_returns_false_when_deadline_in_past() {
    let lease = ReadLease::new();
    let past = now_ms().saturating_sub(1_000);
    lease.renew(1, past);
    assert!(
        !lease.is_valid(now_ms()),
        "is_valid must return false when deadline is in the past"
    );
}

#[test]
fn test_is_valid_returns_false_on_fresh_lease() {
    let lease = ReadLease::new();
    assert!(
        !lease.is_valid(now_ms()),
        "fresh lease (deadline=0) must be invalid"
    );
}

#[test]
fn test_is_valid_returns_false_after_revoke() {
    let lease = ReadLease::new();
    lease.renew(1, now_ms() + 5_000);
    lease.revoke();
    assert!(
        !lease.is_valid(now_ms()),
        "is_valid must return false after revoke"
    );
}

#[test]
fn test_is_valid_ignores_term() {
    // is_valid() only checks deadline, not term — any term works if deadline is in future
    let lease = ReadLease::new();
    lease.renew(99, now_ms() + 5_000);
    // is_valid does NOT require knowing the term
    assert!(
        lease.is_valid(now_ms()),
        "is_valid must not care about term"
    );
}

#[test]
fn test_is_valid_consistent_with_is_valid_for_leader() {
    // When both term and deadline match, is_valid and is_valid_for_leader agree.
    let lease = ReadLease::new();
    let deadline = now_ms() + 5_000;
    lease.renew(7, deadline);
    let now = now_ms();
    assert_eq!(
        lease.is_valid(now),
        lease.is_valid_for_leader(7, now),
        "is_valid and is_valid_for_leader must agree when term matches"
    );
}

// ── Arc sharing semantics ─────────────────────────────────────────────────────

#[test]
fn test_arc_read_lease_visible_across_clones() {
    let lease = Arc::new(ReadLease::new());
    let reader: Arc<ReadLease> = Arc::clone(&lease);

    lease.renew(1, now_ms() + 5_000);
    assert!(
        reader.is_valid_for_leader(1, now_ms()),
        "renew on original must be visible via clone"
    );

    lease.invalidate(2);
    assert!(
        !reader.is_valid_for_leader(1, now_ms()),
        "invalidate on original must be visible via clone"
    );
}

// ── concurrent safety (basic) ─────────────────────────────────────────────────

#[test]
fn test_concurrent_renew_does_not_panic() {
    let lease = Arc::new(ReadLease::new());
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for term in 1u64..=3 {
        let l: Arc<ReadLease> = Arc::clone(&lease);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            let deadline = now_ms() + 5_000;
            l.renew(term, deadline);
        }));
    }
    // reader thread
    {
        let l: Arc<ReadLease> = Arc::clone(&lease);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            let _ = l.is_valid_for_leader(1, now_ms());
        }));
    }
    for h in handles {
        h.join().expect("thread panicked");
    }
}
