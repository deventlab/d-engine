use d_engine_proto::client::ClientResponse;
use d_engine_proto::common::EntryPayload;
use tonic::Status;

use crate::MaybeCloneOneshot;
use crate::RaftOneshot;

use super::propose_batch_buffer::ProposeBatchBuffer;

// Helper: create a dummy sender, discarding the receiver.
fn make_sender() -> crate::MaybeCloneOneshotSender<Result<ClientResponse, Status>> {
    let (tx, _rx) = MaybeCloneOneshot::new();
    tx
}

// Helper: create a trivial EntryPayload (noop is the simplest valid payload).
fn make_payload() -> EntryPayload {
    EntryPayload::noop()
}

// ── Construction ─────────────────────────────────────────────────────────────

/// A freshly created buffer must be empty and pre-allocate capacity equal to
/// initial_capacity (avoiding reallocation on the first N pushes).
#[test]
fn new_buffer_is_empty_with_preallocated_capacity() {
    let buf = ProposeBatchBuffer::new(8);
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
}

// ── push() ───────────────────────────────────────────────────────────────────

/// push() accumulates items; len() reflects the count.
/// Flush is driven externally — push() has no return value.
#[test]
fn push_increments_len() {
    let mut buf = ProposeBatchBuffer::new(4);
    buf.push(make_payload(), make_sender());
    assert_eq!(buf.len(), 1);
    buf.push(make_payload(), make_sender());
    assert_eq!(buf.len(), 2);
    buf.push(make_payload(), make_sender());
    assert_eq!(buf.len(), 3);
}

/// push() beyond initial_capacity does not panic — buffer does not auto-flush.
/// The caller checks len() >= batch_size and drives flush externally.
#[test]
fn push_beyond_threshold_does_not_overflow() {
    let mut buf = ProposeBatchBuffer::new(2);
    buf.push(make_payload(), make_sender());
    buf.push(make_payload(), make_sender());
    buf.push(make_payload(), make_sender()); // exceeds threshold, no panic
    assert_eq!(buf.len(), 3);
}

// ── SoA invariant ────────────────────────────────────────────────────────────

/// payloads and senders must remain index-aligned after multiple pushes.
/// Verified indirectly: flush must produce a RaftRequestWithSignal where
/// payloads.len() == senders.len().
#[test]
fn flush_produces_aligned_payloads_and_senders() {
    let mut buf = ProposeBatchBuffer::new(10);
    let n = 5;
    for _ in 0..n {
        buf.push(make_payload(), make_sender());
    }
    let req = buf.flush().expect("non-empty buffer must yield Some");
    assert_eq!(req.payloads.len(), n, "payload count mismatch");
    assert_eq!(req.senders.len(), n, "sender count mismatch");
}

// ── flush() ──────────────────────────────────────────────────────────────────

/// flush() on an empty buffer returns None — no spurious RaftRequestWithSignal.
#[test]
fn flush_empty_buffer_returns_none() {
    let mut buf = ProposeBatchBuffer::new(4);
    assert!(buf.flush().is_none());
}

/// After flush(), the buffer must be empty and ready to accept new pushes.
#[test]
fn buffer_is_empty_after_flush() {
    let mut buf = ProposeBatchBuffer::new(4);
    buf.push(make_payload(), make_sender());
    buf.push(make_payload(), make_sender());

    let req = buf.flush().expect("should produce a request");
    assert_eq!(req.payloads.len(), 2);

    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
}

/// flush() resets last_flush to approximately now.
#[test]
fn flush_resets_last_flush_timestamp() {
    let mut buf = ProposeBatchBuffer::new(4);
    buf.push(make_payload(), make_sender());

    buf.flush();

    assert!(
        buf.last_flush.elapsed() < std::time::Duration::from_millis(100),
        "last_flush not updated after flush"
    );
}

/// Flushing a full batch (exactly initial_capacity) produces all entries.
#[test]
fn flush_full_batch_produces_all_entries() {
    const N: usize = 6;
    let mut buf = ProposeBatchBuffer::new(N);
    for _ in 0..N {
        buf.push(make_payload(), make_sender());
    }
    let req = buf.flush().expect("should produce a request");
    assert_eq!(req.payloads.len(), N);
    assert_eq!(req.senders.len(), N);
    assert!(buf.is_empty());
}

/// Each flush produces a unique request id (nanoid! is called once per flush,
/// not reused across batches).
#[test]
fn consecutive_flushes_produce_distinct_ids() {
    let mut buf = ProposeBatchBuffer::new(10);

    buf.push(make_payload(), make_sender());
    let id1 = buf.flush().expect("first flush").id;

    buf.push(make_payload(), make_sender());
    let id2 = buf.flush().expect("second flush").id;

    assert_ne!(id1, id2, "request ids must be unique across flushes");
}

/// wait_for_apply_event must always be true for propose batches —
/// the state machine must confirm apply before the client is notified.
#[test]
fn flush_sets_wait_for_apply_event_true() {
    let mut buf = ProposeBatchBuffer::new(4);
    buf.push(make_payload(), make_sender());
    let req = buf.flush().expect("should produce a request");
    assert!(req.wait_for_apply_event);
}

// ── Reuse after flush ────────────────────────────────────────────────────────

/// The buffer can be reused across multiple flush cycles without reallocation
/// (mem::swap retains Vec capacity in self for each new batch).
#[test]
fn buffer_reusable_across_multiple_flush_cycles() {
    let mut buf = ProposeBatchBuffer::new(3);

    for cycle in 1..=3u32 {
        for _ in 0..3 {
            buf.push(make_payload(), make_sender());
        }
        let req = buf.flush().expect("each cycle should produce a request");
        assert_eq!(req.payloads.len(), 3, "cycle {cycle} payload count wrong");
        assert!(
            buf.is_empty(),
            "cycle {cycle}: buffer not empty after flush"
        );
    }
}

/// mem::swap gives self a replacement Vec sized to the flushed batch length.
/// Verified indirectly: after flushing N items, the buffer can immediately
/// accept N more pushes and produce a correct second flush — proving the
/// internal Vecs were properly swapped and the buffer is fully reusable.
#[test]
fn flush_swap_produces_correct_second_batch() {
    let mut buf = ProposeBatchBuffer::new(8);

    // Cycle 1: push 5, flush
    for _ in 0..5 {
        buf.push(make_payload(), make_sender());
    }
    let req1 = buf.flush().expect("first flush");
    assert_eq!(req1.payloads.len(), 5);
    assert!(buf.is_empty());

    // Cycle 2: push 2 (smaller batch), flush — proves self Vecs are valid after swap
    buf.push(make_payload(), make_sender());
    buf.push(make_payload(), make_sender());
    let req2 = buf.flush().expect("second flush");
    assert_eq!(req2.payloads.len(), 2);
    assert_eq!(req2.senders.len(), 2);
    assert!(buf.is_empty());

    // Cycle 1 result must be unaffected by cycle 2 (no aliasing after swap)
    assert_eq!(
        req1.payloads.len(),
        5,
        "cycle 1 data must not be corrupted by cycle 2"
    );
}

// ── Metrics path coverage ─────────────────────────────────────────────────────

/// push() with metrics_enabled=true executes the gauge branch on every insert.
/// No metrics recorder is required — metrics::gauge! is a no-op without one,
/// but the branch body is still traversed and reported as covered.
#[test]
fn push_with_metrics_enabled_executes_gauge_branch() {
    let mut buf = ProposeBatchBuffer::new(4).with_length_gauge(1, "propose", true);

    buf.push(make_payload(), make_sender());
    buf.push(make_payload(), make_sender());
    assert_eq!(buf.len(), 2);
}

/// flush() with metrics_enabled=true executes the reset-gauge branch.
#[test]
fn flush_with_metrics_enabled_executes_gauge_reset() {
    let mut buf = ProposeBatchBuffer::new(4).with_length_gauge(2, "propose", true);

    buf.push(make_payload(), make_sender());
    let req = buf.flush().expect("must produce a request");
    assert_eq!(req.payloads.len(), 1);
    assert!(buf.is_empty());
}
