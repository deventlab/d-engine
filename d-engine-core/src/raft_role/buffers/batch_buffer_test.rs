use super::*;
use batch_buffer::BatchBuffer;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq)]
struct TestRequest;

fn create_test_request() -> TestRequest {
    TestRequest
}

#[test]
fn test_new_initialization() {
    let initial_capacity = 5;
    let buffer = BatchBuffer::<TestRequest>::new(initial_capacity);

    assert!(buffer.buffer.is_empty());
    assert!(buffer.buffer.capacity() >= initial_capacity);
    assert!(buffer.last_flush.elapsed() < Duration::from_secs(1));
}

/// push() accumulates items; len() reflects the count.
#[test]
fn test_push_increments_len() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    buffer.push(create_test_request());
    assert_eq!(buffer.len(), 1);
    buffer.push(create_test_request());
    assert_eq!(buffer.len(), 2);
}

/// push() beyond the initial capacity continues to work — buffer does not auto-flush.
#[test]
fn test_push_beyond_max_does_not_overflow() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    buffer.push(create_test_request()); // exceeds threshold, no panic
    assert_eq!(buffer.len(), 3);
}

/// take_all() drains all items and leaves the buffer empty.
#[test]
fn test_is_empty() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    assert!(buffer.is_empty());

    buffer.push(create_test_request());
    assert!(!buffer.is_empty());

    buffer.take_all();
    assert!(buffer.is_empty());
}

/// take_all() returns all buffered items.
#[test]
fn test_take_all() {
    let mut buffer = BatchBuffer::<TestRequest>::new(5);
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    buffer.push(create_test_request());

    let taken = buffer.take_all();
    assert_eq!(taken.len(), 3);
    assert!(buffer.is_empty());
}

/// take_all() resets the buffer and updates last_flush.
#[test]
fn test_take_all_resets_buffer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    buffer.push(create_test_request());

    let before = Instant::now();
    let taken = buffer.take_all();
    let after = Instant::now();

    assert!(!taken.is_empty());
    assert!(buffer.is_empty());
    assert!(buffer.last_flush >= before);
    assert!(buffer.last_flush <= after + Duration::from_secs(1));
}

/// take_all() returns elements in insertion order (Vec semantics).
#[test]
fn test_take_all_returns_correct_elements() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    let req1 = create_test_request();
    let req2 = create_test_request();

    buffer.push(req1.clone());
    buffer.push(req2.clone());
    let taken = buffer.take_all();

    assert_eq!(taken.len(), 2);
    assert_eq!(taken[0], req1);
    assert_eq!(taken[1], req2);
}

#[test]
fn test_consecutive_take_all_operations() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());

    // 1st take_all
    let taken1 = buffer.take_all();
    assert_eq!(taken1.len(), 1);
    assert!(buffer.buffer.is_empty());

    // 2nd take_all after new pushes
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    let taken2 = buffer.take_all();
    assert_eq!(taken2.len(), 2);
}

/// Buffer can be reused across multiple take_all cycles.
#[test]
fn test_buffer_reuse_after_take_all() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());
    buffer.take_all();

    buffer.push(create_test_request());
    buffer.push(create_test_request());
    assert_eq!(buffer.len(), 2);
}

/// take_all() resets last_flush to approximately now.
#[test]
fn test_take_all_resets_last_flush_timer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());

    let before_take = Instant::now();
    buffer.take_all();
    let after_take = Instant::now();

    assert!(buffer.last_flush >= before_take);
    assert!(buffer.last_flush <= after_take + Duration::from_secs(1));
}

/// Freshly created buffer is empty.
#[test]
fn test_fresh_buffer_is_empty() {
    let buffer = BatchBuffer::<TestRequest>::new(5);

    assert!(buffer.is_empty());
    assert_eq!(buffer.buffer.len(), 0);
}

// ── Metrics path coverage ─────────────────────────────────────────────────────

/// push() with metrics_enabled=true executes the gauge branch.
/// No metrics recorder is required — metrics::gauge! is a no-op without one,
/// but the branch body is still traversed and reported as covered.
#[test]
fn test_push_with_metrics_enabled_executes_gauge_branch() {
    let mut buf = BatchBuffer::<TestRequest>::new(4).with_length_gauge(1, "test_buf", true);

    buf.push(create_test_request());
    buf.push(create_test_request());
    assert_eq!(buf.len(), 2);
}

/// take_all() with metrics_enabled=true executes the reset-gauge branch.
#[test]
fn test_take_all_with_metrics_enabled_executes_gauge_reset() {
    let mut buf = BatchBuffer::<TestRequest>::new(4).with_length_gauge(2, "test_buf", true);

    buf.push(create_test_request());
    let taken = buf.take_all();
    assert_eq!(taken.len(), 1);
    assert!(buf.is_empty());
}
