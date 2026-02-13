use std::time::Duration;

use tokio::time::Instant;

use super::*;

#[derive(Debug, Clone, PartialEq)]
struct TestRequest;

fn create_test_request() -> TestRequest {
    TestRequest
}

#[test]
fn test_new_initialization() {
    let max_size = 5;
    let buffer = BatchBuffer::<TestRequest>::new(max_size);

    assert_eq!(buffer.max_batch_size, max_size);
    assert!(buffer.buffer.is_empty());
    assert!(buffer.buffer.capacity() >= max_size);
    assert!(buffer.last_flush.elapsed() < Duration::from_secs(1));
}

#[test]
fn test_push_below_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.buffer.len(), 2);
}

#[test]
fn test_push_exact_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.push(create_test_request()), Some(2));
    assert_eq!(buffer.buffer.len(), 2);
}

#[test]
fn test_push_exceed_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    assert_eq!(buffer.push(create_test_request()), Some(3));
    assert_eq!(buffer.buffer.len(), 3);
}

/// Test: is_empty returns correct state
#[test]
fn test_is_empty() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    assert!(buffer.is_empty());

    buffer.push(create_test_request());
    assert!(!buffer.is_empty());

    buffer.take();
    assert!(buffer.is_empty());
}

/// Test: take_all returns all items
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

#[test]
fn test_take_resets_buffer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    buffer.push(create_test_request());

    let taken = buffer.take();
    assert!(!taken.is_empty());
    assert!(buffer.buffer.is_empty());
    assert!(buffer.last_flush.elapsed() < Duration::from_millis(50));
}

#[test]
fn test_take_returns_correct_elements() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3);
    let req1 = create_test_request();
    let req2 = create_test_request();

    buffer.push(req1.clone());
    buffer.push(req2.clone());
    let taken = buffer.take();

    assert_eq!(taken.len(), 2);
    assert_eq!(taken[0], req1);
    assert_eq!(taken[1], req2);
}

#[test]
fn test_consecutive_take_operations() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());

    // 1st time take
    let taken1 = buffer.take();
    assert_eq!(taken1.len(), 1);
    assert!(buffer.buffer.is_empty());

    // 2nd time take
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    let taken2 = buffer.take();
    assert_eq!(taken2.len(), 2);
}

#[test]
fn test_buffer_reuse_after_take() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());
    buffer.take();

    buffer.push(create_test_request());
    assert_eq!(buffer.push(create_test_request()), Some(2));
    assert_eq!(buffer.buffer.len(), 2);
}

/// Test: take resets last_flush timer
#[test]
fn test_take_resets_last_flush_timer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2);
    buffer.push(create_test_request());

    let before_take = Instant::now();
    buffer.take();
    let after_take = Instant::now();

    let elapsed = buffer.last_flush.elapsed();
    // last_flush should be very recent (within 100ms)
    assert!(
        elapsed < Duration::from_millis(100),
        "last_flush not reset properly"
    );

    // Verify it's actually set to after before_take
    assert!(buffer.last_flush >= before_take);
    assert!(buffer.last_flush <= after_take + Duration::from_millis(10));
}

/// Test: freshly created buffer is empty
#[test]
fn test_fresh_buffer_is_empty() {
    let buffer = BatchBuffer::<TestRequest>::new(5);

    assert!(buffer.is_empty());
    assert_eq!(buffer.buffer.len(), 0);
}
