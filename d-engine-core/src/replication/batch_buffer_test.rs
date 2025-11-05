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
    let timeout = Duration::from_secs(1);
    let buffer = BatchBuffer::<TestRequest>::new(max_size, timeout);

    assert_eq!(buffer.max_batch_size, max_size);
    assert_eq!(buffer.batch_timeout, timeout);
    assert!(buffer.buffer.is_empty());
    assert!(buffer.buffer.capacity() >= max_size);
    assert!(buffer.last_flush.elapsed() < Duration::from_secs(1));
}

#[test]
fn test_push_below_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_secs(1));
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.buffer.len(), 2);
}

#[test]
fn test_push_exact_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2, Duration::from_secs(1));
    assert_eq!(buffer.push(create_test_request()), None);
    assert_eq!(buffer.push(create_test_request()), Some(2));
    assert_eq!(buffer.buffer.len(), 2);
}

#[test]
fn test_push_exceed_max() {
    let mut buffer = BatchBuffer::<TestRequest>::new(2, Duration::from_secs(1));
    buffer.push(create_test_request());
    buffer.push(create_test_request());
    assert_eq!(buffer.push(create_test_request()), Some(3));
    assert_eq!(buffer.buffer.len(), 3);
}

#[test]
fn test_should_flush_timeout_reached() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_millis(100));
    buffer.push(create_test_request());
    buffer.last_flush = Instant::now() - Duration::from_millis(150);
    assert!(buffer.should_flush());
}

#[test]
fn test_should_flush_timeout_not_reached() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_millis(100));
    buffer.push(create_test_request());
    buffer.last_flush = Instant::now() - Duration::from_millis(50);
    assert!(!buffer.should_flush());
}

#[test]
fn test_should_flush_empty_buffer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_millis(100));
    buffer.last_flush = Instant::now() - Duration::from_millis(150);
    assert!(!buffer.should_flush());
}

#[test]
fn test_take_resets_buffer() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_secs(1));
    buffer.push(create_test_request());

    let taken = buffer.take();
    assert!(!taken.is_empty());
    assert!(buffer.buffer.is_empty());
    assert!(buffer.last_flush.elapsed() < Duration::from_millis(50));
}

#[test]
fn test_take_returns_correct_elements() {
    let mut buffer = BatchBuffer::<TestRequest>::new(3, Duration::from_secs(1));
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
    let mut buffer = BatchBuffer::<TestRequest>::new(2, Duration::from_secs(1));
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
    let mut buffer = BatchBuffer::<TestRequest>::new(2, Duration::from_secs(1));
    buffer.push(create_test_request());
    buffer.take();

    buffer.push(create_test_request());
    assert_eq!(buffer.push(create_test_request()), Some(2));
    assert_eq!(buffer.buffer.len(), 2);
}
