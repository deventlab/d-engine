use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;

use crate::async_task::spawn_task;
use crate::async_task::task_with_timeout_and_exponential_backoff;
use crate::BackoffPolicy;
use crate::Error;

#[tokio::test]
async fn test_task_with_timeout_and_exponential_backoff_success() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let task = move || {
        let counter = counter_clone.clone();
        async move {
            let current = counter.fetch_add(1, Ordering::SeqCst);
            if current == 0 {
                Err(Error::Fatal("First attempt fails".to_string()))
            } else {
                Ok::<_, crate::Error>(current)
            }
        }
    };

    let policy = BackoffPolicy {
        base_delay_ms: 10,
        max_delay_ms: 100,
        timeout_ms: 1000,
        max_retries: 3,
    };

    let result = task_with_timeout_and_exponential_backoff(task, policy).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
    assert_eq!(counter.load(Ordering::SeqCst), 2); // 1 failure + 1 success
}

#[tokio::test]
async fn test_task_with_timeout_and_exponential_backoff_max_retries() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let task = move || {
        let counter = counter_clone.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Err::<u32, _>(Error::Fatal("Always fails".to_string()))
        }
    };

    let policy = BackoffPolicy {
        base_delay_ms: 10,
        max_delay_ms: 100,
        timeout_ms: 1000,
        max_retries: 3,
    };

    let result = task_with_timeout_and_exponential_backoff(task, policy).await;

    assert!(result.is_err());
    assert_eq!(counter.load(Ordering::SeqCst), 3); // 3 retries
}

#[tokio::test]
async fn test_task_with_timeout_and_exponential_backoff_timeout() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let task = move || {
        let counter = counter_clone.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            // Simulate a long-running task that will timeout
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<u32, _>(42)
        }
    };

    let policy = BackoffPolicy {
        base_delay_ms: 10,
        max_delay_ms: 100,
        timeout_ms: 100, // Short timeout
        max_retries: 2,
    };

    let result = task_with_timeout_and_exponential_backoff(task, policy).await;

    assert!(result.is_err());
    // Should have at least 1 attempt (might be 2 depending on timing)
    assert!(counter.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn test_spawn_task() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let task_fn = move || {
        let counter = counter_clone.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let mut handles = Vec::new();
    spawn_task("test_task", task_fn, Some(&mut handles)).await;

    // Wait for the task to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handles.len(), 1);

    // Wait for the handle to complete
    join_all(handles).await;
}

#[tokio::test]
async fn test_spawn_task_without_handles() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let task_fn = move || {
        let counter = counter_clone.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    // Test without passing handles vector
    spawn_task("test_task", task_fn, None).await;

    // Wait for the task to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_spawn_task_with_error() {
    let task_fn = move || async move { Err::<(), _>(Error::Fatal("Task error".to_string())) };

    let mut handles = Vec::new();
    spawn_task("error_task", task_fn, Some(&mut handles)).await;

    // Wait for the task to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(handles.len(), 1);

    // Wait for the handle to complete
    join_all(handles).await;
}
