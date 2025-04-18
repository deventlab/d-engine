use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use tokio::time::sleep;

use super::cluster::find_nearest_lower_number;
use crate::async_task::task_with_timeout_and_exponential_backoff;
use crate::convert::abs_ceil;
use crate::convert::kv;
use crate::convert::vk;
use crate::utils::cluster::is_majority;
use crate::BackoffPolicy;
use crate::Error;
use crate::NetworkError;
use crate::Result;

#[test]
fn test_kv_1() {
    let v = kv(1);
    assert_eq!(1, vk(&v));
    let v = kv(25);
    assert_eq!(25, vk(&v));
}

#[test]
fn test_kv_2() {
    let i = u64::MAX; //max of u64
    let v = kv(i);
    assert_eq!(i, vk(&v));
}
#[test]
fn test_kv_3() {
    let v = kv(1);
    assert_eq!(1, vk(&v));
}

#[test]
fn test_abs_ceil() {
    assert_eq!(1, abs_ceil(0.3));
    assert_eq!(1, abs_ceil(0.5));
    assert_eq!(2, abs_ceil(1.1));
    assert_eq!(2, abs_ceil(1.9));

    let n = 4_f64 / 10.0;
    assert_eq!(1, abs_ceil(n));
}
#[test]
fn test_is_majority() {
    assert!(!is_majority(0, 3));
    assert!(!is_majority(1, 3));
    assert!(is_majority(2, 3));
    assert!(is_majority(3, 3));
}

#[test]
fn test_find_nearest_lower_number() {
    let pool: Arc<DashSet<u64>> = Arc::new(DashSet::new());
    // Populate the DashSet with initial values
    pool.insert(1);
    pool.insert(1000);
    pool.insert(3210);
    pool.insert(1382);
    pool.insert(1483);
    pool.insert(2678);
    pool.insert(1637);
    pool.insert(1902);

    // Target number
    let a: u64 = 1701;

    match find_nearest_lower_number(a, pool.clone()) {
        Some(nearest) => assert_eq!(nearest, 1637),
        None => assert!(false),
    }
}

async fn async_ok() -> Result<()> {
    sleep(Duration::from_millis(100)).await;
    Ok(())
}
async fn async_err() -> Result<()> {
    sleep(Duration::from_millis(100)).await;
    Err(NetworkError::ServiceUnavailable("".to_string()).into())
}

#[tokio::test]
async fn test_task_with_exponential_backoff() {
    // Case 1: when ok task return ok
    let policy = BackoffPolicy {
        max_retries: 3,
        timeout_ms: 100,
        base_delay_ms: 1000,
        max_delay_ms: 3000,
    };
    if let Ok(_) = task_with_timeout_and_exponential_backoff(async_ok, policy.clone()).await {
        assert!(true);
    } else {
        assert!(false);
    }
    // Case 2: when err task return error
    if let Ok(_) = task_with_timeout_and_exponential_backoff(async_err, policy.clone()).await {
        assert!(false);
    } else {
        assert!(true);
    }

    // Case 3: when ok task always failed on timeout error
    let policy = BackoffPolicy {
        max_retries: 3,
        timeout_ms: 1,
        base_delay_ms: 1,
        max_delay_ms: 3,
    };
    if let Ok(_) = task_with_timeout_and_exponential_backoff(async_ok, policy.clone()).await {
        assert!(false);
    } else {
        assert!(true);
    }
}
