use std::thread::sleep;

use crate::time::get_duration_since_epoch;
use crate::time::get_now_as_u32;
use crate::time::get_now_as_u64;
use crate::time::get_now_as_u128;
use crate::time::timestamp_millis;

#[test]
fn test_timestamp_millis() {
    let t1 = timestamp_millis();
    sleep(std::time::Duration::from_millis(10));
    let t2 = timestamp_millis();

    // Ensure time is moving forward
    assert!(t2 > t1);
}

#[test]
fn test_get_duration_since_epoch() {
    let duration = get_duration_since_epoch();
    // Should be a reasonable value (somewhere between 1970 and now)
    assert!(duration.as_secs() > 1609459200); // Greater than 2021-01-01
}

#[test]
fn test_get_now_as_u128() {
    let t1 = get_now_as_u128();
    sleep(std::time::Duration::from_millis(10));
    let t2 = get_now_as_u128();

    // Ensure time is moving forward
    assert!(t2 > t1);
    // Difference should be at least 10ms
    assert!(t2 - t1 >= 10);
}

#[test]
fn test_get_now_as_u64() {
    let t1 = get_now_as_u64();
    sleep(std::time::Duration::from_secs(1));
    let t2 = get_now_as_u64();

    // Ensure time is moving forward by at least 1 second
    assert!(t2 > t1);
}

#[test]
fn test_get_now_as_u32() {
    let t1 = get_now_as_u32();
    sleep(std::time::Duration::from_secs(1));
    let t2 = get_now_as_u32();

    // Ensure time is moving forward by at least 1 second
    assert!(t2 > t1);

    // Test that it's a reasonable value (should be between 2021 and 2038)
    assert!(t1 > 1609459200); // Greater than 2021-01-01
    assert!(t1 < 2147483647); // Less than the 2038 overflow point
}
