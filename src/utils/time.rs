use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[inline]
pub(crate) fn timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[inline]
fn get_duration_since_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards")
}

/// Returns millis
#[inline]
pub(crate) fn get_now_as_u128() -> u128 {
    get_duration_since_epoch().as_millis()
}

/// Returns second
#[inline]
pub(crate) fn get_now_as_u64() -> u64 {
    get_duration_since_epoch().as_secs()
}

/// Returns seconds since epoch as u32
/// # Warning
/// This will overflow on 2038-01-19 (Year 2038 problem)
#[inline]
pub(crate) fn get_now_as_u32() -> u32 {
    get_duration_since_epoch().as_secs() as u32
}
