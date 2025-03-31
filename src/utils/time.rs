use std::time::{SystemTime, UNIX_EPOCH};

/// return minisecond
pub(crate) fn get_now_as_u128() -> u128 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_millis()
}

/// return second
pub(crate) fn get_now_as_u64() -> u64 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_secs()
}

/// return second as u32
pub(crate) fn get_now_as_u32() -> u32 {
    get_now_as_u64() as u32
}
