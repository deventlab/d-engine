use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns seconds since epoch as u32
/// # Warning
/// This will overflow on 2038-01-19 (Year 2038 problem)
#[inline]
pub fn get_now_as_u32() -> u32 {
    get_duration_since_epoch().as_secs() as u32
}

#[inline]
pub fn get_duration_since_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards")
}

/// accept ip either like 127.0.0.1 or docker host name: node1
pub(crate) fn address_str(addr: &str) -> String {
    // Strip existing "http://" or "https://" prefixes if duplicated.
    let normalized = addr.trim_start_matches("http://").trim_start_matches("https://");
    // Re-add a single "http://" prefix (or use HTTPS if needed).
    format!("http://{normalized}")
}
