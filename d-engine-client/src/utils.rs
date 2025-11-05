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

/// Normalizes an address string by ensuring it has a proper scheme prefix.
///
/// This function:
/// - Detects and preserves existing "http://" or "https://" schemes
/// - Defaults to "http://" when no scheme is present
/// - Prevents scheme duplication
///
/// # Examples
/// - "127.0.0.1:9000" -> "http://127.0.0.1:9000"
/// - "http://127.0.0.1:9000" -> "http://127.0.0.1:9000"
/// - "https://127.0.0.1:9000" -> "https://127.0.0.1:9000"
/// - "node1:9000" -> "http://node1:9000"
pub(crate) fn address_str(addr: &str) -> String {
    // Detect if the address already has a scheme
    if addr.starts_with("https://") {
        // Preserve HTTPS scheme
        addr.to_string()
    } else if addr.starts_with("http://") {
        // Preserve HTTP scheme
        addr.to_string()
    } else {
        // No scheme present, default to HTTP
        format!("http://{addr}",)
    }
}
