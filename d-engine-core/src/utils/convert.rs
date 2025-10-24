use bytes::Bytes;
use prost::Message;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use crate::ConvertError;
use crate::Result;

pub fn str_to_u64(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// Convert to big-endian bytes with a single allocation
pub fn u64_to_bytes(value: u64) -> Bytes {
    let bytes = value.to_be_bytes();
    Bytes::copy_from_slice(&bytes)
}

pub fn safe_kv_bytes(key: u64) -> Bytes {
    u64_to_bytes(key)
}

/// Converts a `u64` to an 8-byte array in big-endian byte order.
///
/// # Examples
/// ```rust
/// use d_engine_core::convert::safe_kv;
///
/// let bytes = safe_kv(0x1234_5678_9ABC_DEF0);
/// assert_eq!(bytes, [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]);
/// ```
pub const fn safe_kv(num: u64) -> [u8; 8] {
    num.to_be_bytes()
}

pub fn safe_vk<K: AsRef<[u8]>>(bytes: K) -> Result<u64> {
    let bytes = bytes.as_ref();
    let expected_len = 8;

    if bytes.len() != expected_len {
        return Err(ConvertError::InvalidLength(bytes.len()).into());
    }
    let array: [u8; 8] = bytes.try_into().expect("Guaranteed safe after length check");
    Ok(u64::from_be_bytes(array))
}

pub fn skv(name: String) -> Vec<u8> {
    name.encode_to_vec()
}

/// return (high, low)
pub(crate) fn convert_u128_to_u64_with_high_and_low(n: u128) -> (u64, u64) {
    ((n >> 64) as u64, n as u64)
}
/// return (high, low)
pub(crate) fn convert_high_and_low_fromu64_to_u128(
    high: u64,
    low: u64,
) -> u128 {
    ((high as u128) << 64) | (low as u128)
}

/// abs_ceil(0.3) = 1
/// abs_ceil(0.5) = 1
/// abs_ceil(1.1) = 2
/// abs_ceil(1.9) = 2
pub fn abs_ceil(x: f64) -> u64 {
    x.abs().ceil() as u64
}

// Helper function to classify errors into known types
pub fn classify_error(e: &impl std::fmt::Display) -> String {
    let error_str = e.to_string();
    if error_str.contains("io") || error_str.contains("I/O") {
        "io_error".to_string()
    } else if error_str.contains("corrupt") {
        "corruption".to_string()
    } else if error_str.contains("timeout") {
        "timeout".to_string()
    } else {
        "unknown".to_string()
    }
}
