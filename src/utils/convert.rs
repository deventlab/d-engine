use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use prost::Message;
use sled::IVec;
use tracing::error;

use crate::ConvertError;
use crate::Result;

pub fn str_to_u64(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[deprecated(
    since = "0.2.0",
    note = "Please use the more efficient and safe `u64::to_be_bytes()` instead. For custom lengths use the new safe_kv() method"
)]
pub fn kv(i: u64) -> Vec<u8> {
    // let i = i % SPACE;
    // let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    let k = [
        ((i >> 56) & 0xFF) as u8,
        ((i >> 48) & 0xFF) as u8,
        ((i >> 40) & 0xFF) as u8,
        ((i >> 32) & 0xFF) as u8,
        ((i >> 24) & 0xFF) as u8,
        ((i >> 16) & 0xFF) as u8,
        ((i >> 8) & 0xFF) as u8,
        (i & 0xFF) as u8,
    ];
    k.to_vec()
}

#[deprecated(
    since = "0.2.0",
    note = "Please use the safer `safe_vk()` method instead, which provides proper error handling"
)]
pub fn vk(v: &[u8]) -> u64 {
    if v.is_empty() {
        error!("v is empty");
    }

    // Expand the vector to length 8
    let mut expanded = [0; 8]; // Create a vector of 8 zeros
    let len = v.len();

    // Copy the original vector into the expanded vector
    expanded[..len].copy_from_slice(v); // Copy the elements

    assert_eq!(expanded.len(), 8); // Ensure the length is correct

    let mut result: u64 = 0;
    for &byte in expanded.iter() {
        result = (result << 8) | byte as u64; // Shift left by 8 bits and add
                                              // the byte
    }
    result
}

/// Converts a `u64` to an 8-byte array in big-endian byte order.
///
/// # Examples
/// ```
/// use d_engine::convert::safe_kv;
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

pub fn vki(v: &IVec) -> u64 {
    // Convert `IVec` to a byte slice
    let bytes: &[u8] = v.as_ref();

    // Check if the byte slice is empty
    if bytes.is_empty() {
        error!("v is empty");
    }

    // Ensure the length is correct
    assert_eq!(bytes.len(), 8); // Expecting exactly 8 bytes for u64

    // Compute the u64 value from the byte slice
    let mut result: u64 = 0;
    for &byte in bytes.iter() {
        result = (result << 8) | byte as u64; // Shift left by 8 bits and add
                                              // the byte
    }
    result
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
