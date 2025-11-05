//! Helper functions for converting between u64 and byte arrays.
//!
//! This module provides utility functions for serializing/deserializing
//! u64 values to/from byte arrays used as keys in sled.

use d_engine::StorageError;
use sled::IVec;

/// Convert a u64 value to an 8-byte big-endian vector.
///
/// # Arguments
/// * `value` - The u64 value to convert
///
/// # Returns
/// An 8-byte vector in big-endian byte order
#[inline]
pub(crate) fn safe_kv(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Convert an 8-byte big-endian slice to a u64 value.
///
/// # Arguments
/// * `bytes` - A byte slice to convert
///
/// # Returns
/// `Ok(u64)` if the slice is exactly 8 bytes, `Err` otherwise
#[inline]
pub(crate) fn safe_vk(bytes: &[u8]) -> Result<u64, StorageError> {
    if bytes.len() != 8 {
        return Err(StorageError::DbError(format!(
            "Invalid byte length: expected 8, got {}",
            bytes.len()
        )));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Convert an IVec (sled's byte vector) to a u64 value.
///
/// This is a convenience wrapper around `safe_vk` for sled's IVec type.
#[inline]
pub(crate) fn safe_vk_ivec(ivec: IVec) -> Result<u64, StorageError> {
    safe_vk(&ivec[..])
}
