use std::fmt;

use crate::convert::abs_ceil;
use crate::convert::classify_error;
use crate::convert::safe_kv;
use crate::convert::safe_kv_bytes;
use crate::convert::skv;
use crate::convert::str_to_u64;
use crate::convert::u64_to_bytes;

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[test]
fn test_str_to_u64() {
    // Test that same string produces same hash
    let result1 = str_to_u64("test");
    let result2 = str_to_u64("test");
    assert_eq!(result1, result2);

    // Test that different strings produce different hashes
    let result3 = str_to_u64("test1");
    assert_ne!(result1, result3);

    // Test empty string
    let result4 = str_to_u64("");
    assert_eq!(result4, str_to_u64(""));
}

#[test]
fn test_safe_kv() {
    // Test basic conversion
    let result = safe_kv(0x1234_5678_9ABC_DEF0);
    assert_eq!(result, [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]);

    // Test zero
    let result = safe_kv(0);
    assert_eq!(result, [0, 0, 0, 0, 0, 0, 0, 0]);

    // Test max value
    let result = safe_kv(u64::MAX);
    assert_eq!(result, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
}
#[test]
fn test_skv() {
    // Test string encoding
    let name = "test".to_string();
    let result = skv(name);
    // The exact encoding depends on prost, but it should not be empty
    assert!(!result.is_empty());
}

#[test]
fn test_abs_ceil() {
    // Test basic cases
    assert_eq!(abs_ceil(0.3), 1);
    assert_eq!(abs_ceil(0.5), 1);
    assert_eq!(abs_ceil(1.1), 2);
    assert_eq!(abs_ceil(1.9), 2);

    // Test negative values (absolute value)
    assert_eq!(abs_ceil(-0.3), 1);
    assert_eq!(abs_ceil(-1.9), 2);

    // Test zero
    assert_eq!(abs_ceil(0.0), 0);
}

#[test]
fn test_classify_error() {
    // Test IO error classification
    let io_error = TestError("I/O error reading file");
    assert_eq!(classify_error(&io_error), "io_error");

    // Test corruption error classification
    let corruption_error = TestError("Data corrupt in block 123");
    assert_eq!(classify_error(&corruption_error), "corruption");

    // Test timeout error classification
    let timeout_error = TestError("Request timeout after 30s");
    assert_eq!(classify_error(&timeout_error), "timeout");

    // Test unknown error classification
    let unknown_error = TestError("Some other error");
    assert_eq!(classify_error(&unknown_error), "unknown");
}

#[test]
fn test_u64_to_bytes() {
    // Test basic conversion
    let result = u64_to_bytes(0x1234_5678_9ABC_DEF0);
    assert_eq!(result.as_ref(), &[
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0
    ]);

    // Test zero
    let result = u64_to_bytes(0);
    assert_eq!(result.as_ref(), &[0, 0, 0, 0, 0, 0, 0, 0]);

    // Test max value
    let result = u64_to_bytes(u64::MAX);
    assert_eq!(result.as_ref(), &[
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
    ]);
}

#[test]
fn test_safe_kv_bytes() {
    // Verify it delegates to u64_to_bytes
    let key = 0x1234_5678_9ABC_DEF0;
    assert_eq!(safe_kv_bytes(key), u64_to_bytes(key));
}
