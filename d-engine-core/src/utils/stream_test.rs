use crate::stream::encode_varint;
use crate::stream::encoded_len_varint;

#[test]
fn test_encoded_len_varint() {
    // Test small values
    assert_eq!(encoded_len_varint(0), 1);
    assert_eq!(encoded_len_varint(127), 1);

    // Test values that need more bytes
    assert_eq!(encoded_len_varint(128), 2);
    assert_eq!(encoded_len_varint(16383), 2);
    assert_eq!(encoded_len_varint(16384), 3);
}

#[test]
fn test_encode_varint() {
    // Test encoding small value
    let mut buf = Vec::new();
    encode_varint(127, &mut buf);
    assert_eq!(buf, vec![127]);

    // Test encoding value that needs 2 bytes
    buf.clear();
    encode_varint(128, &mut buf);
    assert_eq!(buf, vec![0x80, 0x01]);

    // Test encoding value that needs 3 bytes
    buf.clear();
    encode_varint(16384, &mut buf);
    assert_eq!(buf, vec![0x80, 0x80, 0x01]);
}
