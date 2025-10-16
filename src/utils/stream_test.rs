use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::stream::create_production_snapshot_stream;
use crate::stream::encode_varint;
use crate::stream::encoded_len_varint;

// Simple test message
#[derive(Clone, PartialEq, Message)]
pub struct TestMessage {
    #[prost(string, tag = "1")]
    pub data: String,
}

#[tokio::test]
async fn test_create_production_snapshot_stream() {
    let (tx, rx) = mpsc::channel(10);
    let max_message_size = 1024 * 1024; // 1MB

    // Create the stream
    let mut stream = create_production_snapshot_stream::<TestMessage>(rx, max_message_size);

    // Send a test message
    let test_msg = TestMessage {
        data: "test".to_string(),
    };
    tx.send(Ok(Arc::new(test_msg))).await.unwrap();

    // Close the channel
    drop(tx);

    // Receive from the stream
    let received = stream.next().await;
    assert!(received.is_some());

    let result = received.unwrap();
    assert!(result.is_ok());
    let msg = result.unwrap();
    assert_eq!(msg.data, "test");

    // Stream should be done now
    assert!(stream.next().await.is_none());
}

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
