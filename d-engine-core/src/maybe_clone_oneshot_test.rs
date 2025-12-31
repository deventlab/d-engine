use std::sync::Arc;

use tokio::sync::Mutex;

use crate::maybe_clone_oneshot::MaybeCloneOneshot;
use crate::maybe_clone_oneshot::RaftOneshot;

#[tokio::test]
async fn test_oneshot_basic_send_recv() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = 42;

    tx.send(value).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_clone_sender() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let tx_cloned = tx.clone();

    let value = 100;
    tx_cloned.send(value).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_clone_receiver() {
    let (tx, rx) = MaybeCloneOneshot::new();
    let mut rx_cloned = rx.clone();

    let value = 123;
    tx.send(value).expect("Failed to send");
    let received = rx_cloned.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_multiple_clones() {
    let (tx, rx1) = MaybeCloneOneshot::new();
    let rx2 = rx1.clone();
    let tx_cloned = tx.clone();

    let value = 999;
    tx_cloned.send(value).expect("Failed to send");

    let mut rx1_mut = rx1;
    let mut rx2_mut = rx2;
    let received1 = rx1_mut.recv().await.expect("Failed to receive rx1");
    let received2 = rx2_mut.recv().await.expect("Failed to receive rx2");

    assert_eq!(received1, value);
    assert_eq!(received2, value);
}

#[tokio::test]
async fn test_oneshot_with_string() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = "test_message".to_string();

    tx.send(value.clone()).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_concurrent_receivers() {
    let (tx, rx) = MaybeCloneOneshot::new();
    let rx_clone1 = rx.clone();
    let rx_clone2 = rx.clone();

    let value = 555;
    tx.send(value).expect("Failed to send");

    let handle1 = tokio::spawn(async move {
        let mut r = rx_clone1;
        r.recv().await.expect("Failed in task 1")
    });

    let handle2 = tokio::spawn(async move {
        let mut r = rx_clone2;
        r.recv().await.expect("Failed in task 2")
    });

    let result1 = handle1.await.expect("Task 1 failed");
    let result2 = handle2.await.expect("Task 2 failed");

    assert_eq!(result1, value);
    assert_eq!(result2, value);
}

#[tokio::test]
async fn test_oneshot_with_vector() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = vec![1, 2, 3, 4, 5];

    tx.send(value.clone()).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_stress_multiple_sends() {
    for i in 0..100 {
        let (tx, mut rx) = MaybeCloneOneshot::new();
        tx.send(i).expect("Failed to send");
        let received = rx.recv().await.expect("Failed to receive");
        assert_eq!(received, i);
    }
}

#[tokio::test]
async fn test_oneshot_clone_chain() {
    let (tx, rx) = MaybeCloneOneshot::new();
    let rx1 = rx.clone();
    let rx2 = rx1.clone();
    let mut rx3 = rx2.clone();

    let value = 777;
    tx.send(value).expect("Failed to send");

    let received = rx3.recv().await.expect("Failed to receive");
    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_sender_clone_and_send() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let tx1 = tx.clone();
    let tx2 = tx1.clone();

    let value = 444;
    tx2.send(value).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_with_custom_struct() {
    #[derive(Debug, Clone, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
    }

    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = TestData {
        id: 42,
        name: "test".to_string(),
    };

    tx.send(value.clone()).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
    assert_eq!(received.id, 42);
    assert_eq!(received.name, "test");
}

#[tokio::test]
async fn test_oneshot_arc_shared_state() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let shared_data = Arc::new(Mutex::new(vec![1, 2, 3]));
    let shared_clone = Arc::clone(&shared_data);

    tx.send(shared_clone).expect("Failed to send");
    let received_arc = rx.recv().await.expect("Failed to receive");
    let guard = received_arc.lock().await;

    assert_eq!(guard.len(), 3);
    assert_eq!(guard[0], 1);
    assert_eq!(guard[1], 2);
    assert_eq!(guard[2], 3);
}

#[tokio::test]
async fn test_oneshot_receiver_as_future() {
    let (tx, rx) = MaybeCloneOneshot::new();
    let value = 333;

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        tx.send(value).expect("Failed to send");
    });

    let received = rx.await.expect("Failed to receive");
    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_with_zero() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = 0;

    tx.send(value).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_with_max_u64() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = u64::MAX;

    tx.send(value).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
}

#[tokio::test]
async fn test_oneshot_empty_string() {
    let (tx, mut rx) = MaybeCloneOneshot::new();
    let value = String::new();

    tx.send(value.clone()).expect("Failed to send");
    let received = rx.recv().await.expect("Failed to receive");

    assert_eq!(received, value);
    assert!(received.is_empty());
}
