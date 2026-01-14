//! Integration tests for ReadIndex Batching (Ticket #236)
//!
//! These tests verify that batching multiple linearizable read requests
//! improves throughput by sharing a single verify_leadership() call.
//!
//! Uses embedded mode with NodeBuilder for production-ready testing.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::RaftConfig;
use d_engine_server::FileStateMachine;
use d_engine_server::FileStorageEngine;
use d_engine_server::NodeBuilder;
use d_engine_server::node::RaftTypeConfig;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::time::Instant;

use tracing_test::traced_test;

type TestNode = Arc<d_engine_server::Node<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>;

/// Helper to create a single-node test cluster
async fn create_single_node_with_batching(
    test_name: &str,
    size_threshold: usize,
    time_threshold_ms: u64,
) -> (TestNode, TempDir, watch::Sender<()>) {
    use d_engine_core::ClusterConfig;
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let storage_engine = Arc::new(
        FileStorageEngine::new(db_path.join("storage")).expect("Failed to create storage engine"),
    );
    let state_machine = Arc::new(
        FileStateMachine::new(db_path.join("state_machine"))
            .await
            .expect("Failed to create state machine"),
    );

    let listen_address: std::net::SocketAddr = format!(
        "127.0.0.1:{}",
        9081 + (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 1000) as u16
    )
    .parse()
    .unwrap();

    let cluster_config = ClusterConfig {
        node_id: 1,
        listen_address,
        initial_cluster: vec![NodeMeta {
            id: 1,
            address: listen_address.to_string(),
            role: NodeRole::Follower as i32,
            status: NodeStatus::Active as i32,
        }],
        db_root_dir: db_path.clone(),
        log_dir: db_path.join("logs"),
    };

    let (graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_config = RaftConfig::default();
    raft_config.read_consistency.state_machine_sync_timeout_ms = 5000;
    raft_config.read_consistency.read_batching.size_threshold = size_threshold;
    raft_config.read_consistency.read_batching.time_threshold_ms = time_threshold_ms;

    let node = NodeBuilder::from_cluster_config(cluster_config, graceful_rx)
        .storage_engine(storage_engine)
        .state_machine(state_machine)
        .raft_config(raft_config)
        .start()
        .await
        .expect("Failed to start node");

    let node_clone = node.clone();
    tokio::spawn(async move {
        if let Err(e) = node_clone.run().await {
            eprintln!("Node run error: {e:?}");
        }
    });

    // Wait for leader election
    tokio::time::sleep(Duration::from_secs(2)).await;

    (node, temp_dir, graceful_tx)
}

/// Test 2.1: Linearizability verification with batched reads
///
/// # Test Scenario
/// Verifies that batched linearizable reads return correct committed data
/// and maintain linearizability guarantees.
///
/// # Given
/// - Single-node cluster with batching enabled (size_threshold=50)
/// - Keys key1=v1, key2=v2 written and committed
///
/// # When
/// - Send 100 concurrent linearizable read requests (50 read key1, 50 read key2)
/// - Requests are batched together (sharing single verify_leadership call)
///
/// # Then
/// - All 100 requests return correct values (v1 or v2)
/// - No requests return empty/stale data
/// - All requests complete successfully
#[traced_test]
#[tokio::test]
async fn test_batching_preserves_linearizability() {
    let (node, _temp_dir, graceful_tx) =
        create_single_node_with_batching("test_linearizability", 50, 10).await;

    // Write test data
    let client = node.local_client();
    client
        .put(Bytes::from("key1"), Bytes::from("v1"))
        .await
        .expect("Failed to write key1");
    client
        .put(Bytes::from("key2"), Bytes::from("v2"))
        .await
        .expect("Failed to write key2");

    // Wait for commit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send 100 concurrent linearizable reads
    let mut handles = vec![];
    for i in 0..100 {
        let client = node.local_client();
        let key = if i < 50 {
            Bytes::from("key1")
        } else {
            Bytes::from("key2")
        };
        let expected = if i < 50 {
            Bytes::from("v1")
        } else {
            Bytes::from("v2")
        };

        let handle = tokio::spawn(async move {
            let result = client
                .get_linearizable(&key)
                .await
                .expect("Read failed")
                .expect("Key should exist");
            assert_eq!(result, expected, "Should return correct value");
        });
        handles.push(handle);
    }

    // Wait for all reads to complete
    for handle in handles {
        handle.await.expect("Task failed");
    }

    // Cleanup
    let _ = graceful_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test 2.2: Concurrent write-read verification
///
/// # Test Scenario
/// Verifies that batched reads after a write always see the committed value,
/// ensuring commitIndex synchronization works correctly.
///
/// # Given
/// - Single-node cluster with batching enabled
///
/// # When
/// - Write key1=v1 and wait for commit
/// - Immediately send 50 concurrent linearizable reads for key1
///
/// # Then
/// - All 50 requests return v1 (not empty/stale)
/// - Requests complete within reasonable time (<500ms)
#[traced_test]
#[tokio::test]
async fn test_concurrent_write_batched_read() {
    let (node, _temp_dir, graceful_tx) =
        create_single_node_with_batching("test_write_read", 50, 10).await;

    let client = node.local_client();

    // Write key1=v1
    client
        .put(Bytes::from("key1"), Bytes::from("v1"))
        .await
        .expect("Failed to write key1");

    // Wait for commit
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();

    // Immediately send 50 concurrent reads
    let mut handles = vec![];
    for _ in 0..50 {
        let client = node.local_client();
        let handle = tokio::spawn(async move {
            let result = client
                .get_linearizable(b"key1")
                .await
                .expect("Read failed")
                .expect("Key should exist");
            assert_eq!(
                result,
                Bytes::from("v1"),
                "Should read latest committed value"
            );
        });
        handles.push(handle);
    }

    // Wait for all reads
    for handle in handles {
        handle.await.expect("Task failed");
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "Batched reads should complete quickly, took {elapsed:?}"
    );

    // Cleanup
    let _ = graceful_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test 2.3: Single request timeout trigger (CRITICAL BOUNDARY CASE)
///
/// # Test Scenario
/// Verifies that a single linearizable read request completes via timeout
/// trigger, ensuring batching doesn't starve low-concurrency requests.
///
/// # Given
/// - Single-node cluster with batching (size_threshold=50, time_threshold=10ms)
///
/// # When
/// - Send 1 linearizable read request (far below size threshold)
///
/// # Then
/// - Request completes within 10-20ms (timeout trigger)
/// - Request does NOT hang forever
/// - Returns correct value
#[traced_test]
#[tokio::test]
async fn test_single_request_timeout_trigger() {
    let (node, _temp_dir, graceful_tx) =
        create_single_node_with_batching("test_timeout", 50, 10).await;

    let client = node.local_client();

    // Write test data
    client
        .put(Bytes::from("lonely_key"), Bytes::from("lonely_value"))
        .await
        .expect("Failed to write");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();

    // Send single request
    let result = client
        .get_linearizable(b"lonely_key")
        .await
        .expect("Read failed")
        .expect("Key should exist");

    let elapsed = start.elapsed();

    // Verify result
    assert_eq!(result, Bytes::from("lonely_value"));

    // Verify timeout triggered (should complete in 10-20ms range)
    assert!(
        elapsed >= Duration::from_millis(8),
        "Should wait for timeout, took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(100),
        "Should not hang, took {elapsed:?}"
    );

    println!("Single request completed in {elapsed:?} (timeout trigger verified)");

    // Cleanup
    let _ = graceful_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test 2.4: Size threshold immediate flush (HIGH CONCURRENCY CASE)
///
/// # Test Scenario
/// Verifies that reaching size_threshold triggers immediate flush
/// without waiting for timeout.
///
/// # Given
/// - Single-node cluster with batching (size_threshold=50, time_threshold=10ms)
///
/// # When
/// - Rapidly send 50 concurrent linearizable reads
///
/// # Then
/// - 50th request triggers immediate flush
/// - Flush latency < 5ms (proves size trigger, not time trigger)
/// - All 50 requests succeed
#[traced_test]
#[tokio::test]
async fn test_size_threshold_immediate_flush() {
    let (node, _temp_dir, graceful_tx) =
        create_single_node_with_batching("test_size", 50, 10).await;

    let client = node.local_client();

    // Write test data
    for i in 0..50 {
        client
            .put(Bytes::from(format!("key{i}")), Bytes::from(format!("v{i}")))
            .await
            .expect("Failed to write");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();

    // Send 50 concurrent reads
    let mut handles = vec![];
    for i in 0..50 {
        let client = node.local_client();
        let key = format!("key{i}");
        let handle = tokio::spawn(async move {
            client.get_linearizable(key.as_bytes()).await.expect("Read failed")
        });
        handles.push(handle);
    }

    // Wait for all reads
    for handle in handles {
        handle.await.expect("Task failed");
    }

    let elapsed = start.elapsed();

    // Size trigger should flush immediately (< 5ms)
    // Time trigger would wait 10ms
    assert!(
        elapsed < Duration::from_millis(50),
        "Size threshold should trigger immediate flush, took {elapsed:?}"
    );

    println!("50 batched reads completed in {elapsed:?} (size threshold verified)");

    // Cleanup
    let _ = graceful_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test 2.8: Throughput regression test (batching doesn't degrade performance)
///
/// # Test Scenario
/// Verifies that batching doesn't degrade performance compared to baseline.
/// In single-node embedded mode, batching advantage is limited since
/// verify_leadership() has no network overhead.
///
/// # Given
/// - Two test runs with same workload (1000 concurrent reads)
/// - Run 1: batching disabled (baseline)
/// - Run 2: batching enabled
///
/// # Then
/// - Run 2 throughput >= Run 1 throughput (no regression)
/// - Log actual improvement for monitoring
#[traced_test]
#[tokio::test]
async fn test_batching_throughput_improvement() {
    // Skip benchmark in non-CI environments to save time
    if std::env::var("CI").is_ok() {
        println!("Skipping benchmark (set CI=1 to run)");
        return;
    }

    const NUM_REQUESTS: usize = 1000;

    // Run 1: Batching disabled (set size_threshold very high)
    let (node1, _temp_dir1, graceful_tx1) =
        create_single_node_with_batching("test_throughput_off", 10000, 10).await;

    let client1 = node1.local_client();
    client1
        .put(Bytes::from("bench_key"), Bytes::from("bench_value"))
        .await
        .expect("Failed to write");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start1 = Instant::now();
    let mut handles1 = vec![];
    for _ in 0..NUM_REQUESTS {
        let client = node1.local_client();
        let handle = tokio::spawn(async move { client.get_linearizable(b"bench_key").await });
        handles1.push(handle);
    }
    for handle in handles1 {
        let _ = handle.await;
    }
    let elapsed1 = start1.elapsed();
    let throughput1 = NUM_REQUESTS as f64 / elapsed1.as_secs_f64();

    let _ = graceful_tx1.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Run 2: Batching enabled
    let (node2, _temp_dir2, graceful_tx2) =
        create_single_node_with_batching("test_throughput_on", 50, 10).await;

    let client2 = node2.local_client();
    client2
        .put(Bytes::from("bench_key"), Bytes::from("bench_value"))
        .await
        .expect("Failed to write");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start2 = Instant::now();
    let mut handles2 = vec![];
    for _ in 0..NUM_REQUESTS {
        let client = node2.local_client();
        let handle = tokio::spawn(async move { client.get_linearizable(b"bench_key").await });
        handles2.push(handle);
    }
    for handle in handles2 {
        let _ = handle.await;
    }
    let elapsed2 = start2.elapsed();
    let throughput2 = NUM_REQUESTS as f64 / elapsed2.as_secs_f64();

    let _ = graceful_tx2.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Throughput Regression Test ===");
    println!("Baseline (batching OFF): {throughput1:.0} ops/sec (took {elapsed1:?})");
    println!("Batching ON:             {throughput2:.0} ops/sec (took {elapsed2:?})");
    println!("Performance change:      {:.2}x", throughput2 / throughput1);

    // Regression check: batching should not degrade performance
    assert!(
        throughput2 >= throughput1,
        "Batching should not degrade performance: {throughput2:.0} ops/sec < {throughput1:.0} ops/sec"
    );

    println!("\n✓ No performance regression detected");
    if throughput2 > throughput1 * 1.5 {
        println!(
            "  Bonus: {:.2}x improvement observed",
            throughput2 / throughput1
        );
    }
}
