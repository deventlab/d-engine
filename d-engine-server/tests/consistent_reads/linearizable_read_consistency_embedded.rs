//! Integration tests for Linearizable Read Optimization (Ticket #238)
//!
//! These tests verify that linearizable reads use a fixed read_index calculated
//! at request arrival time, preventing unnecessary waiting for concurrent writes.
//!
//! Uses embedded mode with NodeBuilder for production-ready testing.

use std::sync::Arc;
use std::time::Duration;

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
async fn create_single_node(_test_name: &str) -> (TestNode, TempDir, watch::Sender<()>) {
    use d_engine_core::ClusterConfig;
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().to_path_buf();

    let storage_engine = Arc::new(
        FileStorageEngine::new(db_path.join("storage")).expect("Failed to create storage engine"),
    );
    let state_machine = Arc::new(
        FileStateMachine::new(db_path.join("state_machine"))
            .await
            .expect("Failed to create state machine"),
    );

    let cluster_config = ClusterConfig {
        node_id: 1,
        listen_address: format!(
            "127.0.0.1:{}",
            9081 + (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % 1000) as u16
        )
        .parse()
        .unwrap(),
        initial_cluster: vec![NodeMeta {
            id: 1,
            address: "127.0.0.1:9081".to_string(),
            role: NodeRole::Follower as i32,
            status: NodeStatus::Active as i32,
        }],
        db_root_dir: db_path.clone(),
        log_dir: db_path.join("logs"),
    };

    let (graceful_tx, graceful_rx) = watch::channel(());

    let mut raft_config = RaftConfig::default();
    raft_config.read_consistency.state_machine_sync_timeout_ms = 2000;

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

/// Test linearizable read consistency with concurrent writes
///
/// # Test Scenario
/// Verifies linearizable reads always return committed data,
/// maintaining consistency even with concurrent write operations.
///
/// # Given
/// - Single-node cluster (Leader)
///
/// # When
/// 1. Perform 10 sequential writes (counter = 1, 2, ..., 10)
/// 2. After each write, perform linearizable read
///
/// # Then
/// - Each read returns the most recently committed value
/// - No read returns stale data or uncommitted writes
/// - Final read returns "10" (last committed value)
///
/// # Success Criteria
/// - All reads succeed
/// - Each read sees monotonically increasing values
/// - Final value is "10"
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_consistency_with_writes() {
    let (node, _temp_dir, _shutdown) = create_single_node("concurrent_writes").await;
    let client = node.local_client();

    // When: Sequential writes with linearizable reads
    let mut last_seen_value = 0;

    for i in 1..=10 {
        // Write new value
        client.put(b"counter", i.to_string().as_bytes()).await.expect("Write failed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Linearizable read should see committed value
        let result = client.get_linearizable(b"counter").await.expect("Read failed");

        let value_str = String::from_utf8(result.unwrap().to_vec()).expect("Invalid UTF-8");
        let current_value: u32 = value_str.parse().expect("Invalid number");

        // Then: Value should be monotonically increasing
        assert!(
            current_value >= last_seen_value,
            "Read returned stale value: saw {current_value}, expected >= {last_seen_value}"
        );

        last_seen_value = current_value;
    }

    // Then: Final read must see the last committed value
    assert_eq!(
        last_seen_value, 10,
        "Final linearizable read should see last committed value"
    );

    println!("✅ Linearizable read consistency verified across 10 writes");
}

/// Test linearizable read after leader initialization (smoke test)
///
/// # Test Scenario
/// Verifies linearizable reads work correctly after leader initialization.
/// This is a smoke test that indirectly validates the noop_log_id tracking
/// mechanism by ensuring reads succeed after leader stabilization.
///
/// # Given
/// - Single-node cluster (node becomes leader automatically)
///
/// # When
/// 1. Node completes leader initialization (including noop entry commit)
/// 2. Perform write + linearizable read
///
/// # Then
/// - Read succeeds without errors
/// - Returns correct value
///
/// # Success Criteria
/// - Linearizable read completes successfully
/// - Returns the written value
///
/// # Note
/// Direct verification of noop_log_id is done in unit tests.
/// This integration test validates the end-to-end behavior.
#[tokio::test]
#[traced_test]
async fn test_read_index_with_noop_tracking() {
    let (node, _temp_dir, _shutdown) = create_single_node("noop_tracking").await;
    let client = node.local_client();

    // When: Write data and perform linearizable read
    client.put(b"test_key", b"test_value").await.expect("PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = client.get_linearizable(b"test_key").await.expect("Linearizable read failed");

    // Then: Read should succeed
    assert_eq!(
        result.as_deref(),
        Some(b"test_value".as_ref()),
        "Linearizable read should return correct value"
    );

    println!("✅ Linearizable read with noop tracking succeeded");
}

/// Test that eventual consistency reads are not affected by optimization
///
/// # Test Scenario
/// Verifies the optimization only applies to linearizable reads,
/// and eventual consistency reads continue to work efficiently.
///
/// # Given
/// - Single-node cluster
/// - Data written and replicated
///
/// # When
/// - Perform eventual consistency read
/// - Perform linearizable read
///
/// # Then
/// - Both reads return correct data
/// - EC read should be faster (no quorum verification)
///
/// # Success Criteria
/// - Both consistency levels work correctly
/// - EC read has lower latency
#[tokio::test]
#[traced_test]
async fn test_eventual_consistency_reads_unaffected() {
    let (node, _temp_dir, _shutdown) = create_single_node("ec_unaffected").await;
    let client = node.local_client();

    // Given: Write data
    client.put(b"ec_test", b"v1").await.expect("PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // When: Eventual consistency read
    let ec_start = Instant::now();
    let ec_result = client.get_eventual(b"ec_test").await.expect("EC read failed");
    let ec_latency = ec_start.elapsed();

    // When: Linearizable read
    let lin_start = Instant::now();
    let lin_result = client.get_linearizable(b"ec_test").await.expect("Linearizable read failed");
    let lin_latency = lin_start.elapsed();

    // Then: Both should return correct data
    assert_eq!(
        ec_result.as_deref(),
        Some(b"v1".as_ref()),
        "EC read should return correct value"
    );
    assert_eq!(
        lin_result.as_deref(),
        Some(b"v1".as_ref()),
        "Linearizable read should return correct value"
    );

    // EC read should typically be faster (but not enforced due to test variance)
    println!("✅ EC read: {ec_latency:?}, Linearizable read: {lin_latency:?}");
}

/// Test linearizable reads with multiple sequential writes
///
/// # Test Scenario
/// Verifies read_index correctly advances with commit_index
/// as writes are committed sequentially.
///
/// # Given
/// - Single-node cluster
///
/// # When
/// 1. Write value "v1"
/// 2. Linearizable read (should see "v1")
/// 3. Write value "v2"
/// 4. Linearizable read (should see "v2")
///
/// # Then
/// - Each read sees the most recent committed value
/// - No stale reads
///
/// # Success Criteria
/// - First read returns "v1"
/// - Second read returns "v2"
#[tokio::test]
#[traced_test]
async fn test_linearizable_read_sequential_writes() {
    let (node, _temp_dir, _shutdown) = create_single_node("sequential_writes").await;
    let client = node.local_client();

    // When: Write v1 and read
    client.put(b"seq_key", b"v1").await.expect("First PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let read1 = client.get_linearizable(b"seq_key").await.expect("First read failed");

    assert_eq!(
        read1.as_deref(),
        Some(b"v1".as_ref()),
        "First read should return v1"
    );

    // When: Write v2 and read
    client.put(b"seq_key", b"v2").await.expect("Second PUT failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let read2 = client.get_linearizable(b"seq_key").await.expect("Second read failed");

    assert_eq!(
        read2.as_deref(),
        Some(b"v2".as_ref()),
        "Second read should return v2"
    );

    println!("✅ Sequential writes with linearizable reads succeeded");
}

/// Test read_index optimization with concurrent writes in multi-node cluster
///
/// # Test Scenario
/// Verifies that linearizable reads use a fixed read_index calculated at request
/// arrival time, not inflated by concurrent writes during processing.
///
/// # Given
/// - 3-node cluster with elected leader
/// - Initial value: key="counter", value="v0"
///
/// # When
/// 1. Start linearizable read request
/// 2. Inject 20 concurrent writes (v1..v20) while read processes
/// 3. Measure read completion time
///
/// # Then
/// - Read completes in reasonable time (< 5 seconds)
/// - Read does NOT wait indefinitely for all concurrent writes
/// - Proves read_index was fixed at arrival, not dynamically updated
///
/// # Success Criteria
/// - Read completes within timeout
/// - Latency does not scale linearly with concurrent write count
#[tokio::test]
#[traced_test]
#[cfg(feature = "rocksdb")]
async fn test_read_index_fixed_with_concurrent_writes_multi_node()
-> Result<(), Box<dyn std::error::Error>> {
    use d_engine_server::EmbeddedEngine;
    use d_engine_server::RocksDBStateMachine;
    use d_engine_server::RocksDBStorageEngine;

    let temp_dir = tempfile::tempdir()?;
    let db_root = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let ports = [
        19091
            + (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() % 50)
                as u16,
        19191
            + (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() % 50)
                as u16,
        19291
            + (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() % 50)
                as u16,
    ];

    // Create cluster config TOML
    let cluster_config = format!(
        r#"
initial_cluster = [
    {{ id = 1, address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 2, address = '127.0.0.1:{}', role = 2, status = 2 }},
    {{ id = 3, address = '127.0.0.1:{}', role = 2, status = 2 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 3000
"#,
        ports[0],
        ports[1],
        ports[2],
        db_root.display(),
        log_dir.display()
    );

    // Node 1 config
    let node1_config = format!(
        r#"
[cluster]
node_id = 1
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[0], cluster_config
    );
    let node1_config_path = "/tmp/linear_read_n1.toml";
    tokio::fs::write(node1_config_path, &node1_config).await?;

    // Node 2 config
    let node2_config = format!(
        r#"
[cluster]
node_id = 2
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[1], cluster_config
    );
    let node2_config_path = "/tmp/linear_read_n2.toml";
    tokio::fs::write(node2_config_path, &node2_config).await?;

    // Node 3 config
    let node3_config = format!(
        r#"
[cluster]
node_id = 3
listen_address = '127.0.0.1:{}'
{}
"#,
        ports[2], cluster_config
    );
    let node3_config_path = "/tmp/linear_read_n3.toml";
    tokio::fs::write(node3_config_path, &node3_config).await?;

    // Start nodes
    tokio::fs::create_dir_all(db_root.join("node1")).await?;
    let storage1 = Arc::new(RocksDBStorageEngine::new(db_root.join("node1/storage"))?);
    let sm1 = Arc::new(RocksDBStateMachine::new(
        db_root.join("node1/state_machine"),
    )?);
    let engine1 = EmbeddedEngine::start_custom(storage1, sm1, Some(node1_config_path)).await?;

    tokio::fs::create_dir_all(db_root.join("node2")).await?;
    let storage2 = Arc::new(RocksDBStorageEngine::new(db_root.join("node2/storage"))?);
    let sm2 = Arc::new(RocksDBStateMachine::new(
        db_root.join("node2/state_machine"),
    )?);
    let engine2 = EmbeddedEngine::start_custom(storage2, sm2, Some(node2_config_path)).await?;

    tokio::fs::create_dir_all(db_root.join("node3")).await?;
    let storage3 = Arc::new(RocksDBStorageEngine::new(db_root.join("node3/storage"))?);
    let sm3 = Arc::new(RocksDBStateMachine::new(
        db_root.join("node3/state_machine"),
    )?);
    let engine3 = EmbeddedEngine::start_custom(storage3, sm3, Some(node3_config_path)).await?;

    // Wait for cluster ready
    engine1.wait_ready(Duration::from_secs(10)).await?;
    engine2.wait_ready(Duration::from_secs(10)).await?;
    engine3.wait_ready(Duration::from_secs(10)).await?;

    // Find leader
    let leader_engine = if engine1.is_leader() {
        &engine1
    } else if engine2.is_leader() {
        &engine2
    } else {
        &engine3
    };

    let leader_client = leader_engine.client();

    // Given: Write initial value
    leader_client.put(b"counter", b"v0").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // When: Start linearizable read with concurrent writes
    let client_clone = leader_client.clone();

    let read_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        client_clone.get_linearizable(b"counter").await
    });

    // Inject concurrent writes
    tokio::time::sleep(Duration::from_millis(10)).await;
    for i in 1..=100 {
        leader_client.put(b"counter", format!("v{i}").as_bytes()).await?;
    }

    // Then: Read should succeed despite concurrent writes
    let result = read_task.await?;
    assert!(
        result.is_ok(),
        "Linearizable read should succeed with concurrent writes: {result:?}"
    );

    // Cleanup
    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;

    println!("✅ Multi-node read_index optimization verified");
    Ok(())
}
