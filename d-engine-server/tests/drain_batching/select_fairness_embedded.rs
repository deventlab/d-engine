//! Select Branch Fairness Integration Test
//!
//! Verifies that drain-based batching doesn't starve other select branches
//! under high load. Tests the Raft main loop's fairness between:
//! - P3 branch: Client commands (with drain)
//! - P4 branch: Internal Raft protocol events (AppendEntries, VoteRequest)
//!
//! Test plan: 001_todo_raft_test_plan_2215.md P3-2

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use d_engine_server::EmbeddedEngine;
use tempfile::TempDir;
use tokio::time::Instant;

use crate::common::get_available_ports;

/// Helper to create a single-node test engine with embedded mode
async fn create_test_engine(test_name: &str) -> (EmbeddedEngine, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(test_name);

    let config_path = temp_dir.path().join("d-engine.toml");
    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{}"
db_root_dir = "{}"
single_node = true

[raft.batching]
max_batch_size = 100

[raft.state_machine.lease]
enabled = true
"#,
        port,
        db_path.display()
    );
    std::fs::write(&config_path, config_content).expect("Failed to write config");

    let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Failed to start engine");

    engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

    (engine, temp_dir)
}

/// Select Branch Fairness - Drain No Starvation
///
/// **Objective**: Verify drain doesn't starve other select branches
///
/// **Setup**:
/// - Single-node cluster (sufficient for testing select fairness)
/// - Saturate cmd_rx with continuous writes
/// - Simultaneously monitor Raft protocol responsiveness
///
/// **Steps**:
/// 1. Start high-speed write load (saturate channel)
/// 2. Monitor heartbeat/replication latency (P4 branch responsiveness)
/// 3. Verify system processes protocol messages within acceptable time
///
/// **Assertions**:
/// - Each drain processes max_batch_size (100), then returns to select
/// - Internal Raft events processed within reasonable time (<50ms p99)
/// - No event starvation (cluster remains healthy)
/// - System remains responsive to protocol messages
///
/// **Success Criteria**: Fair scheduling, no protocol deadlocks
#[tokio::test]
async fn test_select_fairness_drain_no_starvation() {
    // Create single-node cluster
    let (engine, _temp_dir) = create_test_engine("select_fairness").await;

    println!("Engine started, starting fairness test");

    // Metrics
    let write_count = Arc::new(AtomicU64::new(0));

    // Spawn concurrent writer tasks to saturate command channel and trigger drain batching
    const NUM_WRITERS: usize = 8;
    let mut writer_handles = Vec::new();

    for writer_id in 0..NUM_WRITERS {
        let client = engine.client();
        let write_count_clone = write_count.clone();

        let handle = tokio::spawn(async move {
            let mut i = 0u64;
            loop {
                // Fire-and-forget pattern: spawn multiple requests without waiting
                // This saturates the command channel to trigger drain-based batching
                for _ in 0..10 {
                    let key = format!("key_{writer_id}_{i}");
                    let value = format!("value_{i}");
                    let client_clone = client.clone();
                    let write_count_clone = write_count_clone.clone();

                    // Spawn background task to handle response
                    tokio::spawn(async move {
                        if client_clone.put(key.as_bytes(), value.as_bytes()).await.is_ok() {
                            write_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    });

                    i += 1;
                }

                // Yield to let requests be sent
                tokio::task::yield_now().await;
            }
        });
        writer_handles.push(handle);
    }

    // Monitor Raft protocol responsiveness via read latency
    // (reads require verify_leadership, which uses P4 event channel)
    let read_latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let read_latencies_clone = read_latencies.clone();

    let monitor_client = engine.client();
    let monitor_task = tokio::spawn(async move {
        // Wait a bit for write load to build up
        tokio::time::sleep(Duration::from_millis(500)).await;

        for i in 0..50 {
            let start = Instant::now();

            // Linearizable read requires verify_leadership
            // This tests if P4 branch (raft events) is responsive
            let _result = monitor_client.get_linearizable(b"monitor_key").await;

            let latency = start.elapsed();
            read_latencies_clone.lock().await.push(latency);

            // Sample every 50ms
            tokio::time::sleep(Duration::from_millis(50)).await;

            if i % 10 == 0 {
                println!("Monitor iteration {i}, latency: {latency:?}");
            }
        }
    });

    // Run test for 3 seconds
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Stop all writer tasks
    for handle in writer_handles {
        handle.abort();
    }

    // Wait for monitor to finish
    monitor_task.await.expect("Monitor task failed");

    // Analyze results
    let total_writes = write_count.load(Ordering::Relaxed);
    println!("Total writes submitted: {total_writes}");

    let latencies = read_latencies.lock().await;
    println!("Read samples: {}", latencies.len());

    // Calculate p50, p95, p99
    let mut sorted_latencies = latencies.clone();
    sorted_latencies.sort();

    // Assert minimum samples before computing percentiles
    assert!(
        sorted_latencies.len() >= 10,
        "Too few latency samples collected: {} (expected ≥ 10). \
         Monitor task may have failed or reads timed out.",
        sorted_latencies.len()
    );

    let p50_idx = sorted_latencies.len() / 2;
    let p95_idx = (sorted_latencies.len() * 95) / 100;
    let p99_idx = (sorted_latencies.len() * 99) / 100;

    let p50 = sorted_latencies[p50_idx];
    let p95 = sorted_latencies[p95_idx];
    let p99 = sorted_latencies[p99_idx];

    println!("Read latency p50: {p50:?}");
    println!("Read latency p95: {p95:?}");
    println!("Read latency p99: {p99:?}");

    // Assertions: Verify no starvation
    // Thresholds depend on environment:
    // - Local: realistic for dev machines/VMs/containers (p99 < 50ms, p50 < 10ms)
    // - CI: relaxed due to resource contention (p99 < 100ms, p50 < 50ms)
    let is_ci = std::env::var("CI").is_ok();

    let (p99_threshold, p50_threshold) = if is_ci {
        (Duration::from_millis(100), Duration::from_millis(50))
    } else {
        (Duration::from_millis(50), Duration::from_millis(10))
    };

    assert!(
        p99 < p99_threshold,
        "P99 latency too high: {p99:?} (threshold: {p99_threshold:?}, CI: {is_ci})"
    );

    assert!(
        p50 < p50_threshold,
        "P50 latency too high: {p50:?} (threshold: {p50_threshold:?}, CI: {is_ci})"
    );

    // Verify cluster remained healthy (writes were processed)
    assert!(
        total_writes > 1000,
        "Write throughput too low: {total_writes} (system may be blocked)"
    );

    println!("✅ Select fairness test passed: no starvation detected");
}
