//! Watch Performance Gate Test
//!
//! This test enforces performance thresholds for the watch mechanism.
//! It will FAIL if watch overhead exceeds acceptable limits.
//!
//! Run with: cargo test --release --test watch_performance_gate --features rocksdb,watch --
//! --ignored
//!
//! Performance Thresholds:
//! - 100 watchers: < 10% overhead on PUT operations
//! - 1000 watchers: < 15% overhead on PUT operations

#![cfg(all(feature = "watch", feature = "rocksdb"))]
use d_engine_server::{RocksDBStateMachine, RocksDBStorageEngine};

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use d_engine_server::api::EmbeddedEngine;
use tempfile::TempDir;

/// Helper: Create a test engine
async fn create_test_engine() -> (EmbeddedEngine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("db");

    // Create config
    let config_path = temp_dir.path().join("d-engine.toml");
    let port = 50000 + (std::process::id() % 10000);
    let config_content = format!(
        r#"
[cluster]
listen_address = "127.0.0.1:{port}"

[raft.watch]
event_queue_size = 10000
watcher_buffer_size = 100
"#
    );
    std::fs::write(&config_path, config_content).expect("Failed to write config");

    // Create storage and state machine
    let storage_path = db_path.join("storage");
    let sm_path = db_path.join("state_machine");
    tokio::fs::create_dir_all(&storage_path)
        .await
        .expect("Failed to create storage dir");
    tokio::fs::create_dir_all(&sm_path).await.expect("Failed to create sm dir");

    let storage =
        Arc::new(RocksDBStorageEngine::new(storage_path).expect("Failed to create storage"));
    let state_machine =
        Arc::new(RocksDBStateMachine::new(sm_path).expect("Failed to create state machine"));

    let engine =
        EmbeddedEngine::start_custom(storage, state_machine, Some(config_path.to_str().unwrap()))
            .await
            .expect("Failed to start engine");

    // Wait for ready
    engine.wait_ready(Duration::from_secs(5)).await.expect("Engine not ready");

    (engine, temp_dir)
}

/// Measure average latency of 100 PUT operations
async fn measure_put_latency(watcher_count: usize) -> f64 {
    let (engine, _temp_dir) = create_test_engine().await;

    // Register watchers
    let mut _watchers = Vec::new();
    for i in 0..watcher_count {
        let key = format!("watch_key_{i}").into_bytes();
        let watcher = engine.client().watch(&key).expect("Failed to register watcher");
        _watchers.push(watcher);
    }

    // Give watchers time to register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Measure 100 PUT operations
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("key_{i}").into_bytes();
        let value = format!("value_{i}").into_bytes();
        engine.client().put(&key, &value).await.expect("PUT failed");
    }
    let elapsed = start.elapsed();

    // Cleanup
    engine.stop().await.expect("Failed to stop engine");

    elapsed.as_secs_f64()
}

#[tokio::test]
#[ignore] // Run explicitly with: cargo test --release --ignored
async fn test_watch_overhead_100_watchers_gate() {
    println!("\nPerformance Gate: Testing 100 watchers overhead...\n");

    // Measure baseline (no watchers)
    println!("Measuring baseline (0 watchers)...");
    let baseline = measure_put_latency(0).await;
    println!("   Baseline: {baseline:.3}s for 100 PUTs");

    // Measure with 100 watchers
    println!("Measuring with 100 watchers...");
    let with_100 = measure_put_latency(100).await;
    println!("   With 100 watchers: {with_100:.3}s for 100 PUTs");

    // Calculate overhead
    let overhead_percent = (with_100 - baseline) / baseline * 100.0;
    println!("\nOverhead: {overhead_percent:.2}%");

    // Gate check
    const THRESHOLD: f64 = 10.0;
    println!("Threshold: < {THRESHOLD:.1}%");

    if overhead_percent < THRESHOLD {
        println!("PASS: Performance gate passed!");
    } else {
        println!("FAIL: Performance gate failed!");
    }

    assert!(
        overhead_percent < THRESHOLD,
        "\n\nPerformance Gate FAILED\n\
         \n\
         Watch overhead with 100 watchers is {overhead_percent:.2}%, exceeding {THRESHOLD:.1}% threshold.\n\
         \n\
         This indicates a performance regression. Possible causes:\n\
         - Lock contention in watch system\n\
         - Inefficient event dispatching\n\
         - Broadcast channel bottleneck\n\
         \n\
         Run detailed benchmark for analysis:\n\
         cargo bench --bench watch_overhead --features rocksdb,watch\n"
    );
}

#[tokio::test]
#[ignore] // Run explicitly with: cargo test --release --ignored
async fn test_watch_overhead_1000_watchers_gate() {
    println!("\nPerformance Gate: Testing 1000 watchers overhead...\n");

    // Measure baseline (no watchers)
    println!("Measuring baseline (0 watchers)...");
    let baseline = measure_put_latency(0).await;
    println!("   Baseline: {baseline:.3}s for 100 PUTs");

    // Measure with 1000 watchers
    println!("Measuring with 1000 watchers...");
    let with_1000 = measure_put_latency(1000).await;
    println!("   With 1000 watchers: {with_1000:.3}s for 100 PUTs");

    // Calculate overhead
    let overhead_percent = (with_1000 - baseline) / baseline * 100.0;
    println!("\nOverhead: {overhead_percent:.2}%");

    // Gate check (more lenient threshold for 1000 watchers)
    const THRESHOLD: f64 = 15.0;
    println!("Threshold: < {THRESHOLD:.1}%");

    if overhead_percent < THRESHOLD {
        println!("PASS: Performance gate passed!");
    } else {
        println!("FAIL: Performance gate failed!");
    }

    assert!(
        overhead_percent < THRESHOLD,
        "\n\nPerformance Gate FAILED\n\
         \n\
         Watch overhead with 1000 watchers is {overhead_percent:.2}%, exceeding {THRESHOLD:.1}% threshold.\n\
         \n\
         This indicates a performance regression. Possible causes:\n\
         - Lock contention in watch system\n\
         - Inefficient event dispatching\n\
         - Broadcast channel bottleneck\n\
         \n\
         Run detailed benchmark for analysis:\n\
         cargo bench --bench watch_overhead --features rocksdb,watch\n"
    );
}
