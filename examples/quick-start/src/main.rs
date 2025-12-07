//! d-engine embedded mode: single-node quick-start example
//!
//! This demonstrates the minimal setup to embed d-engine in a Rust application.
//! All KV operations run locally with <0.1ms latency.

use d_engine::prelude::*;
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting d-engine in embedded mode...\n");

    // Start embedded engine with RocksDB (auto-creates directories)
    let engine = EmbeddedEngine::with_rocksdb("./data/single-node", None).await?;

    // Wait for node initialization
    engine.ready().await;
    println!("✓ Node initialized");

    // Wait for leader election (single-node: instant)
    let leader = engine.wait_leader(Duration::from_secs(5)).await?;
    println!(
        "✓ Leader elected: node {} (term {})\n",
        leader.leader_id, leader.term
    );

    // Get the embedded KV client (in-process, zero-copy)
    let client = engine.client();

    // Run application logic
    run_demo(client).await?;

    // Graceful shutdown
    println!("\nShutting down...");
    engine.stop().await?;
    println!("Done");

    Ok(())
}

async fn run_demo(client: &LocalKvClient) -> Result<(), Box<dyn Error>> {
    println!("=== d-engine Embedded Mode Demo ===");
    println!("All operations: local-first, <0.1ms latency\n");

    // Store workflow state
    println!("1. Store workflow state");
    client.put("workflow:status".as_bytes().to_vec(), b"running".to_vec()).await?;
    println!("   ✓ workflow:status = running");

    // Read it back
    println!("2. Read workflow state");
    let value = client.get_eventual("workflow:status".as_bytes().to_vec()).await?;
    if let Some(v) = value {
        println!("   ✓ workflow:status = {}", String::from_utf8_lossy(&v));
    }

    // Store multiple tasks
    println!("3. Store task results");
    for i in 1..=3 {
        let key = format!("task:{i}");
        client.put(key.as_bytes().to_vec(), b"completed".to_vec()).await?;
        println!("   ✓ {key} stored");
    }

    // Retrieve all tasks
    println!("4. Retrieve task results");
    for i in 1..=3 {
        let key = format!("task:{i}");
        if let Some(v) = client.get_eventual(key.as_bytes().to_vec()).await? {
            println!("   ✓ {key} = {}", String::from_utf8_lossy(&v));
        }
    }

    println!("\n=== Demo Complete ===");
    println!("All data persisted locally and durable");
    println!("To scale to cluster: see config/cluster-node*.toml\n");

    Ok(())
}
