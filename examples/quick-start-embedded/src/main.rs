//! d-engine embedded mode: single-node quick-start example
//!
//! This demonstrates the minimal setup to embed d-engine in a Rust application.
//! All KV operations run locally with <0.1ms latency.

use d_engine::prelude::*;
use std::error::Error as StdError;
use std::time::Duration;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn StdError>> {
    // Optional: control d-engine logs via RUST_LOG env (default: warn)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    println!("🚀 Starting d-engine...");

    // Start embedded engine with explicit config
    // Config file specifies data directory and other settings
    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

    // Wait for leader election (single-node: instant)
    let leader = engine.wait_ready(Duration::from_secs(5)).await?;

    println!("✅ d-engine ready!");
    println!("   → Data directory: ./data/single-node");
    println!("   → Node ID: {}", leader.leader_id);
    println!("   → Listening on: 127.0.0.1:9081");
    println!("\n─────────────────────────────────────");

    // Get the embedded KV client (in-process, zero-copy)
    let client = engine.client();

    // Run application logic
    run_demo(client).await?;

    println!("─────────────────────────────────────");
    println!("🛑 Shutting down...");
    engine.stop().await?;
    println!("✅ Stopped cleanly");

    Ok(())
}

async fn run_demo(client: &EmbeddedClient) -> std::result::Result<(), Box<dyn StdError>> {
    println!("=== Quick Start Demo ===");

    // Store workflow state
    println!("1. Store workflow state");
    client
        .put("workflow:status".as_bytes().to_vec(), b"running".to_vec())
        .await
        .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
    println!("   ✓ workflow:status = running");

    // Read it back
    println!("2. Read workflow state");
    let value = client
        .get_linearizable("workflow:status".as_bytes().to_vec())
        .await
        .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
    if let Some(v) = value {
        println!("   ✓ workflow:status = {}", String::from_utf8_lossy(&v));
    }

    // Store multiple tasks
    println!("3. Store task results");
    for i in 1..=3 {
        let key = format!("task:{i}");
        client
            .put(key.as_bytes().to_vec(), b"completed".to_vec())
            .await
            .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
        println!("   ✓ {key} stored");
    }

    // Retrieve all tasks with linearizable reads
    println!("4. Retrieve task results (linearizable)");
    for i in 1..=3 {
        let key = format!("task:{i}");
        if let Some(v) = client
            .get_linearizable(key.as_bytes().to_vec())
            .await
            .map_err(|e| Box::new(e) as Box<dyn StdError>)?
        {
            println!("   ✓ {key} = {}", String::from_utf8_lossy(&v));
        } else {
            eprintln!("   ✗ Failed to read {key} (should never happen after successful PUT)");
        }
    }

    println!("\n=== Demo Complete ===");
    println!("All data persisted locally and durable");
    println!(
        "Scale to cluster: https://docs.rs/d-engine/latest/d_engine/docs/examples/single_node_expansion/index.html\n"
    );

    Ok(())
}
