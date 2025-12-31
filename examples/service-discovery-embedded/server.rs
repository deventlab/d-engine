//! Embedded Service Discovery Example
//!
//! Demonstrates using d-engine in embedded mode for service discovery.
//! Features:
//! - In-process Watch API (zero network overhead)
//! - LocalKvClient (zero serialization overhead)
//! - Automatic lifecycle management

use d_engine::EmbeddedEngine;
use d_engine_core::watch::WatchEventType;
use std::error::Error;
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    println!("Starting embedded d-engine for service discovery...\n");

    // Start embedded engine with explicit config file
    // This automatically handles node startup, storage creation, and background tasks
    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

    // Wait for leader election (single-node cluster elects itself immediately)
    let leader = engine.wait_ready(Duration::from_secs(5)).await?;
    println!(
        "✓ Cluster is ready: leader {} (term {})",
        leader.leader_id, leader.term
    );

    // Get local client (zero-overhead)
    let client = engine.client();

    // --- DEMO: In-process Watch ---

    let service_key = "services/payment-service/node1";
    println!("\n=== Starting In-process Watcher ===");
    println!("Watching key: {service_key}");

    // Register watcher directly on the engine
    let watcher = engine.watch(service_key)?;

    // Spawn a background task to process watch events
    // This simulates the "Watcher" component running inside the same process
    tokio::spawn(async move {
        // Get the event receiver from the handle
        let (_, _, mut receiver) = watcher.into_receiver();

        while let Some(event) = receiver.recv().await {
            let value = String::from_utf8_lossy(&event.value);
            match event.event_type {
                e if e == WatchEventType::Put as i32 => {
                    println!(
                        "\n[WATCHER] Service Updated: {} -> {}",
                        String::from_utf8_lossy(&event.key),
                        value
                    );
                }
                e if e == WatchEventType::Delete as i32 => {
                    println!(
                        "\n[WATCHER] Service Removed: {}",
                        String::from_utf8_lossy(&event.key)
                    );
                }
                _ => {
                    eprintln!("[WATCHER] Unknown event type: {}", event.event_type);
                }
            }
        }
    });

    // --- DEMO: Service Registration (Write) ---

    println!("\n=== Registering Service (Write) ===");
    let endpoint = "10.0.0.5:8080";
    println!("Registering: {service_key} -> {endpoint}");

    // Perform local write (direct to Raft core)
    client.put(service_key, endpoint).await?;

    // Give time for watcher to print
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- DEMO: Service Update ---

    println!("\n=== Updating Service ===");
    let new_endpoint = "10.0.0.5:9090";
    println!("Updating: {service_key} -> {new_endpoint}");
    client.put(service_key, new_endpoint).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- DEMO: Service Unregistration ---

    println!("\n=== Unregistering Service ===");
    client.delete(service_key).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Demo Complete ===");
    println!("Press Ctrl+C to exit...");

    // Wait for shutdown signal
    signal::ctrl_c().await?;

    // Graceful shutdown
    println!("\nShutting down...");
    engine.stop().await?;
    println!("Done");

    Ok(())
}
