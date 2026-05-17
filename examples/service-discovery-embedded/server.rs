//! Embedded Service Discovery Example
//!
//! Demonstrates using d-engine in embedded mode for service discovery.
//! Features:
//! - Exact-key Watch: react to changes on a single specific key
//! - Prefix Watch: maintain a live registry of all nodes in a service namespace
//! - In-process Watch API (zero network overhead, zero serialization overhead)
//! - Automatic lifecycle management

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use d_engine::EmbeddedEngine;
use d_engine_core::watch::WatchEventType;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    println!("Starting embedded d-engine for service discovery...\n");

    let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

    let leader = engine.wait_ready(Duration::from_secs(5)).await?;
    println!(
        "✓ Cluster ready: leader {} (term {})\n",
        leader.leader_id, leader.term
    );

    let client = engine.client();

    // -----------------------------------------------------------------------
    // DEMO 1: Exact-key watch
    //
    // Use case: a sidecar process watching its own config key.
    // One watcher, one key, reacts to PUT and DELETE on that specific key.
    // -----------------------------------------------------------------------

    let config_key = b"/config/payment-service/timeout";
    println!("=== Demo 1: Exact-key Watch ===");
    println!("Key: {}", String::from_utf8_lossy(config_key));

    let exact_watcher = engine.client().watch(config_key)?;
    let (_, _, mut exact_rx) = exact_watcher.into_receiver();

    tokio::spawn(async move {
        while let Some(event) = exact_rx.recv().await {
            let key = String::from_utf8_lossy(&event.key);
            match event.event_type {
                e if e == WatchEventType::Put as i32 => {
                    let value = String::from_utf8_lossy(&event.value);
                    println!(
                        "[exact-watch] config changed: {key} = {value}  (revision={})",
                        event.revision
                    );
                }
                e if e == WatchEventType::Delete as i32 => {
                    println!(
                        "[exact-watch] config deleted: {key}  (revision={})",
                        event.revision
                    );
                }
                _ => {}
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    client.put(config_key, b"30s").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    client.put(config_key, b"60s").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // -----------------------------------------------------------------------
    // DEMO 2: Prefix watch — live load-balancer registry
    //
    // Use case: an API Gateway needs a live view of all payment-service nodes.
    // When any node registers, updates its endpoint, or deregisters, the
    // gateway's in-memory routing table updates instantly — no polling.
    //
    // One prefix watcher on "/services/payment/" replaces N per-node watchers.
    // The registry is a shared HashMap updated by the background task and read
    // by the main routing loop.
    // -----------------------------------------------------------------------

    let service_prefix = b"/services/payment/";
    println!("\n=== Demo 2: Prefix Watch — Live Load-Balancer Registry ===");
    println!("Namespace: {}", String::from_utf8_lossy(service_prefix));

    // Shared registry: key → endpoint, readable from any thread
    let registry: Arc<Mutex<HashMap<Vec<u8>, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let registry_bg = Arc::clone(&registry);

    let prefix_watcher = engine.client().watch_prefix(service_prefix)?;
    let (_, _, mut prefix_rx) = prefix_watcher.into_receiver();

    tokio::spawn(async move {
        while let Some(event) = prefix_rx.recv().await {
            let key_str = String::from_utf8_lossy(&event.key).to_string();
            let mut reg = registry_bg.lock().unwrap();

            match event.event_type {
                e if e == WatchEventType::Put as i32 => {
                    let value = String::from_utf8_lossy(&event.value).to_string();
                    println!(
                        "[prefix-watch] node up:   {key_str} → {value}  (revision={})",
                        event.revision
                    );
                    reg.insert(event.key.to_vec(), value);
                }
                e if e == WatchEventType::Delete as i32 => {
                    println!(
                        "[prefix-watch] node down: {key_str}  (revision={})",
                        event.revision
                    );
                    reg.remove(event.key.as_ref());
                }
                e if e == WatchEventType::Canceled as i32 => {
                    // Buffer overflow: registry may be stale.
                    // Production code: re-scan the prefix then re-watch (ticket #301).
                    println!(
                        "[prefix-watch] CANCELED — buffer overflow on {key_str}; registry may be stale"
                    );
                    reg.clear();
                }
                _ => {}
            }

            // Print current routing table after every change
            print_registry(&reg);
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Simulate three nodes coming online
    println!("\n--- Three nodes registering ---");
    client
        .put(b"/services/payment/node1", b"10.0.0.1:8080")
        .await?;
    client
        .put(b"/services/payment/node2", b"10.0.0.2:8080")
        .await?;
    client
        .put(b"/services/payment/node3", b"10.0.0.3:8080")
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Simulate node2 updating its endpoint (rolling restart)
    println!("\n--- node2 rolling restart: endpoint changes ---");
    client
        .put(b"/services/payment/node2", b"10.0.0.2:9090")
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Simulate node1 crashing (TTL expiry or explicit deregister)
    println!("\n--- node1 deregisters (crash / graceful shutdown) ---");
    client.delete(b"/services/payment/node1").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Show final registry state via a direct read (what routing would use)
    {
        let reg = registry.lock().unwrap();
        println!("\n✓ Final routing table ({} nodes):", reg.len());
        let mut entries: Vec<_> = reg.iter().collect();
        entries.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in entries {
            println!("    {} → {}", String::from_utf8_lossy(k), v);
        }
    }

    println!("\n=== Demo complete. Press Ctrl+C to exit. ===");
    signal::ctrl_c().await?;

    println!("\nShutting down...");
    engine.stop().await?;
    println!("Done");

    Ok(())
}

fn print_registry(reg: &HashMap<Vec<u8>, String>) {
    if reg.is_empty() {
        println!("  routing table: (empty)");
    } else {
        println!("  routing table ({} nodes):", reg.len());
        let mut entries: Vec<_> = reg.iter().collect();
        entries.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in entries {
            println!("    {} → {}", String::from_utf8_lossy(k), v);
        }
    }
}
