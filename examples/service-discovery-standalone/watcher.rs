//! Watcher client for service discovery example
//!
//! Demonstrates the Watch API for receiving real-time service updates.
//! This is the "read path" in service discovery pattern.

use anyhow::Result;
use clap::Parser;
use d_engine_client::Client;
use d_engine_client::protocol::{WatchEventType, WatchResponse};
use futures::StreamExt;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "watcher")]
#[command(about = "Watch for service changes in d-engine")]
struct Cli {
    /// d-engine server endpoint
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    endpoint: String,

    /// Key to watch (e.g., "services/api-gateway/node1")
    #[arg(short, long)]
    key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    println!("Connecting to: {}", cli.endpoint);
    println!("Watching key: {}\n", cli.key);

    // Connect to d-engine cluster
    let client = Client::builder(vec![cli.endpoint.clone()])
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(30))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect: {e:?}"))?;

    // Pattern: Read-then-Watch
    // 1. First, read current value (if any)
    println!("=== Current State ===");
    let current = client
        .get_eventual(&cli.key)
        .await
        .map_err(|e| anyhow::anyhow!("Read failed: {e:?}"))?;
    if let Some(result) = current {
        println!("  {} = {}", cli.key, String::from_utf8_lossy(&result.value));
    } else {
        println!("  {} = (not found)", cli.key);
    }

    // 2. Start watching for future changes
    println!("\n=== Watching for Changes (Ctrl+C to exit) ===\n");

    let mut stream = client
        .watch(&cli.key)
        .await
        .map_err(|e| anyhow::anyhow!("Watch failed: {e:?}"))?;

    while let Some(event_result) = stream.next().await {
        match event_result {
            Ok(response) => {
                print_watch_event(&cli.key, &response);
            }
            Err(e) => {
                eprintln!("Watch error: {e:?}");
                break;
            }
        }
    }

    println!("\nWatch stream ended");
    Ok(())
}

fn print_watch_event(
    key: &str,
    response: &WatchResponse,
) {
    let event_type = WatchEventType::try_from(response.event_type).ok();

    match event_type {
        Some(WatchEventType::Put) => {
            let value = String::from_utf8_lossy(&response.value);
            println!("[PUT] {key} = {value}");
        }
        Some(WatchEventType::Delete) => {
            println!("[DELETE] {key}");
        }
        None => {
            let event_type = response.event_type;
            println!("[UNKNOWN] {key} (event_type={event_type})");
        }
    }
}
