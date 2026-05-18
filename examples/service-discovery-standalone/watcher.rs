//! Watcher client for service discovery example
//!
//! Demonstrates both the exact-key Watch API and the prefix Watch API.
//!
//! # Exact-key watch (default)
//!
//! ```bash
//! cargo run --bin watcher -- --key /services/payment/node1
//! ```
//!
//! Watches a single specific key. Use this when you own exactly one resource
//! and need to react to its changes (e.g. your own config entry).
//!
//! # Prefix watch (--prefix)
//!
//! ```bash
//! cargo run --bin watcher -- --key /services/payment/ --prefix
//! ```
//!
//! Watches an entire namespace. Every key that starts with the prefix fires an
//! event — new nodes joining, existing nodes updating their endpoint, nodes
//! deregistering. Use this to maintain a live service registry without
//! registering per-node watchers.
//!
//! # Reconnection pattern
//!
//! When the stream ends (server restart, buffer overflow CANCELED event), this
//! example demonstrates the zero-race-window reconnect pattern:
//! 1. Register watch FIRST (server buffers events immediately)
//! 2. Scan current state
//! 3. Drain watch buffer, skipping events at revision ≤ scan revision

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use d_engine::protocol::{WatchEventType, WatchResponse};
use d_engine::{Client, ClientApi, ScanResult};
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "watcher")]
#[command(about = "Watch for service changes in d-engine (exact key or prefix namespace)")]
struct Cli {
    /// d-engine server endpoint
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    endpoint: String,

    /// Key to watch (exact) or prefix to watch (with --prefix).
    /// Prefix must start and end with '/', e.g. /services/payment/
    #[arg(short, long)]
    key: String,

    /// Watch all keys under this prefix instead of an exact key.
    /// When set, --key is treated as a path prefix.
    #[arg(short, long, default_value_t = false)]
    prefix: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    println!("Connecting to: {}", cli.endpoint);
    if cli.prefix {
        println!("Mode: prefix watch");
        println!("Namespace: {}\n", cli.key);
    } else {
        println!("Mode: exact key watch");
        println!("Key: {}\n", cli.key);
    }

    let client = Client::builder(vec![cli.endpoint.clone()])
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(30))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect: {e:?}"))?;

    if cli.prefix {
        run_prefix_watch(&client, &cli.key).await
    } else {
        run_exact_watch(&client, &cli.key).await
    }
}

// ---------------------------------------------------------------------------
// Exact-key watch
// ---------------------------------------------------------------------------

async fn run_exact_watch(
    client: &Client,
    key: &str,
) -> Result<()> {
    // Pattern: Read-then-Watch
    // Read current value first so the caller sees the starting state before
    // any events arrive. This avoids the race between "what is it now" and
    // "what did it just change to".
    println!("=== Current State ===");
    let current = client
        .get_eventual(key)
        .await
        .map_err(|e| anyhow::anyhow!("Read failed: {e:?}"))?;
    match current {
        Some(v) => println!("  {key} = {}", String::from_utf8_lossy(&v)),
        None => println!("  {key} = (not found)"),
    }

    println!("\n=== Watching for Changes (Ctrl+C to exit) ===\n");

    let mut stream = client
        .watch(key)
        .await
        .map_err(|e| anyhow::anyhow!("Watch failed: {e:?}"))?;

    while let Some(event_result) = stream.next().await {
        match event_result {
            Ok(response) => print_event(&response),
            Err(e) => {
                eprintln!("Watch error: {e:?}");
                break;
            }
        }
    }

    println!("\nWatch stream ended");
    Ok(())
}

// ---------------------------------------------------------------------------
// Prefix watch — live service registry (zero race-window reconnect pattern)
// ---------------------------------------------------------------------------
//
// Business scenario: an API Gateway needs a live view of all nodes belonging
// to /services/payment/. When node1 registers, updates its endpoint, or
// crashes (delete), the gateway reacts immediately — no polling required.
//
// One prefix watcher replaces N per-node exact-key watchers.
//
// Reconnection strategy (zero race window):
//   1. Register watch FIRST — server starts buffering events immediately
//   2. scan_prefix() — linearizable snapshot at revision R
//   3. Drain watch buffer, skip events where event.revision <= R
//   On CANCELED (buffer overflow) or stream error → restart from step 1

async fn run_prefix_watch(
    client: &Client,
    prefix: &str,
) -> Result<()> {
    println!("=== Prefix Watch: Live Service Registry ===");
    println!("Namespace: {prefix}");
    println!("Events update the in-memory registry in real time.\n");

    loop {
        // Step 1: register watch FIRST — server buffers events from this moment.
        // On transient errors (leader change, startup race) continue the loop rather
        // than propagating — the reconnect pattern must be resilient to watch setup failure.
        let mut stream = match client.watch_prefix(prefix).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[watch error] {e:?} — reconnecting in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // Step 2: linearizable scan — any write before/during scan is in the snapshot
        let snapshot: ScanResult = match client.scan_prefix(prefix).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[scan error] {e:?} — reconnecting in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let scan_revision = snapshot.revision;
        let mut registry: HashMap<Vec<u8>, String> = snapshot
            .entries
            .into_iter()
            .map(|entry| (entry.0.to_vec(), String::from_utf8_lossy(&entry.1).to_string()))
            .collect();

        println!("[connected] watching {prefix} (scan_revision={scan_revision})");
        print_registry(&registry);

        // Step 3: drain watch buffer, skip events already captured by the scan
        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(response) => {
                    if response.revision <= scan_revision {
                        continue; // already in scan snapshot
                    }
                    let canceled = apply_event_to_registry(&response, &mut registry);
                    print_registry(&registry);
                    if canceled {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[stream error] {e:?} — reconnecting in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }

        println!("[reconnecting] re-establishing prefix watch for {prefix}\n");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Returns `true` if the event was `CANCELED` (caller should break and reconnect).
fn apply_event_to_registry(
    response: &WatchResponse,
    registry: &mut HashMap<Vec<u8>, String>,
) -> bool {
    let key = response.key.to_vec();
    let key_str = String::from_utf8_lossy(&response.key);
    let event_type = WatchEventType::try_from(response.event_type).ok();

    match event_type {
        Some(WatchEventType::Put) => {
            let value = String::from_utf8_lossy(&response.value).to_string();
            println!(
                "[PUT   ] {key_str} = {value}  (revision={})",
                response.revision
            );
            registry.insert(key, value);
            false
        }
        Some(WatchEventType::Delete) => {
            println!("[DELETE] {key_str}  (revision={})", response.revision);
            registry.remove(&key);
            false
        }
        Some(WatchEventType::Canceled) => {
            // Buffer overflow: missed events after this revision.
            // Return true → caller breaks inner loop → reconnects.
            println!(
                "[CANCELED] buffer overflow — missed events after revision {}; reconnecting",
                response.revision
            );
            registry.clear();
            true
        }
        None => {
            println!("[UNKNOWN] {key_str} (event_type={})", response.event_type);
            false
        }
    }
}

fn print_registry(registry: &HashMap<Vec<u8>, String>) {
    if registry.is_empty() {
        println!("  registry: (empty — waiting for registrations or reconnecting)");
    } else {
        println!("  registry ({} nodes):", registry.len());
        let mut entries: Vec<_> = registry.iter().collect();
        entries.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in entries {
            println!("    {} → {}", String::from_utf8_lossy(k), v);
        }
    }
}

fn print_event(response: &WatchResponse) {
    let event_type = WatchEventType::try_from(response.event_type).ok();
    let key = String::from_utf8_lossy(&response.key);

    match event_type {
        Some(WatchEventType::Put) => {
            let value = String::from_utf8_lossy(&response.value);
            println!("[PUT   ] {key} = {value}  (revision={})", response.revision);
        }
        Some(WatchEventType::Delete) => {
            println!("[DELETE] {key}  (revision={})", response.revision);
        }
        Some(WatchEventType::Canceled) => {
            println!(
                "[CANCELED] {key} — buffer overflow; re-sync and re-register"
            );
        }
        None => {
            println!("[UNKNOWN] {key} (event_type={})", response.event_type);
        }
    }
}
