//! Admin tool for service discovery example
//!
//! Registers and unregisters service endpoints in d-engine.
//! Demonstrates the "write path" in service discovery pattern.

use anyhow::Result;
use clap::{Parser, Subcommand};
use d_engine_client::Client;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "admin")]
#[command(about = "Manage service registrations in d-engine")]
struct Cli {
    /// d-engine server endpoint
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Register a service endpoint
    Register {
        /// Service name (e.g., "api-gateway")
        #[arg(short, long)]
        name: String,

        /// Instance ID (e.g., "node1")
        #[arg(short, long)]
        instance: String,

        /// Endpoint address (e.g., "192.168.1.10:8080")
        #[arg(short, long)]
        endpoint: String,
    },

    /// Unregister a service endpoint
    Unregister {
        /// Service name
        #[arg(short, long)]
        name: String,

        /// Instance ID
        #[arg(short, long)]
        instance: String,
    },

    /// List all registered services (for a given service name)
    List {
        /// Service name to list
        #[arg(short, long)]
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Connect to d-engine cluster
    let client = Client::builder(vec![cli.endpoint.clone()])
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(3))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect: {e:?}"))?;

    match cli.command {
        Commands::Register {
            name,
            instance,
            endpoint,
        } => {
            // Key format: services/{service_name}/{instance_id}
            let key = format!("services/{name}/{instance}");

            client
                .kv()
                .put(&key, &endpoint)
                .await
                .map_err(|e| anyhow::anyhow!("Put failed: {e:?}"))?;

            println!("✓ Registered: {key} -> {endpoint}");
        }

        Commands::Unregister { name, instance } => {
            let key = format!("services/{name}/{instance}");

            client
                .kv()
                .delete(&key)
                .await
                .map_err(|e| anyhow::anyhow!("Delete failed: {e:?}"))?;

            println!("✓ Unregistered: {key}");
        }

        Commands::List { name } => {
            // Note: d-engine v1 doesn't support prefix scan
            // In production, maintain an index key (see service-discovery-pattern.md)
            println!("Listing services for: {name}");
            println!("Note: Prefix scan not supported in v1.");
            println!("Workaround: Maintain an index key like 'services/{name}_index'");

            // Try to read index if exists
            let index_key = format!("services/{name}_index");
            let result = client
                .kv()
                .get(&index_key)
                .await
                .map_err(|e| anyhow::anyhow!("Get failed: {e:?}"))?;
            if let Some(result) = result {
                let instances = String::from_utf8_lossy(&result.value);
                println!("Registered instances: {instances}");
            } else {
                println!("No index found. Register services first.");
            }
        }
    }

    Ok(())
}
