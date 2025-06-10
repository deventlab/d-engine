use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use d_engine::client::ClientApiError;
use d_engine::ClientBuilder;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:9083")]
    endpoints: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Put {
        key: String,
        value: String,
    },
    Update {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Get {
        key: String,
    },
    Lget {
        key: String,
    },
    /// Join a new node to the cluster
    Join {
        /// Node ID for the new member
        #[clap(long)]
        node_id: u32,

        /// Network address of the new node
        #[clap(long)]
        address: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Create client connection
    let client = ClientBuilder::new(cli.endpoints.split(',').map(String::from).collect())
        .connect_timeout(std::time::Duration::from_secs(10))
        .request_timeout(std::time::Duration::from_secs(10))
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create client: {:?}", e))?;

    match cli.command {
        Commands::Put { key, value } | Commands::Update { key, value } => handle_write(&client, key, value).await,
        Commands::Delete { key } => handle_delete(&client, key).await,
        Commands::Get { key } => handle_read(&client, key, false).await,
        Commands::Lget { key } => handle_read(&client, key, true).await,
        Commands::Join { node_id, address } => handle_cluster_command(&client, node_id, address).await,
    }
}

async fn handle_write(
    client: &d_engine::Client,
    key: String,
    value: String,
) -> Result<()> {
    println!("Writing key-value({}-{}) pair", key, value);
    client
        .kv()
        .put(&key, value.as_bytes())
        .await
        .map(|_| println!("Success"))
        .map_err(|e: ClientApiError| anyhow::anyhow!("Write error: {:?}", e))
}

async fn handle_delete(
    client: &d_engine::Client,
    key: String,
) -> Result<()> {
    println!("Deleting key({})", key);
    client
        .kv()
        .delete(&key)
        .await
        .map(|_| println!("Success"))
        .map_err(|e: ClientApiError| anyhow::anyhow!("Delete error: {:?}", e))
}

async fn handle_read(
    client: &d_engine::Client,
    key: String,
    linearizable: bool,
) -> Result<()> {
    println!(
        "Performing {} read, key: {}",
        if linearizable { "linearizable" } else { "regular" },
        key
    );

    let result = client
        .kv()
        .get(&key, linearizable)
        .await
        .map_err(|e: ClientApiError| anyhow::anyhow!("Read error: {:?}", e))?;

    match result {
        Some(value) => {
            println!("Value: {:?}", value);
        }
        None => println!("Key not found"),
    }

    Ok(())
}

async fn handle_cluster_command(
    client: &d_engine::Client,
    node_id: u32,
    address: String,
) -> crate::Result<()> {
    let response = client
        .cluster()
        .join_cluster(node_id, address)
        .await
        .map_err(|e: ClientApiError| anyhow::anyhow!("Join cluster error: {:?}", e))?;

    if response.success {
        println!("Node joined successfully!");
        println!("Cluster configuration version: {}", response.config_version);
        println!("Leader ID: {}", response.leader_id);
    } else {
        eprintln!("Join failed: {}", response.error);
    }

    Ok(())
}
