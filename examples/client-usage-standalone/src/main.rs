use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use d_engine::{
    Client, ClientApi, ClientApiError, ClientBuilder,
    protocol::{ClientResult, ReadConsistencyPolicy},
};

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
    Put { key: u64, value: u64 },
    Delete { key: u64 },
    Get { key: u64 },
    Sget { key: u64 },
    Lget { key: u64 },
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
        .map_err(|e| anyhow::anyhow!("Failed to create client: {e:?}"))?;

    match cli.command {
        Commands::Put { key, value } => handle_write(&client, key, value).await,
        Commands::Delete { key } => handle_delete(&client, key).await,
        Commands::Get { key } => handle_read(&client, key, "e").await,
        Commands::Sget { key } => handle_read(&client, key, "s").await,
        Commands::Lget { key } => handle_read(&client, key, "l").await,
    }
}

async fn handle_write(
    client: &Client,
    key: u64,
    value: u64,
) -> Result<()> {
    println!("Writing key-value({key}-{value}) pair");
    client
        .kv()
        .put(safe_kv(key), safe_kv(value))
        .await
        .map(|_| println!("Success"))
        .map_err(|e: ClientApiError| anyhow::anyhow!("Write error: {e:?}"))
}

async fn handle_delete(
    client: &Client,
    key: u64,
) -> Result<()> {
    println!("Deleting key({key})");
    client
        .kv()
        .delete(safe_kv(key))
        .await
        .map(|_| println!("Success"))
        .map_err(|e: ClientApiError| anyhow::anyhow!("Delete error: {e:?}"))
}

async fn handle_read(
    client: &Client,
    key: u64,
    consistency: &str,
) -> Result<()> {
    let policy = match consistency {
        "l" | "linearizable" => ReadConsistencyPolicy::LinearizableRead,
        "s" | "sequential" | "lease" => ReadConsistencyPolicy::LeaseRead,
        "e" | "eventual" => ReadConsistencyPolicy::EventualConsistency,
        _ => ReadConsistencyPolicy::LinearizableRead, // Default to linearizable
    };

    let result = client
        .kv()
        .get_with_policy(safe_kv(key), Some(policy))
        .await
        .map_err(|e: ClientApiError| anyhow::anyhow!("Read error: {e:?}"))?;

    match result {
        Some(ClientResult { key: _, value }) => {
            let value = safe_vk(&value).unwrap();
            println!("{value:?}");
        }
        None => println!("Key not found"),
    }

    Ok(())
}

pub fn safe_vk<K: AsRef<[u8]>>(bytes: K) -> crate::Result<u64> {
    let bytes = bytes.as_ref();
    let expected_len = 8;

    if bytes.len() != expected_len {
        return Err(anyhow::anyhow!(
            "Invalid length for value key conversion: expected {}, got {}",
            expected_len,
            bytes.len()
        ));
    }
    let array: [u8; 8] = bytes.try_into().expect("Guaranteed safe after length check");
    Ok(u64::from_be_bytes(array))
}

/// Converts a `u64` to an 8-byte array in big-endian byte order.
///
/// # Examples
/// ```
/// use convert::safe_kv;
///
/// let bytes = safe_kv(0x1234_5678_9ABC_DEF0);
/// assert_eq!(bytes, [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]);
/// ```
pub const fn safe_kv(num: u64) -> [u8; 8] {
    num.to_be_bytes()
}
