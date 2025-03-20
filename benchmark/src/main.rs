use benchmark::Args;
use clap::Parser;
use dengine::{
    client_config::ClientConfig,
    grpc::rpc_service::{
        rpc_service_client::RpcServiceClient, ClientCommand, ClientProposeRequest,
    },
    utils::util::kv,
    ChannelPool, ClientPool, Error, NewClient,
};
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::{fs::OpenOptions, time::Instant};
use tokio::task;
use tonic::transport::Channel;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Initialize logger
    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("./logs/b.log")
        .expect("Should succeed to open log file");

    let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse command-line arguments
    let cli = Args::parse();
    let main_start = Instant::now();

    // Extract key variables
    let max_clients = cli.clients;
    let max_conns = cli.conns;
    let total = cli.total;
    let key_size = cli.key_size as u64;
    let value_size = cli.value_size as u64;
    let in_batch = cli.batch as bool;

    let cfg = ClientConfig {
        bootstrap_urls: cli.endpoints.clone(),
        connect_timeout_in_ms: 100,
        request_timeout_in_ms: 100,
        concurrency_limit_per_connection: 8192,
        tcp_keepalive_in_secs: 3600,
        http2_keep_alive_interval_in_secs: 300,
        http2_keep_alive_timeout_in_secs: 20,
        max_frame_size: 12582912,
        initial_connection_window_size: 12582912,
        initial_stream_window_size: 12582912,
        buffer_size: 65536,
    };

    let leader_addr = match get_leader_address(
        cli.endpoints[0].clone(),
        cfg.clone(),
        max_conns,
        max_clients,
    )
    .await
    {
        Some(addr) => {
            info!("Leader address: {}", addr);
            addr
        }
        None => {
            error!("Failed to retrieve leader address.");
            return;
        }
    };

    let channel_pool = ChannelPool::new(leader_addr, max_conns, cfg.clone());
    let client_pool = ClientPool::new(channel_pool, max_clients);
    client_pool.init_pool();

    let max_per_client = calculate_max_per_client(total, max_clients);

    let command = create_command(key_size, value_size);
    // Spawn tasks to send requests
    let mut tasks = FuturesUnordered::new();
    for i in 0..max_clients {
        if let Ok(cli) = client_pool.take(i) {
            let r = command.clone();
            let cli_clone = cli.clone();
            tasks.push(if in_batch {
                task::spawn(send_requests_in_batch(
                    cli_clone,
                    max_per_client,
                    i,
                    key_size,
                    value_size,
                ))
                .boxed()
            } else {
                task::spawn(send_requests(cli_clone, r, max_per_client, i)).boxed()
            });
        }
    }

    // Wait for all tasks to complete
    let mut success_count = 0;
    while let Some(result) = tasks.next().await {
        match result {
            Ok(successes) => success_count += successes,
            Err(e) => error!("Task failed with error: {:?}", e),
        }
    }

    // Display summary
    println!(
        "Summary:\n  Total: {}, Success: {}, Failed: {},\n  Total Time: {:?} sec",
        total,
        success_count,
        total - success_count,
        Instant::now().duration_since(main_start).as_secs()
    );
}

/// Retrieve the leader address from the cluster.
async fn get_leader_address(
    endpoint: String,
    cfg: ClientConfig,
    max_conns: usize,
    max_clients: usize,
) -> Option<String> {
    let channel_pool = ChannelPool::new(endpoint, max_conns, cfg.clone());
    let client_pool = ClientPool::new(channel_pool, max_clients);
    client_pool.init_pool();

    if let Ok(cli) = client_pool.take(0) {
        NewClient::get_leader_address(cli).await
    } else {
        None
    }
}

/// Create a command with specified key and value sizes.
fn create_command(key_size: u64, value_size: u64) -> ClientProposeRequest {
    let command = ClientCommand::insert(kv(key_size), kv(value_size));

    ClientProposeRequest {
        client_id: 1,
        commands: vec![command],
    }
}

/// Create a command with specified key and value sizes.
fn create_batch_commands(key_size: u64, value_size: u64, n: usize) -> ClientProposeRequest {
    let mut commands = Vec::new();
    for _ in 1..=n {
        let command = ClientCommand::insert(kv(key_size), kv(value_size));
        commands.push(command);
    }

    ClientProposeRequest {
        client_id: 1,
        commands,
    }
}

/// Calculate the maximum number of requests per client.
fn calculate_max_per_client(total: usize, client_count: usize) -> usize {
    if client_count > total {
        1
    } else {
        total / client_count
    }
}

/// Send requests for a specific client.
async fn send_requests(
    client: RpcServiceClient<Channel>,
    request: ClientProposeRequest,
    max_per_client: usize,
    client_index: usize,
) -> usize {
    let mut successes = 0;

    for j in 0..max_per_client {
        let n = client_index * max_per_client + j;
        let r = request.clone();
        let start = Instant::now();

        match NewClient::send_write_request(client.clone(), r).await {
            Ok(_) => {
                debug!(
                    "[S] {} - takes: {:?}",
                    n,
                    Instant::now().duration_since(start).as_millis()
                );
                successes += 1;
            }
            Err(Error::NodeIsNotLeaderError) => {
                error!("[F] {} - NodeIsNotLeaderError", n);
            }
            Err(Error::GeneralServerError(e)) => {
                error!("[F] {} - GeneralServerError: {:?}", n, e);
            }
            Err(Error::RpcTimeout) => {
                error!(
                    "[F] {} - timeout after: {:?}",
                    n,
                    Instant::now().duration_since(start).as_millis()
                );
            }
            Err(e) => {
                error!("[F] {} - error: {:?}", n, e);
            }
        }
    }

    successes
}

/// Send requests for a specific client.
async fn send_requests_in_batch(
    client: RpcServiceClient<Channel>,
    max_per_client: usize,
    client_index: usize,
    key_size: u64,
    value_size: u64,
) -> usize {
    let commands = create_batch_commands(key_size, value_size, max_per_client);
    let mut successes = 0;

    let n = client_index * max_per_client + 0;
    let start = Instant::now();

    match NewClient::send_write_request(client, commands).await {
        Ok(_) => {
            debug!(
                "[S] {} - takes: {:?}",
                n,
                Instant::now().duration_since(start).as_millis()
            );
            successes += max_per_client;
        }
        Err(Error::NodeIsNotLeaderError) => {
            error!("[F] {} - NodeIsNotLeaderError", n);
        }
        Err(Error::GeneralServerError(e)) => {
            error!("[F] {} - GeneralServerError{:?}", n, e);
        }
        Err(Error::RpcTimeout) => {
            error!(
                "[F] {} - timeout after: {:?}",
                n,
                Instant::now().duration_since(start).as_millis()
            );
        }
        Err(e) => {
            error!("[F] {} - error: {:?}", n, e);
        }
    }

    successes
}
#[cfg(test)]
mod tests {
    use crate::calculate_max_per_client;

    #[test]
    fn test_calculate_max_per_client() {
        assert_eq!(2, calculate_max_per_client(10, 5));
        assert_eq!(3, calculate_max_per_client(10, 3));
        assert_eq!(1, calculate_max_per_client(10, 11));
    }
}
