use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use clap::Parser;
use clap::Subcommand;
use d_engine_client::Client;
use d_engine_client::ClientApiError;
use d_engine_client::ClientBuilder;
use d_engine_proto::client::ReadConsistencyPolicy;
use hdrhistogram::Histogram;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rand::SeedableRng;
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = "8")]
    key_size: usize,

    #[arg(long, default_value = "256")]
    value_size: usize,

    #[arg(long, default_value = "10000")]
    total: u64,

    #[arg(long, default_value_t = 1)]
    conns: usize,

    #[arg(long, default_value_t = 1)]
    clients: usize,

    #[arg(long, required = true)]
    endpoints: Vec<String>,

    #[arg(long, default_value = "false")]
    sequential_keys: bool,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Put,
    Range {
        #[arg(long, default_value = "l")]
        consistency: String,
    },
}

struct ClientPool {
    clients: Vec<Arc<Client>>,
    counter: AtomicU64,
}

impl Clone for ClientPool {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
            counter: AtomicU64::new(self.counter.load(Ordering::Relaxed)),
        }
    }
}

impl ClientPool {
    async fn new(
        endpoints: Vec<String>,
        pool_size: usize,
    ) -> Result<Self, ClientApiError> {
        let mut clients = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let client = ClientBuilder::new(endpoints.clone())
                .connect_timeout(Duration::from_secs(10))
                .request_timeout(Duration::from_secs(10))
                .enable_compression(false)
                .build()
                .await?;
            clients.push(Arc::new(client));
        }
        Ok(Self {
            clients,
            counter: AtomicU64::new(0),
        })
    }

    fn next(&self) -> Arc<Client> {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        let len = self.clients.len() as u64;
        self.clients[(idx % len) as usize].clone()
    }
}
#[allow(dead_code)]
fn generate_key(
    sequential: bool,
    size: usize,
    counter: u64,
) -> String {
    if sequential {
        format!("{:0width$}", counter, width = size)
    } else {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(size)
            .collect()
    }
}

fn generate_prefixed_key(
    sequential: bool,
    key_size: usize,
    index: u64,
) -> String {
    if sequential {
        let max_value = 10u64.pow(key_size as u32) - 1;
        let value = max_value.saturating_sub(index);
        format!("{:0width$}", value, width = key_size)
    } else {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(index);
        (0..key_size)
            .map(|_| rng.sample(Alphanumeric))
            .map(char::from)
            .collect()
    }
}

fn generate_value(size: usize) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    (0..size).map(|_| rng.r#gen()).collect()
}

struct BenchmarkStats {
    histogram: Mutex<Histogram<u64>>,
    total_ops: AtomicU64,
}
impl Default for BenchmarkStats {
    fn default() -> Self {
        let histogram = Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("Failed to create histogram");
        Self {
            histogram: Mutex::new(histogram),
            total_ops: AtomicU64::new(0),
        }
    }
}
impl BenchmarkStats {
    fn new() -> Self {
        let histogram =
            Mutex::new(Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("Failed to create histogram"));
        Self {
            histogram,
            total_ops: AtomicU64::new(0),
        }
    }

    fn record(
        &self,
        duration: Duration,
    ) {
        let micros = duration.as_micros() as u64;
        let mut hist = self.histogram.lock().unwrap();
        hist.record(micros).unwrap();
        self.total_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(
        &self,
        total_time: Duration,
    ) {
        println!("Summary:");
        println!("Total time:\t{:.2} s", total_time.as_secs_f64());
        println!(" Requests:\t{}", self.total_ops.load(Ordering::Relaxed));
        println!(
            "Throughput:\t{:.2} ops/sec",
            self.total_ops.load(Ordering::Relaxed) as f64 / total_time.as_secs_f64()
        );

        println!("\nLatency distribution (μs):");
        let hist = self.histogram.lock().unwrap();
        println!(" Avg\t{:.2}", hist.mean());
        println!(" Min\t{:.2}", hist.min());
        println!(" Max\t{:.2}", hist.max());
        println!(" p50\t{:.2}", hist.value_at_quantile(0.5));
        println!(" p90\t{:.2}", hist.value_at_quantile(0.9));
        println!(" p99\t{:.2}", hist.value_at_quantile(0.99));
        println!(" p99.9\t{:.2}", hist.value_at_quantile(0.999));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let stats = Arc::new(BenchmarkStats::new());
    let semaphore = Arc::new(Semaphore::new(cli.conns));

    // Initialize the connection pool
    let endpoints = cli.endpoints.clone();
    println!("Initializing client connection with: {:?}", &endpoints);
    let client_pool = ClientPool::new(endpoints, cli.conns)
        .await
        .expect("Failed to create client pool");

    // Generate initial counter
    let key_counter = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(cli.clients);

    for _ in 0..cli.clients {
        let stats = stats.clone();
        let client_pool = client_pool.clone();
        let semaphore = semaphore.clone();
        let key_counter = key_counter.clone();
        let cli = cli.clone();

        let handle = tokio::spawn(async move {
            while stats.total_ops.load(Ordering::Relaxed) < cli.total {
                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let client = client_pool.next();
                let op_start = Instant::now();
                let counter = key_counter.fetch_add(1, Ordering::Relaxed);

                match &cli.command {
                    Commands::Put => {
                        let key = generate_prefixed_key(cli.sequential_keys, cli.key_size, counter);
                        let value = generate_value(cli.value_size);
                        if let Err(e) = client.kv().put(&key, &value).await {
                            eprintln!("Put error: {e:?}");
                            continue;
                        }
                    }
                    Commands::Range { consistency } => {
                        let key = generate_prefixed_key(cli.sequential_keys, cli.key_size, counter);

                        let policy = match consistency.as_str() {
                            "l" | "linearizable" => ReadConsistencyPolicy::LinearizableRead,
                            "s" | "sequential" | "lease" => ReadConsistencyPolicy::LeaseRead,
                            "e" | "eventual" => ReadConsistencyPolicy::EventualConsistency,
                            _ => ReadConsistencyPolicy::LinearizableRead, // Default to linearizable
                        };

                        match client.kv().get_with_policy(key, Some(policy)).await {
                            Err(e) => {
                                eprintln!("Get error: {e:?}");
                                continue;
                            }
                            Ok(_v) => {
                                // println!("Get: {:?}", v);
                            }
                        }
                    }
                }

                stats.record(op_start.elapsed());
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    futures::future::join_all(handles).await;

    // Output statistics
    stats.summary(start_time.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_keys() {
        // Test the order keys from high to low
        assert_eq!(generate_key(true, 6, 0), "999999");
        assert_eq!(generate_key(true, 6, 1), "999998");
        assert_eq!(generate_key(true, 3, 0), "999");
        assert_eq!(generate_key(true, 3, 1), "998");
    }

    #[test]
    fn test_fixed_length() {
        // Ensure that all generated keys maintain a fixed length
        assert_eq!(generate_key(true, 5, 123).len(), 5);
        assert_eq!(generate_key(true, 10, 456).len(), 10);
        assert_eq!(generate_key(false, 8, 42).len(), 8);
    }

    #[test]
    fn test_large_index() {
        // Test large index values ​​(will saturate to minimum value)
        assert_eq!(generate_key(true, 3, 9999), "000"); // Saturate to minimum value
    }

    #[test]
    fn test_random_keys() {
        // Test the determinism and randomness of the random key
        let key1 = generate_key(false, 8, 42);
        let key2 = generate_key(false, 8, 42);
        assert_eq!(key1, key2);

        let key3 = generate_key(false, 8, 43);
        assert_ne!(key1, key3);
    }
}
