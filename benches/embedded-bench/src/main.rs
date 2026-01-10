use d_engine::EmbeddedEngine;
use d_engine::protocol::ReadConsistencyPolicy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use rand::distributions::Alphanumeric;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

// Batch test configuration constants
const BATCH_TEST_TOTAL_REQUESTS: u64 = 100000;
const BATCH_TEST_CONCURRENT_CLIENTS: usize = 100;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Operation mode: local (in-process benchmark), server (HTTP API), client (HTTP benchmark)
    #[arg(long, default_value = "local")]
    mode: String,

    /// Path to config file
    #[arg(long, default_value = "./config/n1.toml")]
    config_path: String,

    /// HTTP server port (server mode only)
    #[arg(long, default_value = "9001")]
    port: u16,

    /// Health check port (server mode only)
    #[arg(long, default_value = "8008")]
    health_port: u16,

    /// HAProxy endpoint (client mode only)
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    endpoint: String,

    #[arg(long, default_value = "8")]
    key_size: usize,

    #[arg(long, default_value = "256")]
    value_size: usize,

    #[arg(long, default_value = "10000")]
    total: u64,

    #[arg(long, default_value_t = 1)]
    clients: usize,

    #[arg(long, default_value = "false")]
    sequential_keys: bool,

    /// Limit key space to cycle through 0..key_space (enables testing with repeated keys)
    #[arg(long)]
    key_space: Option<u64>,

    /// Verify write results by reading back (put command only)
    #[arg(long, default_value = "false")]
    verify_write: bool,

    /// Run all benchmark tests in sequence (batch mode)
    #[arg(long, default_value = "false")]
    batch: bool,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Put,
    Get {
        #[arg(long, default_value = "l")]
        consistency: String,
    },
}

#[derive(Deserialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct GetResponse {
    value: Option<String>,
}

struct BenchmarkStats {
    histogram: Mutex<Histogram<u64>>,
    total_ops: AtomicU64,
}

impl BenchmarkStats {
    fn new() -> Self {
        let histogram = Mutex::new(
            Histogram::<u64>::new_with_bounds(1, 600_000_000, 3)
                .expect("Failed to create histogram"),
        );
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

fn generate_prefixed_key(
    sequential: bool,
    key_size: usize,
    index: u64,
    key_space: Option<u64>,
) -> String {
    let effective_index = key_space.map_or(index, |space| index % space);

    if sequential {
        let max_value = 10u64.pow(key_size as u32) - 1;
        let value = max_value.saturating_sub(effective_index);
        format!("{value:0key_size$}")
    } else {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(effective_index);
        (0..key_size).map(|_| rng.sample(Alphanumeric)).map(char::from).collect()
    }
}

fn generate_value(size: usize) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    (0..size).map(|_| rng.r#gen()).collect()
}

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let config_path = std::env::var("CONFIG_PATH").ok();
    let mut cli = Cli::parse();

    if let Some(path) = config_path {
        cli.config_path = path;
    }

    match cli.mode.as_str() {
        "local" => run_local_benchmark(cli).await,
        "server" => run_http_server(cli).await,
        "client" => run_http_client_benchmark(cli).await,
        _ => {
            eprintln!(
                "Invalid mode: {}. Use 'local', 'server', or 'client'",
                cli.mode
            );
            std::process::exit(1);
        }
    }
}

/// Run a single benchmark test with specified parameters
#[allow(clippy::too_many_arguments)]
async fn run_benchmark_task(
    engine: &Arc<EmbeddedEngine>,
    test_name: &str,
    command: Commands,
    total: u64,
    clients: usize,
    key_size: usize,
    value_size: usize,
    sequential_keys: bool,
    key_space: Option<u64>,
    verify_write: bool,
) {
    println!("\n========================================");
    println!("{test_name}");
    println!("========================================");

    let stats = Arc::new(BenchmarkStats::new());
    let key_counter = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let mut handles = Vec::with_capacity(clients);

    for _ in 0..clients {
        let engine = engine.clone();
        let stats = stats.clone();
        let key_counter = key_counter.clone();
        let command = command.clone();

        let handle = tokio::spawn(async move {
            while key_counter.load(Ordering::Relaxed) < total {
                let counter = key_counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_prefixed_key(sequential_keys, key_size, counter, key_space);

                let op_start = Instant::now();

                match &command {
                    Commands::Put => {
                        let value = generate_value(value_size);
                        match engine.client().put(key.as_bytes().to_vec(), value.clone()).await {
                            Ok(_) => {
                                if verify_write {
                                    match engine
                                        .client()
                                        .get_linearizable(key.as_bytes().to_vec())
                                        .await
                                    {
                                        Ok(Some(read_value)) if read_value == value => {}
                                        _ => continue,
                                    }
                                }
                            }
                            Err(_) => continue,
                        }
                    }
                    Commands::Get { consistency } => {
                        let result = match consistency.as_str() {
                            "l" | "linearizable" => {
                                engine.client().get_linearizable(key.as_bytes().to_vec()).await
                            }
                            "s" | "sequential" | "lease" => {
                                engine
                                    .client()
                                    .get_with_consistency(
                                        key.as_bytes().to_vec(),
                                        ReadConsistencyPolicy::LeaseRead,
                                    )
                                    .await
                            }
                            "e" | "eventual" => {
                                engine.client().get_eventual(key.as_bytes().to_vec()).await
                            }
                            _ => engine.client().get_linearizable(key.as_bytes().to_vec()).await,
                        };

                        if result.is_err() {
                            continue;
                        }
                    }
                }

                stats.record(op_start.elapsed());
            }
        });

        handles.push(handle);
    }

    futures::future::join_all(handles).await;
    stats.summary(start_time.elapsed());
}

/// Run all benchmark tests in batch mode
async fn run_batch_tests(
    engine: &Arc<EmbeddedEngine>,
    cli: &Cli,
) {
    println!("\n╔════════════════════════════════════════╗");
    println!("║  Running Batch Benchmark Tests        ║");
    println!("╚════════════════════════════════════════╝\n");

    // Test 1: Single client write (10K requests)
    if let Err(e) = run_single_batch_test(
        engine,
        "Single Client Write (10K requests)",
        Commands::Put,
        10_000,
        1,
        cli,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    // 2-second delay between tests for cluster stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 2: High concurrency write (100K requests)
    if let Err(e) = run_single_batch_test(
        engine,
        "High Concurrency Write (100K requests)",
        Commands::Put,
        BATCH_TEST_TOTAL_REQUESTS,
        BATCH_TEST_CONCURRENT_CLIENTS,
        cli,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    // 2-second delay between tests for cluster stabilization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 3: Linearizable read (100K requests)
    if let Err(e) = run_single_batch_test(
        engine,
        "Linearizable Read (100K requests)",
        Commands::Get {
            consistency: "l".to_string(),
        },
        BATCH_TEST_TOTAL_REQUESTS,
        BATCH_TEST_CONCURRENT_CLIENTS,
        cli,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 4: Lease-based read (100K requests)
    if let Err(e) = run_single_batch_test(
        engine,
        "Lease-Based Read (100K requests)",
        Commands::Get {
            consistency: "s".to_string(),
        },
        BATCH_TEST_TOTAL_REQUESTS,
        BATCH_TEST_CONCURRENT_CLIENTS,
        cli,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 5: Eventual consistency read (100K requests)
    if let Err(e) = run_single_batch_test(
        engine,
        "Eventual Consistency Read (100K requests)",
        Commands::Get {
            consistency: "e".to_string(),
        },
        BATCH_TEST_TOTAL_REQUESTS,
        BATCH_TEST_CONCURRENT_CLIENTS,
        cli,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 6: Hot-key test (100K requests, 10 keys)
    let mut cli_hotkey = cli.clone();
    cli_hotkey.key_space = Some(10);
    if let Err(e) = run_single_batch_test(
        engine,
        "Hot-Key Test (100K requests, 10 keys)",
        Commands::Get {
            consistency: "l".to_string(),
        },
        BATCH_TEST_TOTAL_REQUESTS,
        BATCH_TEST_CONCURRENT_CLIENTS,
        &cli_hotkey,
    )
    .await
    {
        eprintln!("Batch test failed: {e}. Stopping all tests.");
        return;
    }

    println!("\n╔════════════════════════════════════════╗");
    println!("║  All Batch Tests Completed!           ║");
    println!("╚════════════════════════════════════════╝\n");
}

/// Helper function to run a single test in batch mode
async fn run_single_batch_test(
    engine: &Arc<EmbeddedEngine>,
    test_name: &str,
    command: Commands,
    total: u64,
    clients: usize,
    cli: &Cli,
) -> Result<(), String> {
    run_benchmark_task(
        engine,
        test_name,
        command,
        total,
        clients,
        cli.key_size,
        cli.value_size,
        cli.sequential_keys,
        cli.key_space,
        cli.verify_write,
    )
    .await;
    Ok(())
}

async fn run_local_benchmark(cli: Cli) {
    println!("Starting local benchmark mode...");

    let engine = Arc::new(
        EmbeddedEngine::start_with(&cli.config_path)
            .await
            .expect("Failed to start engine"),
    );

    let leader_info = match engine.wait_ready(Duration::from_secs(15)).await {
        Ok(info) => info,
        Err(err) => {
            eprintln!("Failed to wait for engine readiness: {err}");
            std::process::exit(1);
        }
    };

    println!("Leader elected: {}", leader_info.leader_id);
    println!("Node ID: {}", engine.node_id());

    // Setup Ctrl+C handler
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(());

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        eprintln!("\nReceived Ctrl+C, shutting down...");
        let _ = shutdown_tx.send(());
    });

    // Check if running in batch mode
    if cli.batch {
        if engine.is_leader() {
            run_batch_tests(&engine, &cli).await;
        } else {
            println!(
                "This node is Follower, keeping cluster membership alive during batch tests..."
            );
            println!("Press Ctrl+C to shutdown.");
            let _ = shutdown_rx.changed().await;
        }
    } else {
        // Single test mode (original behavior)
        let should_run_benchmark = match (&cli.command, engine.is_leader()) {
            (Commands::Put, true) => true, // Leader runs write tests
            (Commands::Get { consistency: _ }, true) => true, // Leader runs all read tests
            (Commands::Get { consistency }, false)
                if consistency == "e" || consistency == "eventual" =>
            {
                true
            } // Follower runs eventual read tests
            _ => false,                    // All other cases: idle
        };

        if should_run_benchmark {
            let role = if engine.is_leader() {
                "Leader"
            } else {
                "Follower"
            };
            println!("This node is {role}, starting benchmark...");

            let stats = Arc::new(BenchmarkStats::new());
            let key_counter = Arc::new(AtomicU64::new(0));
            let start_time = Instant::now();

            let mut handles = Vec::with_capacity(cli.clients);

            for _ in 0..cli.clients {
                let engine = engine.clone();
                let stats = stats.clone();
                let key_counter = key_counter.clone();
                let cli = cli.clone();

                let handle = tokio::spawn(async move {
                    while key_counter.load(Ordering::Relaxed) < cli.total {
                        let counter = key_counter.fetch_add(1, Ordering::Relaxed);
                        let key = generate_prefixed_key(
                            cli.sequential_keys,
                            cli.key_size,
                            counter,
                            cli.key_space,
                        );

                        let op_start = Instant::now();

                        match &cli.command {
                            Commands::Put => {
                                let value = generate_value(cli.value_size);
                                match engine
                                    .client()
                                    .put(key.as_bytes().to_vec(), value.clone())
                                    .await
                                {
                                    Ok(_) => {
                                        // Write succeeded - optionally verify
                                        if cli.verify_write {
                                            match engine
                                                .client()
                                                .get_linearizable(key.as_bytes().to_vec())
                                                .await
                                            {
                                                Ok(Some(read_value)) if read_value == value => {
                                                    // Verification passed
                                                }
                                                Ok(Some(_)) => {
                                                    eprintln!(
                                                        "Write verification failed: value mismatch"
                                                    );
                                                    continue;
                                                }
                                                Ok(None) => {
                                                    eprintln!(
                                                        "Write verification failed: key not found"
                                                    );
                                                    continue;
                                                }
                                                Err(_) => {
                                                    eprintln!(
                                                        "Write verification failed: read error"
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        // Write failed - skip recording
                                        continue;
                                    }
                                }
                            }
                            Commands::Get { consistency } => {
                                let result = match consistency.as_str() {
                                    "l" | "linearizable" => {
                                        engine
                                            .client()
                                            .get_linearizable(key.as_bytes().to_vec())
                                            .await
                                    }
                                    "s" | "sequential" | "lease" => {
                                        engine
                                            .client()
                                            .get_with_consistency(
                                                key.as_bytes().to_vec(),
                                                ReadConsistencyPolicy::LeaseRead,
                                            )
                                            .await
                                    }
                                    "e" | "eventual" => {
                                        engine.client().get_eventual(key.as_bytes().to_vec()).await
                                    }
                                    _ => {
                                        engine
                                            .client()
                                            .get_linearizable(key.as_bytes().to_vec())
                                            .await
                                    }
                                };

                                // Only record successful operations
                                if result.is_err() {
                                    continue;
                                }
                            }
                        }

                        // Record latency only for successful operations
                        stats.record(op_start.elapsed());
                    }
                });

                handles.push(handle);
            }

            futures::future::join_all(handles).await;
            stats.summary(start_time.elapsed());

            println!("\nBenchmark completed. Press Ctrl+C to shutdown.");
            let _ = shutdown_rx.changed().await;
        } else {
            let role = if engine.is_leader() {
                "Leader"
            } else {
                "Follower"
            };
            println!(
                "This node is {role}, keeping cluster membership alive (no benchmark to run)..."
            );
            println!("Press Ctrl+C to shutdown.");
            let _ = shutdown_rx.changed().await;
        }
    }

    // Graceful shutdown
    match Arc::try_unwrap(engine) {
        Ok(engine) => {
            engine.stop().await.expect("Failed to stop engine");
        }
        Err(_) => {
            eprintln!("Warning: Cannot stop engine (Arc still has references)");
        }
    }
}

async fn run_http_server(cli: Cli) {
    println!("Starting HTTP server mode...");
    println!("Business API port: {}", cli.port);
    println!("Health check port: {}", cli.health_port);

    let engine = Arc::new(
        EmbeddedEngine::start_with(&cli.config_path)
            .await
            .expect("Failed to start engine"),
    );

    let leader_info = match engine.wait_ready(Duration::from_secs(5)).await {
        Ok(info) => info,
        Err(err) => {
            eprintln!("Failed to wait for engine readiness: {err}");
            std::process::exit(1);
        }
    };

    println!("Leader elected: {}", leader_info.leader_id);
    println!("Node ID: {}", engine.node_id());

    // Start health check server
    let health_engine = engine.clone();
    let health_port = cli.health_port;
    tokio::spawn(async move {
        start_health_check_server(health_engine, health_port).await;
    });

    // Start business API server
    start_business_server(engine, cli.port).await;
}

async fn start_health_check_server(
    engine: Arc<EmbeddedEngine>,
    port: u16,
) {
    let app = Router::new()
        .route("/primary", get(health_primary))
        .route("/replica", get(health_replica))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind health check server");

    println!("Health check server listening on port {port}");

    axum::serve(listener, app).await.expect("Health check server failed");
}

async fn health_primary(State(engine): State<Arc<EmbeddedEngine>>) -> StatusCode {
    if engine.is_leader() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn health_replica(State(engine): State<Arc<EmbeddedEngine>>) -> StatusCode {
    if !engine.is_leader() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn start_business_server(
    engine: Arc<EmbeddedEngine>,
    port: u16,
) {
    let app = Router::new()
        .route("/kv", post(handle_put))
        .route("/kv/:key", get(handle_get))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind business server");

    println!("Business API server listening on port {port}");

    axum::serve(listener, app).await.expect("Business server failed");
}

async fn handle_put(
    State(engine): State<Arc<EmbeddedEngine>>,
    Json(req): Json<PutRequest>,
) -> StatusCode {
    match engine.client().put(req.key.into_bytes(), req.value.into_bytes()).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn handle_get(
    State(engine): State<Arc<EmbeddedEngine>>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    match engine.client().get_eventual(key.into_bytes()).await {
        Ok(value) => Ok(Json(GetResponse {
            value: value.map(|v| String::from_utf8_lossy(&v).to_string()),
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn run_http_client_benchmark(cli: Cli) {
    println!("Starting HTTP client benchmark mode...");
    println!("Target endpoint: {}", cli.endpoint);

    let client = reqwest::Client::new();
    let stats = Arc::new(BenchmarkStats::new());
    let key_counter = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let mut handles = Vec::with_capacity(cli.clients);

    for _ in 0..cli.clients {
        let client = client.clone();
        let stats = stats.clone();
        let key_counter = key_counter.clone();
        let cli = cli.clone();

        let handle = tokio::spawn(async move {
            while key_counter.load(Ordering::Relaxed) < cli.total {
                let counter = key_counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_prefixed_key(
                    cli.sequential_keys,
                    cli.key_size,
                    counter,
                    cli.key_space,
                );

                let op_start = Instant::now();

                match &cli.command {
                    Commands::Put => {
                        let value =
                            String::from_utf8_lossy(&generate_value(cli.value_size)).to_string();
                        let _ = client
                            .post(format!("{}/kv", cli.endpoint))
                            .json(&serde_json::json!({
                                "key": key,
                                "value": value
                            }))
                            .send()
                            .await;
                    }
                    Commands::Get { .. } => {
                        let _ = client.get(format!("{}/kv/{}", cli.endpoint, key)).send().await;
                    }
                }

                stats.record(op_start.elapsed());
            }
        });

        handles.push(handle);
    }

    futures::future::join_all(handles).await;
    stats.summary(start_time.elapsed());
}
