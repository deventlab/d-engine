use clap::Parser;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use d_engine::EmbeddedEngine;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Deserialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct GetResponse {
    value: Option<String>,
}

#[derive(Debug, clap::Parser)]
struct Cli {
    #[clap(long, default_value = "8080")]
    port: u16,
    #[clap(long, default_value = "8008")]
    health_port: u16,
    #[clap(long)]
    config_path: String,
}

#[tokio::main]
async fn main() {
    // ============================================================
    // Application Setup (Your Code)
    // ============================================================
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

    println!("Starting HTTP server mode...");
    println!("Business API port: {}", cli.port);
    println!("Health check port: {}", cli.health_port);
    println!("Config path: {}", cli.config_path);

    // ============================================================
    // d-engine Integration (Start & Wait for Leader Election)
    // ============================================================
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

    println!(
        "✓ d-engine ready - Leader: {}, Node ID: {}",
        leader_info.leader_id,
        engine.node_id()
    );

    // ============================================================
    // Application Layer: Start HTTP Services
    // ============================================================
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Health check server (for load balancers)
    let health_handle = tokio::spawn({
        let engine = engine.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            start_health_check_server(engine, cli.health_port, shutdown_rx).await;
        }
    });

    // Business API server (your KV service)
    let business_handle = tokio::spawn({
        let engine = engine.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            start_business_server(engine, cli.port, shutdown_rx).await;
        }
    });

    // ============================================================
    // Graceful Shutdown Handling (Application Responsibility)
    // ============================================================
    println!("Press Ctrl+C to shutdown gracefully");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down gracefully...");
        }
    }

    // Stop HTTP servers
    let _ = shutdown_tx.send(());
    let _ = tokio::join!(health_handle, business_handle);

    // ============================================================
    // d-engine Cleanup (Stop & Flush Data)
    // ============================================================
    println!("Stopping embedded engine...");
    match Arc::try_unwrap(engine) {
        Ok(engine) => {
            if let Err(e) = engine.stop().await {
                eprintln!("Error during engine shutdown: {e}");
            }
        }
        Err(_) => {
            eprintln!("Warning: Cannot stop engine - references still exist");
        }
    }

    println!("✓ Shutdown complete");
}

async fn start_health_check_server(
    engine: Arc<EmbeddedEngine>,
    port: u16,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let app = Router::new()
        .route("/primary", get(health_primary))
        .route("/replica", get(health_replica))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind health check server");

    println!("Health check server listening on port {port}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .expect("Health check server failed");
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
    mut shutdown_rx: watch::Receiver<()>,
) {
    let app = Router::new()
        .route("/kv", post(handle_put))
        .route("/kv/:key", get(handle_get))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind business server");

    println!("Business API server listening on port {port}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .expect("Business server failed");
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
