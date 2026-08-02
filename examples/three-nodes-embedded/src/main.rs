use clap::Parser;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use d_engine::{ClientApiError, DefaultEmbeddedEngine, ErrorCode};
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
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = std::env::var("CONFIG_PATH").ok();
    let mut cli = Cli::parse();
    if let Some(path) = config_path {
        cli.config_path = path;
    }

    println!(
        "starting (api::{} health::{} config::{})",
        cli.port, cli.health_port, cli.config_path
    );

    // ============================================================
    // d-engine Integration (Start & Wait for Leader Election)
    // ============================================================
    let engine = Arc::new(
        DefaultEmbeddedEngine::start_with(&cli.config_path)
            .await
            .expect("Failed to start engine"),
    );

    let leader_info = match engine.wait_ready(Duration::from_secs(5)).await {
        Ok(info) => info,
        Err(err) => {
            eprintln!("✗ node failed to become ready: {err}");
            std::process::exit(1);
        }
    };

    println!(
        "✓ node {} ready — leader {}",
        engine.node_id(),
        leader_info.leader_id,
    );

    // ============================================================
    // Application Layer: Start HTTP Services
    // ============================================================
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // Health check server (for load balancers)
    let mut health_handle = tokio::spawn({
        let engine = engine.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            start_health_check_server(engine, cli.health_port, shutdown_rx).await;
        }
    });

    // Business API server (your KV service)
    let mut business_handle = tokio::spawn({
        let engine = engine.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            start_business_server(engine, cli.port, shutdown_rx).await;
        }
    });

    // ============================================================
    // Graceful Shutdown Handling (Application Responsibility)
    // ============================================================
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            if let Err(e) = res {
                eprintln!("✗ failed to install Ctrl+C handler: {e}");
            }
            println!("shutting down...");
        }
        _ = &mut health_handle => {
            eprintln!("✗ health check server task exited unexpectedly");
        }
        _ = &mut business_handle => {
            eprintln!("✗ business server task exited unexpectedly");
        }
    }

    // Stop HTTP servers
    let _ = shutdown_tx.send(());
    let _ = tokio::join!(health_handle, business_handle);

    // ============================================================
    // d-engine Cleanup (Stop & Flush Data)
    // ============================================================
    match Arc::try_unwrap(engine) {
        Ok(engine) => {
            if let Err(e) = engine.stop().await {
                eprintln!("✗ error during shutdown: {e}");
            }
        }
        Err(_) => {
            eprintln!("✗ warning: cannot stop engine - references still exist");
        }
    }

    println!("✓ shutdown complete");
}

async fn start_health_check_server(
    engine: Arc<DefaultEmbeddedEngine>,
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

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .expect("Health check server failed");
}

async fn health_primary(State(engine): State<Arc<DefaultEmbeddedEngine>>) -> StatusCode {
    if engine.is_leader() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn health_replica(State(engine): State<Arc<DefaultEmbeddedEngine>>) -> StatusCode {
    if !engine.is_leader() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn start_business_server(
    engine: Arc<DefaultEmbeddedEngine>,
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

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .expect("Business server failed");
}

fn is_not_leader(e: &ClientApiError) -> bool {
    e.code() == ErrorCode::NotLeader
}

async fn handle_put(
    State(engine): State<Arc<DefaultEmbeddedEngine>>,
    Json(req): Json<PutRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    match engine.client().put(req.key.into_bytes(), req.value.into_bytes()).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({}))),
        Err(e) if is_not_leader(&e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "not_leader",
                "hint": "Use GET /primary on the health port to find the leader, then retry the write there."
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("{e}") })),
        ),
    }
}

async fn handle_get(
    State(engine): State<Arc<DefaultEmbeddedEngine>>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    match engine.client().get_eventual(key.into_bytes()).await {
        Ok(value) => Ok(Json(GetResponse {
            value: value.map(|v| String::from_utf8_lossy(&v).to_string()),
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
