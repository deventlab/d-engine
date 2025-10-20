# D-Engine

[![Crates.io](https://img.shields.io/crates/v/d-engine.svg)](https://crates.io/crates/d-engine)
[![docs.rs](https://docs.rs/d-engine/badge.svg)](https://docs.rs/d-engine)
[![codecov](https://codecov.io/gh/deventlab/d-engine/graph/badge.svg?token=K3BEDM45V8)](https://codecov.io/gh/deventlab/d-engine)
![Static Badge](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
[![CI](https://github.com/deventlab/d-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/d-engine/actions/workflows/ci.yml)

**d-engine** is a lightweight and strongly consistent Raft consensus engine written in Rust. It is a base to build reliable and scalable distributed systems. **Designed for resource efficiency**, d-engine employs a single-threaded event-driven architecture that maximizes single CPU core performance while minimizing resource overhead. It plans to provide a production-ready implementation of the Raft consensus algorithm, with support for pluggable storage backends, observability, and runtime flexibility.

---

## Features

- **Strong Consistency**: Full production-grade implementation of the Raft consensus protocol, including leader election, log replication, membership changes, and fault-tolerant state replication.
- **Flexible Read Consistency**: Supports three-tier read policies — Linearizable, Lease-Based, and Eventual — allowing fine-grained control over consistency and latency.
- **Pluggable Storage**: Compatible with multiple storage backends such as RocksDB, Sled, and Raw File, with an abstraction layer for custom implementations.
- **Configurable Snapshotting**: Built-in snapshot generation with customizable policies, on-demand creation, or the option to disable snapshots entirely.
- **Dynamic Cluster Membership**: New nodes can join as learners and be promoted to followers based on configurable synchronization conditions.
- **Optimized Resource Usage**: Single-threaded, event-driven architecture maximizing single-core throughput while minimizing memory and disk overhead.
- **Deep Observability**: Exposes rich metrics, structured logs, and tracing data for integration with your own monitoring stack (e.g., Prometheus + Grafana).

---

## Quick Start

```no_run
use d_engine::NodeBuilder;
use d_engine::FileStorageEngine;
use d_engine::FileStateMachine;
use tokio::sync::watch;
use tracing::error;
use tracing::info;
use std::path::PathBuf;
use std::sync::Arc;


#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (graceful_tx, graceful_rx) = watch::channel(());

    let path = PathBuf::from("/tmp/db");
    let storage_engine = Arc::new(FileStorageEngine::new(path.join("storage")).unwrap());
    let state_machine =
        Arc::new(FileStateMachine::new(path.join("state_machine")).await.unwrap());

    let node = NodeBuilder::new(None, graceful_rx.clone())
        .storage_engine(storage_engine)
        .state_machine(state_machine)
        .build()
        .start_rpc_server()
        .await
        .ready()
        .expect("start node failed.");


    if let Err(e) = node.run().await {
        error!("node stops: {:?}", e);
    } else {
        info!("node stops.");
    }
}
```

## Core Concepts

![Data Flow](https://www.mermaidchart.com/raw/67aa2040-9292-4aed-b5cd-44621245f1c4?theme=light&version=v0.1&format=svg)

For production deployments, a minimum cluster size of **3 nodes** is required.
