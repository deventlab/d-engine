# d-engine-server

Production-ready Raft consensus server implementation. Provides strongly-consistent distributed key-value storage with pluggable backends.

## What is d-engine-server?

d-engine-server is the server runtime for d-engine - it combines:

- **Raft consensus protocol** (from d-engine-core)
- **gRPC networking** for cluster communication
- **Pluggable storage backends** (File, RocksDB)
- **State machine** for executing client commands

Think of it as: **Raft protocol + Storage + Network = Running Server**

## Architecture

```
┌─────────────────────────────────────────────────────┐
│ d-engine-server                                     │
│                                                      │
│  ┌────────────────────────────────────────────────┐ │
│  │ Node (Raft node lifecycle)                     │ │
│  │  - Leader election                             │ │
│  │  - Log replication                             │ │
│  │  - Cluster membership                          │ │
│  └──────────┬──────────────────┬──────────────────┘ │
│             │                  │                     │
│  ┌──────────▼────────┐  ┌─────▼──────────────────┐ │
│  │ StorageEngine     │  │ StateMachine           │ │
│  │  - Log storage    │  │  - Apply commands      │ │
│  │  - Metadata       │  │  - Snapshots           │ │
│  │  - HardState      │  │  - KV operations       │ │
│  └───────────────────┘  └────────────────────────┘ │
│                                                      │
│  ┌────────────────────────────────────────────────┐ │
│  │ gRPC Services                                  │ │
│  │  - RaftClientService (client read/write)      │ │
│  │  - RaftService (peer-to-peer replication)     │ │
│  │  - RaftClusterService (membership changes)    │ │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Key Components

**Node** - The Raft consensus participant

- Manages node lifecycle (follower → candidate → leader)
- Coordinates log replication across cluster
- Handles client requests and membership changes

**StorageEngine** - Persistent storage for Raft state

- `LogStore`: Raft log entries
- `MetaStore`: Term, voted_for, commit_index

**StateMachine** - Application state executor

- Applies committed log entries to KV store
- Generates snapshots for log compaction
- Restores state from snapshots

## Quick Start

### 1. Single Node (Development)

```rust
use d_engine_server::{NodeBuilder, FileStorageEngine, FileStateMachine};
use std::sync::Arc;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    // Create storage components
    let storage = Arc::new(
        FileStorageEngine::new(PathBuf::from("/tmp/raft-log"))?
    );
    let state_machine = Arc::new(
        FileStateMachine::new(PathBuf::from("/tmp/raft-sm")).await?
    );

    // Build and start node
    let node = NodeBuilder::new(None, shutdown_rx)
        .storage_engine(storage)
        .state_machine(state_machine)
        .start()
        .await?;

    // Run until shutdown
    node.run().await?;
    Ok(())
}
```

### 2. Three-Node Cluster (Production)

**Node 1** (`cluster.yaml`):

```yaml
cluster_id: "prod-cluster"
node_id: 1
rpc_addr: "0.0.0.0:9081"
peers:
  - id: 2
    addr: "node2:9082"
  - id: 3
    addr: "node3:9083"
```

**Start node**:

```rust
let node = NodeBuilder::new(Some("cluster.yaml"), shutdown_rx)
    .storage_engine(storage)
    .state_machine(state_machine)
    .start()
    .await?;

node.run().await?;
```

Repeat for nodes 2 and 3 with their respective configs.

## Storage Backends

### File Storage (Default)

Simple file-based storage. Good for development and small deployments.

```rust
use d_engine_server::{FileStorageEngine, FileStateMachine};

let storage = Arc::new(FileStorageEngine::new("/tmp/logs")?);
let sm = Arc::new(FileStateMachine::new("/tmp/sm").await?);
```

**Characteristics**:

- One file per log entry
- Simple snapshot files
- No external dependencies

### RocksDB Storage (Production)

High-performance embedded database. Recommended for production.

```toml
[dependencies]
d-engine-server = { version = "0.2", features = ["rocksdb"] }
```

```rust
use d_engine_server::{RocksDBStorageEngine, RocksDBStateMachine};

let storage = Arc::new(RocksDBStorageEngine::new("/data/logs")?);
let sm = Arc::new(RocksDBStateMachine::new("/data/sm").await?);
```

**Characteristics**:

- LSM-tree storage engine
- Efficient compaction
- Better write throughput

## Custom Storage Backend

Implement `StorageEngine` and `StateMachine` traits:

```rust
use d_engine_server::{StorageEngine, StateMachine, LogStore, MetaStore};
use d_engine_core::{Entry, Result};

struct MyStorageEngine { /* ... */ }

impl StorageEngine for MyStorageEngine {
    type LogStore = MyLogStore;
    type MetaStore = MyMetaStore;

    fn log_store(&self) -> Arc<Self::LogStore> { /* ... */ }
    fn meta_store(&self) -> Arc<Self::MetaStore> { /* ... */ }
}

#[async_trait]
impl StateMachine for MyStateMachine {
    async fn apply(&mut self, entry: Entry) -> Result<Vec<u8>> {
        // Apply committed log entry
    }

    async fn snapshot(&self) -> Result<Vec<u8>> {
        // Generate snapshot
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        // Restore from snapshot
    }
}
```

## Client Integration

d-engine-server exposes three gRPC services:

### 1. RaftClientService (Client Operations)

```protobuf
service RaftClientService {
    rpc HandleClientWrite (ClientWriteRequest) returns (ClientResponse);
    rpc HandleClientRead (ClientReadRequest) returns (ClientResponse);
}
```

Use **d-engine-client** (Rust) or generate bindings for your language:

```bash
# Generate Go client
protoc --go_out=. --go-grpc_out=. proto/client/*.proto
```

### 2. RaftService (Internal - Peer Replication)

For cluster nodes to communicate (not for application use).

### 3. RaftClusterService (Membership Management)

```protobuf
service RaftClusterService {
    rpc AddNode (AddNodeRequest) returns (AddNodeResponse);
    rpc RemoveNode (RemoveNodeRequest) returns (RemoveNodeResponse);
}
```

## Resources

- **d-engine-client**: Rust client library
- **d-engine-proto**: Protocol buffer definitions
- **d-engine-core**: Core Raft implementation
- **d-engine-docs**: Architecture and design documentation

## License

See LICENSE file in repository root.
