# d-engine - Distributed Coordination Engine

**d-engine** is a lightweight distributed coordination engine written in Rust,
designed for embedding into applications that need strong consistency—the consensus
layer for building reliable distributed systems.

## Recommended Entry Point

```toml
[dependencies]
d-engine = "0.2"  # This is all you need
```

## Quick Start

### Embedded Mode (Rust)

```rust,ignore
use d_engine::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = EmbeddedEngine::start("./data").await?;
    engine.wait_ready(Duration::from_secs(5)).await?;

    let client = engine.client();
    client.put(b"hello".to_vec(), b"world".to_vec()).await?;
    let value = client.get_linearizable(b"hello".to_vec()).await?;
    println!("{}", String::from_utf8_lossy(&value.unwrap()));

    engine.stop().await?;
    Ok(())
}
```

**→ [5-Minute Embedded Guide](crate::docs::quick_start_5min)**

### Standalone Mode (Any Language)

```bash
cd examples/three-nodes-standalone
make start-cluster
```

**→ [Standalone Guide](crate::docs::quick_start_standalone)**

## Client API

- [`EmbeddedClient`] → [`EmbeddedEngine::client()`]
- [`GrpcClient`] → [`ClientBuilder::build()`]

Both expose identical methods — pick your mode, the API below applies to both. No need to import `ClientApi` directly.

**Write**

| Method                               | Description                                    |
| ------------------------------------ | ---------------------------------------------- |
| [`EmbeddedClient::put`]              | Write a key-value pair                         |
| [`EmbeddedClient::put_with_ttl`]     | Write with expiry (key auto-expires after TTL) |
| [`EmbeddedClient::delete`]           | Delete a key                                   |
| [`EmbeddedClient::compare_and_swap`] | Atomic CAS                                     |

**Read**

| Method                                    | Description                                         |
| ----------------------------------------- | --------------------------------------------------- |
| [`EmbeddedClient::get_eventual`]          | Fast local read (may be slightly stale)             |
| [`EmbeddedClient::get_linearizable`]      | Linearizable read (goes through leader)             |
| [`EmbeddedClient::get`]                   | Read with default consistency policy                |
| [`EmbeddedClient::get_multi`]             | Read multiple keys                                  |
| [`EmbeddedClient::get_multi_with_policy`] | Read multiple keys with explicit consistency policy |
| [`EmbeddedClient::scan_prefix`]           | Prefix scan                                         |
| [`EmbeddedClient::get_lease`]             | Lease-based read (optimized linearizable)           |

**Watch**

| Method                                        | Description                              |
| --------------------------------------------- | ---------------------------------------- |
| [`EmbeddedClient::watch`]                     | Watch a key for changes                  |
| [`EmbeddedClient::watch_with_options`]        | Watch a key with full options            |
| [`EmbeddedClient::watch_prefix`]              | Watch all keys under a prefix            |
| [`EmbeddedClient::watch_prefix_with_options`] | Watch prefix with options (e.g. prev_kv) |
| [`EmbeddedEngine::watch_membership`]          | Watch cluster membership changes         |

**Cluster**

| Method                            | Description                |
| --------------------------------- | -------------------------- |
| [`EmbeddedClient::list_members`]  | List all cluster members   |
| [`EmbeddedClient::get_leader_id`] | Get current leader node ID |

## Documentation Index

### Getting Started

- [Is d-engine Right for You?](crate::docs::use_cases) - Common use cases
- [Integration Modes](crate::docs::integration_modes) - Embedded vs Standalone
- [Quick Start - Embedded](crate::docs::quick_start_5min)
- [Quick Start - Standalone](crate::docs::quick_start_standalone)

### Guides by Role

#### Client Developers

- [Read Consistency](crate::docs::client_guide::read_consistency) - Choosing consistency policies
- [Error Handling](crate::docs::client_guide::error_handling)

#### Server Operators

- [Customize Storage Engine](crate::docs::server_guide::customize_storage_engine)
- [Customize State Machine](crate::docs::server_guide::customize_state_machine)
- [Consistency Tuning](crate::docs::server_guide::consistency_tuning)
- [Watch Feature](crate::docs::server_guide::watch_feature)
- [Snapshot Guarantees](crate::docs::server_guide::snapshot_guarantees)

### Examples & Performance

- [HA Deployment with Load Balancing](crate::docs::examples::ha_deployment_load_balancing) - Production HA setup
- [Single Node Expansion](crate::docs::examples::single_node_expansion) - Scale from 1 to 3 nodes
- [Three-Node Standalone Cluster](crate::docs::examples::three_nodes_cluster) - Standalone mode cluster
- [Throughput Optimization](crate::docs::performance::throughput_optimization_guide)
- [Benchmarking Guide](crate::docs::performance::benchmarking_guide) - Run embedded and standalone benchmarks
