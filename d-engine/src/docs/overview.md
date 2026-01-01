# d-engine - Distributed Coordination Engine

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for
embedding into applications that need strong consistency—the consensus layer for building
reliable distributed systems.

## 🚀 New to d-engine? Start Here

Follow this learning path to get started quickly:

```text
1. Is d-engine Right for You? (1 minute)
   ↓
2. Choose Integration Mode (1 minute)
   ↓
3a. Quick Start - Embedded (5 minutes)
   OR
3b. Quick Start - Standalone (5 minutes)
   ↓
4. Scale to Cluster (optional)
```

**→ Start: [Is d-engine Right for You?](crate::docs::use_cases)**

## Crate Organization

| Crate               | Purpose                         |
| ------------------- | ------------------------------- |
| **d-engine-proto**  | Protocol definitions (Prost)    |
| **d-engine-core**   | Core Raft algorithm & traits    |
| **d-engine-client** | Client library for applications |
| **d-engine-server** | Server runtime implementation   |

## Quick Start

### Embedded Mode (Rust)

```rust,ignore
use d_engine::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = EmbeddedEngine::with_rocksdb("./data", None).await?;
    engine.wait_ready(std::time::Duration::from_secs(5)).await?;

    let client = engine.client();
    client.put(b"key".to_vec(), b"value".to_vec()).await?;

    Ok(())
}
```

**→ [5-Minute Embedded Guide](crate::docs::quick_start_5min)**

### Standalone Mode (Any Language)

```bash
cd examples/three-nodes-cluster
make start-cluster
```

**→ [Standalone Guide](crate::docs::quick_start_standalone)**

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

### Examples & Performance

- [Single Node Expansion](crate::docs::examples::single_node_expansion) - Scale from 1 to 3 nodes
- [Throughput Optimization](crate::docs::performance::throughput_optimization_guide)

## License

MIT or Apache-2.0
