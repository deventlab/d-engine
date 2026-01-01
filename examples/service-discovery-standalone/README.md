# Service Discovery Example

Demonstrates d-engine's **Watch API** through a practical service discovery use case.

**What you'll learn:**

- How to use the Watch API for real-time change notifications
- Implementing the read-then-watch pattern
- Building reactive systems without polling

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                     d-engine Cluster                   │
│  (stores service registry: /services/{name}/{instance})│
└────────────────────┬───────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
    ┌─────▼─────┐         ┌─────▼─────┐
    │   Admin   │         │  Watcher  │
    │  (writes) │         │  (reads)  │
    │           │         │           │
    │ Register/ │         │ Watch +   │
    │ Unregister│         │ Cache     │
    └───────────┘         └───────────┘
```

## Prerequisites

**Start the d-engine server cluster first** using the three-nodes-cluster example:

```bash
# In another terminal, start at least one node
cd examples/three-nodes-cluster
make start-cluster  # or: make start-node-1
```

## Quick Start

```bash
# Terminal 1: Start watcher (waits for changes)
cargo run --bin watcher -- --key "services/api-gateway/node1"

# Terminal 2: Register service
cargo run --bin admin -- register --name api-gateway --instance node1 --endpoint "192.168.1.10:8080"

# Watcher terminal will show: [PUT] services/api-gateway/node1 = 192.168.1.10:8080

# Unregister service
cargo run --bin admin -- unregister --name api-gateway --instance node1

# Watcher terminal will show: [DELETE] services/api-gateway/node1
```

## Components

| Binary    | Purpose                                   |
| --------- | ----------------------------------------- |
| `admin`   | Registers/unregisters service endpoints   |
| `watcher` | Watches for service changes via Watch API |

## Key Concepts Demonstrated

### Primary: Watch API Usage

1. **Watch API** — Real-time change notifications without polling
2. **Read-then-Watch Pattern** — Read current state, then watch for changes
3. **Event Handling** — Process PUT/DELETE events reactively

### Secondary: Service Discovery Pattern

- Using d-engine as a service registry
- EventualConsistency reads for fast lookups
- Key-value schema design for services

## Related Documentation

- Service Discovery Pattern (coming soon on docs.rs)
- [Watch Feature Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/watch_feature/index.html)
- [Read Consistency Guide](https://docs.rs/d-engine/latest/d_engine/docs/client_guide/read_consistency/index.html)
