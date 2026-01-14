# d-engine Standalone Mode - Client Usage Example

Command-line example demonstrating CRUD operations with d-engine server using gRPC client.

---

## About This Example

This example shows how to use **d-engine in Standalone Mode** - where d-engine runs as a separate server process and your application connects via gRPC.

d-engine supports two integration modes:

- **Embedded Mode**: d-engine runs inside your Rust application (ultra-low latency, <0.1ms)
- **Standalone Mode** ⭐ **(this example)**: d-engine runs as a separate server (language-agnostic, gRPC API)

---

## Features Demonstrated

- **Put/Update**: Create or update key-value pairs (write operations)
- **Delete**: Remove keys from the cluster
- **Get**: Read values with eventual consistency (fast, may read stale data)
- **Lease Read**: Read with lease-based consistency (balanced performance/consistency)
- **Linearizable Read**: Read with strong consistency guarantees (latest committed data)

---

## Prerequisites

**Start a d-engine server first:**

```bash
cd examples/three-nodes-standalone
make start-cluster
```

The server should be running on default endpoints:

- Node 1: `http://127.0.0.1:9081`
- Node 2: `http://127.0.0.1:9082`
- Node 3: `http://127.0.0.1:9083`

---

## Usage

### Basic Commands

```bash
# Put key-value pair (creates or updates)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" put 42 100

# Get value (eventually consistent - fastest read)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" get 42

# Lease read (balanced consistency)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" sget 42

# Linearizable read (strongly consistent - guaranteed latest value)
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" lget 42

# Delete key
cargo run -- --endpoints "http://127.0.0.1:9083,http://127.0.0.1:9082,http://127.0.0.1:9081" delete 42
```
