# d-engine Quick-Start: Embedded Mode

Minimal example of embedding d-engine in a Rust application.

## What is Embedded Mode?

- **Zero gRPC overhead**: Runs in your application process
- **Sub-millisecond latency**: Direct KV operations via `EmbeddedClient`, no network serialization
- **Single binary**: No external dependencies or services to manage
- **Production-ready durability**: All writes persisted via Raft consensus and RocksDB

## Prerequisites

- Rust 1.88+ ([install](https://rustup.rs/))
- ~500MB disk space

## Quick Start (5 minutes)

```bash
# Clone and build
cd examples/quick-start-embedded
make run
```

That's it! The engine starts with:

- Data stored in `./data/single-node` (configured in `d-engine.toml`)
- Single-node mode (no cluster)
- Default port 9081

**Customize**: Edit `d-engine.toml` to change data directory or port.

**Log Control**: Use `RUST_LOG` environment variable to adjust verbosity:

```bash
# Default: WARN level (minimal output)
make run

# Show detailed operations
RUST_LOG=info make run

# Debug mode
RUST_LOG=debug make run

# Trace d-engine internals only
RUST_LOG=d_engine=trace make run
```

You'll see:

```
🚀 Starting d-engine...
✅ d-engine ready!
   → Data directory: ./data/single-node
   → Node ID: 1
   → Listening on: 127.0.0.1:9081

─────────────────────────────────────
=== Quick Start Demo ===
1. Store workflow state
   ✓ workflow:status = running
2. Read workflow state
   ✓ workflow:status = running
3. Store task results
   ✓ task:1 stored
   ✓ task:2 stored
   ✓ task:3 stored
4. Retrieve task results
   ✓ task:1 = completed
   ✓ task:2 = completed
   ✓ task:3 = completed

=== Demo Complete ===
All data persisted locally and durable
Scale to cluster: https://docs.rs/d-engine/latest/d_engine/docs/examples/single_node_expansion/index.html

─────────────────────────────────────
🛑 Shutting down...
✅ Stopped cleanly
```

## How It Works

The example demonstrates 3 simple steps:

```rust
// 1. Start embedded engine with config file
let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

// 2. Wait for leader election (single-node: instant)
let leader = engine.wait_ready(Duration::from_secs(5)).await?;

// 3. Get the embedded KV client (in-process, zero-copy)
let client = engine.client();
```

Then use it like a local HashMap:

```rust
client.put("key".as_bytes().to_vec(), b"value".to_vec()).await?;
let value = client.get_eventual("key".as_bytes().to_vec()).await?;
```

All operations run locally in your process. No network calls. No serialization overhead.

## Single-Node Mode

This example runs a single-node d-engine. The node automatically becomes leader in <100ms and is ready for operations.

**Why single-node matters:**

- Full data durability (Raft consensus + RocksDB persistence)
- Zero external dependencies
- Costs $100/month instead of $300+ for forced 3-node setups
- Perfect for startups, edge computing, or smaller deployments

## Scaling to 3-Node Cluster

When you need high availability, add peers to your configuration. Your application code stays unchanged.

See [Single-Node Expansion](https://docs.rs/d-engine/latest/d_engine/docs/examples/single_node_expansion/index.html) for step-by-step instructions.

## Using This as a Template

Copy this example and modify `src/main.rs`:

1. Replace `run_demo()` with your own business logic
2. Use `client.put()` and `client.get_eventual()` for distributed state
3. Keep everything else as-is

Example:

```rust
async fn my_app(client: &EmbeddedClient) -> Result<(), Box<dyn Error>> {
    // Your code here
    client.put("user:1:name".as_bytes().to_vec(), b"alice".to_vec()).await?;
    client.put("user:1:email".as_bytes().to_vec(), b"alice@example.com".to_vec()).await?;
    Ok(())
}
```

## Data Location

- **Binary**: `target/release/quick-start`
- **Data**: `data/single-node/` (RocksDB files)

## Troubleshooting

**Build fails**

```bash
rustup update
cargo clean
make build
```

**Port conflict**

```bash
make clean
make run
```

**Data corruption**

```bash
rm -rf data/
make run
```

## Next Steps

- Read the full [quick-start guide](https://docs.rs/d-engine/latest/d_engine/docs/quick_start_5min/index.html)
- Learn about [scaling to clusters](https://docs.rs/d-engine/latest/d_engine/docs/examples/single_node_expansion/index.html)
- Explore more [examples](../)

## Key Takeaways

✅ Single node = full durability  
✅ Zero serialization overhead  
✅ Sub-millisecond latency  
✅ Scales to 3+ nodes without code changes  
✅ Production-ready today
