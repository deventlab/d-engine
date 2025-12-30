# d-engine Quick-Start: Embedded Mode

Minimal example of embedding d-engine in a Rust application.

## What is Embedded Mode?

- **Zero gRPC overhead**: Runs in your application process
- **Sub-millisecond latency**: Direct KV operations via `LocalKvClient`, no network serialization
- **Single binary**: No external dependencies or services to manage
- **Production-ready durability**: All writes persisted via Raft consensus and RocksDB

## Prerequisites

- Rust 1.88+ ([install](https://rustup.rs/))
- ~500MB disk space

## Quick Start (5 minutes)

```bash
# Clone and build
cd examples/quick-start
make run
```

That's it! The engine starts with:

- Data stored in `./data/single-node`
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
Starting d-engine in embedded mode...
Node 1 initialized
Node ready for operations
=== d-engine Embedded Mode Demo ===
All operations: local-first, <0.1ms latency

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
Press Ctrl+C to exit
```

## How It Works

The example demonstrates 5 key steps:

```rust
// 1. Create storage engine (persists Raft logs)
let storage = Arc::new(RocksDBStorageEngine::new(path)?);

// 2. Create state machine (persists KV data)
let state_machine = Arc::new(RocksDBStateMachine::new(path)?);

// 3. Create shutdown signal
let (shutdown_tx, shutdown_rx) = watch::channel(());

// 4. Build the Raft node
let node = NodeBuilder::new(None, shutdown_rx)
    .storage_engine(storage)
    .state_machine(state_machine)
    .start()
    .await?;

// 5. Get the embedded KV client (in-process, zero-copy)
let client = node.local_client();
```

Then use it like a local HashMap:

```rust
client.put("key", b"value").await?;
let value = client.get("key").await?;
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

See [Single-Node Expansion](../../../d-engine-docs/src/docs/examples/single-node-expansion.md) for step-by-step instructions.

## Using This as a Template

Copy this example and modify `src/main.rs`:

1. Replace `demo_kv_operations()` with your own business logic
2. Use `client.put()` and `client.get()` for distributed state
3. Keep everything else as-is

Example:

```rust
async fn my_app(client: &LocalKvClient) -> Result<(), Box<dyn Error>> {
    // Your code here
    client.put("user:1:name", b"alice").await?;
    client.put("user:1:email", b"alice@example.com").await?;
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

- Read the full [quick-start guide](../../../d-engine-docs/src/docs/quick-start-5min.md)
- Learn about [scaling to clusters](../../../d-engine-docs/src/docs/examples/single-node-expansion.md)
- Explore more [examples](../)

## Key Takeaways

✅ Single node = full durability  
✅ Zero serialization overhead  
✅ Sub-millisecond latency  
✅ Scales to 3+ nodes without code changes  
✅ Production-ready today
