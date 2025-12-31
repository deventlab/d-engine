# Watch Feature

The Watch feature provides real-time notifications when keys change in d-engine, enabling event-driven architectures without polling.

## Quick Start

### Enable Watch

Edit `config/base/raft.toml`:

```toml
[raft.watch]
event_queue_size = 1000       # Global broadcast channel buffer
watcher_buffer_size = 10      # Per-watcher buffer
enable_metrics = false        # Detailed logging (default: false)
```

> **Note**: Watch feature is controlled by the `watch` feature flag in `Cargo.toml` at compile time. If compiled with the feature, it's always enabled.

### Client Usage

**Key Insight**: Watch works on **any node** (Leader or Follower) because events trigger after Raft consensus → `StateMachine.apply()`.

```rust,ignore
use d_engine_client::Client;
use futures::StreamExt;

// Multi-node connection (recommended for high availability)
let client = Client::builder(vec![
    "http://node1:9081".to_string(),
    "http://node2:9082".to_string(),
    "http://node3:9083".to_string(),
])
.build()
.await?;

// Watch connects to any available node
let mut stream = client.watch("my_key").await?;

// Receive events
while let Some(event) = stream.next().await {
    match event {
        Ok(event) => {
            println!("Event: {:?} Key: {:?} Value: {:?}",
                event.event_type, event.key, event.value);
        }
        Err(e) => eprintln!("Watch error: {:?}", e),
    }
}
```

### Embedded Usage

For in-process usage (e.g., `EmbeddedEngine`), you can use the `watch()` method directly:

```rust,ignore
use d_engine::d_engine_server::embedded::EmbeddedEngine;

// Initialize engine with config enabling watch
let engine = EmbeddedEngine::start_with("./data", Some("d-engine.toml")).await?;

// Start watching
let mut watcher = engine.watch("my_key").await?;

// Spawn a task to handle events
tokio::spawn(async move {
    while let Some(event) = watcher.receiver_mut().recv().await {
        println!("Event: {:?}", event);
    }
});

// Or use into_receiver() for long-lived streams (disables auto-cleanup)
let (id, key, receiver) = watcher.into_receiver();
```

## Configuration

### `event_queue_size`

- **Type**: usize
- **Default**: `1000`
- **Description**: Global broadcast channel buffer size. When full, oldest events are dropped (lagging receivers).
- **Memory**: ~24 bytes per slot (1000 slots ≈ 24KB)
- **Tuning**:
  - Low traffic (< 1K writes/sec): 500-1000
  - Medium traffic (1K-10K writes/sec): 1000-2000
  - High traffic (> 10K writes/sec): 2000-5000

### `watcher_buffer_size`

- **Type**: usize
- **Default**: `10`
- **Description**: Per-watcher channel buffer. Each watcher gets its own mpsc channel.
- **Memory**: ~240 bytes per slot per watcher (10 slots × 100 watchers ≈ 240KB)
- **Tuning**:
  - Fast consumers (< 1ms): 5-10
  - Normal consumers (1-10ms): 10-20
  - Slow consumers (> 10ms): 20-50

### `enable_metrics`

- **Type**: boolean
- **Default**: `false`
- **Description**: Enable detailed metrics and warnings for lagging receivers. Adds ~0.001% overhead.

## Behavior

### Event Delivery

- **Guarantee**: At-most-once delivery
- **Order**: FIFO per key (events from StateMachine apply order)
- **Dropped Events**: When buffers are full, events are dropped (non-blocking design)
- **Lagging**: Broadcast channel drops oldest events when receivers are slow

### Lifecycle

- **Registration**: Watch starts receiving events immediately after registration
- **Cleanup**: Watcher is automatically unregistered when handle is dropped
- **No History**: Watch only receives future events (not historical changes)

### Limitations (v1)

- **Exact Key Match Only**: Prefix/range watch not supported
- **No Persistence**: Watchers lost on server restart
- **No Replay**: Cannot receive historical events

## Performance

### Write Path Overhead

- **When Enabled**: < 0.01% overhead (broadcast is fire-and-forget)
- **When Disabled**: Zero overhead (feature-gated)

### Latency

- **End-to-End**: ~100µs typical (StateMachine apply → watcher receive)
- **Throughput**: Linear scaling up to 10K+ watchers

### Memory Usage

```text
Base:    24KB (broadcast channel at default 1000)
Watchers: 240 bytes × buffer_size × watcher_count
Example:  100 watchers × 10 buffer = 240KB
```

## Disabling Watch

To completely disable Watch and reclaim resources, exclude the `watch` feature when building:

```toml
# Cargo.toml - disable watch feature
[dependencies]
d-engine = { version = "0.2", default-features = false }
# Or explicitly list only needed features
d-engine = { version = "0.2", features = ["rocksdb"] }
```

**Effect**:

- Watch code is not compiled into binary
- Zero memory and CPU overhead
- Smaller binary size

## Use Cases

### Event-Driven Queue (d-queue)

```text
// Watch for new messages in a queue
watch(b"queue:messages:count")
```

### Distributed Lock Release

```text
// Wait for lock to be released
watch(b"lock:my_resource")
```

### Configuration Updates

```text
// Monitor config changes
watch(b"config:feature_flags")
```

## Error Handling

### `UNAVAILABLE`

- **Cause**: Watch feature not compiled (missing `watch` feature flag)
- **Solution**: Rebuild with `--features watch` or add to Cargo.toml dependencies

### Stream Ends

- **Cause**: Server shutdown or network error
- **Solution**: Reconnect and re-register watch

### Lagged Events

- **Cause**: Broadcast buffer full (slow consumer)
- **Detection**: `broadcast::RecvError::Lagged(n)` in logs
- **Solution**: Increase `event_queue_size` or process events faster

## Best Practices

1. **Start Watch Before Write**: To avoid missing events, start watching before performing writes
2. **Read + Watch Pattern**: Read current value first, then watch for future changes
3. **Idempotent Handlers**: Handle duplicate events gracefully (at-most-once delivery)
4. **Buffer Tuning**: Monitor lagged events and adjust buffer sizes accordingly
5. **Reconnect Logic**: Implement automatic reconnection on stream errors

## Watch Reliability and Reconnection

### Understanding Watch Event Guarantees

Watch events are **only triggered after Raft consensus**:

```text
Write → Raft Consensus → StateMachine.apply() → broadcast::send()
                                                        ↓
                                                WatchDispatcher → Watchers
```

**Key Guarantees**:

- ✅ Only **committed** writes trigger watch events (no "dirty" reads)
- ✅ Events are delivered **in order** (sequential apply + FIFO broadcast)
- ✅ Events work on **all nodes** (Follower watchers receive events via local apply)
- ✅ **No duplicate events** (each apply_index triggers exactly once)

**What Watch Does NOT Guarantee**:

- ❌ At-least-once delivery (events can be lost if client disconnects)
- ❌ Exactly-once processing (clients must implement idempotency)
- ❌ Persistence (events are in-memory only, not stored)

### Embedded Mode: Engine Crash Behavior

When `EmbeddedEngine` is dropped or crashes:

```rust,ignore
let engine = EmbeddedEngine::start_with("./data", config).await?;
let mut watcher = engine.watch(b"key").await?;

// If engine crashes or is dropped:
// - All active watchers' channels are closed
// - watcher.recv() returns None
// - This is indistinguishable from normal stream completion
```

**Recommended Pattern: Restart on Stream End**

```rust,ignore
loop {
    let engine = EmbeddedEngine::start_with("./data", config).await?;
    let mut watcher = engine.watch(b"key").await?;

    while let Some(event) = watcher.recv().await {
        handle_event(event);
    }

    // Stream ended - check if engine is still alive
    if !engine.is_running() {
        warn!("Engine shut down, exiting watch loop");
        break;
    }

    // Engine restarted unexpectedly, re-register watcher
    warn!("Watch stream ended unexpectedly, re-registering...");
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### Standalone Mode: gRPC Stream Behavior

When the server crashes or connection is lost:

```rust,ignore
let client = Client::builder(vec!["http://node1:9081".to_string()])
    .build()
    .await?;

let mut stream = client.watch(b"key").await?;

// If server crashes or restarts:
// - Stream ends with Status::UNAVAILABLE error
// - Client receives: "Watch stream closed: server may have shut down or restarted"
// - Must reconnect and re-register watcher
```

**Recommended Pattern: Automatic Reconnection with Exponential Backoff**

```rust,ignore
use tokio::time::{sleep, Duration};

async fn watch_with_retry(
    servers: Vec<String>,
    key: &[u8],
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        match establish_watch(&servers, key).await {
            Ok(mut stream) => {
                info!("Watch established for key: {:?}", key);
                backoff = Duration::from_secs(1); // Reset backoff on success

                // Process events
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(event) => {
                            handle_event(event);
                        }
                        Err(e) => {
                            warn!("Watch stream error: {}", e);
                            break; // Exit inner loop, will retry
                        }
                    }
                }

                warn!("Watch stream ended, reconnecting...");
            }
            Err(e) => {
                error!("Failed to establish watch: {}", e);
            }
        }

        // Exponential backoff
        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

async fn establish_watch(
    servers: &[String],
    key: &[u8],
) -> Result<impl Stream<Item = Result<WatchEvent>>> {
    let client = Client::builder(servers.to_vec()).build().await?;
    let stream = client.watch(key).await?;
    Ok(stream)
}
```

### Network Partition Behavior

Watch events respect Raft consensus guarantees:

```text
Network Partition Example:
[Node1, Node2 (minority)] | [Node3, Node4, Node5 (majority)]

1. Node2 has active watcher for "key"
2. Partition occurs
3. Majority elects Node3 as Leader, writes "key=value"
4. Node2 does NOT receive event (log not replicated yet)
5. Partition heals
6. Node2 catches up via Raft log replay
7. Node2's StateMachine applies "key=value"
8. Watcher on Node2 receives event ✅
```

**Key Insight**: Watchers on partitioned nodes will **not** receive "dirty" events during partition. They receive events **only after** Raft log is replicated and applied locally.

### Error Handling Checklist

When implementing watch clients, ensure:

- [ ] Handle `None`/stream end as potential connection loss (not just "no more events")
- [ ] Implement exponential backoff for reconnection (avoid overwhelming server)
- [ ] Log watcher re-registration for debugging
- [ ] Consider circuit breaker pattern for unstable networks
- [ ] Implement idempotent event handlers (events may arrive during recovery)
- [ ] Handle `Status::UNAVAILABLE` errors from gRPC explicitly
- [ ] Set appropriate client timeout values

### Troubleshooting Connection Issues

**Problem**: Watch stream ends immediately

```text
Possible causes:
1. Watch feature not compiled - Check: cargo build --features watch
2. Server restarted - Solution: Implement retry logic above
3. Network timeout - Solution: Increase gRPC keep-alive settings
```

**Problem**: Events arrive out of order

```text
This should NEVER happen. If observed:
1. Check if using multiple clients with different clocks
2. Verify StateMachine apply is sequential
3. File a bug report with logs
```

**Problem**: Missing events after reconnection

```text
Expected behavior: Watch provides at-most-once delivery
Solution: Implement "Read + Watch" pattern:
1. Read current value
2. Start watching
3. Process events from watch stream
4. On reconnect: Repeat from step 1
```

## Example: Read-Watch Pattern

```rust,ignore
// 1. Read current value
let response = client.read(ReadRequest {
    client_id: 1,
    key: b"counter".to_vec(),
}).await?;

let current_value = response.into_inner().value;
println!("Current: {}", current_value);

// 2. Start watching for future changes
let mut stream = client.watch(WatchRequest {
    client_id: 1,
    key: b"counter".to_vec(),
}).await?.into_inner();

// 3. Process future updates
while let Some(event) = stream.next().await {
    println!("Updated: {:?}", event?.value);
}
```

## Troubleshooting

### No Events Received

- Verify key matches exactly (case-sensitive, byte-for-byte)
- Ensure writes are happening after watch registration

### High Lagged Event Rate

- Increase `event_queue_size` for global bursts
- Increase `watcher_buffer_size` for slow consumers
- Reduce number of watchers per key
- Process events faster (async handlers)

### High Memory Usage

- Reduce `event_queue_size`
- Reduce `watcher_buffer_size`
- Limit number of concurrent watchers
- Consider disabling Watch if not needed

## Architecture

```text
┌─────────────┐
│ StateMachine│
│  apply()    │
└──────┬──────┘
       │ broadcast::send (fire-and-forget, <10ns)
       ▼
┌─────────────────────────┐
│ Broadcast Channel       │
│ (tokio::sync::broadcast)│
│ - Capacity: 1000        │
│ - Overwrites on full    │
└──────┬──────────────────┘
       │ subscribe
       ▼
┌─────────────────────────┐
│  WatchDispatcher        │
│  (tokio task)           │
│  - Recv from broadcast  │
│  - Match key in DashMap │
│  - Send to mpsc         │
└──────┬──────────────────┘
       │
       ├─► Per-Watcher mpsc Channel ──► Embedded Client (Rust struct)
       │
       └─► Per-Watcher mpsc Channel ──► gRPC Stream (Protobuf)
```

**Key Design Points**:

- **StateMachine Decoupling**: Broadcast is fire-and-forget, never blocks apply
- **Unified Path**: Embedded and Standalone share same WatchRegistry/Dispatcher
- **Lock-Free**: DashMap for concurrent registration, no global mutex

## See Also

- [Customize State Machine](crate::docs::server_guide::customize_state_machine) - Integrate Watch notifications
- [Performance Optimization](crate::docs::performance::throughput_optimization_guide) - Optimization guidelines
