# Watch Feature

The Watch feature provides real-time notifications when keys change in d-engine, enabling event-driven architectures without polling.

## Quick Start

### Enable Watch

Edit `config/base/raft.toml`:

```toml
[raft.watch]
enabled = true                # Enable Watch feature (default: true)
event_queue_size = 1000       # Global event buffer
watcher_buffer_size = 10      # Per-watcher buffer
enable_metrics = false        # Detailed logging (default: false)
```

### Client Usage

```rust,ignore
use d_engine_client::Client;
use d_engine_client::protocol::WatchRequest;
use futures::StreamExt;

// Connect to server
let client = Client::builder()
    .endpoints(vec!["http://127.0.0.1:9081".to_string()])
    .build()
    .await?;

// Start watching a key
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
let engine = EmbeddedEngine::with_rocksdb("./data", Some("d-engine.toml"))?;

// Start watching
let mut watcher = engine.watch("my_key").await?;

// Spawn a task to handle events
tokio::spawn(async move {
    while let Some(result) = watcher.recv().await {
        match result {
            Ok(event) => println!("Event: {:?}", event),
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
});
```

## Configuration

### `enabled`

- **Type**: boolean
- **Default**: `true`
- **Description**: Master switch for Watch feature. When `false`, all Watch RPCs return `UNAVAILABLE` with zero overhead.

### `event_queue_size`

- **Type**: usize
- **Default**: `1000`
- **Description**: Global event queue buffer size. Larger values reduce dropped events under burst load.
- **Memory**: ~24 bytes per slot (1000 slots ≈ 24KB)
- **Tuning**:
  - Low traffic (< 1K writes/sec): 500-1000
  - Medium traffic (1K-10K writes/sec): 1000-2000
  - High traffic (> 10K writes/sec): 2000-5000

### `watcher_buffer_size`

- **Type**: usize
- **Default**: `10`
- **Description**: Per-watcher channel buffer. Each watcher gets its own buffer.
- **Memory**: ~240 bytes per slot per watcher (10 slots × 100 watchers ≈ 240KB)
- **Tuning**:
  - Fast consumers (< 1ms): 5-10
  - Normal consumers (1-10ms): 10-20
  - Slow consumers (> 10ms): 20-50

### `enable_metrics`

- **Type**: boolean
- **Default**: `false`
- **Description**: Enable detailed metrics and warnings for dropped events. Adds ~0.001% overhead.

## Behavior

### Event Delivery

- **Guarantee**: At-most-once delivery
- **Order**: FIFO per key
- **Dropped Events**: When buffers are full, events are dropped (non-blocking design)

### Lifecycle

- **Registration**: Watch starts receiving events immediately after registration
- **Cleanup**: Watcher is automatically unregistered when client disconnects
- **No History**: Watch only receives future events (not historical changes)

### Limitations (v1)

- **Exact Key Match Only**: Prefix/range watch not supported
- **No Persistence**: Watchers lost on server restart
- **No Replay**: Cannot receive historical events

## Performance

### Write Path Overhead

- **When Enabled**: < 0.01% overhead (lock-free `try_send`)
- **When Disabled**: Zero overhead (single Option check)

### Latency

- **End-to-End**: ~19µs (notify → receive)
- **Throughput**: Linear scaling up to 10K+ watchers

### Memory Usage

```text
Base:    24KB (event queue at default 1000)
Watchers: 240 bytes × buffer_size × watcher_count
Example:  100 watchers × 10 buffer = 240KB
```

## Disabling Watch

To completely disable Watch and reclaim resources:

```toml
[raft.watch]
enabled = false
```

**Effect**:

- WatchManager is not created
- All Watch RPC calls return `Status::UNAVAILABLE`
- Zero memory and CPU overhead

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

- **Cause**: Watch is disabled (`enabled = false`)
- **Solution**: Set `enabled = true` in config and restart

### Stream Ends

- **Cause**: Server shutdown or network error
- **Solution**: Reconnect and re-register watch

### Dropped Events

- **Cause**: Buffers full (slow consumer)
- **Solution**: Increase `watcher_buffer_size` or process events faster
- **Detection**: Enable `enable_metrics = true` to see warnings

## Best Practices

1. **Start Watch Before Write**: To avoid missing events, start watching before performing writes
2. **Read + Watch Pattern**: Read current value first, then watch for future changes
3. **Idempotent Handlers**: Handle duplicate events gracefully (at-most-once delivery)
4. **Buffer Tuning**: Monitor dropped events and adjust buffer sizes accordingly
5. **Reconnect Logic**: Implement automatic reconnection on stream errors

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

- Check if Watch is enabled (`enabled = true`)
- Verify key matches exactly (case-sensitive, byte-for-byte)
- Ensure writes are happening after watch registration

### High Dropped Event Rate

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
│   Client    │
│  (gRPC)     │
└──────┬──────┘
       │ Watch RPC
       ▼
┌─────────────────────────┐
│  WatchManager           │
│  ┌──────────────────┐   │
│  │ Global Queue     │◄──┼─── notify_put/delete (from StateMachine)
│  │ (lock-free)      │   │
│  └────────┬─────────┘   │
│           │              │
│      ┌────▼──────┐       │
│      │Dispatcher │       │
│      │ (async)   │       │
│      └────┬──────┘       │
│           │              │
│      ┌────▼──────────┐   │
│      │Per-Watcher Ch │   │
│      │(tokio::mpsc)  │   │
│      └────┬──────────┘   │
└───────────┼──────────────┘
            │
            ▼
       gRPC Stream
```

## See Also

- [Customize State Machine](customize-state-machine.md) - Integrate Watch notifications
- [Configuration Reference](../configuration.md) - Full config options
- [Performance Tuning](../performance.md) - Optimization guidelines
