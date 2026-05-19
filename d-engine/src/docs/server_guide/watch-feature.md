# Watch Feature

The Watch feature provides real-time notifications when keys change in d-engine, enabling event-driven architectures without polling.

## Quick Start

### Enable Watch

Edit `config/base/raft.toml`:

```toml
[raft.watch]
event_queue_size = 10240      # Global broadcast channel buffer
watcher_buffer_size = 256     # Per-watcher buffer (raised from 10 in v0.2.4)
enable_metrics = false        # Detailed logging (default: false)
max_watcher_count = 10000     # Hard cap on total active watchers (optional)
```

> **Note**: Watch feature is controlled by the `watch` feature flag in `Cargo.toml` at compile time. If compiled with the feature, it's always enabled.

### Client Usage

**Key Insight**: Watch works on **any node** (Leader or Follower) because events trigger after Raft consensus → `StateMachine.apply()`.

```rust,ignore
use d_engine::{Client, ClientApi};
use futures::StreamExt;

// Multi-node connection (recommended for high availability)
let client = Client::builder(vec![
    "http://node1:9081".to_string(),
    "http://node2:9082".to_string(),
    "http://node3:9083".to_string(),
])
.build()
.await?;

// Exact-key watch — reacts to changes on a single key
let mut stream = client.watch("/config/timeout").await?;

while let Some(event) = stream.next().await {
    match event {
        Ok(event) => {
            println!("revision={} type={:?} key={:?} value={:?}",
                event.revision, event.event_type, event.key, event.value);
        }
        Err(e) => eprintln!("Watch error: {:?}", e),
    }
}

// Prefix watch — reacts to any key under a namespace
// prefix must start and end with '/'
let mut stream = client.watch_prefix("/services/payment/").await?;

while let Some(event) = stream.next().await {
    // event.key is the specific child key that changed, e.g. "/services/payment/node1"
    // event.revision is the Raft applied index — use it to detect gaps on reconnect
}
```

### Embedded Usage

For in-process usage (e.g., `EmbeddedEngine`), use the client's `watch()` method:

```rust,ignore
use d_engine::EmbeddedEngine;

// Initialize engine with config enabling watch
let engine = EmbeddedEngine::start_with("d-engine.toml").await?;

// Watch via client (unified API)
let mut watcher = engine.client().watch("my_key")?;

// Spawn a task to handle events
tokio::spawn(async move {
    while let Some(event) = watcher.receiver_mut().recv().await {
        println!("Event: {:?}", event);
    }
});

// Or use into_receiver() for long-lived streams (disables auto-cleanup)
let (id, key, receiver) = watcher.into_receiver();

// Prefix watch (embedded) — same API as exact-key, but registers on a namespace
let prefix_watcher = engine.client().watch_prefix(b"/services/payment/")?;
let (_, _, mut prefix_rx) = prefix_watcher.into_receiver();
tokio::spawn(async move {
    while let Some(event) = prefix_rx.recv().await {
        // event.key is the specific child key that changed
        // event.revision is the Raft applied index
        println!("child key changed: {:?} revision={}", event.key, event.revision);
    }
});
```

## Prefix Watch

Prefix watch fires for every key whose path begins with a given prefix.
The prefix must start and end with `/`, e.g. `/services/payment/`.

### Slash-boundary semantics

```text
prefix = "/services/payment/"

FIRES for:
  /services/payment/node1      ✅ child key
  /services/payment/node2      ✅ child key
  /services/payment/node1/sub  ✅ deeper child

DOES NOT fire for:
  /services/payment            ❌ missing trailing slash - exact key, not a child
  /services/                   ❌ different prefix level
  /services/paymentextra/node  ❌ different namespace
```

### Reconnection pattern (zero race window)

On disconnect or `CANCELED` (buffer overflow), use **watch-first, scan-second** to eliminate the race window:

```rust,ignore
loop {
    // Step 1: register watch FIRST — server buffers events from this moment
    let mut watcher = client.watch_prefix("/services/payment/").await?;

    // Step 2: linearizable scan — any write before/during scan is in the snapshot
    let snapshot = client.scan_prefix("/services/payment/").await?;
    let scan_revision = snapshot.revision;
    let mut registry: HashMap<Bytes, Bytes> = snapshot.entries.into_iter().collect();

    // Step 3: drain watch buffer, skip events already captured by the scan
    while let Some(event) = watcher.next().await {
        match event {
            Ok(e) if e.revision <= scan_revision => continue, // already in snapshot
            Ok(e) if e.event_type == WatchEventType::Canceled => break,
            Ok(e) => apply_to_registry(e, &mut registry),
            Err(_) => break,
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}
```

**Why this is race-free:** any write before the watch registered appears in the scan; any write after watch registered (even during the scan) is buffered. The two sets are non-overlapping when filtered by `revision ≤ scan_revision`.

### Revision field

Every `WatchEvent` carries `revision: u64` — the Raft applied index when the write was committed.
Use it to detect gaps after reconnection:

```rust,ignore
let last_revision = event.revision;
// After reconnect: if new events arrive with revision > last_revision + 1,
// events were dropped — trigger a full re-sync.
```

## Configuration

### `event_queue_size`

- **Type**: usize
- **Default**: `10240`
- **Description**: Global broadcast channel buffer size. When full, oldest events are dropped (lagging receivers).
- **Memory**: ~24 bytes per slot (10240 slots ≈ 246KB)
- **Tuning**:
  - Low traffic (< 1K writes/sec): 1000-5000
  - Medium traffic (1K-10K writes/sec): 5000-20000
  - High traffic (> 10K writes/sec): 20000-50000

### `watcher_buffer_size`

- **Type**: usize
- **Default**: `256` (raised from 10 in v0.2.4 — reduces spurious CANCELED events under burst load)
- **Description**: Per-watcher channel buffer. Each watcher gets its own mpsc channel.
- **Memory**: ~240 bytes per slot per watcher (256 slots × 100 watchers ≈ 6MB)
- **Tuning**:
  - Fast consumers (< 1ms): 64-256
  - Normal consumers (1-10ms): 256-512
  - Slow consumers (> 10ms): 512-1024

### `max_watcher_count`

- **Type**: usize
- **Default**: `i64::MAX` (effectively unlimited)
- **Description**: Hard cap on total active watchers (exact + prefix combined). `register()` and `watch_prefix()` return an error once this limit is reached. Set to a finite value to prevent runaway watcher growth in multi-tenant deployments.

### `heartbeat_interval_ms`

- **Type**: u64
- **Default**: `0` (disabled)
- **Description**: If non-zero, the `WatchDispatcher` sends a `Progress` heartbeat event to all active watchers at this interval (±10% jitter). Useful for detecting a silent stream that has stalled due to no writes. Set to `0` to rely on application-level keepalives instead.
- **Recommended**: `5000` (5 seconds) for long-lived connections; `0` for write-heavy workloads where events are frequent.

### `enable_metrics`

- **Type**: boolean
- **Default**: `false`
- **Description**: Enable detailed metrics and warnings for lagging receivers. Adds ~0.001% overhead.

## Behavior

### Event Delivery

- **Guarantee**: At-most-once delivery
- **Order**: FIFO per key (events from StateMachine apply order)
- **Dropped Events**: When a per-watcher buffer overflows, the watcher receives a `CANCELED` sentinel event and is forcibly unregistered
- **Lagging**: Broadcast channel drops oldest events when receivers are slow (global buffer)
- **CANCELED Event**: `WatchEventType::Canceled` with `ErrorCode::WATCH_BUFFER_OVERFLOW` signals that the watcher was forcibly terminated. Client must re-sync via Read API and re-register.

### Lifecycle

- **Registration**: Watch starts receiving events immediately after registration
- **Cleanup**: Watcher is automatically unregistered when handle is dropped
- **No History**: Watch only receives future events (not historical changes)

### prev_kv (Previous Value)

When registering a watcher with `prev_kv: true` (embedded API) or `prev_kv: true` in the gRPC `WatchRequest`, each `Put` and `Delete` event carries the **value the key held before the write** in `event.prev_value`. Watchers registered with `prev_kv: false` (the default) always receive an empty `prev_value`.

```rust,ignore
// Embedded: request previous value on every event
let watcher = engine.client().watch_with_prev_kv(b"my_key")?;
while let Some(event) = watcher.receiver_mut().recv().await {
    if event.event_type == WatchEventType::Put {
        println!("was: {:?}, now: {:?}", event.prev_value, event.value);
    }
}
```

**Cost**: When at least one `prev_kv` watcher is active, the state machine reads the old value from storage before each `apply_chunk`. This is a single point-in-time read per write batch — not per watcher — so the overhead scales with write rate, not watcher count.

### Progress Heartbeat

When `heartbeat_interval_ms > 0` in config, the dispatcher periodically sends a `WatchEventType::Progress` event to every active watcher. The event carries the current `revision` (Raft applied index) but empty `key` and `value`. Use it to:

- Detect a silent stream that is alive but has no writes
- Confirm your client's channel is still open before a long idle period
- Drive time-based state machine logic without polling

```rust,ignore
match event.event_type {
    WatchEventType::Put    => handle_put(&event),
    WatchEventType::Delete => handle_delete(&event),
    WatchEventType::Canceled => { re_register(); break; }
    WatchEventType::Progress => {
        // Stream is alive; revision is current applied index
        println!("heartbeat revision={}", event.revision);
    }
}
```

**Jitter**: The interval has ±10% random jitter to avoid thundering-herd when many watchers share the same interval.

### Limitations

- **No Persistence**: Watchers lost on server restart — re-register on reconnect
- **No Replay**: Watch delivers future events only; use the Read API to re-sync current state after reconnect
- **Prefix format**: Prefix must start and end with `/` — arbitrary byte-range watch is not supported

## Performance

### Write Path Overhead

- **When Enabled**: < 0.01% overhead (broadcast is fire-and-forget)
- **When Disabled**: Zero overhead (feature-gated)

### Latency

- **End-to-End**: ~100µs typical (StateMachine apply → watcher receive)
- **Throughput**: Linear scaling up to 10K+ watchers

### Memory Usage

```text
Base:    246KB (broadcast channel at default 10240)
Watchers: 240 bytes × (buffer_size + 1) × watcher_count
Example:  100 watchers × 11 slots = 264KB
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

### `WATCH_BUFFER_OVERFLOW` (ErrorCode 5001)

- **Cause**: Per-watcher channel full — the client is consuming events too slowly
- **Detection**: Receive an event where `event_type == WatchEventType::Canceled` and `error == ErrorCode::WATCH_BUFFER_OVERFLOW`
- **Effect**: The watcher is forcibly unregistered server-side; no further events will be delivered on this stream
- **Solution**:
  1. Re-sync state via the Read API to get the current value
  2. Re-register the watch to receive future changes
  3. Increase `watcher_buffer_size` to give slow consumers more headroom

## Best Practices

1. **Start Watch Before Write**: To avoid missing events, start watching before performing writes
2. **Read + Watch Pattern**: Read current value first, then watch for future changes
3. **Idempotent Handlers**: Handle duplicate events gracefully (at-most-once delivery)
4. **Buffer Tuning**: Monitor lagged events and adjust buffer sizes accordingly
5. **Reconnect Logic**: Implement automatic reconnection on stream errors
6. **Handle CANCELED**: Always check for `WatchEventType::Canceled` — treat it as a forced disconnect. Re-sync via Read API then re-register the watch before processing further events.
7. **Handle Progress**: If `heartbeat_interval_ms > 0`, your event handler must have an arm for `WatchEventType::Progress` (non-exhaustive match will fail to compile otherwise). Silently ignore it or use the `revision` field for liveness tracking.

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
let engine = EmbeddedEngine::start_with(config).await?;
let mut watcher = engine.client().watch(b"key")?;

// If engine crashes or is dropped:
// - All active watchers' channels are closed
// - watcher.recv() returns None
// - This is indistinguishable from normal stream completion
```

**Recommended Pattern: Restart on Stream End**

```rust,ignore
loop {
    let engine = EmbeddedEngine::start_with(config).await?;
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
│ StateMachine│  revision = Raft applied index
│  apply()    │
└──────┬──────┘
       │ broadcast::send (fire-and-forget, <10ns)
       ▼
┌─────────────────────────┐
│ Broadcast Channel       │
│ (tokio::sync::broadcast)│
│ - Capacity: 10240       │
│ - Overwrites on full    │
└──────┬──────────────────┘
       │ subscribe
       ▼
┌──────────────────────────────────────────────┐
│  WatchDispatcher  (single tokio task)        │
│                                              │
│  1. exact  DashMap<key, Vec<Watcher>>        │
│     O(1) lookup per event                   │
│                                              │
│  2. prefix DashMap<prefix, Vec<Watcher>>     │
│     prefix_segments(key) → O(depth) lookups │
│     depth = number of '/' in key            │
└──────┬───────────────────────────────────────┘
       │
       ├─► Per-Watcher mpsc Channel ──► Embedded Client (Rust struct)
       │
       └─► Per-Watcher mpsc Channel ──► gRPC Stream (Protobuf)
```

**Key Design Points**:

- **StateMachine Decoupling**: Broadcast is fire-and-forget, never blocks apply
- **Two DashMaps**: Exact and prefix watchers are stored separately for O(1) exact lookup and O(depth) prefix dispatch — no linear scan over all registered prefixes
- **Unified Path**: Embedded and Standalone share the same WatchRegistry/Dispatcher
- **Lock-Free**: DashMap sharding, no global mutex

## See Also

- [Customize State Machine](crate::docs::server_guide::customize_state_machine) - Integrate Watch notifications
- [Performance Optimization](crate::docs::performance::throughput_optimization_guide) - Optimization guidelines
