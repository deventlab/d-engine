# Service Discovery — Embedded Mode

Demonstrates d-engine's **Watch API in embedded mode** through two real-world patterns:
in-process exact-key config watch and a prefix-based live load-balancer registry.

**What you'll learn:**

- Demo 1 — Exact-key watch: sidecar-style config reload with zero network overhead
- Demo 2 — Prefix watch: API gateway routing table that updates in real time as nodes join and leave
- `EmbeddedEngine` lifecycle and `wait_ready()`
- Shared state between a background watch task and the main thread (`Arc<Mutex<HashMap>>`)

## Architecture

```text
┌──────────────────────────────────────────────────────────┐
│  Application Process                                     │
│                                                          │
│  ┌─────────────────────────────────────────────────┐     │
│  │  EmbeddedEngine (single-node for demo)          │     │
│  │  ┌─────────────┐   ┌───────────────────────┐   │     │
│  │  │EmbeddedClient│  │   WatchRegistry        │   │     │
│  │  │  put/delete  │  │  exact  DashMap  O(1)  │   │     │
│  │  │  <0.1ms      │  │  prefix DashMap  O(d)  │   │     │
│  │  └─────────────┘   └──────────┬────────────┘   │     │
│  └───────────────────────────────│─────────────────┘     │
│                                  │ mpsc (μs latency)     │
│         ┌────────────────────────┴────────────────┐      │
│         │                                          │      │
│  ┌──────▼──────┐                    ┌─────────────▼──┐   │
│  │ exact-watch │                    │  prefix-watch  │   │
│  │ tokio task  │                    │  tokio task    │   │
│  │             │                    │                │   │
│  │ /config/    │                    │ Arc<Mutex<     │   │
│  │  timeout    │                    │   HashMap>>    │   │
│  └─────────────┘                    └────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
cargo run --bin server
```

No cluster setup needed — the demo starts a single-node embedded engine automatically.

## What the Demo Does

### Demo 1 — Exact-key watch (config reload pattern)

A background task watches `/config/payment-service/timeout`.
The main thread writes two updates; the task prints each change as it arrives.

```text
[exact-watch] config changed: /config/payment-service/timeout = 30s  (revision=1)
[exact-watch] config changed: /config/payment-service/timeout = 60s  (revision=2)
```

**Real-world use case**: a sidecar process that watches its own config key and reloads
settings without a restart — no polling, no config file reload loop.

### Demo 2 — Prefix watch (live load-balancer registry)

A background task watches `/services/payment/` and maintains a `HashMap<key, endpoint>`.
The main thread simulates three nodes registering, one updating its endpoint (rolling
restart), and one deregistering (crash). The routing table is printed after every event.

```text
[prefix-watch] node up:   /services/payment/node1 → 10.0.0.1:8080  (revision=3)
  routing table (1 nodes):
    /services/payment/node1 → 10.0.0.1:8080
...
[prefix-watch] node down: /services/payment/node1  (revision=6)
  routing table (2 nodes):
    /services/payment/node2 → 10.0.0.2:9090
    /services/payment/node3 → 10.0.0.3:8080

Final routing table (2 nodes):
    /services/payment/node2 → 10.0.0.2:9090
    /services/payment/node3 → 10.0.0.3:8080
```

**Real-world use case**: an API gateway keeps a live routing table for a service namespace.
One prefix watcher replaces N per-node exact-key watchers — no matter how many nodes join
or leave, the cost stays constant.

## Key Concepts

| Concept | Exact-key | Prefix |
|---|---|---|
| API | `engine.client().watch(key)?` | `engine.client().watch_prefix(prefix)?` |
| Fires for | One specific key | All keys under the prefix |
| Prefix constraint | — | Must start and end with `/` |
| Typical use | Config reload, distributed lock | Load balancer, DNS-like registry |

### `into_receiver()` vs `receiver_mut()`

```rust,ignore
// Short-lived: auto-cleanup when handle is dropped
let mut watcher = engine.client().watch(key)?;
watcher.receiver_mut().recv().await;

// Long-lived background task: disable auto-cleanup, own the channel
let (id, key, mut rx) = watcher.into_receiver();
tokio::spawn(async move { while let Some(e) = rx.recv().await { ... } });
```

### `CANCELED` event

If the per-watcher buffer overflows, d-engine sends a `CANCELED` sentinel.
In production, respond by clearing the local registry, re-scanning with `scan_prefix`
(#378), and re-watching.

## Key Differences from Standalone

| Aspect        | Standalone (gRPC) | Embedded        |
| ------------- | ----------------- | --------------- |
| Watch latency | ~1ms              | ~μs             |
| Serialization | Protobuf          | None (direct struct) |
| Deployment    | Separate cluster  | App = Raft node |
| Suitable for  | Any language      | Rust apps only  |

## When to Use Embedded Mode

- **Ultra-low latency required** — Watch events need microsecond response
- **Rust-only applications** — EmbeddedClient is Rust-native
- **Co-located architecture** — App and storage in same process

## Related

- [Standalone Example](../service-discovery-standalone/) — gRPC client with `--prefix` flag
- [Watch Feature Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/watch_feature/index.html)
