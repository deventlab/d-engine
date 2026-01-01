# Service Discovery - Embedded Mode

Demonstrates **embedded** service discovery — each application node embeds d-engine directly.

## Architecture (Consul-style)

```
┌─────────────────────────────────────────────────────────┐
│  Application Instance A (Leader)                        │
│  ┌─────────────────────────────────────────────────┐    │
│  │  d-engine (embedded)                            │    │
│  │  ┌───────────────┐  ┌────────────────────────┐  │    │
│  │  │ LocalKvClient │  │ Watch (in-process)     │  │    │
│  │  │ (<0.1ms read) │  │ (μs latency events)    │  │    │
│  │  └───────────────┘  └────────────────────────┘  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
          │ Raft Replication
          ▼
┌─────────────────────────────────────────────────────────┐
│  Application Instance B (Follower)                      │
│  ┌─────────────────────────────────────────────────┐    │
│  │  d-engine (embedded) - same structure           │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Key Differences from Standalone

| Aspect        | Standalone (gRPC) | Embedded        |
| ------------- | ----------------- | --------------- |
| Read latency  | 1-5ms             | <0.1ms          |
| Watch latency | ~1ms              | ~μs             |
| Deployment    | Separate cluster  | App = Raft node |
| Complexity    | Simpler           | More complex    |

## Quick Start

```bash
cargo run --bin server
```

The demo will:

1. Start embedded d-engine in single node
2. Register a service endpoint
3. Show how to read with LocalKvClient
4. Demonstrate in-process watch notifications

## When to Use Embedded Mode

- **Ultra-low latency required** — DNS/service discovery needs <1ms response
- **Each app instance is a Raft participant** — Similar to Consul architecture
- **Rust-only** — LocalKvClient is Rust-native, no gRPC serialization

## Related

- [Standalone Example](../service-discovery-standalone/) — gRPC client mode
