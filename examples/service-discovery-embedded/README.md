# Service Discovery - Embedded Mode

Demonstrates d-engine's **Watch API in embedded mode** — ultra-low latency change notifications through in-process communication.

**What you'll learn:**

- Using Watch API with EmbeddedClient (no gRPC overhead)
- Sub-millisecond read and watch latency
- Embedded d-engine architecture patterns

## Architecture (Consul-style)

```
┌─────────────────────────────────────────────────────────┐
│  Application Instance A (Leader)                        │
│  ┌─────────────────────────────────────────────────┐    │
│  │  d-engine (embedded)                            │    │
│  │  ┌───────────────┐  ┌────────────────────────┐  │    │
│  │  │EmbeddedClient │  │ Watch (in-process)     │  │    │
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
3. Show how to read with EmbeddedClient
4. Demonstrate in-process watch notifications

## Key Concepts Demonstrated

### Primary: Embedded Watch API

- **EmbeddedClient** — In-process KV operations with <0.1ms latency
- **Watch notifications** — Microsecond-level event delivery
- **Zero serialization** — Direct memory access, no gRPC overhead

### Secondary: Embedded Architecture

- Each app instance embeds d-engine (similar to Consul)
- Raft replication between embedded nodes
- Service discovery as a practical use case

## When to Use Embedded Mode

- **Ultra-low latency required** — Watch events need microsecond response
- **Rust-only applications** — EmbeddedClient is Rust-native
- **Co-located architecture** — App and storage in same process

## Related

- [Standalone Example](../service-discovery-standalone/) — gRPC client mode
