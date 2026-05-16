# d-engine 🚀

[![Crates.io](https://img.shields.io/crates/v/d-engine.svg)](https://crates.io/crates/d-engine)
[![docs.rs](https://docs.rs/d-engine/badge.svg)](https://docs.rs/d-engine)
[![codecov](https://codecov.io/gh/deventlab/d-engine/graph/badge.svg?token=K3BEDM45V8)](https://codecov.io/gh/deventlab/d-engine)
![Static Badge](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
[![CI](https://github.com/deventlab/d-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/d-engine/actions/workflows/ci.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/deventlab/d-engine)

**d-engine** is a lightweight distributed coordination engine written in Rust,
designed for embedding into applications that need strong consistency—the consensus
layer for building reliable distributed systems.

**Built with a simple vision**: make distributed coordination accessible - cheap
to run, simple to use. **Built on a core philosophy**: choose simple architectures
over complex ones.

d-engine's Raft core uses a single-threaded event loop to guarantee strong consistency
and strict ordering while keeping the codebase clean and performant. Production-ready
Raft implementation with flexible read consistency (Linearizable/Lease-Based/Eventual)
and pluggable storage backends. Start with one node, scale to a cluster when needed.

---

## New in v0.2.4 🎉

- **Async IO Architecture**: Raft event loop is fully non-blocking — WAL writes, state machine apply, and replication all run off the hot path. AppendEntries uses a persistent bidirectional stream per peer; replication is pipelined across followers.
- **Cluster Membership Streaming**: `EmbeddedEngine::watch_membership()` / `GrpcClient::watch_membership()` — subscribe to real-time membership changes in both embedded and standalone modes
- **Simpler Startup**: `EmbeddedEngine::start(data_dir)` and `StandaloneEngine::run(data_dir, shutdown_rx)` — no config file required for common cases
- **Jepsen Validated**: 5 workloads + 6-hour soak test under combined kill/partition/pause faults — see [Correctness Guarantees](https://github.com/deventlab/d-engine-jepsen/blob/main/GUARANTEES.md)

---

## Features

- **Single-Node Start**: Begin with one node, scale to a 3-node cluster with zero downtime
- **EmbeddedEngine**: Zero-overhead in-process access (<0.1ms latency)
- **Strong Consistency**: Full Raft protocol — linearizable writes, configurable read consistency
- **Flexible Read Consistency**: Three-tier model (Linearizable/Lease-Based/Eventual) per request
- **Watch API**: Real-time key change notifications for config updates, service discovery, and more
- **TTL/Lease**: Automatic key expiration for distributed locks and session management
- **Pluggable Storage**: Custom backends supported (RocksDB default; Sled, Raw File, or your own)
- **Modular**: Feature flags (`client`/`server`) — depend only on what you need

---

## Quick Start (Embedded Mode)

```toml
d-engine = "0.2"
```

```rust
use d_engine::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let engine = EmbeddedEngine::start("./data").await.unwrap();
    engine.wait_ready(Duration::from_secs(5)).await.unwrap();

    let client = engine.client();
    client.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
    let value = client.get_linearizable(b"hello".to_vec()).await.unwrap();

    println!("Retrieved: {}", String::from_utf8_lossy(&value.unwrap()));
    engine.stop().await.unwrap();
}
```

**→ Full example:** [examples/quick-start-embedded](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-embedded)

---

## Integration Modes

### Embedded Mode — In-Process

```toml
d-engine = "0.2"
```

**Use when**: Building Rust applications that need distributed coordination  
**Why**: Zero-overhead (<0.1ms), single binary, zero network cost

> **Performance**: AWS EC2 3-node cluster — **64K writes/sec**, **181K linearizable reads/sec**, sub-millisecond latency. See [benches/reports/v0.2.4/](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.4) for details.

**→ Examples:**

- [Quick Start Embedded](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-embedded) — Minimal setup
- [Service Discovery Embedded](https://github.com/deventlab/d-engine/tree/main/examples/service-discovery-embedded) — Watch API

---

### Standalone Mode — Separate Service

```toml
d-engine = { version = "0.2", features = ["client"], default-features = false }
```

**Use when**: Application and d-engine run as separate processes  
**Why**: Language-agnostic (Go/Python/Java/Rust), independent scaling, easier operations

> **Performance**: 36K writes/sec, 51K linearizable reads/sec via gRPC. For maximum throughput, embedded mode is 1.8× faster on writes and 3.5× on reads. See [benches/reports/v0.2.4/](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.4).

**→ Example:** [Quick Start Standalone (Go client)](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-standalone)

---

### Custom Storage Backends

```toml
d-engine = { version = "0.2", features = ["server"], default-features = false }
```

Implement the `StorageEngine` and `StateMachine` traits for custom backends:

- [Storage Engine Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/index.html#implementing-custom-storage-engines)
- [State Machine Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/index.html#implementing-custom-state-machines)
- [Sled Storage Example](https://github.com/deventlab/d-engine/tree/main/examples/sled-cluster)

---

## Performance

### d-engine v0.2.4 vs etcd

![d-engine vs etcd comparison](https://raw.githubusercontent.com/deventlab/d-engine/main/benches/reports/v0.2.4/d-engine_comparison_v0.2.4.png)

### d-engine v0.2.4 vs v0.2.3

![d-engine v0.2.4 vs v0.2.3 comparison](https://raw.githubusercontent.com/deventlab/d-engine/main/benches/reports/v0.2.4/d-engine_v0.2.3_vs_v0.2.4_embedded_mode.png)

```bash
open benches/reports/
```

---

## Maintainer Philosophy

d-engine is maintained by a single author with a clear vision. We value quality over quantity:

- **PRs are not guaranteed to be merged** — even good code may be declined if it conflicts with roadmap priorities
- **Response time varies** — active development takes precedence over PR reviews
- **Breaking changes are OK pre-1.0** — we prioritize getting it right over backward compatibility

---

## Contributing

d-engine follows the 20/80 rule — solve real production problems, not experiments.
Read [CONTRIBUTING.md](https://github.com/deventlab/d-engine/blob/main/CONTRIBUTING.md) and open an issue before feature PRs. Bug fixes are always welcome.

**Prerequisites**: Rust 1.89+, Tokio runtime, Protobuf compiler

```bash
# Run all tests (fast, parallel with nextest)
make test
```

Follow Rust community standards (rustfmt, clippy). Write unit tests for all new features.

---

## FAQ

**Why 3 nodes for HA?**  
Raft requires majority quorum (N/2 + 1). A 3-node cluster tolerates 1 failure.

**Can I start with 1 node?**  
Yes. Scale to 3 nodes later with zero downtime (see `examples/single-node-expansion/`).

**How do I customize storage?**  
Implement the `StorageEngine` and `StateMachine` traits (see Custom Storage Backends above).

**Production-ready?**  
Core Raft engine is production-grade (1000+ tests, [Jepsen validated](https://github.com/deventlab/d-engine-jepsen/blob/main/GUARANTEES.md)). API is stabilizing toward v1.0. Pre-1.0 versions may introduce breaking changes (documented in [MIGRATION_GUIDE.md](https://github.com/deventlab/d-engine/blob/main/MIGRATION_GUIDE.md)).

---

## Supported Platforms

- Linux: x86_64, aarch64
- macOS: x86_64, aarch64

## License

Licensed under [MIT](https://en.wikipedia.org/wiki/MIT_License#License_terms) or [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0), at your option.
