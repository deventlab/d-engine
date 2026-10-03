# d-engine 🚀

[![Crates.io](https://img.shields.io/crates/v/d-engine.svg)](https://crates.io/crates/d-engine)
[![docs.rs](https://docs.rs/d-engine/badge.svg)](https://docs.rs/d-engine)
[![codecov](https://codecov.io/gh/deventlab/d-engine/graph/badge.svg?token=K3BEDM45V8)](https://codecov.io/gh/deventlab/d-engine)
![Static Badge](https://img.shields.io/badge/license-MIT%20%7C%20Apache--2.0-blue)
[![CI](https://github.com/deventlab/d-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/deventlab/d-engine/actions/workflows/ci.yml)
[![DeepWiki](https://img.shields.io/badge/DeepWiki-d--engine-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/deventlab/d-engine)

**d-engine** is a lightweight, embeddable distributed coordination engine for Rust — strong-consistency KV, watch streams, and leader election, running inside your process.

**Built with a simple vision**: make distributed coordination accessible - cheap
to run, simple to use. **Built on a core philosophy**: choose simple architectures
over complex ones.

d-engine's Raft core uses a single-threaded event loop to guarantee strong consistency
and strict ordering while keeping the codebase clean and performant. Production-ready
Raft implementation with flexible read consistency (Linearizable/Lease-Based/Eventual)
and pluggable storage backends. Start with one node, scale to a cluster when needed.

---

![d-engine 3-node failover demo](./assets/demo.gif)

**→ Full example:** [examples/three-nodes-embedded](https://github.com/deventlab/d-engine/tree/main/examples/three-nodes-embedded)

---

## New in v0.2.4 🎉

- **Async IO Architecture**: Inbound event loop is fully non-blocking — WAL writes, state machine apply, and replication all run off the hot path. AppendEntries uses a persistent bidirectional stream per peer; replication is pipelined across followers.
- **Cluster Membership Streaming**: `EmbeddedEngine::watch_membership()` / `GrpcClient::watch_membership()` — subscribe to real-time membership changes in both embedded and standalone modes
- **Simpler Startup**: `EmbeddedEngine::start(data_dir)` and `StandaloneEngine::run(data_dir, shutdown_rx)` — no config file required for common cases
- **Jepsen Validated**: 5 workloads + 6-hour soak test under combined kill/partition/pause faults — see [Correctness Guarantees](https://github.com/deventlab/d-engine-jepsen/blob/main/GUARANTEES.md)

---

## Features

- **Single-Node Start**: Begin with one node, scale to a 3-node cluster with zero downtime
- **EmbeddedEngine**: Zero-overhead in-process access (<0.1ms latency)
- **Strong Consistency**: Full Raft protocol — linearizable writes, configurable read consistency
- **Flexible Read Consistency**: Three-tier model (Linearizable/Lease-Based/Eventual) per request
- **Watch API**: Real-time key change notifications for configuration updates, state changes, and cluster events
- **TTL**: Automatic key expiration for session management and short-lived entries
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
    let engine = DefaultEmbeddedEngine::start("./data").await.unwrap();
    engine.wait_ready(Duration::from_secs(5)).await.unwrap();

    let client = engine.client();
    client.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
    let value = client.get_linearizable(b"hello".to_vec()).await.unwrap();

    println!("Retrieved: {}", String::from_utf8_lossy(&value.unwrap()));
    engine.stop().await.unwrap();
}
```

**→ Full example:** [examples/quick-start-embedded](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-embedded)

**→ New to d-engine?** [5-minute guide](https://docs.rs/d-engine/latest/d_engine/#-new-to-d-engine-start-here)

---

## Integration Modes

### Embedded Mode — In-Process

```toml
d-engine = "0.2"
```

**Use when**: Building Rust applications that need distributed coordination  
**Why**: Zero-overhead (<0.1ms), single binary, zero network cost

> **Performance**: AWS EC2 3-node cluster — **110K writes/sec**, **327K linearizable reads/sec**, sub-millisecond latency. See [benches/reports/v0.2.4/](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.4) for details.

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

> **Performance**: 59K writes/sec, 77K linearizable reads/sec via gRPC. For maximum throughput, embedded mode is 1.9× faster on writes and 4.2× on reads. See [benches/reports/v0.2.4/](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.4).

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

### d-engine v0.2.5 vs etcd

![d-engine vs etcd comparison](https://raw.githubusercontent.com/deventlab/d-engine/main/benches/reports/v0.2.5/d-engine_comparison_v0.2.5.png)

### d-engine v0.2.5 vs v0.2.4

![d-engine v0.2.5 vs v0.2.4 comparison](https://raw.githubusercontent.com/deventlab/d-engine/main/benches/reports/v0.2.5/d-engine_v0.2.5_vs_v0.2.4_embedded_mode.png)

```bash
open benches/reports/
```

---

## Maintainer Philosophy

d-engine has a focused roadmap maintained by a single author.
We welcome bug fixes unconditionally. For feature PRs, please open
an issue first — new features are evaluated against roadmap fit,
not just code quality. Breaking changes before v1.0 are documented
in MIGRATION_GUIDE.md.

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

**Migrating from etcd?**  
d-engine is not a drop-in replacement. See [API compatibility and migration gaps](https://docs.rs/d-engine/latest/d_engine/docs/use_cases/index.html#migrating-from-etcd) before porting — lease keepalive, multi-key transactions, and auth are not supported; built-in distributed lock requires DIY via CAS.

---

## Supported Platforms

- Linux: x86_64, aarch64
- macOS: x86_64, aarch64

## License

Licensed under [MIT](https://en.wikipedia.org/wiki/MIT_License#License_terms) or [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0), at your option.
