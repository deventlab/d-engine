# Configuration Reference

> **Note**: The `config/base/` and `config/overlays/` directories in this repo exist
> for d-engine's own internal demo purpose only. They have no practical
> use for application developers. In your own project, you only need a single
> `d-engine.toml` with the settings you want to override.

This directory contains annotated reference files — one per configuration section.
They document every available option so you know what can be tuned.

## In your application

Write a minimal `d-engine.toml` with only the fields you need — all omitted fields
use built-in defaults.

**Single node:**

```toml
[cluster]
db_root_dir = "./data"
```

**3-node cluster (one file per node):**

```toml
[cluster]
node_id = 1
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 1, status = 3 },
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 3 },
    { id = 3, address = "0.0.0.0:9083", role = 1, status = 3 },
]
db_root_dir = "./db/1"
```

See [examples/](https://github.com/deventlab/d-engine/tree/main/examples) for complete
working configurations.

For all available options with descriptions and defaults, see the
[`d_engine_core::config`](https://docs.rs/d-engine-core/latest/d_engine_core/config/)
API documentation.

## Reference files

| File              | What it covers                                     |
| ----------------- | -------------------------------------------------- |
| `cluster.toml`    | Node identity, peer addresses, data directory      |
| `raft.toml`       | Batching, read consistency, snapshots, replication |
| `network.toml`    | gRPC timeouts, connection windows, TCP tuning      |
| `retry.toml`      | Retry policies for RPC operations                  |
| `tls.toml`        | TLS certificates and mutual auth                   |
| `monitoring.toml` | Prometheus metrics port                            |
| `storage.toml`    | Storage backend options (unified DB mode)          |
