# d-engine-proto

[![Crates.io](https://img.shields.io/crates/v/d-engine-proto.svg)](https://crates.io/crates/d-engine-proto)
[![docs.rs](https://docs.rs/d-engine-proto/badge.svg)](https://docs.rs/d-engine-proto)

gRPC protocol definitions for d-engine - for building non-Rust clients

---

## What is this?

This crate contains the Protocol Buffer definitions and generated Rust code for d-engine's gRPC API. It's the foundation for building clients in **any programming language** that supports gRPC.

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for embedding into applications that need strong consistency—the consensus layer for building reliable distributed systems.

---

## When to use this crate

- ✅ Building **Python/Go/Java/C#** clients for d-engine clusters
- ✅ Need raw `.proto` files for code generation
- ✅ Contributing to d-engine protocol development
- ✅ Implementing custom protocol extensions

---

## When NOT to use this crate

- ❌ **Writing Rust applications** → Use [`d-engine`](https://crates.io/crates/d-engine) or [`d-engine-client`](https://crates.io/crates/d-engine-client) for higher-level APIs
- ❌ **Need a working client immediately** → See language-specific client libraries (coming soon)

---

## Quick Start

### For Non-Rust Developers

The `.proto` files are included in the source repository. Generate client code for your language:

```bash
# Python example
protoc --python_out=. --grpc_python_out=. \
  proto/d_engine/common.proto \
  proto/d_engine/client.proto

# Go example
protoc --go_out=. --go-grpc_out=. \
  proto/d_engine/common.proto \
  proto/d_engine/client.proto

# Java example
protoc --java_out=. --grpc-java_out=. \
  proto/d_engine/common.proto \
  proto/d_engine/client.proto
```

### For Rust Developers

```toml
# Don't use this directly - use the high-level crates instead
[dependencies]
d-engine = { version = "0.2", features = ["client"] }
```

---

## Protocol Structure

This crate provides protobuf-generated types organized by service area:

- **`common`** - Core Raft types (LogId, Entry, etc.)
- **`error`** - Error types and metadata
- **`client`** - Client request/response types
- **`server`** - Server-to-server communication types

---

## Documentation

For language-specific integration guides:
- [Go Client Guide](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/go-client.md)
- [Error Handling](https://github.com/deventlab/d-engine/blob/main/docs/src/docs/client_guide/error-handling.md)

---

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`d-engine`](https://crates.io/crates/d-engine) | **Recommended** - Unified API for Rust apps |
| [`d-engine-client`](https://crates.io/crates/d-engine-client) | High-level Rust client library |
| [`d-engine-server`](https://crates.io/crates/d-engine-server) | Server implementation |
| [`d-engine-core`](https://crates.io/crates/d-engine-core) | Pure Raft algorithm |

---

## License

MIT or Apache-2.0
