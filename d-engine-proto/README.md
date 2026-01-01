# d-engine-proto

[![Crates.io](https://img.shields.io/crates/v/d-engine-proto.svg)](https://crates.io/crates/d-engine-proto)
[![docs.rs](https://docs.rs/d-engine-proto/badge.svg)](https://docs.rs/d-engine-proto)

gRPC protocol definitions for d-engine - foundation for all client implementations

---

## What is this?

This crate contains the Protocol Buffer definitions and generated Rust code for d-engine's gRPC API. It's the foundation for building clients in **any programming language** that supports gRPC.

**d-engine** is a lightweight distributed coordination engine written in Rust, designed for embedding into applications that need strong consistency—the consensus layer for building reliable distributed systems.

---

## When to use this crate

- ✅ Building **non-Rust clients** (Python/Go/C#) for d-engine clusters
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

**Step 1: Get the proto files**

Clone the d-engine repository or download the proto files:

```bash
git clone https://github.com/deventlab/d-engine.git
cd d-engine/d-engine-proto/proto
```

**Step 2: Generate client code**

```bash
# Go example (✅ Tested - see https://github.com/deventlab/d-engine/tree/main/examples/quick-start-standalone)
protoc -I. \
  --go_out=./go \
  --go_opt=module=github.com/deventlab/d-engine/proto \
  --go-grpc_out=./go \
  --go-grpc_opt=module=github.com/deventlab/d-engine/proto \
  proto/common.proto \
  proto/error.proto \
  proto/client/client_api.proto

# Python example (⚠️ Command verified, end-to-end integration not yet tested)
protoc --python_out=./out \
  proto/common.proto \
  proto/error.proto \
  proto/client/client_api.proto
```

**Language Support Status:**

- ✅ **Go** - Production-ready with [working example](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-standalone)
- ⚠️ **Python** - Proto generation verified, client integration pending
- 🔜 **Other languages** - Community contributions welcome

**Note:**

- Commands assume you're in the `d-engine-proto` directory
- The proto files have dependencies - always include `common.proto` and `error.proto` when generating client code
- For Go: adjust `module=` option to match your project's module path

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

- [Go Client Example](https://github.com/deventlab/d-engine/tree/main/examples/quick-start-standalone)
- [Error Handling](https://docs.rs/d-engine/latest/d_engine/docs/client_guide/error_handling/index.html)

---

## Related Crates

| Crate                                                         | Purpose                                     |
| ------------------------------------------------------------- | ------------------------------------------- |
| [`d-engine`](https://crates.io/crates/d-engine)               | **Recommended** - Unified API for Rust apps |
| [`d-engine-client`](https://crates.io/crates/d-engine-client) | High-level Rust client library              |
| [`d-engine-server`](https://crates.io/crates/d-engine-server) | Server implementation                       |
| [`d-engine-core`](https://crates.io/crates/d-engine-core)     | Pure Raft algorithm                         |

---

## License

MIT or Apache-2.0
