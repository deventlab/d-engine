# d-engine-client

[![Crates.io](https://img.shields.io/crates/v/d-engine-client.svg)](https://crates.io/crates/d-engine-client)
[![docs.rs](https://docs.rs/d-engine-client/badge.svg)](https://docs.rs/d-engine-client)

Client library for interacting with d-engine Raft clusters

---

## ⚠️ You Probably Don't Need This Crate

**Use [`d-engine`](https://crates.io/crates/d-engine) instead:**

```toml
[dependencies]
d-engine = { version = "0.2", features = ["client"] }
```

This provides the same API with simpler dependency management. The `d-engine-client` crate is automatically included when you enable the `client` feature.

---

## For Contributors

This crate exists for architectural reasons:

- **Clean boundaries** - Separates client logic from server implementation
- **Faster builds** - Workspace members can depend on client without pulling server deps
- **Testing** - Enables isolated client testing

If you're building an application, use `d-engine` with `features = ["client"]` instead.

---

## Quick Reference

See [`d-engine` README](https://github.com/deventlab/d-engine/blob/main/README.md) for usage examples and documentation.

---

## License

MIT or Apache-2.0
