# d-engine Examples

Usage examples demonstrating different d-engine integration modes.

---

## Quick Start Examples

**New to d-engine? Start here:**

| Example                                           | Mode       | Time  | Description                                             |
| ------------------------------------------------- | ---------- | ----- | ------------------------------------------------------- |
| [quick-start-embedded](quick-start-embedded/)     | Embedded   | 5 min | Single-process Rust app with zero-overhead local client |
| [quick-start-standalone](quick-start-standalone/) | Standalone | 5 min | Go client connecting to 3-node d-engine cluster         |

---

## Integration Modes

### Embedded Mode (Single Process)

| Example                                                   | Description                                |
| --------------------------------------------------------- | ------------------------------------------ |
| [quick-start-embedded](quick-start-embedded/)             | Basic embedded usage with local KV client  |
| [service-discovery-embedded](service-discovery-embedded/) | Service discovery pattern in embedded mode |

**Use case**: Maximum performance, zero network overhead, single-process applications

---

### Standalone Mode (Client-Server)

| Example                                                       | Description                                   |
| ------------------------------------------------------------- | --------------------------------------------- |
| [quick-start-standalone](quick-start-standalone/)             | Go client + 3-node cluster                    |
| [service-discovery-standalone](service-discovery-standalone/) | Service discovery with separate client/server |
| [client_usage](client_usage/)                                 | Rust gRPC client examples                     |

**Use case**: Language flexibility, separate services, microservices architecture

---

## Cluster Management

| Example                                         | Description                           |
| ----------------------------------------------- | ------------------------------------- |
| [three-nodes-cluster](three-nodes-cluster/)     | Bootstrap 3-node cluster from scratch |
| [single-node-expansion](single-node-expansion/) | Start with 1 node, expand to 3 nodes  |

---

## Storage Backend Examples

| Example                       | Description                             |
| ----------------------------- | --------------------------------------- |
| [sled-cluster](sled-cluster/) | Alternative with Sled embedded database |

---

## Requirements

- **Rust**: 1.85+
- **Go**: 1.20+ (for Go examples)
- **Protobuf**: libprotoc 3.0+ (for code generation)
- **OS**: Linux/macOS

---

## Running Examples

```bash
# Rust examples
cd examples/quick-start-embedded
cargo run

# Go examples
cd examples/quick-start-standalone
make run
```

See individual example READMEs for detailed instructions.
