# Service Discovery — Standalone Mode

Demonstrates d-engine's **Watch API** through a practical service discovery use case,
using the gRPC client against a running d-engine cluster.

**What you'll learn:**

- Exact-key watch: react to changes on a single specific key
- Prefix watch: maintain a live in-memory service registry without per-node watchers
- Read-then-watch pattern: avoid the race between "current state" and "future events"
- Reconnection with `CANCELED` (buffer overflow) handling

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                     d-engine Cluster                   │
│  (stores service registry: /services/{name}/{node})    │
└──────────────────────┬─────────────────────────────────┘
                       │ gRPC
          ┌────────────┴────────────┐
          │                         │
    ┌─────▼─────┐             ┌─────▼──────────────────┐
    │   admin   │             │        watcher          │
    │  (writes) │             │                         │
    │           │             │  --key /svc/pay/node1   │
    │ register  │             │  exact-key watch        │
    │ unregister│             │  ── or ──               │
    └───────────┘             │  --key /services/pay/   │
                              │  --prefix               │
                              │  prefix watch           │
                              │  (live registry HashMap)│
                              └─────────────────────────┘
```

## Prerequisites

Start a d-engine node first:

```bash
cd examples/three-nodes-standalone
make start-cluster   # or: make start-node-1
```

## Quick Start

### Mode 1 — Exact-key watch

Watches a single key. Prints current value on startup, then streams PUT/DELETE events.

```bash
# Terminal 1: watch one specific node
make run-watcher

# Terminal 2: register / update / deregister
make register
make unregister
```

Expected watcher output:

```
=== Current State ===
  /services/payment/node1 = (not found)

=== Watching for Changes (Ctrl+C to exit) ===

[PUT   ] /services/payment/node1 = 10.0.0.1:8080  (revision=3)
[DELETE] /services/payment/node1  (revision=4)
```

### Mode 2 — Prefix watch (namespace observer)

One watcher covers the entire `/services/payment/` namespace.
Every node that registers, updates, or deregisters fires an event.
The watcher maintains a live `HashMap` registry and prints it after each change.

```bash
# Terminal 1: watch the whole payment namespace
make run-watcher-prefix

# Terminal 2: simulate cluster churn
make register           # node1
make register-node2     # node2
make register-node3     # node3
make unregister-node2   # node2 crashes
```

Expected watcher output:

```
[PUT   ] /services/payment/node1 = 10.0.0.1:8080  (revision=3)
  registry (1 nodes):
    /services/payment/node1 → 10.0.0.1:8080
[PUT   ] /services/payment/node2 = 10.0.0.2:8080  (revision=4)
  registry (2 nodes):
    /services/payment/node1 → 10.0.0.1:8080
    /services/payment/node2 → 10.0.0.2:8080
...
[DELETE] /services/payment/node2  (revision=6)
  registry (2 nodes):
    /services/payment/node1 → 10.0.0.1:8080
    /services/payment/node3 → 10.0.0.3:8080
```

## Key Concepts

| Concept | Exact-key (`watch`) | Prefix (`watch_prefix`) |
|---|---|---|
| Scope | One specific key | All keys under a namespace |
| Typical use | Own config entry, distributed lock | API gateway routing table, load balancer |
| Watchers registered | N (one per node) | 1 |
| Reconnection | Re-read + re-watch the key | Re-scan prefix + re-watch (ticket #301) |

### `revision` field

Every event carries `revision: u64` — the Raft applied index when the write committed.
After reconnect, compare the first new event's revision to your last known revision:
if `new_revision > last_revision + 1`, events were dropped — trigger a full re-sync.

### `CANCELED` event

When a watcher's buffer overflows (server writes faster than client consumes),
d-engine sends a `CANCELED` sentinel and forcibly unregisters the watcher.
The prefix watch loop in this example handles it: clear the local registry and reconnect.

## Components

| Binary    | Purpose                                         |
|-----------|-------------------------------------------------|
| `admin`   | Registers/unregisters service endpoints         |
| `watcher` | Watches for service changes (exact or prefix)   |

## Related

- [Embedded Mode Example](../service-discovery-embedded/) — in-process watch, zero gRPC overhead
- [Watch Feature Guide](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/watch_feature/index.html)
