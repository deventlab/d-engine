# Snapshot Module

This module defines the interface and expected behaviors for snapshot generation and application in the Raft protocol implementation of `d-engine`.

## Snapshot Design Philosophy

Snapshotting is a critical component of any Raft-based system. However, the exact strategy for generating and applying snapshots can vary widely depending on the type of storage engine, system requirements, and performance characteristics.

Rather than enforce a single implementation, `d-engine` **intentionally leaves the snapshot generation strategy open to the developer**. This provides flexibility for different use cases and backend storage systems.

### Pluggable Snapshot Strategy

Different databases and distributed systems adopt different approaches to snapshotting. For example:

- Some systems take full on-disk snapshots by pausing the world.
- Others implement incremental snapshotting using copy-on-write or changelogs.
- Some persist in-memory state periodically or through a background thread.

To accommodate this diversity, `d-engine` provides:

- **A trait-based interface** that defines the core behaviors required by any snapshot implementation.
- **A reference implementation** based on the `sled` embedded database to demonstrate how a snapshot strategy can be implemented.

### Included Example

The `sled`-based snapshot implementation included in `d-engine` is meant to serve as a **working demo**, not a production-ready solution. It demonstrates:

- How to implement the `SnapshotBuilder` and `SnapshotApplier` traits.
- A simple full-state serialization approach using `sled` key-value pairs.
- How to restore state from a binary snapshot blob.

## **Install Snapshot**

### Sequence diagram

```mermaid
sequenceDiagram
    participant Leader
    participant Follower
    participant StateMachine

    Leader->>Follower: Send chunked stream Stream<SnapshotChunk>
    Follower->>StateMachine: Create temporary database instance
    loop Chunk processing
        Follower->>Temporary DB: Write chunk data
    end
    Follower->>StateMachine: Atomically replace main database
    StateMachine->>sled: ArcSwap::store(new_db)
    Follower->>Leader: Return success response
```

## **Snapshot Transfer**

**Use Stream-based Chunking**:

1. **Memory Efficiency**: Transmit snapshots as a sequence of chunks via streaming RPC to avoid loading entire snapshots into memory.
2. **Fault Tolerance**: Implement retry logic per chunk (e.g., CRC checks) and resume from the last failed chunk.
3. **Parallelism**: Allow concurrent processing of received chunks (e.g., writing to disk while receiving).
4. **Flow Control**: Dynamically adjust chunk size based on network conditions (e.g., 4MB–16MB chunks).
    

#### Stream<Chunk> vs. Direct Chunk (Even Chunk Size Is Controlled)
# Stream<Chunk> vs. Direct Chunk (Even Chunk Size Is Controlled)

| Criteria           | Stream<Chunk>                                    | Direct Chunk (One Chunk per Request)                       |
|--------------------|--------------------------------------------------|------------------------------------------------------------|
| Connection Overhead | Single long-lived connection for all chunks.    | New TCP/TLS handshake per chunk (high overhead).           |
| Order Guarantee    | Chunks are sent/received in order (built-in).     | Requires manual tracking of chunk sequence.                |
| Atomicity          | Stream can be canceled mid-transfer (e.g., leader change). | Partial failures leave inconsistent state (no cleanup). |
| Error Recovery     | Resumable from last failed chunk (server retains state). | Client must track progress and retry manually.           |
| Backpressure       | Built-in flow control (gRPC manages data pacing). | No backpressure; clients/server may overload.              |
| Code Complexity    | Server handles chunks as a unified stream.       | Client must orchestrate chunk order and retries.           |

The Leader continuously sends snapshot chunks to the Follower through an RPC stream.

```mermaid
sequenceDiagram
    participant Leader
    participant Follower

    %% Leader initiates the snapshot installation using a streaming RPC.
    Leader->>Follower: Initiate InstallSnapshot (Streaming RPC)
    loop Continuously send all chunks
        %% Leader sends snapshot chunks sequentially (e.g., chunk 0 to 99).
        Leader->>Follower: Send chunk 0-Total
    end
    %% After all chunks are sent, Follower responds with a final success message.
    Follower->>Leader: Final response (success = true)

```    
---
### Module responsbilitiies - Statemachine and StateMachineHandler

#### Generating a new snapshot:
1. [**StateMachine**] Generate new DB based on the temporary file provided by the [**StateMachineHandler**] → 
2. [**StateMachine**] Generate data → 
3. [**StateMachine**] Dump current DB into the new DB → 
3. [**StateMachineHandler**] Finalize the snapshot and updating the snapshot version.

#### Applying a snapshot:
1. [**StateMachineHandler**] Snapshot chunk reception and validation → 
2. [**StateMachineHandler**] Write chunks into a temporary file until success → 
3. [**StateMachineHandler**] Error handling and sends error response back to the sender and terminates the process → 
4. After all chunks have been successfully processed and validated, the [**StateMachineHandler**] finalizes the snapshot →  
5. [**StateMachineHandler**] Passing the snapshot file to the [**StateMachine**] → 
6. [**StateMachine**] Apply Snapshot and do online replacement - replacing the old state with the new one based on the snapshot. 

#### Cleaning up old snapshots:
[**StateMachineHandler**] automatically maintains old snapshots according to version policies, while the **StateMachine** is not aware of it.