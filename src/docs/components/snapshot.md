# Snapshot Module


Defines interface and behaviors for snapshot operations in `d-engine` Raft implementation.

## Design Philosophy

- **Pluggable strategy**: Intentional flexibility for snapshot policies and storage implementations
- **Zero-enforced approach**: No default snapshot strategy - implement based on your storage needs
- **Reference implementation**: `sled`-based demo included (not production-ready)

## Core Concepts

### Snapshot Transfer: Bidirectional Streaming Design

```mermaid
sequenceDiagram
    participant Learner
    participant Leader
    
    Learner->>Leader: 1. Request snapshot
    Leader->>Learner: 2. Stream chunks
    Learner->>Leader: 3. Send ACKs
    Leader->>Learner: 4. Retransmit missing chunks
```


1. **Dual-channel communication**:
    - Data channel: Leader → Learner (SnapshotChunk stream)
    - Feedback channel: Learner → Leader (SnapshotAck stream)
2. **Key benefits**:
    - **Flow control**: ACKs regulate transmission speed
    - **Reliability**: Per-chunk CRC verification
    - **Resumability**: Selective retransmission of failed chunks
    - **Backpressure**: Explicit backoff when receiver lags
3. **Sequence**:

```mermaid
sequenceDiagram
    Learner->>Leader: StreamSnapshot(initial ACK)
    Leader-->>Learner: Chunk 0 (metadata)
    Learner->>Leader: ACK(seq=0, status=Accepted)
    Leader-->>Learner: Chunk 1
    Learner->>Leader: ACK(seq=1, status=ChecksumMismatch)
    Leader-->>Learner: Chunk 1 (retransmit)
    Learner->>Leader: ACK(seq=1, status=Accepted)
    loop Remaining chunks
        Leader-->>Learner: Chunk N
        Learner->>Leader: ACK(seq=N)
    end
```

#### The bidirectional snapshot transfer is implemented as a state machine, coordinating both data transmission (chunks) and control feedback (ACKs) between Leader and Learner.

```mermaid
stateDiagram-v2
    [*] --> Waiting
    Waiting --> ProcessingChunk: Data available
    Waiting --> ProcessingAck: ACK received
    Waiting --> Retrying: Retry timer triggered
    ProcessingChunk --> Waiting
    ProcessingAck --> Waiting
    Retrying --> Waiting
    Waiting --> Completed: All chunks sent & acknowledged
```

**Data Flow Summary**:
```text
Leader                          Learner
  |                                |
  |--> Stream<SnapshotChunk> ----> |
  |                                |
  |<-- Stream<SnapshotAck> <------ |
  |                                |
```


### **Snapshot Policies**

Fully customizable through **`SnapshotPolicy`** trait:

```rust,ignore
pub trait SnapshotPolicy {
    fn should_create_snapshot(&self, ctx: &SnapshotContext) -> bool;
}

pub struct SnapshotContext {
    pub last_included_index: u64,
    pub last_applied_index: u64,
    pub current_term: u64,
    pub unapplied_entries: usize,
}
```
Enable customized snapshot policy:

```rust,ignore
let node = NodeBuilder::new()
    .with_snapshot_policy(Arc::new(TimeBasedPolicy {
        interval: Duration::from_secs(3600),
    }))
    .build();
```

By default, Size-Based Policy is enabled.

#### Snaphot policy been used when generate new snapshot
```mermaid
sequenceDiagram
    participant Leader
    participant Policy
    participant StateMachine

    Leader->>Policy: Check should_create_snapshot()
    Policy-->>Leader: true/false
    alt Should create
        Leader->>StateMachine: Initiate snapshot creation
    end
```

**Common policy types**:

- Size-based (default)
- Time-based intervals
- Hybrid approaches
- External metric triggers


### Leader generate snapshot sequence diagram

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

### **Transfer Mechanics**

1. **Chunking**:
    - Fixed-size chunks (configurable 4-16MB)
    - First chunk contains metadata
    - CRC32 checksum per chunk
2. **Rate limiting**:

```rust,ignore
if config.max_bandwidth_mbps > 0 {
    let min_duration = chunk_size_mb / config.max_bandwidth_mbps;
    tokio::time::sleep(min_duration).await;
}
```

1. **Error recovery**:
    - Checksum mismatches trigger single-chunk retransmission
    - Out-of-order detection resets stream position
    - 10-second ACK timeout fails entire transfer

### **Module Responsibilities**

| **Component** | **Responsibilities** |
| --- | --- |
| **StateMachineHandler** | - Chunk validation- Temporary file management- ACK generation- Error handling- Snapshot finalization |
| **StateMachine** | - Snapshot application- Online state replacement- Consistency guarantees |

#### Generating a new snapshot:
1. [**StateMachine**] Generate new DB based on the temporary file provided by the [**StateMachineHandler**] → 
2. [**StateMachine**] Generate data → 
3. [**StateMachine**] Dump current DB into the new DB → 
3. [**StateMachineHandler**] Verify policy conditions and finalize the snapshot and updating the snapshot version.

#### Applying a snapshot:
1. [**StateMachineHandler**] Snapshot chunk reception and validation → 
2. [**StateMachineHandler**] Write chunks into a temporary file until success → 
3. [**StateMachineHandler**] Error handling and sends error response back to the sender and terminates the process → 
4. After all chunks have been successfully processed and validated, the [**StateMachineHandler**] finalizes the snapshot →  
5. [**StateMachineHandler**] Passing the snapshot file to the [**StateMachine**] → 
6. [**StateMachine**] Apply Snapshot and do online replacement - replacing the old state with the new one based on the snapshot. 

#### Cleaning up old snapshots:
[**StateMachineHandler**] automatically maintains old snapshots according to version policies, while the **StateMachine** is not aware of it.

## **Purge Log Design**

### Leader Purge Log State Management

```mermaid
sequenceDiagram
    participant Leader
    participant StateMachine
    participant Storage

    Leader->>StateMachine: create_snapshot()
    StateMachine->>Leader: SnapshotMeta(last_included_index=100)
    
    Note over Leader,Storage: Critical Atomic Operation
    Leader->>Storage: persist_last_purged(100) # 1. Update in-memory last_purged_index<br/>2. Flush to disk atomically
    
    Leader->>Leader: scheduled_purge_upto = Some(100) # Schedule async task
    Leader->>Background: trigger purge_task(100)
    
    Background->>Storage: physical_delete_logs(0..100)
    Storage->>Leader: notify_purge_complete(100)
    Leader->>Storage: verify_last_purged(100) # Optional consistency check

```

### Follower Purge Log State Management
```mermaid
sequenceDiagram
    participant Leader
    participant Follower
    participant FollowerStorage

    Leader->>Follower: InstallSnapshot RPC (last_included=100)
    
    Note over Follower,FollowerStorage: Protocol-Required Atomic Update
    Follower->>FollowerStorage: 1. Apply snapshot to state machine<br/>2. persist_last_purged(100)
    
    Follower->>Follower: last_purged_index = Some(100) # Volatile state update
    Follower->>Follower: pending_purge = Some(100) # Mark background task
    
    Follower->>Background: schedule_log_purge(100)
    Background->>FollowerStorage: delete_logs(0..100)
    FollowerStorage->>Follower: purge_complete(100)
    Follower->>Follower: pending_purge = None # Clear task status
```
