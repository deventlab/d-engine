_A robust mechanism for expanding Raft clusters with clear separation between roles, designed for financial-grade systems._

---

### **Business Objectives**

1. **Read Load Distribution**

   New nodes initially join as Learners, sync data incrementally, and may be promoted to Followers to serve read traffic.

2. **High Availability for Mission-Critical Systems**

   For systems like RocketMQ-on-DLedger, a minimum 3-node Raft group is required to support automatic failover.

3. **Offline Analysis or Disaster Recovery**

   Nodes may join as Learners for non-voting roles, syncing data asynchronously for use cases like TiFlash-style OLAP or backup.

---

### **Architecture Principles**

| **Principle**                 | **Description**                                                            |
| ----------------------------- | -------------------------------------------------------------------------- |
| **Even-Count Expansion Only** | New nodes must be added in pairs (e.g., Node 4 and Node 5 together).       |
| **Sequential Join Handling**  | Leader handles node joins strictly one at a time.                          |
| **Atomic Join Semantics**     | Node must either fully join as Active Follower, or rollback on failure.    |
| **Retry on Join Failure**     | Joining includes retry logic due to possible wait time for peer readiness. |

---

## **Join Flow: Atomic Pair Join Protocol**

_Example: Node 4 joins via seed node 192.168.1.1_

### 1. Node4 Startup

-> Contacts seed node (`192.168.1.1`) to get Leader address

-> Sends `JoinRequest(role=learner, purpose=general)` to Leader

### 2. Leader Processing

**a. Immediate Actions:**

- Initiates `ConfChangeV2` proposal:

  ```yaml
  - Add Node4 as Joining Learner
  - Set timeout timestamp (now + 30s
  ```

**b. Response:**

- Returns `JoinResponse`:

  ```json
  {
    "current_leader": "192.168.1.1",
    "assigned_group": "none (standalone node)"
  }
  ```

### 3. Post-Proposal Commit

**a. Node4 State Transition**

-> Status: `Syncing Learner`

**b. Leader Action**

```yaml
-> Proactively pushes `InstallSnapshot`
```

**c. Synchronization**

```yaml
-> Node4 begins incremental log sync
```

### 4. Node4 Sync Completion

-> Sends `ReadyNotification(match_index=X)`

-> Status: `Ready Learner`

### 5. Leader Decision Workflow

**a. Conditional Checks:**

```rust,ignore
case KeepAsLearner:  // Analytical node
    Mark as ActiveLearner
case PromotePair:    // Pairing required
    Find another Ready Learner
    -> If Node5 found -> Initiate dual-node promotion
    -> If none found -> Start pairing timer (5min)

```

**b. Timer Expiry Handling:**

```yaml
-> Demote to `StandbyLearner`

-> Trigger manual OPS intervention
```

### **6. Successful Promotion**

**a. Cluster Update**

```yaml
-> `ConfChange` commits
```

**b. Role Transition**

```yaml
-> Node4 becomes `ActiveFollower`
```

**c. Operational Integration**

```yaml
-> Joins read load balancing pool
```
