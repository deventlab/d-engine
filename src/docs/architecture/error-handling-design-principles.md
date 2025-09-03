This section outlines the core principles for distinguishing between **protocol logic errors** (expected business failures) and **system-level errors** (unrecoverable faults) across the entire project. Developers extending this codebase should strictly follow these guidelines to ensure consistency and reliability.

## **1. Core Principles**

| **Error Type** | **Description** | **Handling Strategy** |
| --- | --- | --- |
| **Protocol Logic Errors** | Failures dictated by protocol rules (e.g., term mismatches, log inconsistencies). | - Return `Ok(())` with a protocol-compliant response (e.g., `success: false`). |
| **System-Level Errors** | Critical failures (e.g., I/O errors, channel disconnections, state corruption). | - Return `Error` to halt normal operation.- Trigger recovery mechanisms (retry/alert). |
| **Illegal Operation Errors** | Violations of the Raft state machine rules (e.g., invalid role transitions). | - Return `Error` immediately.- Indicates **bugs in code logic** and must be fixed. |

---

## **2. Examples**

### **Example 1: Protocol Logic Error**

```ignore
// Reject stale AppendEntries request (term < current_term)
if req.term < self.current_term() {
    let resp = AppendEntriesResponse { success: false, term: self.current_term() };
    sender.send(Ok(resp))?;  // Protocol error → return Ok(())
    return Ok(());
}
```

### **Example 2: System-Level Error**

```ignore
// Fail to persist logs (return Error)
ctx.raft_log().append_entries(&req.entries).await?; // Propagates storage error
```

### **Example 3: Illegal Operation Error**

```ignore
// Follower illegally attempts to become Leader
impl RaftRoleState for FollowerState {
    fn become_leader(&self) -> Result<LeaderState> {
        warn!("Follower cannot directly become Leader");
        Err(Error::IllegalOperation("follower → leader")) // Hard error → return Error
    }
}
```

---

## **3. Best Practices**

---

1. **Atomic State Changes**
    
    Ensure critical operations (e.g., role transitions) are atomic. If a step fails after partial execution, return `Error` to avoid inconsistent states.
    
2. **Error Classification**
    
    Define explicit error types to distinguish recoverable and non-recoverable failures:
    
    ```ignore
    enum Error {
        Protocol(String),         // Debugging only (e.g., "term mismatch")
        Storage(io::Error),       // Retry or alert
        Network(NetworkError),    // Reconnect logic
        IllegalOperation(&'static str), // Code bug → must fix
    }
    ```
    
3. **Handle Illegal Operations Strictly**
    - Log illegal operations at `error!` level.
    - Add unit tests to ensure invalid transitions return `Error`.
    
    ```ignore
    #[test]
    fn follower_cannot_become_leader() {
        let follower = FollowerState::new();
        assert!(matches!(
            follower.become_leader(),
            Err(Error::IllegalOperation("follower → leader"))
        ));
    }
    ```
    

---

### **4. Extending to the Entire Project**

These principles apply to **all components**:

- **RPC Clients/Servers**:
    - Treat invalid RPC sequences (e.g., duplicate requests) as **protocol errors** (`Ok` with response).
    - Treat connection resets as **system errors** (`Error`).
- **State Machines**:
    - Invalid operations (e.g., applying non-committed logs) → **Protocol errors**.
    - Disk write failures → **System errors**.
- **Cluster Management**:
    - Node join conflicts (e.g., duplicate IDs) → **Protocol errors**.
    - Invalid role transitions (e.g., Follower → Leader) → **Illegal Operation errors**.

---

By adhering to these rules, your extensions will maintain the same robustness and predictability as the core implementation.