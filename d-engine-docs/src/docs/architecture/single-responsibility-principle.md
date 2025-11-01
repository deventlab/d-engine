This project strictly follows the **Single Responsibility Principle (SRP)** to ensure modularity and maintainability. Developers extending the codebase **must** adhere to this principle to preserve clean separation of concerns. Below are key guidelines and examples.

---

### **1. Core Design Philosophy**

- **Main Loop Responsibility**:

  The `main_loop` (in `src/core/raft.rs`) **only orchestrates event routing**. It:
  1. Listens for events (e.g., RPCs, timers).
  2. Delegates handling to the **current role** (e.g., `LeaderState`, `FollowerState`).
  3. Manages role transitions (e.g., `Leader → Follower`).

- **Role-Specific Logic**:

  Each role (`LeaderState`, `FollowerState`, etc.) **owns its state and behavior**. For example:
  - `LeaderState` handles log replication and heartbeat management.
  - `FollowerState` processes leader requests and election timeouts.

---

### **2. Key Rules for Developers**

### **Rule 1: No Cross-Role Logic**

A role **must never** directly modify another role’s state or handle its events.

**Bad Example** (Violates SRP):

```ignore
// ❌ LeaderState handling Follower-specific logic
impl LeaderState {
    async fn handle_append_entries(...) {
        if need_step_down {
            self.become_follower(); // SRP violation: Leader manages Follower state
            self.follower_handle_entries(...); // SRP violation: Leader handles Follower logic
        }
    }
}
```

**Correct Approach**:

```ignore
// ✅ Leader sends a role transition event to main_loop
impl LeaderState {
    async fn handle_append_entries(...) -> Result<()> {
        if need_step_down {
            role_tx.send(RoleEvent::BecomeFollower(...))?; // Delegate transition
            return Ok(()); // Exit immediately
        }
        // ... (Leader-specific logic)
    }
}
```

---

### **Rule 2: Atomic Role Transitions**

When a role changes (e.g., `Leader → Follower`), the new role **immediately takes over** event handling.

**Example**:

```ignore
// main_loop.rs (simplified)
loop {
    let event = event_rx.recv().await;
    match current_role {
        Role::Leader(leader) => {
            leader.handle_event(event).await?;
            // If leader sends RoleEvent::BecomeFollower, update `current_role` here
        }
        Role::Follower(follower) => { ... }
    }
}
```

---

### **Rule 3: State Isolation**

Role-specific data (e.g., `LeaderState.next_index`) **must not** leak into other roles.

**Bad Example**:

```ignore
// ❌ FollowerState accessing LeaderState internals
impl FollowerState {
    fn reset_leader_index(&mut self, leader: &LeaderState) {
        self.match_index = leader.next_index; // SRP violation
    }
}
```

**Correct Approach**:

```ignore
// ✅ LeaderState persists its state to disk before stepping down
impl LeaderState {
    async fn step_down(&mut self, ctx: &RaftContext) -> Result<()> {
        ctx.save_state(self.next_index).await?; // Isolate state
        Ok(())
    }
}

// ✅ FollowerState loads state from disk
impl FollowerState {
    async fn load_state(ctx: &RaftContext) -> Result<Self> {
        let next_index = ctx.load_state().await?;
        Ok(Self { match_index: next_index })
    }
}
```

---

### **3. Folder Structure Alignment**

The codebase enforces SRP through role-specific modules:

```text
src/core/raft_role/
├── leader_state.rs          # Leader-only logic (e.g., log replication)
├── follower_state.rs        # Follower-only logic (e.g., election timeout)
├── candidate_state.rs       # Candidate election logic
└── mod.rs                   # Role transitions and common interfaces
```

---

### **4. Adding New Features**

When extending the project:

1. **Identify Ownership**: Should the logic belong to a role, the main loop, or a utility module?
2. **Avoid Hybrid Roles**: Never create a `LeaderFollowerHybridState`. Use role transitions instead.
3. **Example**: Adding a "Learner" role:
   - Create `learner_state.rs` with Learner-specific logic.
   - Update `main_loop` to handle `Role::Learner`.
   - Add `RoleEvent::BecomeLearner` for transitions.

---

### **5. Why SRP Matters Here**

- **Predictability**: Each role’s behavior is isolated and testable.
- **Safer Changes**: Modifying `FollowerState` won’t accidentally break `LeaderState`.
- **Protocol Compliance**: Raft requires strict role separation; SRP enforces this naturally.

By following these rules, your contributions will align with the project’s design philosophy.
