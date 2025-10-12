# Read Consistency Guide

This guide explains how to control read consistency guarantees in your client applications.

## Overview

D-Engine provides three read consistency policies that let you balance between **performance** and **data freshness** based on your application needs.

**Important**: All policies return **correct data** from committed state. The difference is whether you read the **latest** committed state or a **slightly older** committed state.

## Available Policies

### 1. LinearizableRead (Strongest Consistency)

Guarantees you read the **most recent** committed value across the entire cluster.

**How it works**: Leader verifies its leadership with a quorum before serving the read.

**Use when**:

- Financial transactions
- Critical operations requiring absolute latest data
- Strict consistency is mandatory

**Trade-off**: Highest latency due to network round-trip to verify quorum.

```rust
// Example usage
let value = client
    .get_with_policy(key, Some(ReadConsistencyPolicy::LinearizableRead))
    .await?;
```

### 2. LeaseRead (Balanced)

Provides **near-latest** data with significantly better performance.

**How it works**: Leader serves reads locally during its lease period without contacting followers.

**Use when**:

- You need strong consistency with better performance
- Small clock drift between nodes is acceptable
- Most production use cases

**Trade-off**: Assumes bounded clock drift between nodes (typically negligible).

```rust
let value = client
    .get_with_policy(key, Some(ReadConsistencyPolicy::LeaseRead))
    .await?;
```

### 3. EventualConsistency (Best Performance)

Reads from **any node** without additional consistency checks.

**How it works**: Directly reads from local state machine on any node (leader, follower, or candidate).

**Use when**:

- Caching and monitoring data
- Dashboard metrics and analytics
- High read throughput is critical
- Stale data (seconds old) is acceptable

**Trade-off**: May return data that is slightly behind the latest committed state.

```rust
let value = client
    .get_eventual(key)  // Convenience method
    .await?;

// Or explicitly:
let value = client
    .get_with_policy(key, Some(ReadConsistencyPolicy::EventualConsistency))
    .await?;
```

## Server Default Policy

If you **don't specify** a consistency policy, the server's default configuration is used:

```rust
// Uses server's default policy
let value = client.get(key).await?;
```

Server default is configured in `raft.read_consistency.default_policy` (see server configuration).

### Client Override

Clients can override the server default on a per-request basis if the server allows it:

- Check `raft.read_consistency.allow_client_override` in server config
- If enabled, your per-request policy takes precedence
- If disabled, server enforces its default policy

## Quick Reference

| Policy                  | Latency | Freshness    | Node Types  | Use Case                |
| ----------------------- | ------- | ------------ | ----------- | ----------------------- |
| **LinearizableRead**    | High    | Latest       | Leader only | Financial, critical ops |
| **LeaseRead**           | Medium  | Near-latest  | Leader only | Most production apps    |
| **EventualConsistency** | Lowest  | May be stale | Any node    | Caching, analytics      |

## Common Patterns

### Critical Read with Fallback

```rust
// Try linearizable first, fallback to lease on timeout
match timeout(
    Duration::from_millis(100),
    client.get_with_policy(key, Some(ReadConsistencyPolicy::LinearizableRead))
).await {
    Ok(result) => result?,
    Err(_) => client.get_with_policy(key, Some(ReadConsistencyPolicy::LeaseRead)).await?,
}
```

### Mixed Read Patterns

```rust
// Critical read
let balance = client.get_linearizable(&account_balance_key).await?;

// Dashboard metrics (eventual is fine)
let stats = client.get_eventual(&dashboard_stats_key).await?;
```

## Key Takeaways

1. **All policies return correct data** - never corrupted or invalid
2. **Choose based on your use case** - don't over-engineer with LinearizableRead everywhere
3. **EventualConsistency != eventual correctness** - data is always from a valid committed state
4. **Default to LeaseRead** for most applications - good balance of performance and consistency
5. **Server default applies** when you don't specify a policy

## Further Reading

- Server Configuration: See `src/config/raft.rs` for `ReadConsistencyConfig`
