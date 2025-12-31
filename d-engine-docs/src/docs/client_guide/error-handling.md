# Error Handling

d-engine returns errors through two channels depending on your integration mode:

- **Standalone (gRPC)**: `ErrorCode` in response
- **Embedded (Rust)**: `Result<T, E>` from API

Both modes use the same error categories defined in `proto/error.proto`.

---

## Error Categories

### Network Errors (1000-1999)

Connection problems. Retry with backoff.

| Code                 | When                     | Action                               |
| -------------------- | ------------------------ | ------------------------------------ |
| `CONNECTION_TIMEOUT` | Network slow/unreachable | Retry after 3-5s                     |
| `INVALID_ADDRESS`    | Malformed URL            | Fix address                          |
| `LEADER_CHANGED`     | Leader re-elected        | Retry with new leader (see metadata) |

### Business Errors (4000-4999)

Cluster state or request issues. Handle based on error.

| Code                  | When                       | Action                  |
| --------------------- | -------------------------- | ----------------------- |
| `NOT_LEADER`          | Write sent to follower     | Redirect to leader      |
| `CLUSTER_UNAVAILABLE` | < 2/3 nodes available      | Wait and retry          |
| `INVALID_REQUEST`     | Bad request format         | Fix request             |
| `STALE_OPERATION`     | Based on old cluster state | Refresh state and retry |

---

## Handling in Standalone Mode

Use Leader Hint to redirect to the leader:

```go
currentAddr := "127.0.0.1:9081"  // Start with any node

for i := 0; i < maxRedirects; i++ {
    conn, _ := grpc.NewClient(currentAddr, ...)
    client := pb.NewRaftClientServiceClient(conn)

    resp, err := client.HandleClientWrite(ctx, req)
    if err != nil {
        return err  // Network error
    }

    if resp.Error == error_pb.ErrorCode_SUCCESS {
        break  // Success
    }

    // Got NOT_LEADER - follow leader hint
    if resp.Error == error_pb.ErrorCode_NOT_LEADER && resp.Metadata != nil {
        leaderAddr := resp.Metadata.LeaderAddress
        if leaderAddr != nil && *leaderAddr != "" {
            conn.Close()
            currentAddr = *leaderAddr  // Redirect
            continue
        }
    }
}
```

See [Quick Start](../quick-start-standalone.md) for complete working example.

---

## Handling in Embedded Mode

Check `Result`:

```rust,ignore
match client.put(key, value).await {
    Ok(_) => { /* success */ }
    Err(e) => {
        // e is ClientApiError
        match e.code() {
            ErrorCode::NotLeader => { /* handle */ }
            ErrorCode::ClusterUnavailable => { /* retry */ }
            _ => { /* other errors */ }
        }
    }
}
```

---

## Leader Hint

When you get `NOT_LEADER` error, check `metadata.LeaderAddress`:

```go
if resp.Error == error_pb.ErrorCode_NOT_LEADER && resp.Metadata != nil {
    leaderAddr := resp.Metadata.LeaderAddress  // e.g., "0.0.0.0:9082"
    leaderId := resp.Metadata.LeaderId          // e.g., "2"
    // Reconnect to leaderAddr
}
```

This allows immediate redirect instead of trying all nodes.

---

## Retry Strategy

| Error                 | Retry? | How                       |
| --------------------- | ------ | ------------------------- |
| `CONNECTION_TIMEOUT`  | Yes    | Exponential backoff       |
| `LEADER_CHANGED`      | Yes    | Immediate with new leader |
| `NOT_LEADER`          | Yes    | Redirect to leader        |
| `CLUSTER_UNAVAILABLE` | Yes    | Wait longer, don't hammer |
| `INVALID_REQUEST`     | No     | Fix request first         |

---

## See Also

- [Quick Start](../quick-start-standalone.md) - See errors in action
- [Read Consistency](./read-consistency.md) - Consistency vs availability
