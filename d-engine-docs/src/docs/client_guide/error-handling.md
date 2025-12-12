# Error Handling

## Error Categories

d-engine client errors are categorized by layer:

### Network Errors (Retryable)

| Error | Cause | Retry Strategy |
|-------|-------|----------------|
| `ConnectionTimeout` | Network timeout | Retry after 3-5s |
| `LeaderChanged` | Leader re-election | Retry immediately with leader hint |
| `InvalidAddress` | Bad server address | Fix address, no retry |

### Protocol Errors (Client Issue)

| Error | Cause | Action |
|-------|-------|--------|
| `InvalidResponse` | Malformed server response | Check client/server version compatibility |
| `VersionMismatch` | Incompatible API version | Upgrade client or server |

### Storage Errors (Server Issue)

| Error | Cause | Action |
|-------|-------|--------|
| `DiskFull` | Server disk full | Contact server admin |
| `DataCorruption` | Storage corruption detected | Contact server admin |
| `KeyNotExist` | Key not found | Normal case, handle gracefully |

### Business Errors (Retry with Logic)

| Error | Cause | Action |
|-------|-------|--------|
| `NotLeader` | Request sent to follower | Redirect to leader |
| `ClusterUnavailable` | No quorum (< 2/3 nodes) | Retry after cluster recovery |
| `StaleOperation` | Operation based on old state | Refresh state and retry |
| `RateLimited` | Too many requests | Backoff and retry |

## Retry Best Practices

### Automatic Retry (Built-in)

d-engine client handles these automatically:
- Leader election (redirects to new leader)
- Transient network errors (exponential backoff)

### Manual Retry Required

Your application must handle:
- `ClusterUnavailable`: Wait for cluster recovery
- `RateLimited`: Implement backoff
- `StaleOperation`: Refresh state before retry

### Example: Retry with Backoff

```go
func writeWithRetry(client *dengine.Client, key, value string) error {
    maxRetries := 3
    for i := 0; i < maxRetries; i++ {
        err := client.Put(key, value)
        if err == nil {
            return nil
        }
        
        // Check error type
        if isRetryable(err) {
            backoff := time.Duration(1<<i) * time.Second
            time.Sleep(backoff)
            continue
        }
        return err // Non-retryable
    }
    return errors.New("max retries exceeded")
}

func isRetryable(err error) bool {
    // Check error code
    // Network and Business errors are usually retryable
    // Protocol and Storage errors are not
}
```

## Leader Hint

When receiving `NotLeader` or `LeaderChanged`, the error includes a leader hint:

```json
{
  "code": "LeaderChanged",
  "leader_hint": {
    "id": "2",
    "address": "192.168.1.11:9082",
    "last_contact": 1234567890
  }
}
```

Use this to redirect requests to the current leader.

## See Also

- [Client API (Rust)](https://github.com/deventlab/d-engine/blob/main/d-engine-client/src/error.rs) - Full error type definitions
- [Go Client Guide](go-client.md) - Go client usage
