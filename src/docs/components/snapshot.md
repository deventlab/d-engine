# Snapshot documentation

## Three key questions

1. How to generate:
    - STW（Stop The World）
    - Copy-on-Write
    - MVCC (Multi-Version Concurrency Control)
2. When to generate: Triggered based on log length
3. How to transmit: Using InstallSnapshot RPC with streaming transmission

### How to generate

#### Guidelines

1. Non-blocking Async Generation
2. Incremental Snapshots

