# **Raft Log Purge Customization Guideline**

## **Overview**

The **`PurgeExecutor`** enables safe customization of log compaction while maintaining Raft protocol guarantees. Implement storage-specific purge validation without modifying core consensus logic.

## Purge Validation Layers
## **Key Concepts**


### **Validation Layers**

| **Layer** | **Responsibility** | **Example Checks** |
| --- | --- | --- |
| Protocol | Raft state consistency | Commit index ordering |
| Storage | Physical storage constraints | Disk space, I/O health |

## **Implementation Guide**

### **1. Developer Cloud Create Custom Executor**

```rust,ignore
pub struct CloudPurgeExecutor {
    s3_client: Arc<aws_sdk_s3::Client>,
    min_disk_space: u64,
}

#[async_trait]
impl PurgeExecutor for CloudPurgeExecutor {
    async fn validate_purge(&self, ctx: &PurgeContext) -> Result<()> {
        // Storage-specific checkscheck_disk_usage(ctx.snapshot_meta.size)?;
        verify_s3_credentials()?;
        Ok(())
    }

    async fn execute_purge(&self, log_id: LogId) -> Result<()> {
        self.s3_client.delete_objects(...).await?;
        Ok(())
    }
}
```

### **2. Configure in Node Builder**

```rust,ignore
let executor = CloudPurgeExecutor::new(
    s3_client,
    config.min_disk_space
);

NodeBuilder::new()
    .with_purge_executor(executor)
    .build();
```
