/// Tests for EmbeddedReadHandle — the zero-channel direct-SM read path.
///
/// Proof technique (same as embedded_client_fast_path_test):
///   cmd_rx is dropped so any fallback to cmd_tx returns a channel-closed error.
///   Success proves the SM was called directly; error proves fallback was triggered.
#[cfg(test)]
mod embedded_read_handle_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use d_engine_core::config::ReadConsistencyPolicy;
    use d_engine_core::{MockStateMachine, MockTypeConfig, ReadLease, now_ms};
    use tokio::sync::mpsc;

    use super::super::EmbeddedReadHandle;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn valid_lease() -> Arc<ReadLease> {
        let lease = Arc::new(ReadLease::new());
        lease.renew(1, now_ms() + 60_000);
        lease
    }

    fn invalid_lease() -> Arc<ReadLease> {
        let lease = Arc::new(ReadLease::new());
        lease.revoke();
        lease
    }

    fn sm_with_value(value: Bytes) -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_get_multi()
            .returning(move |keys| Ok(keys.iter().map(|_| Some(value.clone())).collect()));
        sm
    }

    fn sm_stopped() -> MockStateMachine {
        // SM stopped: get_multi returns NotServing error
        let mut sm = MockStateMachine::new();
        sm.expect_get_multi().returning(|_| {
            Err(d_engine_core::StorageError::NotServing("state machine stopped".into()).into())
        });
        sm
    }

    /// Build a handle. `drop_cmd_rx=true` closes the cmd channel so any fallback errors.
    fn make_handle(
        sm: MockStateMachine,
        lease: Arc<ReadLease>,
        drop_cmd_rx: bool,
    ) -> EmbeddedReadHandle<MockTypeConfig> {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        if drop_cmd_rx {
            drop(cmd_rx);
        }
        EmbeddedReadHandle::new(Arc::new(sm), lease, cmd_tx)
    }

    // ── Eventual fast path ────────────────────────────────────────────────────

    /// Eventual read goes directly to SM; cmd_rx dropped proves cmd_tx was not used.
    #[tokio::test]
    async fn test_eventual_read_bypasses_cmd_tx() {
        let handle = make_handle(sm_with_value(Bytes::from("v")), valid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::EventualConsistency,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert_eq!(result.unwrap(), Some(Bytes::from("v")));
    }

    /// Eventual read when SM is stopped falls back to cmd_tx.
    /// cmd_rx dropped → fallback returns error (proves fallback was taken, not panic).
    #[tokio::test]
    async fn test_eventual_sm_stopped_falls_back_to_cmd_tx() {
        let handle = make_handle(sm_stopped(), valid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::EventualConsistency,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert!(result.is_err(), "stopped SM must fall back to cmd_tx");
    }

    // ── LeaseRead fast path ───────────────────────────────────────────────────

    /// LeaseRead with valid lease goes directly to SM.
    #[tokio::test]
    async fn test_lease_read_valid_lease_bypasses_cmd_tx() {
        let handle = make_handle(sm_with_value(Bytes::from("lease_val")), valid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::LeaseRead,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert_eq!(result.unwrap(), Some(Bytes::from("lease_val")));
    }

    /// LeaseRead with invalid (revoked) lease falls back to cmd_tx.
    #[tokio::test]
    async fn test_lease_read_invalid_lease_falls_back_to_cmd_tx() {
        let mut sm = MockStateMachine::new();
        // get_multi must NOT be called — lease is invalid
        sm.expect_get_multi().never();

        let handle = make_handle(sm, invalid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::LeaseRead,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert!(result.is_err(), "invalid lease must fall back to cmd_tx");
    }

    /// LeaseRead with valid lease but stopped SM falls back to cmd_tx.
    #[tokio::test]
    async fn test_lease_read_sm_stopped_falls_back_to_cmd_tx() {
        let handle = make_handle(sm_stopped(), valid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::LeaseRead,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert!(result.is_err(), "stopped SM must fall back to cmd_tx");
    }

    // ── LinearizableRead ─────────────────────────────────────────────────────

    /// Linearizable always uses cmd_tx. cmd_rx dropped → error proves cmd_tx was used.
    #[tokio::test]
    async fn test_linearizable_always_uses_cmd_tx() {
        let mut sm = MockStateMachine::new();
        sm.expect_get_multi().never(); // must not be called

        let handle = make_handle(sm, valid_lease(), true);
        let result = handle
            .get(
                b"k",
                ReadConsistencyPolicy::LinearizableRead,
                1,
                Duration::from_millis(100),
            )
            .await;
        assert!(
            result.is_err(),
            "linearizable must always go through cmd_tx"
        );
    }

    // ── get_batch multi-key ───────────────────────────────────────────────────

    /// get_batch returns values for all keys in order.
    #[tokio::test]
    async fn test_get_batch_returns_all_keys_in_order() {
        let mut sm = MockStateMachine::new();
        sm.expect_get_multi().returning(|keys| {
            Ok(keys
                .iter()
                .enumerate()
                .map(|(i, _)| Some(Bytes::from(format!("v{i}"))))
                .collect())
        });

        let handle = make_handle(sm, valid_lease(), true);
        let keys = vec![Bytes::from("k0"), Bytes::from("k1"), Bytes::from("k2")];
        let result = handle
            .get_batch(
                &keys,
                ReadConsistencyPolicy::EventualConsistency,
                1,
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some(Bytes::from("v0")));
        assert_eq!(result[1], Some(Bytes::from("v1")));
        assert_eq!(result[2], Some(Bytes::from("v2")));
    }
}
