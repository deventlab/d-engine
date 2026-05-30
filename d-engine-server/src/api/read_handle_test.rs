/// Unit tests for ReadHandle routing logic.
///
/// ReadHandle is the single source of truth for read routing:
///   Eventual/LeaseRead  → ReadActor fast path; fallback to cmd_tx on LeaseInvalid/SmStopped
///   LinearizableRead    → cmd_tx always
///   SmError             → returned directly, NO cmd_tx fallback
///
/// Proof technique (same as embedded_client_fast_path_test.rs):
///   cmd_rx is dropped so any fallback to cmd_tx returns a Network/channel-closed error.
///   A Business error proves SmError was returned directly.
///   An Ok(value) with cmd_rx dropped proves the fast path was taken.
#[cfg(test)]
mod read_handle_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::read_actor::run_read_actor;
    use bytes::Bytes;
    use d_engine_core::client::{ClientResponse, KvEntry};
    use d_engine_core::config::ReadConsistencyPolicy;
    use d_engine_core::{ClientApiError, ClientCmd, Error, MockStateMachine, ReadLease, now_ms};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use super::super::read_handle::ReadHandle;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn valid_lease() -> Arc<ReadLease> {
        let l = Arc::new(ReadLease::new());
        l.renew(1, now_ms() + 60_000);
        l
    }

    fn revoked_lease() -> Arc<ReadLease> {
        let l = Arc::new(ReadLease::new());
        l.revoke();
        l
    }

    fn expired_lease() -> Arc<ReadLease> {
        let l = Arc::new(ReadLease::new());
        l.renew(1, now_ms().saturating_sub(1));
        l
    }

    fn sm_running_with(value: Bytes) -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(move |keys| Ok(keys.iter().map(|_| Some(value.clone())).collect()));
        sm
    }

    fn sm_running_empty() -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi().returning(|keys| Ok(keys.iter().map(|_| None).collect()));
        sm
    }

    fn sm_stopped() -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| false);
        sm
    }

    fn sm_with_error() -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi().returning(|_| Err(Error::Fatal("disk failure".into())));
        sm
    }

    /// Build a ReadHandle with a live ReadActor.
    ///
    /// `drop_cmd_rx=true` closes the cmd_tx receiver — any cmd_tx fallback
    /// returns a channel-closed Network error (proof the fast path was NOT taken).
    async fn make_handle(
        sm: MockStateMachine,
        lease: Arc<ReadLease>,
        drop_cmd_rx: bool,
    ) -> (ReadHandle, JoinHandle<()>) {
        let (read_tx, read_rx) = mpsc::channel(8);
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        if drop_cmd_rx {
            drop(cmd_rx);
        }
        let handle = tokio::spawn(run_read_actor(read_rx, lease, Arc::new(sm), 64));
        let rh = ReadHandle::new(Some(read_tx), cmd_tx);
        (rh, handle)
    }

    const TIMEOUT: Duration = Duration::from_millis(100);

    // ── Eventual fast path ────────────────────────────────────────────────────

    /// Eventual, SM running, key exists → fast path returns value.
    /// cmd_rx dropped: Ok(value) proves fast path was taken.
    #[tokio::test]
    async fn test_eventual_fast_path_returns_value() {
        let (rh, handle) =
            make_handle(sm_running_with(Bytes::from("v1")), valid_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::EventualConsistency, 1, TIMEOUT).await;
        assert_eq!(result.unwrap(), Some(Bytes::from("v1")));
        drop(rh);
        handle.await.unwrap();
    }

    /// Eventual, SM running, key missing → fast path returns None.
    #[tokio::test]
    async fn test_eventual_fast_path_returns_none_for_missing_key() {
        let (rh, handle) = make_handle(sm_running_empty(), valid_lease(), true).await;
        let result = rh
            .get(
                b"missing",
                ReadConsistencyPolicy::EventualConsistency,
                1,
                TIMEOUT,
            )
            .await;
        assert_eq!(result.unwrap(), None);
        drop(rh);
        handle.await.unwrap();
    }

    /// Eventual, SM stopped → SmStopped → fallback to cmd_tx.
    /// cmd_rx dropped: error is Network (channel-closed), proving cmd_tx fallback triggered.
    #[tokio::test]
    async fn test_eventual_sm_stopped_falls_back_to_cmd_tx() {
        let (rh, handle) = make_handle(sm_stopped(), valid_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::EventualConsistency, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "SmStopped must fall back to cmd_tx (Network error), got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    // ── LeaseRead fast path ───────────────────────────────────────────────────

    /// LeaseRead, valid lease, SM running → fast path returns value.
    #[tokio::test]
    async fn test_lease_read_fast_path_returns_value() {
        let (rh, handle) = make_handle(
            sm_running_with(Bytes::from("lease_val")),
            valid_lease(),
            true,
        )
        .await;
        let result = rh.get(b"k", ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;
        assert_eq!(result.unwrap(), Some(Bytes::from("lease_val")));
        drop(rh);
        handle.await.unwrap();
    }

    /// LeaseRead, revoked lease → LeaseInvalid → fallback to cmd_tx.
    #[tokio::test]
    async fn test_lease_read_revoked_falls_back_to_cmd_tx() {
        let (rh, handle) =
            make_handle(sm_running_with(Bytes::from("x")), revoked_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "revoked lease must fall back to cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    /// LeaseRead, expired lease → LeaseInvalid → fallback to cmd_tx.
    #[tokio::test]
    async fn test_lease_read_expired_falls_back_to_cmd_tx() {
        let (rh, handle) =
            make_handle(sm_running_with(Bytes::from("x")), expired_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "expired lease must fall back to cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    /// LeaseRead, SM stopped (even with valid lease) → LeaseInvalid → fallback.
    #[tokio::test]
    async fn test_lease_read_sm_stopped_falls_back_to_cmd_tx() {
        let (rh, handle) = make_handle(sm_stopped(), valid_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "SM stopped must fall back to cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    // ── SmError — direct return, no cmd_tx fallback ───────────────────────────

    /// sm.get() returns Err → SmError → Business error returned DIRECTLY.
    /// cmd_rx dropped: if fallback were taken, error would be Network.
    /// Business error proves SmError was returned without touching cmd_tx.
    #[tokio::test]
    async fn test_sm_error_returned_directly_no_cmd_tx_fallback() {
        let (rh, handle) = make_handle(sm_with_error(), valid_lease(), true).await;
        let result = rh.get(b"k", ReadConsistencyPolicy::EventualConsistency, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Business { .. })),
            "SmError must produce Business error (direct), not Network (fallback), got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    // ── Linearizable — always cmd_tx ─────────────────────────────────────────

    /// LinearizableRead must never touch ReadActor — always routed to cmd_tx.
    /// cmd_rx dropped: Network error proves cmd_tx was used (not fast path).
    #[tokio::test]
    async fn test_linearizable_always_uses_cmd_tx() {
        // SM has no get() expectation — mockall panics if get() is called (implicit safety net).
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        let (rh, handle) = make_handle(sm, valid_lease(), true).await;

        let result = rh.get(b"k", ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "LinearizableRead must always use cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    // ── No read_tx — always cmd_tx ────────────────────────────────────────────

    /// ReadHandle without read_tx (None) always falls through to cmd_tx for any policy.
    #[tokio::test]
    async fn test_no_read_tx_all_policies_use_cmd_tx() {
        let (cmd_tx, cmd_rx) = mpsc::channel::<d_engine_core::ClientCmd>(1);
        drop(cmd_rx);
        let rh = ReadHandle::new(None, cmd_tx);

        for policy in [
            ReadConsistencyPolicy::EventualConsistency,
            ReadConsistencyPolicy::LeaseRead,
            ReadConsistencyPolicy::LinearizableRead,
        ] {
            let result = rh.get(b"k", policy.clone(), 1, TIMEOUT).await;
            assert!(
                matches!(result, Err(ClientApiError::Network { .. })),
                "no read_tx must always use cmd_tx, policy={policy:?}, got: {:?}",
                result
            );
        }
    }

    // ── ReadActor channel closed ──────────────────────────────────────────────

    /// ReadActor already stopped (read_rx dropped) → send fails → fallback to cmd_tx.
    /// cmd_rx also dropped: Network error proves fallback was taken.
    #[tokio::test]
    async fn test_read_actor_gone_falls_back_to_cmd_tx() {
        let (read_tx, read_rx) = mpsc::channel::<crate::read_actor::ReadCmd>(1);
        let (cmd_tx, cmd_rx) = mpsc::channel::<d_engine_core::ClientCmd>(1);
        drop(read_rx); // ReadActor is gone
        drop(cmd_rx); // cmd_tx also closed — proves fallback was attempted

        let rh = ReadHandle::new(Some(read_tx), cmd_tx);
        let result = rh.get(b"k", ReadConsistencyPolicy::EventualConsistency, 1, TIMEOUT).await;
        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "closed ReadActor must fall back to cmd_tx, got: {:?}",
            result
        );
    }

    // ── cmd_tx path: alignment helper ────────────────────────────────────────

    /// Builds a ReadHandle wired to a one-shot mock Raft responder.
    ///
    /// The responder returns the given sparse `entries` exactly as
    /// `read_from_state_machine` would — caller is responsible for verifying
    /// that `get_batch` re-aligns them to the input key positions.
    async fn make_cmd_tx_handle(entries: Vec<KvEntry>) -> ReadHandle {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            if let Some(ClientCmd::Read(_, resp_tx)) = cmd_rx.recv().await {
                let _ = resp_tx.send(Ok(ClientResponse::read_results(entries)));
            }
        });
        ReadHandle::new(None, cmd_tx)
    }

    // ── cmd_tx path: HashMap re-alignment correctness ─────────────────────────

    /// cmd_tx returns sparse KvEntry with middle key missing.
    /// get_batch must produce [Some, None, Some] — length 3, not 2.
    /// This is the core regression for the HashMap re-alignment fix.
    #[tokio::test]
    async fn test_get_batch_cmd_tx_middle_key_missing_aligned() {
        let entries = vec![
            KvEntry {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            KvEntry {
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            },
        ];
        let rh = make_cmd_tx_handle(entries).await;
        let keys = vec![
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"k3"),
        ];
        let result = rh
            .get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.len(), 3, "length must equal keys.len()");
        assert_eq!(result[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(
            result[1], None,
            "missing key must be None at correct position"
        );
        assert_eq!(result[2], Some(Bytes::from_static(b"v3")));
    }

    /// cmd_tx returns empty entries (all keys missing).
    /// get_batch must produce [None, None, None] — length 3, not 0.
    #[tokio::test]
    async fn test_get_batch_cmd_tx_all_keys_missing_returns_all_none() {
        let rh = make_cmd_tx_handle(vec![]).await;
        let keys = vec![
            Bytes::from_static(b"x"),
            Bytes::from_static(b"y"),
            Bytes::from_static(b"z"),
        ];
        let result = rh
            .get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert!(
            result.iter().all(|v| v.is_none()),
            "all missing keys must be None: {:?}",
            result
        );
    }

    /// cmd_tx returns sparse entries with first key missing.
    /// get_batch must produce [None, Some, Some].
    #[tokio::test]
    async fn test_get_batch_cmd_tx_first_key_missing_aligned() {
        let entries = vec![
            KvEntry {
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            },
            KvEntry {
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            },
        ];
        let rh = make_cmd_tx_handle(entries).await;
        let keys = vec![
            Bytes::from_static(b"missing"),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"k3"),
        ];
        let result = rh
            .get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], None);
        assert_eq!(result[1], Some(Bytes::from_static(b"v2")));
        assert_eq!(result[2], Some(Bytes::from_static(b"v3")));
    }

    /// cmd_tx returns sparse entries with last key missing.
    /// get_batch must produce [Some, Some, None].
    #[tokio::test]
    async fn test_get_batch_cmd_tx_last_key_missing_aligned() {
        let entries = vec![
            KvEntry {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            KvEntry {
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            },
        ];
        let rh = make_cmd_tx_handle(entries).await;
        let keys = vec![
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"missing"),
        ];
        let result = rh
            .get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(result[1], Some(Bytes::from_static(b"v2")));
        assert_eq!(result[2], None);
    }

    /// cmd_tx returns all keys — positive case, no alignment work needed.
    #[tokio::test]
    async fn test_get_batch_cmd_tx_all_keys_exist_aligned() {
        let entries = vec![
            KvEntry {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            KvEntry {
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            },
        ];
        let rh = make_cmd_tx_handle(entries).await;
        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = rh
            .get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(result[1], Some(Bytes::from_static(b"v2")));
    }

    // ── get_batch() multi-key fast path ──────────────────────────────────────

    fn sm_running_with_multi(vals: Vec<(&'static [u8], &'static [u8])>) -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi().returning(move |keys| {
            Ok(keys
                .iter()
                .map(|k| {
                    for (key, val) in &vals {
                        if &k[..] == *key {
                            return Some(Bytes::from_static(val));
                        }
                    }
                    None
                })
                .collect())
        });
        sm
    }

    /// Multi-key Eventual batch: all keys found → Vec positionally ordered.
    /// cmd_rx dropped: Ok proves fast path was taken.
    #[tokio::test]
    async fn test_get_batch_eventual_fast_path_returns_ordered_values() {
        let sm = sm_running_with_multi(vec![(b"k1", b"v1"), (b"k2", b"v2")]);
        let (rh, handle) = make_handle(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = rh
            .get_batch(
                &keys,
                ReadConsistencyPolicy::EventualConsistency,
                1,
                TIMEOUT,
            )
            .await;

        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(values[1], Some(Bytes::from_static(b"v2")));
        drop(rh);
        handle.await.unwrap();
    }

    /// Multi-key Eventual batch: some keys missing → None at correct positions.
    #[tokio::test]
    async fn test_get_batch_eventual_partial_missing_keys() {
        let sm = sm_running_with_multi(vec![(b"k1", b"v1")]);
        let (rh, handle) = make_handle(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"missing")];
        let result = rh
            .get_batch(
                &keys,
                ReadConsistencyPolicy::EventualConsistency,
                1,
                TIMEOUT,
            )
            .await;

        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(values[1], None);
        drop(rh);
        handle.await.unwrap();
    }

    /// Multi-key LeaseRead: valid lease → fast path returns all values.
    #[tokio::test]
    async fn test_get_batch_lease_read_valid_lease_fast_path() {
        let sm = sm_running_with_multi(vec![(b"k1", b"v1"), (b"k2", b"v2")]);
        let (rh, handle) = make_handle(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = rh.get_batch(&keys, ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;

        assert!(
            result.is_ok(),
            "multi-key LeaseRead fast path must succeed: {:?}",
            result.err()
        );
        drop(rh);
        handle.await.unwrap();
    }

    /// Multi-key LeaseRead: revoked lease → LeaseInvalid → fallback to cmd_tx.
    #[tokio::test]
    async fn test_get_batch_lease_revoked_falls_back_to_cmd_tx() {
        let sm = sm_running_with_multi(vec![(b"k1", b"v1")]);
        let (rh, handle) = make_handle(sm, revoked_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = rh.get_batch(&keys, ReadConsistencyPolicy::LeaseRead, 1, TIMEOUT).await;

        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "revoked lease must fall back to cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }

    /// Multi-key Linearizable: always goes through cmd_tx (no ReadActor).
    /// cmd_rx dropped: Network error proves cmd_tx was used.
    #[tokio::test]
    async fn test_get_batch_linearizable_always_uses_cmd_tx() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        // no expect_get() — mockall panics if get() is called
        let (rh, handle) = make_handle(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = rh.get_batch(&keys, ReadConsistencyPolicy::LinearizableRead, 1, TIMEOUT).await;

        assert!(
            matches!(result, Err(ClientApiError::Network { .. })),
            "Linearizable get_batch must always use cmd_tx, got: {:?}",
            result
        );
        drop(rh);
        handle.await.unwrap();
    }
}
