/// Fast-path tests for EmbeddedClient via ReadActor.
///
/// These tests verify that EventualConsistency and LeaseRead requests are
/// routed to the ReadActor (bypassing cmd_tx), and fall back to cmd_tx when
/// the ReadActor returns LeaseInvalid, SmStopped, or its channel is closed.
///
/// Proof technique: cmd_rx is dropped so any fallback to cmd_tx returns a
/// channel-closed error — success proves the fast path was taken; error proves
/// the fallback triggered.
#[cfg(test)]
mod fast_path_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::read_actor::run_read_actor;
    use bytes::Bytes;
    use d_engine_core::MockTypeConfig;
    use d_engine_core::config::ReadConsistencyPolicy;
    use d_engine_core::{MockStateMachine, ReadLease, now_ms};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use super::super::embedded_client::EmbeddedClient;
    use super::super::read_handle::ReadHandle;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_event_tx() -> mpsc::Sender<d_engine_core::RaftEvent> {
        let (tx, _rx) = mpsc::channel(1);
        tx
    }

    /// Builds a client wired to a live ReadActor.
    ///
    /// `drop_cmd_rx=true` closes the receiving end — any fallback to cmd_tx
    /// will fail with a channel-closed error, proving the fast path was NOT taken.
    async fn client_with_read_actor(
        sm: MockStateMachine,
        lease: Arc<ReadLease>,
        drop_cmd_rx: bool,
    ) -> (EmbeddedClient<MockTypeConfig>, JoinHandle<()>) {
        let (read_tx, read_rx) = mpsc::channel(8);
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        if drop_cmd_rx {
            drop(cmd_rx);
        }

        let ra_handle = tokio::spawn(run_read_actor(read_rx, lease, Arc::new(sm), 64));
        let client = EmbeddedClient::new_internal(
            make_event_tx(),
            cmd_tx.clone(),
            1,
            Duration::from_millis(100),
        )
        .with_read_handle(ReadHandle::new(Some(read_tx), cmd_tx));
        (client, ra_handle)
    }

    fn valid_lease() -> Arc<ReadLease> {
        let lease = Arc::new(ReadLease::new());
        lease.renew(1, now_ms() + 60_000);
        lease
    }

    fn revoked_lease() -> Arc<ReadLease> {
        let lease = Arc::new(ReadLease::new());
        lease.revoke();
        lease
    }

    fn expired_lease() -> Arc<ReadLease> {
        let lease = Arc::new(ReadLease::new());
        lease.renew(1, now_ms().saturating_sub(1));
        lease
    }

    fn sm_running_with_value(value: Bytes) -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(move |keys| Ok(keys.iter().map(|_| Some(value.clone())).collect()));
        sm
    }

    fn sm_running_missing_key() -> MockStateMachine {
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

    // ── LeaseRead fast path ───────────────────────────────────────────────────

    /// LeaseRead with valid lease and running SM → ReadActor returns value.
    /// cmd_tx is closed: success proves the fast path was taken.
    #[tokio::test]
    async fn test_get_lease_bypasses_cmd_tx_when_lease_valid() {
        let sm = sm_running_with_value(Bytes::from("fast_value"));
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let result = client.get_with_consistency(b"key1", ReadConsistencyPolicy::LeaseRead).await;
        assert_eq!(result.unwrap(), Some(Bytes::from("fast_value")));

        drop(client); // drop read_tx so ReadActor exits
        handle.await.unwrap();
    }

    /// LeaseRead with valid lease and missing key → ReadActor returns None.
    #[tokio::test]
    async fn test_get_lease_returns_none_for_missing_key() {
        let sm = sm_running_missing_key();
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let result =
            client.get_with_consistency(b"missing", ReadConsistencyPolicy::LeaseRead).await;
        assert_eq!(result.unwrap(), None);

        drop(client);
        handle.await.unwrap();
    }

    /// LeaseRead with revoked lease → ReadActor returns LeaseInvalid → fallback to cmd_tx.
    /// cmd_tx closed → error proves fallback was triggered.
    #[tokio::test]
    async fn test_get_lease_fallback_when_lease_revoked() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        // get_multi() must not be called — ReadActor short-circuits on LeaseInvalid
        sm.expect_get_multi().never();

        let (client, handle) = client_with_read_actor(sm, revoked_lease(), true).await;
        let result = client.get_with_consistency(b"k", ReadConsistencyPolicy::LeaseRead).await;
        assert!(result.is_err(), "revoked lease must fall back to cmd_tx");

        drop(client);
        handle.await.unwrap();
    }

    /// LeaseRead with expired deadline → ReadActor returns LeaseInvalid → fallback.
    #[tokio::test]
    async fn test_get_lease_fallback_when_lease_expired() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi().never();

        let (client, handle) = client_with_read_actor(sm, expired_lease(), true).await;
        let result = client.get_with_consistency(b"k", ReadConsistencyPolicy::LeaseRead).await;
        assert!(result.is_err(), "expired lease must fall back to cmd_tx");

        drop(client);
        handle.await.unwrap();
    }

    /// LeaseRead with SM stopped → ReadActor returns LeaseInvalid → fallback.
    #[tokio::test]
    async fn test_get_lease_fallback_when_sm_not_running() {
        let sm = sm_stopped();
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;
        let result = client.get_with_consistency(b"k", ReadConsistencyPolicy::LeaseRead).await;
        assert!(result.is_err(), "non-running SM must fall back to cmd_tx");

        drop(client);
        handle.await.unwrap();
    }

    // ── EventualConsistency fast path ─────────────────────────────────────────

    /// EventualConsistency with running SM → fast path via ReadActor.
    /// Lease is irrelevant for Eventual reads.
    #[tokio::test]
    async fn test_get_eventual_bypasses_cmd_tx_when_sm_running() {
        let sm = sm_running_with_value(Bytes::from("stale_but_fast"));
        // Eventual does not check lease — use a revoked/empty lease to confirm
        let (client, handle) = client_with_read_actor(sm, revoked_lease(), true).await;

        let result = client
            .get_with_consistency(b"key1", ReadConsistencyPolicy::EventualConsistency)
            .await;
        assert_eq!(result.unwrap(), Some(Bytes::from("stale_but_fast")));

        drop(client);
        handle.await.unwrap();
    }

    /// EventualConsistency with SM stopped → ReadActor returns SmStopped → fallback.
    #[tokio::test]
    async fn test_get_eventual_fallback_when_sm_not_running() {
        let sm = sm_stopped();
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;
        let result = client
            .get_with_consistency(b"k", ReadConsistencyPolicy::EventualConsistency)
            .await;
        assert!(result.is_err(), "non-running SM must fall back to cmd_tx");

        drop(client);
        handle.await.unwrap();
    }

    // ── No fast path without read_tx ──────────────────────────────────────────

    /// Client without with_read_handle() (read_tx=None) always routes through cmd_tx.
    #[tokio::test]
    async fn test_no_fast_path_without_read_handle() {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        drop(cmd_rx);
        let client: EmbeddedClient<MockTypeConfig> =
            EmbeddedClient::new_internal(make_event_tx(), cmd_tx, 1, Duration::from_millis(100));

        let r1 = client.get_with_consistency(b"k", ReadConsistencyPolicy::LeaseRead).await;
        assert!(
            r1.is_err(),
            "without read_tx, must go through (closed) cmd_tx"
        );

        let r2 = client
            .get_with_consistency(b"k", ReadConsistencyPolicy::EventualConsistency)
            .await;
        assert!(
            r2.is_err(),
            "without read_tx, must go through (closed) cmd_tx"
        );
    }

    /// LinearizableRead always routes through cmd_tx, never ReadActor.
    #[tokio::test]
    async fn test_linearizable_always_routes_through_cmd_tx() {
        // ReadActor is running with a valid lease and SM — but Linearizable must NOT use it.
        let sm = sm_running_with_value(Bytes::from("should_not_see_this"));
        // cmd_tx closed → error proves Linearizable hit cmd_tx, not ReadActor
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let result =
            client.get_with_consistency(b"k", ReadConsistencyPolicy::LinearizableRead).await;
        assert!(result.is_err(), "LinearizableRead must always use cmd_tx");

        drop(client);
        handle.await.unwrap();
    }

    /// sm.get() returning Err is surfaced directly as ClientApiError::Business —
    /// SmError causes an immediate return without falling back to cmd_tx.
    ///
    /// Proof: cmd_rx is dropped (cmd_tx closed). A cmd_tx fallback would produce
    /// ClientApiError::Network (channel-closed); SmError produces ClientApiError::Business.
    /// The error variant is the discriminant.
    #[tokio::test]
    async fn test_sm_error_returned_directly_without_cmd_tx_fallback() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(|_| Err(d_engine_core::Error::Fatal("disk I/O failure".into())));

        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;
        let result = client
            .get_with_consistency(b"k", ReadConsistencyPolicy::EventualConsistency)
            .await;

        assert!(
            matches!(result, Err(d_engine_core::ClientApiError::Business { .. })),
            "SmError must produce Business error (direct return), not Network (cmd_tx fallback): {:?}",
            result
        );

        drop(client);
        handle.await.unwrap();
    }

    /// ReadActor channel closed (engine stopping) → fallback to cmd_tx.
    #[tokio::test]
    async fn test_fast_path_fallback_when_read_actor_channel_closed() {
        let sm = sm_running_with_value(Bytes::from("v"));
        let (read_tx, read_rx) = mpsc::channel::<crate::read_actor::ReadCmd>(1);
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        drop(cmd_rx); // cmd_tx closed for fallback verification

        // Drop read_rx immediately → channel is closed from the receiver side.
        drop(read_rx);

        let client: EmbeddedClient<MockTypeConfig> = EmbeddedClient::new_internal(
            make_event_tx(),
            cmd_tx.clone(),
            1,
            Duration::from_millis(100),
        )
        .with_read_handle(ReadHandle::new(Some(read_tx), cmd_tx));

        // Send to a closed channel → is_ok() == false → falls through to cmd_tx → error
        let result = client.get_with_consistency(b"k", ReadConsistencyPolicy::LeaseRead).await;
        assert!(
            result.is_err(),
            "closed read_tx must fall back to (closed) cmd_tx"
        );

        // Suppress unused SM warning
        drop(sm);
    }

    // ── get_multi_with_consistency fast path ──────────────────────────────────

    fn sm_running_with_multi_values(vals: Vec<(&'static [u8], &'static [u8])>) -> MockStateMachine {
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

    /// get_multi_with_consistency Eventual with valid SM → ReadActor fast path.
    /// cmd_rx dropped: Ok proves fast path was taken.
    #[tokio::test]
    async fn test_get_multi_eventual_fast_path_returns_values() {
        let sm = sm_running_with_multi_values(vec![(b"k1", b"v1"), (b"k2", b"v2")]);
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = client
            .get_multi_with_consistency(&keys, ReadConsistencyPolicy::EventualConsistency)
            .await;

        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(values[1], Some(Bytes::from_static(b"v2")));
        drop(client);
        handle.await.unwrap();
    }

    /// get_multi_with_consistency LeaseRead with valid lease → fast path.
    #[tokio::test]
    async fn test_get_multi_lease_read_fast_path_returns_values() {
        let sm = sm_running_with_multi_values(vec![(b"k1", b"v1")]);
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"missing")];
        let result =
            client.get_multi_with_consistency(&keys, ReadConsistencyPolicy::LeaseRead).await;

        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
        assert_eq!(values[1], None);
        drop(client);
        handle.await.unwrap();
    }

    /// get_multi_with_consistency LeaseRead revoked → fallback to cmd_tx.
    #[tokio::test]
    async fn test_get_multi_lease_revoked_falls_back_to_cmd_tx() {
        let sm = sm_running_with_multi_values(vec![(b"k1", b"v1")]);
        let (client, handle) = client_with_read_actor(sm, revoked_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1")];
        let result =
            client.get_multi_with_consistency(&keys, ReadConsistencyPolicy::LeaseRead).await;

        assert!(result.is_err(), "revoked lease must fall back to cmd_tx");
        drop(client);
        handle.await.unwrap();
    }

    /// get_multi_with_consistency LinearizableRead always uses cmd_tx.
    /// cmd_rx dropped: error proves cmd_tx was used (not ReadActor).
    #[tokio::test]
    async fn test_get_multi_linearizable_always_routes_through_cmd_tx() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        // no expect_get() — mockall panics if get() is called
        let (client, handle) = client_with_read_actor(sm, valid_lease(), true).await;

        let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
        let result = client
            .get_multi_with_consistency(&keys, ReadConsistencyPolicy::LinearizableRead)
            .await;

        assert!(result.is_err(), "LinearizableRead must always use cmd_tx");
        drop(client);
        handle.await.unwrap();
    }
}
