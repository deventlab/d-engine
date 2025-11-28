//! Unit tests for leader change notification mechanism

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    use crate::election::ElectionHandler;
    use crate::replication::ReplicationHandler;
    use crate::state_machine_handler::DefaultStateMachineHandler;
    use crate::storage::log_store::memory_log_store::MemoryLogStore;
    use crate::storage::meta_store::memory_meta_store::MemoryMetaStore;
    use crate::test_utils::in_memory_state_machine::InMemoryStateMachine;
    use crate::test_utils::local_transport::LocalTransport;
    use crate::test_utils::membership::InMemoryMembership;
    use crate::test_utils::purge_executor::NoPurgeExecutor;
    use crate::{
        Raft, RaftContext, RaftCoreHandlers, RaftEvent, RaftNodeConfig, RaftRole, RaftStorageHandles,
        RoleEvent, SignalParams, TypeConfig,
    };

    // Test TypeConfig
    struct TestTypeConfig;
    impl TypeConfig for TestTypeConfig {
        type LogStore = MemoryLogStore;
        type MetaStore = MemoryMetaStore;
        type StateMachine = InMemoryStateMachine;
        type Transport = LocalTransport;
        type Membership = InMemoryMembership;
    }

    async fn create_test_raft() -> (
        Raft<TestTypeConfig>,
        mpsc::UnboundedReceiver<(Option<u32>, u64)>,
    ) {
        let node_id = 1;
        let (role_tx, role_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel(100);
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let log_store = Arc::new(MemoryLogStore::new());
        let meta_store = Arc::new(MemoryMetaStore::new());
        let state_machine = Arc::new(InMemoryStateMachine::new());
        let transport = Arc::new(LocalTransport::new(node_id));
        let membership = Arc::new(InMemoryMembership::new(node_id));

        let my_role = RaftRole::new_follower(node_id, 0, None, log_store.clone(), meta_store.clone());

        let state_machine_handler = Arc::new(DefaultStateMachineHandler::new(
            node_id,
            state_machine.clone(),
            log_store.clone(),
        ));

        let ctx = RaftContext {
            node_id,
            storage: RaftStorageHandles {
                raft_log: log_store.clone(),
                state_machine: state_machine.clone(),
            },
            transport,
            membership,
            handlers: RaftCoreHandlers {
                election_handler: ElectionHandler::new(node_id),
                replication_handler: ReplicationHandler::new(node_id),
                state_machine_handler,
                purge_executor: Arc::new(NoPurgeExecutor),
            },
            node_config: Arc::new(RaftNodeConfig::default()),
        };

        let signal_params = SignalParams::new(role_tx, role_rx, event_tx, event_rx, shutdown_rx);

        let mut raft = Raft::new(node_id, my_role, ctx.storage, ctx.transport, ctx.handlers, ctx.membership, signal_params, ctx.node_config);

        // Register leader change listener
        let (leader_change_tx, leader_change_rx) = mpsc::unbounded_channel();
        raft.register_leader_change_listener(leader_change_tx);

        (raft, leader_change_rx)
    }

    #[tokio::test]
    async fn test_leader_change_listener_registration() {
        let (raft, _rx) = create_test_raft().await;
        // If we can create raft with listener, registration works
        assert_eq!(raft.node_id, 1);
    }

    #[tokio::test]
    async fn test_notify_leader_change_become_follower_with_leader() {
        let (raft, mut rx) = create_test_raft().await;

        // Simulate becoming follower with known leader
        let leader_id = Some(2);
        let term
