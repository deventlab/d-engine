use std::net::SocketAddr;
use std::net::TcpListener;

use d_engine_core::RaftNodeConfig;

/// A listen address on a port that is free right now.
///
/// Tests start many nodes in parallel processes. A fixed port, including the default one, makes
/// them collide: the node that loses the race cannot bind and `start()` fails. The operating
/// system picks the port here (bind to port 0) and the listener is released again, because the
/// node binds the address itself and config validation rejects port 0.
pub fn free_listen_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("ephemeral port has a local address")
}

/// Default node config for a test, listening on a free port.
///
/// The config is not validated: `start_node` validates it.
pub fn test_node_config() -> RaftNodeConfig {
    let mut config = RaftNodeConfig::new().expect("default config loads");
    config.cluster.listen_address = free_listen_address();
    config
}

/// Points `RaftNodeConfig::new()` at a free listen port for as long as the guard lives.
///
/// For tests that start a node through `EmbeddedEngine::start`, which takes no config argument and
/// always builds its config from the environment. The variable is process-wide, so the tests that
/// use it must not run in threads of one process at the same time: nextest runs every test in its
/// own process.
pub struct ListenAddressEnv;

impl ListenAddressEnv {
    const VAR: &'static str = "RAFT__CLUSTER__LISTEN_ADDRESS";

    pub fn set() -> Self {
        unsafe { std::env::set_var(Self::VAR, free_listen_address().to_string()) };
        Self
    }
}

impl Drop for ListenAddressEnv {
    fn drop(&mut self) {
        unsafe { std::env::remove_var(Self::VAR) };
    }
}
