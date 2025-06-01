mod append_entries;
mod client_manager;
mod cluster_start_stop;
mod common;
mod election;
mod snapshot;

static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
    env_logger::init();
});

pub fn enable_logger() {
    *LOGGER_INIT;
    println!("setup logger for unit test.");
}

// Unit test port base: 6xxxx
// Integration test port base: 3xxxx
#[allow(dead_code)]
pub(crate) const CLUSTER_PORT_BASE: u64 = 30100;
pub(crate) const APPEND_ENNTRIES_PORT_BASE: u64 = 30200;
pub(crate) const ELECTION_PORT_BASE: u64 = 30300;
