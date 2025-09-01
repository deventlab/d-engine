mod append_entries;
mod client_manager;
mod cluster_start_stop;
mod common;
mod election;
mod join_cluster;
mod snapshot;

// Unit test port base: 6xxxx
// Integration test port base: 3xxxx
pub(crate) const CLUSTER_PORT_BASE: u16 = 30100;
pub(crate) const APPEND_ENNTRIES_PORT_BASE: u16 = 30200;
pub(crate) const ELECTION_PORT_BASE: u16 = 30300;
pub(crate) const JOIN_CLUSTER_PORT_BASE: u16 = 30400;
pub(crate) const SNAPSHOT_PORT_BASE: u16 = 30500;
