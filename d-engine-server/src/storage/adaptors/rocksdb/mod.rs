mod rocksdb_state_machine;
mod rocksdb_storage_engine;
mod rocksdb_unified_engine;

pub use rocksdb_state_machine::*;
pub use rocksdb_storage_engine::*;
pub use rocksdb_unified_engine::RocksDBUnifiedEngine;

#[cfg(test)]
mod rocksdb_state_machine_test;

#[cfg(test)]
mod rocksdb_storage_engine_test;

#[cfg(test)]
mod rocksdb_unified_engine_test;

// Column family names — single source of truth for all RocksDB adaptors
pub(super) const LOG_CF: &str = "logs";
pub(super) const META_CF: &str = "meta";
pub(super) const STATE_MACHINE_CF: &str = "state_machine";
pub(super) const STATE_MACHINE_META_CF: &str = "state_machine_meta";
