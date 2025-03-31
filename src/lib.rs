mod client;
mod config;
mod core;
mod membership;
mod metrics;
mod network;
mod node;
mod storage;
mod type_config;
pub mod utils;

pub use core::*;

pub use client::*;
pub use config::*;
pub use membership::*;
pub use metrics::*;
pub use network::*;
pub use node::*;
pub use storage::*;
pub use type_config::*;
pub use utils::*;

//-----------------------------------------------------------
// Test utils

#[cfg(test)]
pub mod test_utils;
//-----------------------------------------------------------
// Autometrics
/// autometrics: https://docs.autometrics.dev/rust/adding-alerts-and-slos
use autometrics::objectives::Objective;
use autometrics::objectives::ObjectiveLatency;
use autometrics::objectives::ObjectivePercentile;
const API_SLO: Objective = Objective::new("api")
    .success_rate(ObjectivePercentile::P99_9)
    .latency(ObjectiveLatency::Ms10, ObjectivePercentile::P99);

//-----------------------------------------------------------
// This will ensure `env_logger` is only initialized once.

#[derive(Debug)]
pub struct NewLeaderInfo {
    pub term: u64,
    pub leader_id: u32,
}
