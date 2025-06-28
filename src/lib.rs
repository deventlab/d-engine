#![doc = include_str!("docs/overview.md")]

// #![warn(missing_docs)]

pub mod client;
pub mod config;
mod docs;
pub mod node;
pub mod proto;
#[doc(hidden)]
pub mod storage;

#[doc(hidden)]
pub use client::*;
#[doc(hidden)]
pub use config::*;
#[doc(hidden)]
pub use node::*;

mod constants;
mod core;
mod errors;
mod membership;
mod metrics;
mod network;
mod type_config;

#[doc(hidden)]
pub use core::*;

pub use errors::*;
pub(crate) use membership::*;
pub(crate) use metrics::*;
pub(crate) use network::*;
pub(crate) use storage::*;

#[doc(hidden)]
pub mod utils;
#[doc(hidden)]
pub use type_config::*;
#[doc(hidden)]
pub use utils::*;

//-----------------------------------------------------------
// Test utils
#[cfg(test)]
#[doc(hidden)]
pub mod test_utils;

//-----------------------------------------------------------
// Autometrics
/// autometrics: https://docs.autometrics.dev/rust/adding-alerts-and-slos
use autometrics::objectives::Objective;
use autometrics::objectives::ObjectiveLatency;
use autometrics::objectives::ObjectivePercentile;

#[doc(hidden)]
const API_SLO: Objective = Objective::new("api")
    .success_rate(ObjectivePercentile::P99_9)
    .latency(ObjectiveLatency::Ms10, ObjectivePercentile::P99);
