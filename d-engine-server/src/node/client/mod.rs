//! Client APIs for embedded d-engine access.

mod local_kv;
#[cfg(test)]
mod local_kv_test;

pub use local_kv::LocalClientError;
pub use local_kv::LocalKvClient;
