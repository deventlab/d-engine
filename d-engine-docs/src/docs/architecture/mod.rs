//! # Architecture Documentation
//!
//! This section covers D-Engine's architecture and design principles. These documents
//! explain core components, how they interact, and the design decisions behind them.
//!
//! ## Topics
//!
//! - [Single Responsibility Principle](self::single_responsibility_principle) - Core design
//!   philosophy
//! - [Error Handling Design](self::error_handling_design_principles) - Error management approach
//! - [Raft Role Implementation](self::raft_role) - Leader, follower and candidate behaviors
//! - [Snapshot Design](self::snapshot_module_design) - State transfer and recovery
//! - [Node Joining](self::new_node_join_architecture) - Dynamic cluster membership
//! - [Election Design](self::election_design) - Leader election process
//! - [Log Persistence](self::raft_log_persistence_architecture) - Durable storage design

pub mod single_responsibility_principle {
    #![doc = include_str!("single-responsibility-principle.md")]
}

pub mod error_handling_design_principles {
    #![doc = include_str!("error-handling-design-principles.md")]
}

pub mod raft_role {
    #![doc = include_str!("raft-role.md")]
}

pub mod snapshot_module_design {
    #![doc = include_str!("snapshot-module-design.md")]
}

pub mod node_join_architecture {
    #![doc = include_str!("node-join-architecture.md")]
}

pub mod election_design {
    #![doc = include_str!("election-design.md")]
}

pub mod raft_log_persistence_architecture {
    #![doc = include_str!("raft-log-persistence-architecture.md")]
}
