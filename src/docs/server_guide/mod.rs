//! # Server Guide
//!
//! Documentation for server operators and backend developers implementing
//! D-Engine in their systems. Covers deployment, configuration, and customization.
//!
//! ## Topics
//!
//! - [Custom Storage Engines](self::customize_storage_engine) - Implementing storage backends
//! - [Custom State Machines](self::customize_state_machine) - Building application-specific state machines

pub mod customize_storage_engine {
    #![doc = include_str!("customize-storage-engine.md")]
}

pub mod customize_state_machine {
    #![doc = include_str!("customize-state-machine.md")]
}
