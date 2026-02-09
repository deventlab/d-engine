//! # Leader Tick and Heartbeat Piggybacking Integration Tests
//!
//! Verifies the Leader's tick behavior in the drain-based batch architecture:
//! 1. Heartbeat as a secondary flush trigger (primary is drain-based recv/try_recv)
//! 2. Heartbeat piggybacking: capturing buffered commands during heartbeat
//! 3. Empty buffer heartbeat: maintaining Leader status without pending commands
//! 4. Metrics distinction: correctly tracking drain vs heartbeat triggers
//!
//! ## Architecture Context
//!
//! In drain-based architecture:
//! - **Primary flush path**: Drain via recv() blocking on command channel, then try_recv() draining pending
//! - **Secondary flush path**: Heartbeat timer (safety net for buffered commands)
//! - **Heartbeat interval**: Configurable (default 100ms), must be < election timeout (300ms)
//!
//! Single-node embedded mode eliminates cluster coordination complexity, allowing focus
//! on tick/heartbeat mechanics.
//!
//! ## Test Categories
//!
//! - **T1**: Empty buffer heartbeat - leader maintains status with no pending commands
//! - **T2**: Heartbeat piggybacking - commands flushed via heartbeat trigger
//! - **T3**: Drain as primary path - high load verification (drain >> heartbeat ratio)
//! - **T4**: Heartbeat interval precision - timer accuracy and consistency

mod tick_and_heartbeat_embedded;
