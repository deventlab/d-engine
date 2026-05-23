//! Native client API types — transport-agnostic, no proto dependency.
//!
//! # Design rationale
//!
//! These types form the boundary between d-engine-core and any transport layer
//! (gRPC today, potentially HTTP/QUIC in the future).  By keeping them proto-free,
//! adding a new transport only requires a new adapter crate; d-engine-core itself
//! needs no changes.
//!
//! Raft-internal protocol types (AppendEntriesRequest, VoteRequest, Entry, LogId …)
//! are intentionally NOT here — they remain as proto-generated types because they
//! are d-engine's persistent log format, not a transport concern.

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

// ReadConsistencyPolicy already exists as a native type in crate::config; re-export it
// here so all client-facing types live in one module.
pub use crate::config::ReadConsistencyPolicy;

// ─── Write request ────────────────────────────────────────────────────────────

/// A KV write operation submitted by a client.
///
/// Passed from the transport layer into core via `ClientCmd::Propose`.
/// Core serializes `command` to Raft log bytes internally; the transport layer
/// never needs to touch prost or proto types.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientWriteRequest {
    pub client_id: u32,
    pub command: Option<WriteOperation>,
}

/// Decoded write operation — the unit submitted by a client.
///
/// Mirrors proto `WriteCommand` in shape but carries no prost annotations.
/// Core owns the serialization to Raft log bytes (`WriteOperation → proto::WriteCommand → bytes`);
/// transport adapters work with this native type only.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOperation {
    Insert {
        key: Bytes,
        value: Bytes,
        /// `None` = no expiration. Proto encodes this as `ttl_secs = 0`.
        ttl_secs: Option<u64>,
    },
    Delete {
        key: Bytes,
    },
    CompareAndSwap {
        key: Bytes,
        /// `None` means the key must not exist for the swap to succeed.
        expected: Option<Bytes>,
        new_value: Bytes,
    },
}

// ─── Read request ─────────────────────────────────────────────────────────────

/// A KV read request submitted by a client.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientReadRequest {
    pub client_id: u32,
    pub keys: Vec<Bytes>,
    /// When `None`, the cluster's configured default policy applies.
    pub consistency_policy: Option<ReadConsistencyPolicy>,
}

// ─── Response ─────────────────────────────────────────────────────────────────

/// Response returned to the client after a write or read.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientResponse {
    pub error: ErrorCode,
    /// Present when `error == LeaderChanged` to help the client reconnect.
    pub leader_hint: Option<LeaderHint>,
    /// Suggested retry delay in milliseconds, when present.
    pub retry_after_ms: Option<u64>,
    /// Present on success only.
    pub result: Option<ClientResponsePayload>,
}

/// Payload carried in a successful `ClientResponse`.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientResponsePayload {
    Write(WriteResult),
    Read(ReadResults),
}

/// Result of a successful write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub succeeded: bool,
}

/// Results of a successful read operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadResults {
    pub entries: Vec<KvEntry>,
}

/// A single key-value pair returned in a read response.
#[derive(Debug, Clone, PartialEq)]
pub struct KvEntry {
    pub key: Bytes,
    pub value: Bytes,
}

// ─── Error / hint types ───────────────────────────────────────────────────────

/// Leader location hint — helps the client redirect after a `LeaderChanged` error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaderHint {
    pub leader_id: u32,
    pub address: String,
}

/// Client-facing error codes — transport-agnostic, mirrors proto `ErrorCode`.
///
/// Values are intentionally kept in sync with proto so that server-layer
/// conversions (`core::ErrorCode ↔ proto::ErrorCode`) are trivial numeric casts.
/// If a new variant is added here, add it to proto's `.proto` file too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum ErrorCode {
    // Success
    Success = 0,
    // Network layer (1000-1999)
    ConnectionTimeout = 1001,
    InvalidAddress = 1002,
    LeaderChanged = 1003,
    JoinError = 1004,
    // Protocol layer (2000-2999)
    InvalidResponse = 2001,
    VersionMismatch = 2002,
    // Storage layer (3000-3999)
    DiskFull = 3001,
    DataCorruption = 3002,
    StorageIoError = 3003,
    StoragePermissionDenied = 3004,
    KeyNotExist = 3005,
    // Business logic (4000-4999)
    NotLeader = 4001,
    StaleOperation = 4002,
    InvalidRequest = 4003,
    RateLimited = 4004,
    ClusterUnavailable = 4005,
    ProposeFailed = 4006,
    TermOutdated = 4007,
    RetryRequired = 4008,
    // Watch layer (5000-5999)
    WatchBufferOverflow = 5001,
    // Unclassified
    General = 8888,
    Uncategorized = 9999,
}

// ─── ErrorCode numeric conversion ─────────────────────────────────────────────

impl TryFrom<i32> for ErrorCode {
    type Error = ();
    fn try_from(n: i32) -> Result<Self, ()> {
        match n {
            0 => Ok(Self::Success),
            1001 => Ok(Self::ConnectionTimeout),
            1002 => Ok(Self::InvalidAddress),
            1003 => Ok(Self::LeaderChanged),
            1004 => Ok(Self::JoinError),
            2001 => Ok(Self::InvalidResponse),
            2002 => Ok(Self::VersionMismatch),
            3001 => Ok(Self::DiskFull),
            3002 => Ok(Self::DataCorruption),
            3003 => Ok(Self::StorageIoError),
            3004 => Ok(Self::StoragePermissionDenied),
            3005 => Ok(Self::KeyNotExist),
            4001 => Ok(Self::NotLeader),
            4002 => Ok(Self::StaleOperation),
            4003 => Ok(Self::InvalidRequest),
            4004 => Ok(Self::RateLimited),
            4005 => Ok(Self::ClusterUnavailable),
            4006 => Ok(Self::ProposeFailed),
            4007 => Ok(Self::TermOutdated),
            4008 => Ok(Self::RetryRequired),
            5001 => Ok(Self::WatchBufferOverflow),
            8888 => Ok(Self::General),
            9999 => Ok(Self::Uncategorized),
            _ => Err(()),
        }
    }
}

// ─── ClientResponse constructors ─────────────────────────────────────────────

impl ClientResponse {
    #[inline]
    pub fn write_success() -> Self {
        Self {
            error: ErrorCode::Success,
            leader_hint: None,
            retry_after_ms: None,
            result: Some(ClientResponsePayload::Write(WriteResult {
                succeeded: true,
            })),
        }
    }

    #[inline]
    pub fn cas_failure() -> Self {
        Self {
            error: ErrorCode::Success,
            leader_hint: None,
            retry_after_ms: None,
            result: Some(ClientResponsePayload::Write(WriteResult {
                succeeded: false,
            })),
        }
    }

    #[inline]
    pub fn read_results(entries: Vec<KvEntry>) -> Self {
        Self {
            error: ErrorCode::Success,
            leader_hint: None,
            retry_after_ms: None,
            result: Some(ClientResponsePayload::Read(ReadResults { entries })),
        }
    }

    #[inline]
    pub fn client_error(error: ErrorCode) -> Self {
        Self {
            error,
            leader_hint: None,
            retry_after_ms: None,
            result: None,
        }
    }

    #[inline]
    pub fn not_leader(leader_hint: Option<LeaderHint>) -> Self {
        Self {
            error: ErrorCode::NotLeader,
            leader_hint,
            retry_after_ms: None,
            result: None,
        }
    }

    #[inline]
    pub fn is_write_success(&self) -> bool {
        self.error == ErrorCode::Success
            && matches!(
                self.result,
                Some(ClientResponsePayload::Write(WriteResult {
                    succeeded: true
                }))
            )
    }

    #[inline]
    pub fn is_term_outdated(&self) -> bool {
        self.error == ErrorCode::TermOutdated
    }

    #[inline]
    pub fn is_propose_failure(&self) -> bool {
        self.error == ErrorCode::ProposeFailed
    }
}
