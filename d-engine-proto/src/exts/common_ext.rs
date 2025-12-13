use crate::common::{
    EntryPayload, MembershipChange, Noop, entry_payload::Payload, membership_change::Change,
};
use bytes::Bytes;

impl EntryPayload {
    pub fn command(command: Bytes) -> Self {
        Self {
            payload: Some(Payload::Command(command)),
        }
    }
    pub fn noop() -> Self {
        Self {
            payload: Some(Payload::Noop(Noop {})),
        }
    }

    pub fn config(change: Change) -> Self {
        Self {
            payload: Some(Payload::Config(MembershipChange {
                change: Some(change),
            })),
        }
    }

    pub fn is_config(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => false,
            Some(Payload::Config(_)) => true,
            Some(Payload::Noop(_)) => false,
            None => false,
        }
    }

    pub fn is_command(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => true,
            Some(Payload::Config(_)) => false,
            Some(Payload::Noop(_)) => false,
            None => false,
        }
    }

    pub fn is_noop(&self) -> bool {
        match self.payload {
            Some(Payload::Command(_)) => false,
            Some(Payload::Config(_)) => false,
            Some(Payload::Noop(_)) => true,
            None => false,
        }
    }
}
