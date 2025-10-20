use crate::common::{
    entry_payload::Payload, membership_change::Change, EntryPayload, MembershipChange, Noop,
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
}
