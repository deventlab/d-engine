use crate::proto::common::membership_change::Change;
use crate::proto::common::Entry;
use crate::proto::common::EntryPayload;

pub struct EntryBuilder {
    index: u64,
    term: u64,
}

impl EntryBuilder {
    pub fn new(
        start_index: u64,
        term: u64,
    ) -> Self {
        Self {
            index: start_index,
            term,
        }
    }

    pub fn command(
        mut self,
        data: &[u8],
    ) -> (Self, Entry) {
        let entry = Entry {
            index: self.index,
            term: self.term,
            payload: Some(EntryPayload::command(data.to_vec())),
        };
        self.index += 1;
        (self, entry)
    }

    pub fn config(
        mut self,
        change: Change,
    ) -> (Self, Entry) {
        let entry = Entry {
            index: self.index,
            term: self.term,
            payload: Some(EntryPayload::config(change)),
        };
        self.index += 1;
        (self, entry)
    }

    pub fn noop(mut self) -> (Self, Entry) {
        let entry = Entry {
            index: self.index,
            term: self.term,
            payload: Some(EntryPayload::noop()),
        };
        self.index += 1;
        (self, entry)
    }
}
