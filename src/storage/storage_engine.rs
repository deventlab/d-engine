use crate::proto::common::{Entry, LogId};
use crate::Result;
use std::ops::RangeInclusive;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait StorageEngine: Send + Sync + 'static {
    fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()>;

    fn get_entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>>;

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>>;

    fn purge_logs(
        &self,
        cutoff_index: LogId,
    ) -> Result<()>;

    fn flush(&self) -> Result<()>;

    fn reset(&self) -> Result<()>;

    fn last_index(&self) -> u64;

    /// Truncates log from specified index onward
    fn truncate(
        &self,
        from_index: u64,
    ) -> Result<()>;

    #[cfg(test)]
    fn db_size(&self) -> Result<u64>;

    #[cfg(test)]
    fn len(&self) -> usize;

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
