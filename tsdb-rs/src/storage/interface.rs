use super::merge::MergeGenericQuerier;
use crate::model::labels::matcher::Matcher;
use crate::querier::{BlockQuerier, BlockSeriesSet, ChunkSeriesEntry};
use anyhow::Result;

pub trait Querier {
    fn select(&mut self, matchers: Vec<Matcher>) -> Result<SeriesSet>;
}

pub enum SeriesSet {
    BlockSeriesSet(BlockSeriesSet),
    NoopSeriesSet,
}

impl Querier for BlockQuerier {
    fn select(&mut self, matchers: Vec<Matcher>) -> Result<SeriesSet> {
        return self
            .inner_select(matchers)
            .map(|v| SeriesSet::BlockSeriesSet(v));
    }
}

impl Iterator for SeriesSet {
    type Item = ChunkSeriesEntry;

    fn next(&mut self) -> Option<ChunkSeriesEntry> {
        use SeriesSet::*;

        match self {
            BlockSeriesSet(v) => v.next(),
            NoopSeriesSet => None,
        }
    }
}
