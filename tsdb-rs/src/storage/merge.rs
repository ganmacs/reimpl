use super::interface::{Querier, SeriesSet};
use crate::model::labels::matcher::Matcher;
use crate::querier::BlockQuerier;
use anyhow::Result;

pub struct MergeGenericQuerier<Q: Querier> {
    queriers: Vec<Q>,
}

pub fn new_generic_querier(queriers: Vec<BlockQuerier>) -> MergeGenericQuerier<BlockQuerier> {
    return MergeGenericQuerier { queriers };
}

impl<Q: Querier> Querier for MergeGenericQuerier<Q> {
    fn select(&mut self, matchers: Vec<Matcher>) -> Result<SeriesSet> {
        if self.queriers.len() == 0 {
            return Ok(SeriesSet::NoopSeriesSet);
        } else if self.queriers.len() == 1 {
            return self.queriers[0].select(matchers);
        }

        todo!()
    }
}
