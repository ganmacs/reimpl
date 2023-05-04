use crate::block;
use crate::index::{IndexReader, Postings};
use crate::model::labels::{matcher::Matcher, Labels};
use anyhow::{anyhow, Result};
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use ulid::Ulid;

pub(crate) fn open(b: Arc<block::Block>) -> BlockQuerier {
    return BlockQuerier::new(b.as_ref());
}

pub struct BlockQuerier {
    block_id: Ulid,
    index: Arc<RwLock<IndexReader>>,
}

#[derive(Debug)]
pub struct ChunkSeriesEntry {
    labels: Labels,
}

impl BlockQuerier {
    pub(crate) fn new(block: &block::Block) -> Self {
        BlockQuerier {
            block_id: block.meta().ulid,
            index: block.index(),
        }
    }

    pub fn inner_select(&mut self, matchers: Vec<Matcher>) -> Result<BlockSeriesSet> {
        let index_reader = self.index.clone();
        let mut reader = index_reader.write().map_err(|e| anyhow!(e.to_string()))?;
        let postings = postings_for_matchers(reader.deref_mut(), matchers)?;

        let index_reader2 = self.index.clone();
        return Ok(BlockSeriesSet::new(index_reader2, postings));
    }
}


pub struct BlockSeriesSet {
    index: Arc<RwLock<IndexReader>>,
    postings: Postings,
}

impl BlockSeriesSet {
    fn new(index: Arc<RwLock<IndexReader>>, postings: Postings) -> Self {
        Self { index, postings }
    }
}

impl Iterator for BlockSeriesSet {
    type Item = ChunkSeriesEntry;

    fn next(&mut self) -> Option<ChunkSeriesEntry> {
        if let Some(p) = self.postings.next() {
            let mut v = self
                .index
                .write()
                .map_err(|e| anyhow!(e.to_string()))
                .unwrap();

            if let Ok(sers) = v.series(p) {
                Some(ChunkSeriesEntry {
                    labels: sers.labels(),
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

fn postings_for_matchers(
    index_reader: &mut IndexReader,
    matchers: Vec<Matcher>,
) -> Result<Postings> {
    let mut its = vec![];
    for m in matchers {
        // TODO: optimize
        its.push(postings_for_matcher(index_reader, m)?);
    }

    return Ok(Postings::new_intersect(its));
}

fn postings_for_matcher(index_reader: &mut IndexReader, matcher: Matcher) -> Result<Postings> {
    match matcher {
        Matcher::MatchEqual(m) => index_reader.postings(&m.name, vec![&m.value]),
        Matcher::MatchNotEqual(m) => {
            todo!();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block;
    use block::Block;
    use std::path::Path;

    #[test]
    fn test_block_querier() {
        // run in prometheus/prometheus
        // v, _ := os.Getwd()
        // db, _ := Open(filepath.Join(v, "tsdb-test"), nil, nil, DefaultOptions(), nil)
        // app := db.Appender(context.Background())

        // app.Append(0, labels.FromStrings("foo", "bar"), 1, 2)
        // app.Append(0, labels.FromStrings("foo", "baz"), 3, 4)
        // app.Append(0, labels.FromStrings("foo", "meh"), 1000*3600*4, 4)
        // for i := 0; i < 100; i++ {
        // 	app.Append(0, labels.FromStrings("bar", strconv.FormatInt(int64(i), 10)), 1000 + int64(i), 0)
        // }

        // app.Commit()
        // db.Compact()
        // db.Close()

        let path = Path::new("tests/index_format_v2/simple2/01GNXGKS4HSZSQ5KX88D79BJTN");
        let b = Block::open(&path).unwrap();
        let mut querier = BlockQuerier::new(&b);
        let mut ret = querier
            .inner_select(vec![Matcher::new_must_matcher("bar", "0")])
            .unwrap();

        assert_eq!(
            Labels::from_string(vec!["bar", "0"]).unwrap(),
            ret.next().unwrap().labels,
        );

        assert!(ret.next().is_none());
    }
}
