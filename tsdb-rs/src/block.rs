use crate::chunks;
use crate::index::IndexReader;
use anyhow::{anyhow, Context as _, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::{fs::File, io::BufReader};
use ulid::Ulid;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
struct BlockStats {
    #[serde(rename = "numSamples")]
    num_samples: i64,
    #[serde(rename = "numSeries")]
    num_series: i64,
    #[serde(rename = "numChunks")]
    num_chunks: i64,
    #[serde(rename = "numTombstones")]
    num_tombstones: Option<i64>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
struct BlockMetaCompaction {
    level: u64,
    sources: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub(crate) struct BlockMeta {
    pub(crate) ulid: Ulid,
    #[serde(rename = "minTime")]
    min_time: i64,
    #[serde(rename = "maxTime")]
    max_time: i64,

    stats: BlockStats,
    compaction: BlockMetaCompaction,
    version: u64,
}

pub struct Block {
    dir: PathBuf,
    meta: Arc<BlockMeta>,
    num_byte_meta: u64,
    chunk_reader: chunks::Reader,
    index_reader: Arc<RwLock<IndexReader>>,
}

pub(crate) const INDEX_FILE_NAME: &str = "index";

const META_FILE_NAME: &str = "meta.json";
const META_VERSION1: u64 = 1;

impl Block {
    pub(crate) fn open<P: AsRef<Path>>(p: &P) -> anyhow::Result<Block> {
        let (meta, num_byte_meta) = read_meta_file(p)?;
        let chunk_reader = chunks::Reader::build(p)?;
        let path = p.as_ref();
        let index_reader = Arc::new(RwLock::new(IndexReader::build(&path.join(INDEX_FILE_NAME))?));
        let meta = Arc::new(meta);

        Ok(Block {
            dir: PathBuf::from(path),
            meta,
            num_byte_meta,
            chunk_reader,
            index_reader,
        })
    }

    pub(crate) fn index(&self) -> Arc<RwLock<IndexReader>> {
        self.index_reader.clone()
    }

    pub(crate) fn meta(&self) -> Arc<BlockMeta> {
        self.meta.clone()
    }
}

pub(super) fn read_meta_file<P: AsRef<Path>>(dir: P) -> Result<(BlockMeta, u64)> {
    let meta_path = dir.as_ref().join(META_FILE_NAME);
    let b = File::open(meta_path).map_err(|e| anyhow!(e))?;
    let size = b.metadata().map(|v| v.len()).map_err(|e| anyhow!(e))?;

    let meta: BlockMeta = serde_json::from_reader(BufReader::new(b)).map_err(|e| anyhow!(e))?;
    if meta.version != META_VERSION1 {
        return Err(anyhow!("unexpected meta file version {:?}", meta.version));
    }

    Ok((meta, size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::labels::matcher::Matcher;
    use crate::querier::BlockQuerier;
    use env_logger::Env;

    fn init() {
        let env = Env::default().default_filter_or("debug");
        let _ = env_logger::Builder::from_env(env).is_test(true).try_init();
    }

    #[test]
    fn test_read_meta_file() {
        init();

        let (actual_data, actual_size) = read_meta_file("tests/index_format_v1").unwrap();
        let expected = BlockMeta {
            ulid: Ulid::from_string("01DXXFZDYD1MQW6079WK0K6EDQ").unwrap(),
            version: 1,
            min_time: 0,
            max_time: 7200000,
            stats: BlockStats {
                num_samples: 102,
                num_series: 102,
                num_chunks: 102,
                num_tombstones: None,
            },
            compaction: BlockMetaCompaction {
                level: 1,
                sources: vec!["01DXXFZDYD1MQW6079WK0K6EDQ".to_string()],
            },
        };

        assert_eq!(expected, actual_data);
        assert_eq!(255, actual_size);
    }

    // #[test]
    // fn test_block_test() {
        // let block = Block::open(&"tests/index_format_v2/simple2").unwrap();
        // let mut querier = BlockQuerier::new(&block);

        // let ret = querier.select(vec![Matcher::new_must_matcher("foo", "bar")]);
    // }
}
