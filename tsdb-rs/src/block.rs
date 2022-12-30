use crate::index;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::BufReader,
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
};

use crate::chunks::{self, ChunkReader};

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
struct BlockMeta {
    ulid: String,
    #[serde(rename = "minTime")]
    min_time: i64,
    #[serde(rename = "maxTime")]
    max_time: i64,

    stats: BlockStats,
    compaction: BlockMetaCompaction,
    version: u64,
}

pub struct Block<CR: ChunkReader> {
    dir: String,
    meta: BlockMeta,
    num_byte_meta: u64,
    chunk_reader: CR,
    index_reader: index::Reader,
}

pub(crate) fn open(dir: String) -> anyhow::Result<Block<chunks::Reader>> {
    let (meta, num_byte_meta) = read_meta_file(&dir)?;
    let chunk_reader = chunks::Reader::build(PathBuf::from(&dir))?;
    let index_reader = index::Reader::build(&dir)?;

    Ok(Block {
        dir,
        meta,
        num_byte_meta,
        chunk_reader,
        index_reader,
    })
}

const META_FILE_NAME: &str = "meta.json";
const META_VERSION1: u64 = 1;

fn read_meta_file(dir: &str) -> Result<(BlockMeta, u64)> {
    log::debug!("reading meta file in {}", dir);

    let meta_path = Path::new(dir).join(META_FILE_NAME);
    let b = File::open(meta_path).map_err(|e| anyhow!(e))?;
    let size = b.metadata().map(|v| v.size()).map_err(|e| anyhow!(e))?;

    let meta: BlockMeta = serde_json::from_reader(BufReader::new(b)).map_err(|e| anyhow!(e))?;
    if meta.version != META_VERSION1 {
        return Err(anyhow!("unexpected meta file version {:?}", meta.version));
    }

    Ok((meta, size))
}

#[cfg(test)]
mod tests {
    use super::*;
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
            ulid: "01DXXFZDYD1MQW6079WK0K6EDQ".to_string(),
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
}
