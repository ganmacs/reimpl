use anyhow::{anyhow, Result, ensure};
use byteorder::{BigEndian, ReadBytesExt};
use std::fs::{self, File};
use std::os::unix::prelude::MetadataExt;
use std::path::{Path, PathBuf};

use super::ChunkReader;

pub struct Reader {
    bs: Vec<File>,
    size: u64,
}

const MAGIC_CHUNK: u32 = 0x85BD40DD;
const MAGIC_CHUNK_SIZE: u64 = 4;
const CHUNKS_FORMAT_V1: u8 = 1;
const CHUNKS_FOMRAT_VERSION_SIZE: u64 = 1;
const SEGMENT_HEADER_PADING_SIZE: u64 = 3;
const SEGMENT_HEADER_SIZE: u64 =
    MAGIC_CHUNK_SIZE + CHUNKS_FOMRAT_VERSION_SIZE + SEGMENT_HEADER_PADING_SIZE;

impl Reader {
    pub fn build<P: AsRef<Path>>(dir: P) -> Result<Reader> {
        let mut open_files = vec![];
        let mut total_size = 0;

        for file in sequence_files(dir)? {
            let mut f = File::open(file).map_err(|e| anyhow!(e))?;
            let size = f.metadata().map_err(|e| anyhow!(e))?.size();

            if size < SEGMENT_HEADER_PADING_SIZE {
                return Err(anyhow!(
                    "invalid size: invalid segment header in segment {:?}",
                    size
                ));
            }

            // verify magic chunk
            let magic_chunk = f.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;
            if magic_chunk != MAGIC_CHUNK {
                return Err(anyhow!("invalid magic number {:#}", magic_chunk));
            }

            // verify chunk format version
            let version = f.read_u8().map_err(|e| anyhow!(e))?;
            if version != CHUNKS_FORMAT_V1 {
                return Err(anyhow!("invalid chunks version {:?}", version));
            }

            total_size += size;
            open_files.push(f);
        }

        Ok(Reader {
            bs: open_files,
            size: total_size,
        })
    }
}

fn sequence_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
    if !dir.as_ref().is_dir() {
        return Err(anyhow!("{:?} is not directory", dir.as_ref()));
    }

    let mut ret = vec![];
    for entry in fs::read_dir(dir).map_err(|e| anyhow!(e))? {
        if let Ok(ent) = entry {
            if let Some(Ok(_)) = ent.file_name().to_str().map(|f| f.parse::<u64>()) {
                ret.push(ent.path())
            }
        }
    }

    Ok(ret)
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
    fn test_sequence_files() {
        init();

        let path = Path::new("tests/index_format_v1/chunks");
        assert_eq!(
            vec![PathBuf::from("tests/index_format_v1/chunks/000001")],
            sequence_files(&path).unwrap()
        );
    }

    #[test]
    fn test_reader_new() {
        init();
        let path = Path::new("tests/index_format_v1/chunks");
        assert_eq!(1844, Reader::build(path).unwrap().size)
    }
}

impl ChunkReader for Reader {
    fn chunk() {}
    fn close() {}
}
