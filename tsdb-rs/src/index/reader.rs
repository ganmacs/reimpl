use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ReadBytesExt};
use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::Path;

pub struct Reader {
    inner: File,
}

const HEADER_LEN: u64 = 5;
const MAGIC_INDEX: u32 = 0xBAAAD700;
const FORMAT_V1: u8 = 1;
const FORMAT_V2: u8 = 1;

impl Reader {
    pub fn build<P: AsRef<Path>>(dir: P) -> Result<Reader> {
        let mut file = File::open(dir).map_err(|e| anyhow!(e))?;
        let size = file.metadata().map_err(|e| anyhow!(e))?.size();

        if HEADER_LEN > size {
            return Err(anyhow!("invalid size: invalid index header {:?}", size));
        }

        let magic_index = file.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;
        if magic_index != MAGIC_INDEX {
            return Err(anyhow!("invalid magic index {:#x}", magic_index));
        }

        let version = file.read_u8().map_err(|e| anyhow!(e))?;
        if version != FORMAT_V1 && version != FORMAT_V2 {
            return Err(anyhow!("invalid version {:?}", version));
        }

        Ok(Reader { inner: file })
    }
}
