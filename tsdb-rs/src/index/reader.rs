use super::error::IndexError as IndexHeaderError;
use super::{
    super::seek_byte::SeekReadBytesExt, FORMAT_V1, FORMAT_V2, HEADER_LEN, INDEX_TOC_LEN,
    MAGIC_INDEX,
};
use anyhow::{anyhow, ensure, Result};
use byteorder::{BigEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

const INDEX_FILE_NAME: &str = "index";

const CRC32_TABLE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

pub struct Reader {
    inner: File,
    toc: Toc,
}

impl Reader {
    pub fn build<P: AsRef<Path>>(dir: P) -> Result<Reader> {
        let mut file = File::open(dir).map_err(|e| anyhow!(e))?;

        let size = file.metadata().map_err(|e| anyhow!(e))?.len();
        ensure!(HEADER_LEN > size, IndexHeaderError::InvalidSize(size));

        let magic_index = file.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;
        ensure!(
            magic_index != MAGIC_INDEX,
            IndexHeaderError::InvalidMagicNumber(magic_index)
        );

        let version = file.read_u8().map_err(|e| anyhow!(e))?;
        ensure!(
            version != FORMAT_V1 && version != FORMAT_V2,
            IndexHeaderError::InvalidIndexVersion(version)
        );

        let toc = new_toc(&mut file)?;

        Ok(Reader { inner: file, toc })
    }
}

#[derive(Debug, PartialEq)]
struct Toc {
    symbols: u64,
    series: u64,
    lable_indeices: u64,
    label_indices_table: u64,
    postings: u64,
    posting_stable: u64,
}

fn new_toc(file: &mut File) -> Result<Toc> {
    let size = file.metadata().map_err(|e| anyhow!(e))?.len();
    ensure!(INDEX_TOC_LEN < size, IndexHeaderError::InvalidTocSize(size));

    // load toc at once
    let mut toc_buf = vec![0; INDEX_TOC_LEN as usize];
    file.read_exact_at(&mut toc_buf, size - INDEX_TOC_LEN)
        .map_err(|e| anyhow!(e))?;

    let mut buf = io::Cursor::new(toc_buf);
    let mut content = vec![0_u8; 6 * 8];
    buf.read_exact(&mut content).map_err(|e| anyhow!(e))?;

    let actual = CRC32_TABLE.checksum(&content);
    let expected_crc = buf.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;

    ensure!(
        expected_crc == actual,
        IndexHeaderError::InvalidChucksum(expected_crc, actual)
    );

    buf.seek(SeekFrom::Start(0)).map_err(|e| anyhow!(e))?;
    Ok(Toc {
        symbols: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
        series: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
        lable_indeices: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
        label_indices_table: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
        postings: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
        posting_stable: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
    })
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
    fn test_new_toc() {
        init();

        let path = Path::new("tests/index_format_v1").join(INDEX_FILE_NAME);
        let mut file = File::open(path).unwrap();
        let toc = new_toc(&mut file).unwrap();
        assert_eq!(
            Toc {
                symbols: 5,
                series: 323,
                lable_indeices: 1806,
                label_indices_table: 4300,
                postings: 2248,
                posting_stable: 4326,
            },
            toc,
        )
    }
}
