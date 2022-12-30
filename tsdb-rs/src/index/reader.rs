use super::error::IndexError as IndexHeaderError;
use super::{FORMAT_V1, FORMAT_V2, HEADER_LEN, INDEX_TOC_LEN, MAGIC_INDEX};
use crate::seek_byte::SeekReadBytesExt;
use anyhow::{anyhow, ensure, Result};
use byteorder::{BigEndian, ReadBytesExt};
use integer_encoding::VarIntReader;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    ops::FnMut,
    path::Path,
    str,
};

const INDEX_FILE_NAME: &str = "index";

const CRC32_TABLE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

#[derive(Debug, PartialEq)]
struct PostingOffset {
    value: String,
    off: u64,
}

pub struct Reader {
    inner: File,
    toc: Toc,
    symbols: Symbols,
    postings: HashMap<String, Vec<PostingOffset>>,
}

impl Reader {
    pub fn build<P: AsRef<Path>>(dir: P) -> Result<Reader> {
        let mut file = File::open(dir).map_err(|e| anyhow!(e))?;

        let size = file.metadata().map_err(|e| anyhow!(e))?.len();
        ensure!(HEADER_LEN < size, IndexHeaderError::InvalidSize(size));

        let magic_index = file.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;
        ensure!(
            magic_index == MAGIC_INDEX,
            IndexHeaderError::InvalidMagicNumber(magic_index)
        );

        let version = file.read_u8().map_err(|e| anyhow!(e))?;
        ensure!(
            version == FORMAT_V1 || version == FORMAT_V2,
            IndexHeaderError::InvalidIndexVersion(version)
        );

        let toc = new_toc(&mut file)?;
        let symbols = new_symbols(&mut file, toc.symbols)?;
        let postings = if version == FORMAT_V1 {
            // TODO
            HashMap::new()
        } else {
            new_postings_offset_table_format_v2(&mut file, toc.postings_table)?
        };

        Ok(Reader {
            inner: file,
            toc,
            symbols,
            postings,
        })
    }
}

const SYMBOL_FACTOR: usize = 32;

#[derive(Debug, PartialEq)]
pub struct Symbols {
    inner: Vec<u8>,
    off: u64,

    offsets: Vec<u64>,
    seen: u64,
}

fn new_symbols(file: &mut File, offset: u64) -> Result<Symbols> {
    let buf = new_decbuf_at(file, offset, Some(CRC32_TABLE))?;
    let mut content = io::Cursor::new(&buf);
    let count = content.read_u32::<BigEndian>().map_err(|e| anyhow!(e))? as usize;

    let mut offsets = vec![];
    let mut seen = 0;
    while seen < count {
        if seen % SYMBOL_FACTOR == 0 {
            // skip len
            offsets.push(content.position() + offset + 4);
        }
        // consume position
        let _ = content.read_varint_bytes().map_err(|e| anyhow!(e))?;
        seen += 1;
    }

    Ok(Symbols {
        inner: buf,
        off: offset,
        offsets,
        seen: seen as u64,
    })
}

trait VarUintByte: VarIntReader + Read {
    fn read_varint_bytes(&mut self) -> io::Result<Vec<u8>> {
        let size = self.read_varint::<u64>()? as usize;
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        return Ok(buf);
    }
}

impl<R: VarIntReader + io::Read> VarUintByte for R {}

fn new_postings_offset_table_format_v2(
    file: &mut File,
    postings_offset: u64,
) -> Result<HashMap<String, Vec<PostingOffset>>> {
    let mut postings: HashMap<String, Vec<PostingOffset>> = HashMap::new();

    // last name, last value,last_off
    let mut prev_value: Option<(Vec<u8>, Vec<u8>, u64)> = None;
    let mut value_count = 0;

    new_postings_offset_table(
        file,
        postings_offset,
        // name are sorted
        |name: Vec<u8>, value: Vec<u8>, _: u64, off: u64, end: bool| {
            let name_str = to_string(&name)?;

            if end {
                return store_value(&mut postings, &mut prev_value);
            }

            if !postings.contains_key(&name_str) {
                postings.insert(name_str.clone(), vec![]);
                store_value(&mut postings, &mut prev_value)?;
                value_count = 0;
            }

            if value_count % SYMBOL_FACTOR == 0 {
                let value = to_string(&value)?;

                postings
                    .get_mut(&name_str)
                    .map(|v| v.push(PostingOffset { value, off }))
                    .ok_or(anyhow!("invalid postings"))?;
                prev_value = None;
            } else {
                prev_value = Some((name, value, off));
            }
            value_count += 1;

            return Ok(());
        },
    )?;

    return Ok(postings);
}

fn to_string<T: AsRef<[u8]>>(buf: T) -> Result<String> {
    str::from_utf8(buf.as_ref())
        .map(|v| v.to_string())
        .map_err(|e| anyhow!(e))
}

fn store_value(
    postings: &mut HashMap<String, Vec<PostingOffset>>,
    value: &mut Option<(Vec<u8>, Vec<u8>, u64)>,
) -> Result<()> {
    if let Some((l_name, l_value, l_off)) = value.take() {
        let value = to_string(&l_value)?;
        postings
            .get_mut(&to_string(&l_name)?)
            .map(|v| v.push(PostingOffset { value, off: l_off }))
            .ok_or(anyhow!("invalid postings table"))?;
    }

    Ok(())
}

fn new_postings_offset_table(
    file: &mut File,
    offset: u64,
    mut f: impl FnMut(Vec<u8>, Vec<u8>, u64, u64, bool) -> Result<()>,
) -> Result<()> {
    let buf = new_decbuf_at(file, offset, Some(CRC32_TABLE))?;
    let mut content = io::Cursor::new(&buf);
    let start = content.position();
    let count = content.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;

    for _ in 0..count {
        let label_offset = content.position() - start;
        let key_count = content.read_varint::<u64>().map_err(|e| anyhow!(e))?;
        ensure!(
            key_count == 2,
            "unexpected number of keys for postings offset table {:0}",
            key_count
        );

        let name = content.read_varint_bytes().map_err(|e| anyhow!(e))?;
        let value = content.read_varint_bytes().map_err(|e| anyhow!(e))?;
        let offset = content.read_varint::<u64>().map_err(|e| anyhow!(e))?;

        f(name, value, offset, label_offset, false)?;
    }

    // sentinel node
    f(vec![], vec![], 0, 0, true)?;

    Ok(())
}

// expect the following binary format
// byte len(4b) | content | (checksum(4b))?
fn new_decbuf_at(
    file: &mut File,
    offset: u64,
    crc32_table: Option<crc::Crc<u32>>,
) -> Result<Vec<u8>> {
    let size = file.metadata().map_err(|e| anyhow!(e))?.len();
    ensure!(
        offset + 4 < size,
        IndexHeaderError::InvalidBufSize(offset + 4, size)
    );

    let len = file
        .read_u32_at::<BigEndian>(offset)
        .map_err(|e| anyhow!(e))?;
    ensure!(
        // TODO: 4 is not needed when crc32 is not given.
        offset + 4 + (len as u64) + 4 < size,
        IndexHeaderError::InvalidBufSize(offset + 4, size)
    );

    let mut buf = vec![0; len as usize];
    file.read_exact(&mut buf).map_err(|e| anyhow!(e))?;

    if let Some(crc32) = crc32_table {
        let expected_crc = file.read_u32::<BigEndian>().map_err(|e| anyhow!(e))?;
        let actual = crc32.checksum(&buf);
        ensure!(
            actual == expected_crc,
            IndexHeaderError::InvalidChucksum(expected_crc, actual)
        );
    }

    Ok(buf)
}

#[derive(Debug, PartialEq)]
struct Toc {
    symbols: u64,
    series: u64,
    lable_indeices: u64,
    label_indices_table: u64,
    postings: u64,
    postings_table: u64,
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
        postings_table: buf.read_u64::<BigEndian>().map_err(|e| anyhow!(e))?,
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
    fn test_reader() {
        let path = Path::new("tests/index_format_v2/simple").join(INDEX_FILE_NAME);
        let reader = Reader::build(path).unwrap();
        let mut r = reader.postings.into_iter().collect::<Vec<_>>();
        r.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            vec![
                (
                    "".to_string(),
                    vec![PostingOffset {
                        value: "".to_string(),
                        off: 4
                    }]
                ),
                (
                    "a".to_string(),
                    vec![PostingOffset {
                        value: "1".to_string(),
                        off: 9
                    }]
                ),
                (
                    "b".to_string(),
                    vec![
                        PostingOffset {
                            value: "1".to_string(),
                            off: 16
                        },
                        PostingOffset {
                            value: "4".to_string(),
                            off: 37
                        }
                    ]
                )
            ],
            r,
        );
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
                postings_table: 4326,
            },
            toc,
        )
    }

    #[test]
    fn test_new_symbols() {
        init();

        let path = Path::new("tests/index_format_v1").join(INDEX_FILE_NAME);
        let mut file = File::open(path).unwrap();
        let toc = new_toc(&mut file).unwrap();

        let symbols = new_symbols(&mut file, toc.symbols).unwrap();
        assert_eq!(5, symbols.off);
        assert_eq!(104, symbols.seen);
        assert_eq!(vec![13, 105, 198, 291], symbols.offsets);
    }
}
