use super::error::IndexError as IndexHeaderError;
use super::postings::Postings;
use super::symbols::{self, Symbols, SYMBOL_FACTOR};
use super::{
    FormatVersion, CRC32_TABLE, FORMAT_V1, FORMAT_V2, HEADER_LEN, INDEX_TOC_LEN, MAGIC_INDEX,
};
use crate::seek_byte::{SeekReadBytesExt, VarUintByte};
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
    name_symbols: HashMap<u64, String>,
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
        let symbols = symbols::new(&mut file, FORMAT_V2, toc.symbols)?;
        let postings = if version == FORMAT_V1 {
            // TODO
            HashMap::new()
        } else {
            new_postings_offset_table_format_v2(&mut file, toc.postings_table)?
        };

        let mut name_symbols: HashMap<u64, String> = HashMap::new();
        for k in postings.keys() {
            if k == "" {
                continue;
            }

            let off = symbols
                .reverse_lookup(k)
                .map_err(|e| anyhow!("reverse symbol lookup {:?}", e))?;
            name_symbols.insert(off, k.to_string());
        }

        Ok(Reader {
            inner: file,
            toc,
            symbols,
            postings,
            name_symbols,
        })
    }

    // values are orderd?
    fn postings(&mut self, name: &str, values: Vec<&str>) -> Result<Postings> {
        let Some(postings) = self.postings.get(name) else {
            return Ok(Postings::new_empty());
        };

        if values.len() == 0 || postings.len() == 0 {
            return Ok(Postings::new_empty());
        }

        let mut value_index = 0;

        // skip values if the minimam posting table's value is greater than it.
        while value_index < values.len() && values[value_index] < postings[0].value.as_str() {
            value_index += 1;
        }

        if value_index == values.len() {
            return Ok(Postings::new_empty());
        }

        let mut res = vec![];
        let mut postings_tbl = new_decbuf_at(&mut self.inner, self.toc.postings_table, None)
            .map(|v| io::Cursor::new(v))?;

        let lable_name_end_offset = postings.last().unwrap().off;
        while value_index < values.len() {
            let i = match postings.binary_search_by(|p| p.value.as_str().cmp(values[value_index])) {
                Ok(v) => v,
                Err(0) => {
                    unreachable!("already checked above");
                }
                Err(v) if v == postings.len() => {
                    // return Ok(Postings::new_empty()),
                    break;
                }
                // check existence from prev entry
                Err(v) => v - 1,
            };

            postings_tbl.set_position(postings[i].off);

            let _key_count = postings_tbl.read_varint::<u64>().map_err(|e| anyhow!(e))?;
            let _label_name = postings_tbl.read_varint_bytes().map_err(|e| anyhow!(e))?;
            // key_count and label_name are  the same till lable_name_end_offset.
            // it's faster to skip than parse
            let skip = (postings_tbl.position() as i64) - (postings[i].off as i64);

            let mut lv = postings_tbl.read_varint_bytes().map_err(|e| anyhow!(e))?;
            let mut label_value = str::from_utf8(lv.as_ref()).map_err(|e| anyhow!(e))?;
            let mut postings_offset = postings_tbl.read_varint::<u64>().map_err(|e| anyhow!(e))?;
            postings_tbl
                .seek(SeekFrom::Current(skip))
                .map_err(|e| anyhow!(e))?;

            // search label_value
            while label_value < values[value_index]
                && postings_tbl.position() <= lable_name_end_offset
            {
                lv = postings_tbl.read_varint_bytes().map_err(|e| anyhow!(e))?;
                label_value = str::from_utf8(lv.as_ref()).map_err(|e| anyhow!(e))?;
                postings_offset = postings_tbl.read_varint::<u64>().map_err(|e| anyhow!(e))?;
                postings_tbl
                    .seek(SeekFrom::Current(skip))
                    .map_err(|e| anyhow!(e))?;
            }

            while value_index < values.len() && values[value_index] <= label_value {
                if values[value_index] == label_value {
                    // expected there are no duplicated values
                    let buf = new_decbuf_at(&mut self.inner, postings_offset, Some(CRC32_TABLE))?;
                    res.push(self.decorder.postings(buf)?);
                }
                value_index += 1;
            }
        }
        return Ok(Postings::new_merge(res));
    }

}

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
                // store the last value for each label name
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

pub(super) trait Sizable {
    fn len(&self) -> Result<usize>;
}

impl Sizable for File {
    fn len(&self) -> Result<usize> {
        self.metadata()
            .map(|v| v.len() as usize)
            .map_err(|e| anyhow!(e))
    }
}

impl<T> Sizable for io::Cursor<T>
where
    T: AsRef<[u8]>,
{
    fn len(&self) -> Result<usize> {
        Ok(self.get_ref().as_ref().len())
    }
}

// expect the following binary format
// byte len(4b) | content | (checksum(4b))?
pub(super) fn new_decbuf_at<T: io::Seek + io::Read + Sizable>(
    inner: &mut T,
    offset: u64,
    crc32_table: Option<crc::Crc<u32>>,
) -> Result<Vec<u8>> {
    let size = inner.len()? as u64;
    ensure!(
        offset + 4 <= size,
        IndexHeaderError::InvalidBufSize(offset + 4, size)
    );

    let len = inner
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

        let mut name_symbols = reader.name_symbols.into_iter().collect::<Vec<_>>();
        name_symbols.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            vec![(4, "a".to_string()), (5, "b".to_string()),],
            name_symbols,
        )
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

        let symbols = symbols::new(&mut file, FORMAT_V2, toc.symbols).unwrap();
        assert_eq!(5, symbols.off);
        assert_eq!(104, symbols.seen);
        assert_eq!(vec![4, 96, 189, 282], symbols.offsets);
    }
}
