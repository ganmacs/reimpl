use super::{FormatVersion, FORMAT_V2};
use crate::index::reader::{new_decbuf_at, Sizable};
use crate::index::CRC32_TABLE;
use crate::seek_byte::VarUintByte;
use anyhow::{anyhow, bail, Result};
use byteorder::{BigEndian, ReadBytesExt};
use std::{io, str};

pub(super) const SYMBOL_FACTOR: usize = 32;

#[derive(Debug, PartialEq)]
pub struct Symbols {
    pub(super) inner: Vec<u8>,
    pub(super) off: u64,

    pub(super) offsets: Vec<u64>,
    pub(super) seen: u64,
    version: FormatVersion,
}

pub(super) fn new<T>(inner: &mut T, version: FormatVersion, offset: u64) -> Result<Symbols>
where
    T: Sizable + io::Read + io::Seek,
{
    let buf = new_decbuf_at(inner, offset, Some(CRC32_TABLE))?;
    let mut content = io::Cursor::new(&buf);
    let count = content.read_u32::<BigEndian>().map_err(|e| anyhow!(e))? as usize;

    let mut offsets = vec![];
    let mut seen = 0;
    while seen < count {
        if seen % SYMBOL_FACTOR == 0 {
            // skip len
            offsets.push(content.position());
        }
        // consume position
        let _ = content.read_varint_bytes().map_err(|e| anyhow!(e))?;
        seen += 1;
    }

    Ok(Symbols::new(buf, offset, offsets, seen as u64, version))
}

impl Symbols {
    fn new(inner: Vec<u8>, off: u64, offsets: Vec<u64>, seen: u64, version: FormatVersion) -> Self {
        Self {
            inner,
            off,
            offsets,
            seen,
            version,
        }
    }

    pub(super) fn lookup(&self, off: u64) -> Result<String> {
        let mut cur = io::Cursor::new(&self.inner);
        if FORMAT_V2 == self.version {
            if off > self.seen {
                bail!("unknown symbol offset {:?}", off);
            }

            let o = (off as usize) / SYMBOL_FACTOR;
            cur.set_position(self.offsets[o]);

            // consume till the `off`
            for _ in (0..((off as usize) - (o * SYMBOL_FACTOR))).rev() {
                cur.read_varint_bytes().map_err(|v| anyhow!(v))?;
            }
        }

        let buf = cur.read_varint_bytes().map_err(|v| anyhow!(v))?;
        let s = str::from_utf8(buf.as_ref()).map_err(|v| anyhow!(v))?;
        Ok(s.to_string())
    }

    pub(super) fn reverse_lookup(&self, sym: &str) -> Result<u64> {
        if self.offsets.len() == 0 {
            return Err(anyhow!("unknown symobl {:?} - no symbols", sym));
        }
        let mut cur = io::Cursor::new(&self.inner);
        let i = self
            .offsets
            .binary_search_by(|off| {
                cur.set_position(*off);
                let v = cur.read_varint_bytes().unwrap();
                let s = str::from_utf8(v.as_ref()).unwrap();

                s.cmp(sym)
            })
            .unwrap_or_else(|v| v);
        let i = if i > 0 { i - 1 } else { i };

        cur.set_position(self.offsets[i]);

        for re in ((i * SYMBOL_FACTOR) as u64)..self.seen {
            let last_symbol = cur.read_varint_bytes().map_err(|v| anyhow!(v))?;
            let s = str::from_utf8(last_symbol.as_ref()).map_err(|v| anyhow!(v))?;

            if s == sym {
                return Ok(re);
            } else if s > sym {
                return Err(anyhow!("Not found: key {:?}", sym));
            }
        }

        Err(anyhow!("Not found: key {:?}", sym))
    }

    pub(super) fn size(&self) -> usize {
        self.offsets.len() * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seek_byte::VarUintByteWriter;
    use byteorder::WriteBytesExt;

    #[test]
    fn test_symbol_reverse_lookup() {
        let sym = Symbols {
            inner: vec![0, 0, 0, 6, 1, 49, 1, 50, 1, 51, 1, 52, 1, 97, 1, 98],
            off: 5,
            offsets: vec![4],
            seen: 6,
            version: FORMAT_V2,
        };

        assert_eq!(4, sym.reverse_lookup("a").unwrap());
        assert_eq!(5, sym.reverse_lookup("b").unwrap());
    }

    #[test]
    fn test_symbol_lookup() {
        let mut buf: Vec<u8> = vec![];

        // Add prefix to the buffer to simulate symbols as part of larger buffer.
        buf.write_varint_str("something").unwrap();
        let start = buf.len();

        buf.write_u32::<BigEndian>(204).unwrap(); // symbol table size
        buf.write_u32::<BigEndian>(100).unwrap(); // symbol count

        let to_string = |i: u8| {
            let vv = vec![i];
            str::from_utf8(&vv).unwrap().to_string()
        };

        for i in 0..100 {
            buf.write_varint_str(to_string(i).as_str()).unwrap();
        }

        let checksum = CRC32_TABLE.checksum(&buf[start + 4..]);
        buf.write_u32::<BigEndian>(checksum).unwrap();

        let mut cur = io::Cursor::new(buf);
        let sym = new(&mut cur, FORMAT_V2, start as u64).unwrap();

        assert_eq!(32, sym.size());

        for i in (0..100).rev() {
            let s = sym.lookup(i).unwrap();
            assert_eq!(to_string(i as u8), s);
        }

        for i in (0..100).rev() {
            let s = sym.reverse_lookup(&to_string(i as u8)).unwrap();
            assert_eq!(i, s);
        }
    }
}
