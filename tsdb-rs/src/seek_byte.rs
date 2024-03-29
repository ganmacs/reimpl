use byteorder::{ReadBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use std::io::{self, Seek, SeekFrom};

pub trait SeekReadBytesExt: io::Read + Seek {
    #[inline]
    fn read_u8_at(&mut self, at: u64) -> io::Result<u8> {
        self.seek(SeekFrom::Start(at))?;
        self.read_u8()
    }

    #[inline]
    fn read_u32_at<T: byteorder::ByteOrder>(&mut self, at: u64) -> io::Result<u32> {
        self.seek(SeekFrom::Start(at))?;
        self.read_u32::<T>()
    }

    #[inline]
    fn read_u64_at<T: byteorder::ByteOrder>(&mut self, at: u64) -> io::Result<u64> {
        self.seek(SeekFrom::Start(at))?;
        self.read_u64::<T>()
    }

    #[inline]
    fn read_exact_at(&mut self, buf: &mut [u8], at: u64) -> io::Result<()> {
        self.seek(SeekFrom::Start(at))?;
        self.read_exact(buf)
    }
}

impl<R: io::Read + Seek> SeekReadBytesExt for R {}

pub(crate) trait VarUintByte: VarIntReader + io::Read {
    fn read_varint_bytes(&mut self) -> io::Result<Vec<u8>> {
        let size = self.read_varint::<u64>()? as usize;
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        return Ok(buf);
    }
}

impl<R: VarIntReader + io::Read> VarUintByte for R {}

pub(crate) trait VarUintByteWriter: VarIntWriter + io::Write {
    fn write_varint_str(&mut self, b: &str) -> io::Result<()> {
        self.write_varint(b.len() as u64)? as usize;
        self.write(b.as_bytes())?;
        Ok(())
    }
}

impl<W: VarIntWriter + io::Write> VarUintByteWriter for W {}
