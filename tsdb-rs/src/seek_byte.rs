use byteorder::ReadBytesExt;
use std::fs::File;
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

impl SeekReadBytesExt for File {}
impl SeekReadBytesExt for &File {}
impl<T> SeekReadBytesExt for io::Cursor<T> where T: AsRef<[u8]> {}
