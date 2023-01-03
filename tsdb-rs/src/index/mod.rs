pub mod error;
pub mod postings;
pub mod reader;
pub mod symbols;

pub type FormatVersion = u8;

const HEADER_LEN: u64 = 5;
const MAGIC_INDEX: u32 = 0xBAAAD700;
const FORMAT_V1: FormatVersion = 1;
const FORMAT_V2: FormatVersion = 2;

const INDEX_TOC_CRC32_LEN: u64 = 4;
const INDEX_TOC_LEN: u64 = 6 * 8 + INDEX_TOC_CRC32_LEN; // 8b*6 + 4b

pub(self) const CRC32_TABLE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

pub use reader::Reader;
