pub mod error;
pub mod reader;

const HEADER_LEN: u64 = 5;
const MAGIC_INDEX: u32 = 0xBAAAD700;
const FORMAT_V1: u8 = 1;
const FORMAT_V2: u8 = 1;

const INDEX_TOC_CRC32_LEN: u64 = 4;
const INDEX_TOC_LEN: u64 = 6 * 8 + INDEX_TOC_CRC32_LEN; // 8b*6 + 4b

pub use reader::Reader;
