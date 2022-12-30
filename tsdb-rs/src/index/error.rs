use super::{FORMAT_V1, FORMAT_V2, HEADER_LEN, MAGIC_INDEX};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("invalid index header (expected {}, got {0:?})", HEADER_LEN)]
    InvalidSize(u64),
    #[error("invalid index magic number (expected {:#x}, got {0:?})", MAGIC_INDEX)]
    InvalidMagicNumber(u32),
    #[error(
        "invalid index magic version (expected {} or {}, got {0:?})",
        FORMAT_V1,
        FORMAT_V2
    )]
    InvalidIndexVersion(u8),

    #[error("invalid index toc size (expected {}, got {0:?})", HEADER_LEN)]
    InvalidTocSize(u64),

    #[error("invlid index toc checksum (expected {0:#x}, got {1:#x})")]
    InvalidChucksum(
        u32,
        u32
    ),
}
