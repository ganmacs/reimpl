use crate::block::{self, Block};
use crate::chunks;
use std::path::Path;

pub fn blocks<P: AsRef<Path>>(p: &P) -> Block<chunks::Reader> {
    block::open(p).unwrap()
}
