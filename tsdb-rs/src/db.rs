use crate::block::{self, Block};
use crate::chunks;

pub fn blocks(dir: String) -> Block<chunks::Reader> {
    block::open(dir).unwrap()
}
