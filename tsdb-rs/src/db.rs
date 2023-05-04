use crate::block::{self, Block};
use crate::querier::{self, BlockQuerier};
use crate::storage::merge::{new_generic_querier, MergeGenericQuerier};
use anyhow::{anyhow, Result};
use log::warn;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use ulid::Ulid;

pub fn open<P: AsRef<Path>>(path: &P) -> Result<DB> {
    // mkdir
    let mut db = DB::open();
    db.reload_blocks(path)?;

    return Ok(db);
}

pub struct DB {
    blocks: Vec<Arc<Block>>,
}

impl DB {
    fn open() -> Self {
        DB { blocks: vec![] }
    }

    fn reload_blocks<P: AsRef<Path>>(&mut self, p: &P) -> Result<()> {
        let blocks = open_blocks(p, self.blocks.clone())?;
        self.blocks = blocks;

        Ok(())
    }

    pub fn querier(&self) -> MergeGenericQuerier<BlockQuerier> {
        let mut queriers: Vec<BlockQuerier> = vec![];
        for b in self.blocks.iter() {
            let querier = querier::open(b.clone());
            queriers.push(querier);
        }

        return new_generic_querier(queriers);
    }
}

fn open_blocks<P: AsRef<Path>>(p: &P, loaded: Vec<Arc<Block>>) -> Result<Vec<Arc<Block>>> {
    let blocks_dir = block_dirs(p).map_err(|e| anyhow!(e))?;
    let mut blocks = vec![];
    for b_dir in blocks_dir {
        let Ok((meta, _)) = block::read_meta_file(&b_dir) else {
                warn!("Failed to read meta.json for a block during open block skipping {:?}", &b_dir);
                continue;
            };

        if let Some(block) = get_block(&loaded, meta.ulid) {
            // TODO: handle corrupted
            blocks.push(block.clone())
        } else {
            let t = Block::open(&b_dir)?;
            blocks.push(Arc::new(t));
        };
    }

    Ok(blocks)
}

fn get_block(blocks: &Vec<Arc<Block>>, id: Ulid) -> Option<Arc<Block>> {
    for b in blocks.iter() {
        if b.meta().ulid == id {
            return Some(b.clone());
        }
    }

    None
}

fn block_dirs<P: AsRef<Path>>(p: &P) -> io::Result<Vec<PathBuf>> {
    let dir = p.as_ref();

    let mut res = vec![];
    for d in dir.read_dir()? {
        let block = d?.path();
        if is_block_dir(&block) {
            res.push(block)
        }
    }

    Ok(res)
}

fn is_block_dir<P: AsRef<Path>>(p: &P) -> bool {
    let path = p.as_ref();
    if !path.metadata().map(|m| m.is_dir()).unwrap_or(false) {
        return false;
    }

    if let Some(file_name) = path.file_name().map(|v| v.to_str()).flatten() {
        if Ulid::from_string(file_name).is_ok() {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_block() {
        let path = Path::new("tests/index_format_v2/simple2");
        let blocks = block_dirs(&path).unwrap();

        assert_eq!(
            vec![Path::new(
                "tests/index_format_v2/simple2/01GNXGKS4HSZSQ5KX88D79BJTN"
            )],
            blocks,
        );
    }
}
