mod reader;

pub use reader::Reader;

pub trait ChunkReader {
    fn close();
    fn chunk();
}
