use std::path::{Path, PathBuf};
use tsdb::db;

fn main() {
    let p = Path::new("tests/index_format_v1");
    let database = db::blocks(&p);
}
