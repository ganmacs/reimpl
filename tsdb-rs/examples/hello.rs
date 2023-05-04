use std::path::Path;
use tsdb;
use tsdb::{Matcher, Querier};

fn main() {
    let p = Path::new("tests/index_format_v2/simple3/");
    let db = tsdb::open(&p).unwrap();
    let mut querier = db.querier();
    for s in querier
        .select(vec![Matcher::new_must_matcher("bar", "0")])
        .unwrap()
    {
        println!("series: {:?}", s);
    }
}
