use crate::resp::Resp;
use bytes::Bytes;

#[derive(Debug)]
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl ToString) -> Self {
        Incr {
            key: key.to_string(),
        }
    }
}

impl From<Incr> for Resp {
    fn from(incr: Incr) -> Self {
        let mut ary = Resp::array();
        ary.push_bulk_strings(Bytes::from("INCR"));
        ary.push_bulk_strings(Bytes::from(incr.key.into_bytes()));
        ary
    }
}
