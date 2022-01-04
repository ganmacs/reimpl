use crate::resp::Resp;
use bytes::Bytes;

#[derive(Debug)]
pub struct Decr {
    key: String,
}

impl Decr {
    pub fn new(key: impl ToString) -> Self {
        Decr {
            key: key.to_string(),
        }
    }
}

impl From<Decr> for Resp {
    fn from(decr: Decr) -> Self {
        let mut ary = Resp::array();
        ary.push_bulk_strings(Bytes::from("DECR"));
        ary.push_bulk_strings(Bytes::from(decr.key.into_bytes()));
        ary
    }
}
