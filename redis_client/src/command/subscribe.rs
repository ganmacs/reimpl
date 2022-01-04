use crate::resp::Resp;
use bytes::Bytes;

#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub fn new(channels: &[String]) -> Self {
        Subscribe {
            channels: channels.to_vec(),
        }
    }
}

impl From<Subscribe> for Resp {
    fn from(sub: Subscribe) -> Self {
        let mut ary = Resp::array();
        ary.push_bulk_strings(Bytes::from("Subscribe".as_bytes()));
        for ch in sub.channels {
            ary.push_bulk_strings(Bytes::from(ch.into_bytes()));
        }

        ary
    }
}
