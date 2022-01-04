use crate::resp::Resp;
use bytes::Bytes;

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    pub fn new(channel: impl ToString, message: Bytes) -> Self {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }
}

impl From<Publish> for Resp {
    fn from(publish: Publish) -> Self {
        let mut ary = Resp::array();
        ary.push_bulk_strings(Bytes::from("Publish".as_bytes()));
        ary.push_bulk_strings(Bytes::from(publish.channel.into_bytes()));
        ary.push_bulk_strings(publish.message);
        ary
    }
}
