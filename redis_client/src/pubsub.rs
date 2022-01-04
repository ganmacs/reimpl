use crate::{error::Error, resp::Resp};
use bytes::Bytes;

#[derive(Debug)]
pub enum Message {
    Subscribe(Bytes, u64), // ["subscribe", message ,num of subscriber]
    Unsubscribe(Bytes),
    Message(String, Bytes),
}

pub(crate) fn parse(resp: &Resp) -> Result<Message, Error> {
    match resp {
        Resp::Array(ary) if ary.len() == 3 => match ary.as_slice() {
            [Resp::BulkString(typ), Resp::BulkString(channel), Resp::BulkString(message)]
                if typ == "message" =>
            {
                let ch = String::from_utf8(channel.to_vec()).unwrap(); //  TODO
                Ok(Message::Message(ch, message.clone()))
            }
            [Resp::BulkString(typ), Resp::BulkString(message), Resp::Integer(r)]
                if typ == "subscribe" =>
            {
                Ok(Message::Subscribe(message.clone(), *r as u64))
            }
            others => Err(Error::Other(
                format!(
                    "unxpected value. arrays(size=3) is expected for pub/sub message: {:?}",
                    others
                )
                .into(),
            )),
        },
        others => Err(Error::Other(
            format!(
                "unxpected value. arrays(size=3) is expected for pub/sub message: {:?}",
                others
            )
            .into(),
        )),
    }
}
