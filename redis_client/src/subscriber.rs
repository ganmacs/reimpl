use crate::{
    client::Client,
    error::Error,
    pubsub::{self, Message},
};

pub struct Subscriber<'a> {
    client: &'a mut Client,
    channels: Vec<String>,
}

impl<'a> Subscriber<'a> {
    pub fn new(client: &'a mut Client, channels: Vec<String>) -> Subscriber {
        Subscriber { client, channels }
    }

    pub async fn next_message(&'a mut self) -> Result<Message, Error> {
        let resp = self.client.read_response().await?;
        pubsub::parse(&resp)
    }
}
