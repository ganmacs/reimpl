use redis_client::{client, error::Error as RErr};

#[tokio::main]
async fn main() -> Result<(), RErr> {
    let mut client = client::connect("127.0.0.1:6379").await?;
    client.ping().await?;

    let r = client.set("key1", "value".into()).await?;
    dbg!(r);

    let r = client.get("key").await?;
    dbg!(r);

    let r = client.get("key1").await?;
    dbg!(r);

    dbg!(client.incr("key22").await?);
    dbg!(client.decr("key22").await?);

    dbg!(client.incr("key1").await?);

    Ok(())
}
