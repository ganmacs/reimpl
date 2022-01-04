use redis_client::{client, error::Error as RErr};

#[tokio::main]
async fn main() -> Result<(), RErr> {
    let mut client = client::connect("127.0.0.1:6379").await?;

    dbg!(client.publish("c1", "bar".into()).await?);

    Ok(())
}
