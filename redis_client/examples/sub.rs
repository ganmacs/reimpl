use redis_client::{client, error::Error as RErr};

#[tokio::main]
async fn main() -> Result<(), RErr> {
    let mut client = client::connect("127.0.0.1:6379").await?;

    let mut subscriber = client.subscribe(vec!["c1".into()]).await?;

    dbg!(subscriber.next_message().await?);

    Ok(())
}
