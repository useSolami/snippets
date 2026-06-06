use std::error::Error;

use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let token = std::env::var("WS_TOKEN").map_err(|_| "WS_TOKEN is not set")?;

    let client = solami::builder()
        .with_rpc(token)
        .build()
        .await?;

    println!("connected to {}", client.ws().url());

    let (mut stream, _unsubscribe) = client.ws().slot_subscribe().await?;
    println!("subscribed to slot updates, waiting for messages...");

    while let Some(slot_info) = stream.next().await {
        println!("slot {} (parent {})", slot_info.slot, slot_info.parent);
    }

    println!("stream closed");
    Ok(())
}
