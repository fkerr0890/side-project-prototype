use std::sync::Arc;

use stun::message::Getter;
use stun::{agent, client, message, xoraddr, Error};
use tokio::net::UdpSocket;

pub async fn stun() -> Result<(), Error> {
    let server = String::from("stun.l.google.com:19302");

    let (handler_tx, mut handler_rx) = tokio::sync::mpsc::unbounded_channel();

    let conn = UdpSocket::bind("0.0.0.0:8080").await?;
    println!("Local address: {}", conn.local_addr()?);

    println!("Connecting to: {server}");
    conn.connect(server).await?;

    let mut client = client::ClientBuilder::new()
        .with_conn(Arc::new(conn))
        .build()?;

    let mut msg = message::Message::new();
    msg.build(&[
        Box::<agent::TransactionId>::default(),
        Box::new(message::BINDING_REQUEST),
    ])?;

    client.send(&msg, Some(Arc::new(handler_tx))).await?;

    if let Some(event) = handler_rx.recv().await {
        let msg = event.event_body?;
        let mut xor_addr = xoraddr::XorMappedAddress::default();
        xor_addr.get_from(&msg)?;
        println!("Got response: {xor_addr}");
    }

    client.close().await?;

    Ok(())
}
