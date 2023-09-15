use crate::node::Node;

use std::io;
use std::net::{SocketAddr, IpAddr, Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use tokio::net::UdpSocket;
use stun::{Error, client, message, agent, xoraddr};
use stun::message::Getter;

impl Node {
    pub async fn connect_peer(&self, remote_public_endpoint: SocketAddrV4, arc_socket: UdpSocket) -> io::Result<()> {
        loop {
            arc_socket.send_to(b"Hello" , remote_public_endpoint).await?;
            let mut buf = [0; 1024];
            let (len, addr) = arc_socket.recv_from(&mut buf).await.unwrap_or_else(|_error| {
                (0 as usize, SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            });
            if len != 0 as usize && addr.to_string() == remote_public_endpoint.to_string() {
                break
            }
        }
        Ok(())
    }
}

pub async fn stun() -> Result<(), Error> {
    let server = String::from("stun.l.google.com:19302");

    let (handler_tx, mut handler_rx) = tokio::sync::mpsc::unbounded_channel();

    let conn = UdpSocket::bind("0.0.0.0:8080").await?;
    println!("Local address: {}", conn.local_addr()?);

    println!("Connecting to: {server}");
    conn.connect(server).await?;

    let mut client = client::ClientBuilder::new().with_conn(Arc::new(conn)).build()?;

    let mut msg = message::Message::new();
    msg.build(&[Box::<agent::TransactionId>::default(), Box::new(message::BINDING_REQUEST)])?;

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