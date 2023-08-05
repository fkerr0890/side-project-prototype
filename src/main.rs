use p2p::node::{Node, EndpointPair};
use p2p::nat_traversal::stun;

use std::io;
use std::net::SocketAddrV4;
use std::env;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::sleep;
use stun::Error;

#[tokio::main]
async fn main() -> io::Result<()> {
    // let my_public_endpoint = SocketAddrV4::new("75.146.180.196".parse().unwrap(), 8080);
    // // sleep(Duration::from_secs(3)).await;
    // let my_private_endpoint = SocketAddrV4::new("0.0.0.0:0".parse().unwrap(), 8080);
    // let my_endpoint_pair = EndpointPair::new(my_public_endpoint, my_private_endpoint);
    // let my_node = Node::new(my_endpoint_pair, String::from("0"), 6);
    // let remote_public_endpoint = SocketAddrV4::new("0.0.0.0:0".parse().unwrap(), 8080);
    // let socket = UdpSocket::bind(my_private_endpoint).await?;
    // my_node.connect_peer(remote_public_endpoint, socket).await?;
    if let Err(err) = stun().await {
        println!("{}", err);
    };

    Ok(())
}
