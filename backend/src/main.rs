use p2p::node::{Node, EndpointPair};
use p2p::nat_traversal::stun;
use uuid::Uuid;

use std::io;
use std::net::SocketAddrV4;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args[1] == "stun" {
        stun().await.unwrap();
        return Ok(())
    }
    let my_public_endpoint = SocketAddrV4::new(args[1].parse().unwrap(), 8080);
    let my_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), args[2].parse().unwrap());
    let my_endpoint_pair = EndpointPair::new(my_public_endpoint, my_private_endpoint);
    let mut my_node = Node::new(my_endpoint_pair, Uuid::new_v4(), 6).await;

    let remote_public_endpoint = SocketAddrV4::new(args[3].parse().unwrap(), args[4].parse().unwrap());
    let remote_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 0);
    let remote_endpoint_pair = EndpointPair::new(remote_public_endpoint, remote_private_endpoint);
    my_node.add_peer(remote_endpoint_pair, 0);

    let node_mutex = Arc::new(Mutex::new(my_node));
    let node_mutex_clone = Arc::clone(&node_mutex);
    tokio::spawn(async move {
        loop {
            {
                let node = node_mutex.lock().await;
                println!("Sending...");
                node.send_heartbeats().await;
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    loop {
        sleep(Duration::from_secs(2)).await;
        let node = node_mutex_clone.lock().await;
        if let Some(error) = node.receive_heartbeat() {
            println!("{}", error);
            break;
        };
    }

    Ok(())
}
