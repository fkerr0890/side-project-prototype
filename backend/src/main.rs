use p2p::node::{EndpointPair, Node};
use tokio::{time::sleep, sync::Mutex};
use uuid::Uuid;
use std::{time::Duration, net::SocketAddrV4, sync::Arc};

#[tokio::main]
async fn main() {
    let my_public_endpoint = SocketAddrV4::new("192.168.0.104".parse().unwrap(), 8080);
    let my_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 8080);
    let my_endpoint_pair = EndpointPair::new(my_public_endpoint, my_private_endpoint);
    let mut my_node = Node::new(my_endpoint_pair, Uuid::new_v4(), 6).await;

    let remote_public_endpoint = SocketAddrV4::new("192.168.0.105".parse().unwrap(), 8080);
    let remote_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 0);
    let remote_endpoint_pair = EndpointPair::new(remote_public_endpoint, remote_private_endpoint);
    my_node.add_peer(remote_endpoint_pair, 0);

    let node_mutex = Arc::new(Mutex::new(my_node));
    let node_mutex_clone = Arc::clone(&node_mutex);
    tokio::spawn(async move {
        loop {
            {
                let node = node_mutex.lock().await;
                node.send_heartbeats().await;
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    loop {
        sleep(Duration::from_secs(2)).await;
        let node = node_mutex_clone.lock().await;
        node.receive_heartbeat().await;
    }
}