use p2p::{node::{EndpointPair, Node}, gateway::{self, OutboundGateway, InboundGateway}, peer::peer_ops, http::{self, ServerContext}};
use tokio::{sync::{mpsc, Mutex}, time::sleep, net::UdpSocket};
use uuid::Uuid;
use std::{time::Duration, net::{SocketAddrV4, SocketAddr}, env, sync::Arc, panic, process, collections::HashMap};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    gateway::IS_NM_HOST.set(args[2] == "p2pclient@test.com").unwrap();
    let (my_public_endpoint, remote_public_endpoint) = if *gateway::IS_NM_HOST.get().unwrap() {
        (SocketAddrV4::new("192.168.0.103".parse().unwrap(), 8080), SocketAddrV4::new("192.168.0.105".parse().unwrap(), 8080))
    } else {
        (SocketAddrV4::new(args[1].parse().unwrap(), 8080), SocketAddrV4::new(args[2].parse().unwrap(), 8080))
    };
    
    let my_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 8080);
    let my_endpoint_pair = EndpointPair::new(my_public_endpoint, my_private_endpoint);    
    let remote_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 0);
    let remote_endpoint_pair = EndpointPair::new(remote_public_endpoint, remote_private_endpoint);
    
    let socket = Arc::new(UdpSocket::bind(my_private_endpoint).await.unwrap());
    let (gateway_egress, node_ingress) = mpsc::unbounded_channel();
    let (node_egress, gateway_ingress) = mpsc::unbounded_channel();
    let (to_http_handler, from_node) = mpsc::unbounded_channel();

    peer_ops::EGRESS.set(node_egress.clone()).unwrap();
    peer_ops::add_initial_peer(remote_endpoint_pair);
    let mut local_hosts = HashMap::new();
    if args.len() > 3 {
        local_hosts.insert(args[3].to_owned(), SocketAddrV4::new(args[4].parse().unwrap(), args[5].parse().unwrap()));
    }
    let mut my_node = Node::new(my_endpoint_pair, Uuid::new_v4(), node_ingress, node_egress, to_http_handler, local_hosts);

    let mut outbound_gateway = OutboundGateway::new(&socket, gateway_ingress);

    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        gateway::log_debug(&panic_info.to_string());
        process::exit(1);
    }));

    tokio::spawn(async move {
        loop {
            outbound_gateway.send().await;
        }
    });
    
    for _ in 0..225 {
        let mut inbound_gateway = InboundGateway::new(&socket, &gateway_egress);
        tokio::spawn(async move {
            loop {
                inbound_gateway.receive().await;
            }
        });
    }

    tokio::spawn(async move {
        loop {
            my_node.receive().await;            
        }
    });

    tokio::spawn(async move {
        loop {
            peer_ops::send_heartbeats(my_endpoint_pair).await;
            sleep(Duration::from_secs(29)).await;
        }
    });

    let server_context = ServerContext::new(&gateway_egress, Arc::new(Mutex::new(from_node)));
    http::tcp_listen(SocketAddr::from(([127,0,0,1], 80)), server_context).await;

}