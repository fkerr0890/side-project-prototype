use p2p::{node::{EndpointPair, SearchRequestProcessor, MessageProcessor}, gateway::{self, OutboundGateway, InboundGateway}, peer::peer_ops, http::{self, ServerContext}};
use tokio::{sync::{mpsc, Mutex}, time::sleep, net::UdpSocket};
use std::{time::Duration, net::{SocketAddrV4, SocketAddr}, env, sync::Arc, panic, process, collections::HashMap};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let (my_public_endpoint, remote_public_endpoint) = (SocketAddrV4::new(args[1].parse().unwrap(), 8080), SocketAddrV4::new(args[2].parse().unwrap(), 8080));
    
    let my_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 8080);
    let my_endpoint_pair = EndpointPair::new(my_public_endpoint, my_private_endpoint);    
    let remote_private_endpoint = SocketAddrV4::new("0.0.0.0".parse().unwrap(), 0);
    let remote_endpoint_pair = EndpointPair::new(remote_public_endpoint, remote_private_endpoint);
    
    let socket = Arc::new(UdpSocket::bind(my_private_endpoint).await.unwrap());
    let (in_gateway_tx, srp_rx) = mpsc::unbounded_channel();
    let (srp_tx, out_gateway_rx) = mpsc::unbounded_channel();
    let (heartbeat_tx, _) = mpsc::unbounded_channel();
    let (to_http_handler, from_srp) = mpsc::unbounded_channel();

    peer_ops::HEARTBEAT_TX.set(heartbeat_tx).unwrap();
    peer_ops::add_initial_peer(remote_endpoint_pair);
    let mut local_hosts = HashMap::new();
    if args.len() > 3 {
        local_hosts.insert(args[3].to_owned(), SocketAddrV4::new(args[4].parse().unwrap(), args[5].parse().unwrap()));
    }
    let mut srp = SearchRequestProcessor::new(MessageProcessor::new(my_endpoint_pair, srp_rx, srp_tx), to_http_handler, local_hosts);

    let mut outbound_gateway = OutboundGateway::new(&socket, out_gateway_rx);

    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        gateway::log_debug(&panic_info.to_string());
        process::exit(1);
    }));

    tokio::spawn(async move {
        loop {
            let Some(_) = outbound_gateway.send().await else { return };
        }
    });
    
    for _ in 0..225 {
        let mut inbound_gateway = InboundGateway::new(&socket, &in_gateway_tx);
        tokio::spawn(async move {
            loop {
                let Ok(_) = inbound_gateway.receive().await else { return };
            }
        });
    }

    tokio::spawn(async move {
        loop {
            let Ok(_) = srp.receive().await else { return };            
        }
    });

    tokio::spawn(async move {
        loop {
            let Ok(_) = peer_ops::send_heartbeats(my_endpoint_pair).await else { return };
            sleep(Duration::from_secs(29)).await;
        }
    });

    let server_context = ServerContext::new(&in_gateway_tx, Arc::new(Mutex::new(from_srp)));
    http::tcp_listen(SocketAddr::from(([127,0,0,1], 80)), server_context).await;

}