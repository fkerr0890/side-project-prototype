use std::{net::{SocketAddrV4, Ipv4Addr, SocketAddr}, fmt::Display, sync::{Arc, Mutex}, collections::HashMap, time::Duration, future};

use serde::{Serialize, Deserialize};
use tokio::{net::UdpSocket, sync::mpsc, time::sleep, fs};
use uuid::Uuid;

use crate::{message_processing::{SearchRequestProcessor, DiscoverPeerProcessor, MessageProcessor, StreamProcessor}, peer::{PeerOps, self}, gateway::{OutboundGateway, InboundGateway, self}, http::{ServerContext, self}, message::{DiscoverPeerMessage, DpMessageKind, Message}, crypto::KeyStore};

pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: String,
    nat_kind: NatKind,
    socket: Arc<UdpSocket>,
    introducer: Option<EndpointPair>
}

impl Node {
    pub async fn new(private_ip: &str, uuid: Uuid, introducer: Option<&EndpointPair>) -> Self {
        let socket = Arc::new(UdpSocket::bind(private_ip).await.unwrap());
        let public_endpoint = SocketAddrV4::new(private_ip[..private_ip.len() - 2].parse().unwrap(), socket.local_addr().unwrap().port());
        let private_endpoint = SocketAddrV4::new(private_ip[..private_ip.len() - 2].parse().unwrap(), socket.local_addr().unwrap().port());
        Self {
            endpoint_pair: EndpointPair::new(public_endpoint, private_endpoint),
            uuid: uuid.simple().to_string(),
            nat_kind: NatKind::Unknown,
            socket,
            introducer: introducer.copied()
        }
    }

    pub fn endpoint_pair(&self) -> EndpointPair { self.endpoint_pair }

    pub async fn listen(&self, is_start: bool, is_end: bool) {
        let (srm_to_srp, srm_from_gateway) = mpsc::unbounded_channel();
        let (srm_to_gateway, srm_from_srp) = mpsc::unbounded_channel();
        let (dpm_to_dpp, dpm_from_gateway) = mpsc::unbounded_channel();
        let (dpm_to_gateway, dpm_from_dpp) = mpsc::unbounded_channel();
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let (to_http_handler, from_srp) = mpsc::unbounded_channel();
        let (sm_to_gateway, sm_from_smp) = mpsc::unbounded_channel();
        let (sm_to_smp, sm_from_gateway) = mpsc::unbounded_channel();
    
        let peer_ops = Arc::new(Mutex::new(PeerOps::new(heartbeat_tx, self.endpoint_pair)));
        let key_store = Arc::new(Mutex::new(KeyStore::new()));
        let peer_ops_clone = peer_ops.clone();
        let mut local_hosts = HashMap::new();
        if is_end {
            local_hosts.insert(String::from("example"), SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000));
        }
        let mut srp = SearchRequestProcessor::new(MessageProcessor::new(self.endpoint_pair, srm_from_gateway, srm_to_gateway), sm_to_gateway.clone(), to_http_handler, local_hosts, &peer_ops, &key_store);
        let mut dpp = DiscoverPeerProcessor::new(MessageProcessor::new(self.endpoint_pair, dpm_from_gateway, dpm_to_gateway), &peer_ops);
        let mut smp = StreamProcessor::new(MessageProcessor::new(self.endpoint_pair, sm_from_gateway, sm_to_gateway), &key_store);
    
        let mut outbound_srm_gateway = OutboundGateway::new(&self.socket, srm_from_srp);
        let mut outbound_dpm_gateway = OutboundGateway::new(&self.socket, dpm_from_dpp);
        let mut outbound_heartbeat_gateway = OutboundGateway::new(&self.socket, heartbeat_rx);
        let mut outbound_stream_gateway = OutboundGateway::new(&self.socket, sm_from_smp);
    
        tokio::spawn(async move {
            loop {
                let Some(_) = outbound_srm_gateway.send().await else { return };
            }
        });
    
        tokio::spawn(async move {
            loop {
                let Some(_) = outbound_dpm_gateway.send().await else { return };
            }
        });

        tokio::spawn(async move {
            loop {
                let Some(_) = outbound_heartbeat_gateway.send().await else { return };
            }
        });

        tokio::spawn(async move {
            while let Some(_) = outbound_stream_gateway.send().await {}
        });
    
        for _ in 0..225 {
            let mut inbound_gateway = InboundGateway::new(&self.socket, &srm_to_srp, &dpm_to_dpp, &sm_to_smp);
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
                let Ok(_) = dpp.receive().await else { return };         
            }
        });

        tokio::spawn(async move {
            while let Ok(_) = smp.receive().await {}
        });
        
        // tokio::spawn(async move {
        //     loop {
        //         {
        //             let Ok(_) = peer_ops.lock().unwrap().send_heartbeats() else { return };
        //         }
        //         sleep(Duration::from_secs(29)).await;
        //     }
        // });

        if let Some(introducer) = self.introducer {
            let mut message = DiscoverPeerMessage::new(DpMessageKind::INeedSome,
                self.endpoint_pair.public_endpoint,
                EndpointPair::default_socket(),
                EndpointPair::default_socket(),
                Uuid::new_v4().simple().to_string(),
                (peer::MAX_PEERS, peer::MAX_PEERS));
            message.add_peer(introducer);
            self.socket.send_to(&bincode::serialize(&message).unwrap(), message.dest()).await.unwrap();
        }
        else {
            gateway::log_debug("No introducer");
        }

        sleep(Duration::from_secs(10)).await;
        let node_info = self.as_node_info(peer_ops_clone, is_start, is_end);
        fs::write(format!("../peer_info/{}.json", node_info.name), serde_json::to_vec(&node_info).unwrap()).await.unwrap();
        
        if is_start {
            let server_context = ServerContext::new(&srm_to_srp, Arc::new(tokio::sync::Mutex::new(from_srp)));
            http::tcp_listen(SocketAddr::from(([127,0,0,1], 8080)), server_context).await;
        }
        else {
            future::pending::<()>().await;
        }
    }

    pub fn as_node_info(&self, peer_ops: Arc<Mutex<PeerOps>>, is_start: bool, is_end: bool) -> NodeInfo {
        let name = if is_start { String::from("START") } else if is_end { String::from("END") } else { self.endpoint_pair.public_endpoint.port().to_string() };
        NodeInfo {
            name,
            port: self.endpoint_pair.public_endpoint.port(),
            uuid: self.uuid.clone(),
            peers: peer_ops.lock().unwrap().peers().iter().map(|(endpoint_pair, score)| (endpoint_pair.public_endpoint.port(), *score)).collect()
        }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Copy, Eq, PartialEq, Debug)]
pub struct EndpointPair {
    pub public_endpoint: SocketAddrV4,
    pub private_endpoint: SocketAddrV4,
}

impl EndpointPair {
    pub fn new(public_endpoint: SocketAddrV4, private_endpoint: SocketAddrV4) -> Self {
        Self {
            public_endpoint,
            private_endpoint
        }
    }

    pub fn default_socket() -> SocketAddrV4 { SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0) }
}

impl Display for EndpointPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Public: {}, Private: {}", self.private_endpoint, self.public_endpoint)
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
enum NatKind {
    Unknown,
    Static,
    Easy,
    Hard
}

#[derive(Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    port: u16,
    uuid: String,
    peers: Vec<(u16, i32)>
}