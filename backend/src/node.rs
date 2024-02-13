use std::{net::{SocketAddrV4, Ipv4Addr, SocketAddr}, fmt::Display, sync::{Arc, Mutex}, collections::HashMap, time::Duration, future, str::FromStr};

use serde::{Serialize, Deserialize};
use tokio::{net::UdpSocket, sync::mpsc, time::sleep, fs};
use uuid::Uuid;

use crate::{crypto::KeyStore, gateway::InboundGateway, http::{self, ServerContext}, message::{DiscoverPeerMessage, DpMessageKind, Heartbeat, InboundMessage, IsEncrypted, Message, UniqueParts}, message_processing::{DiscoverPeerProcessor, MessageProcessor, MessageStaging, SearchRequestProcessor, StreamMessageProcessor}, peer::{self, PeerOps}};

pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: String,
    nat_kind: NatKind,
    socket: Arc<UdpSocket>,
    introducer: Option<EndpointPair>,
    initial_peers: Option<Vec<String>>
}

impl Node {
    pub async fn new(private_ip: String, private_port: String, public_ip: &str, uuid: Uuid, introducer: Option<&EndpointPair>, initial_peers: Option<Vec<String>>) -> Self {
        let socket = Arc::new(UdpSocket::bind(private_ip.clone() + ":" + &private_port).await.unwrap());
        let public_endpoint = SocketAddrV4::new(public_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        let private_endpoint = SocketAddrV4::new(private_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        println!("public: {:?}", public_endpoint);
        Self {
            endpoint_pair: EndpointPair::new(public_endpoint, private_endpoint),
            uuid: uuid.simple().to_string(),
            nat_kind: NatKind::Unknown,
            socket,
            introducer: introducer.copied(),
            initial_peers
        }
    }

    pub fn endpoint_pair(&self) -> EndpointPair { self.endpoint_pair }

    pub async fn listen(&self, is_start: bool, is_end: bool) {
        let (srm_to_srp, srm_from_gateway) = mpsc::unbounded_channel();
        let (dpm_to_dpp, dpm_from_gateway) = mpsc::unbounded_channel();
        let (to_http_handler, from_smp) = mpsc::unbounded_channel();
        let (sm_to_smp, sm_from_gateway) = mpsc::unbounded_channel();
        let (to_staging, from_gateway) = mpsc::unbounded_channel();
    
        let key_store = Arc::new(Mutex::new(KeyStore::new()));
        let peer_ops = Arc::new(Mutex::new(PeerOps::new()));
        let peer_ops_clone = peer_ops.clone();
        let mut local_hosts = HashMap::new();
        if is_end {
            local_hosts.insert(String::from("example"), SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000));
        }
        let mut message_staging = MessageStaging::new(from_gateway, srm_to_srp.clone(), dpm_to_dpp, sm_to_smp.clone(), &key_store, self.endpoint_pair);
        let mut srp = SearchRequestProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops.clone())), srm_from_gateway, sm_to_smp.clone(), local_hosts.clone());
        let mut dpp = DiscoverPeerProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops.clone())), dpm_from_gateway);
        let mut smp = StreamMessageProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, None), sm_from_gateway, local_hosts, to_http_handler);
    
        for _ in 0..225 {
            let mut inbound_gateway = InboundGateway::new(&self.socket, to_staging.clone());
            tokio::spawn(async move {
                loop {
                    if let Err(e) = inbound_gateway.receive().await {
                        println!("Inbound gateway stopped: {}", e);
                        return;
                    }
                }
            });
        }

        tokio::spawn(async move {
            loop {
                if let Err(e) = message_staging.receive().await {
                    println!("MessageStaging stopped: {}", e);
                    return;
                }
            }
        });
    
        tokio::spawn(async move {
            loop {
                if let Err(e) = srp.receive().await {
                    println!("Search request processor stopped: {}", e);
                    return;
                }
            }
        });
    
        tokio::spawn(async move {
            loop {
                if let Err(e) = dpp.receive().await {
                    println!("Discover peer processor stopped: {}", e);
                    return;
                }
            }
        });

        tokio::spawn(async move {
            loop {
                if let Err(e) = smp.receive().await {
                    println!("Stream message processor stopped: {}", e);
                    return;
                }
            }
        });
        
        let heartbeat_mp = MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops));
        tokio::spawn(async move {
            loop {
                if let Err(e) = heartbeat_mp.send_request(&mut Heartbeat::new(), None) {
                    println!("Heartbeats stopped: {}", e);
                    return;
                };
                sleep(Duration::from_secs(29)).await;
            }
        });

        if let Some(introducer) = self.introducer {
            let mut message = DiscoverPeerMessage::new(DpMessageKind::INeedSome,
                EndpointPair::default_socket(),
                Uuid::new_v4().simple().to_string(),
                (peer::MAX_PEERS, peer::MAX_PEERS));
            message.add_peer(introducer);
            let inbound_message = InboundMessage::new(bincode::serialize(&message).unwrap(), IsEncrypted::False, UniqueParts::new(self.endpoint_pair.public_endpoint, message.id().to_owned()));
            self.socket.send_to(&bincode::serialize(&inbound_message).unwrap(), self.endpoint_pair.public_endpoint).await.unwrap();
        }
        else if let Some(initial_peers) = &self.initial_peers {
            for peer in initial_peers {
                let public_endpoint = SocketAddrV4::from_str(peer).unwrap();
                let private_endpoint = SocketAddrV4::from_str(peer).unwrap();
                peer_ops_clone.lock().unwrap().add_initial_peer(EndpointPair::new(public_endpoint, private_endpoint));
            }
        }
        else {
            println!("No introducer");
        }

        sleep(Duration::from_secs(2)).await;
        let node_info = self.as_node_info(peer_ops_clone, is_start, is_end);
        fs::write(format!("../peer_info/{}.json", node_info.name), serde_json::to_vec(&node_info).unwrap()).await.unwrap();
        
        if is_start {
            println!("Tcp listening");
            let server_context = ServerContext::new(srm_to_srp, Arc::new(tokio::sync::Mutex::new(from_smp)), sm_to_smp);
            http::tcp_listen(SocketAddr::from(([127,0,0,1], 8080)), server_context).await;
        }
        else {
            future::pending::<()>().await;
        }
    }

    pub fn as_node_info(&self, peer_ops: Arc<Mutex<PeerOps>>, is_start: bool, is_end: bool) -> NodeInfo {
        let name = if is_start { String::from("START") } else if is_end { String::from("END") + &self.endpoint_pair.public_endpoint.port().to_string() } else { self.endpoint_pair.public_endpoint.port().to_string() };
        NodeInfo {
            name,
            port: self.endpoint_pair.public_endpoint.port(),
            uuid: self.uuid.clone(),
            peers: peer_ops.lock().unwrap().peers_and_scores().iter().map(|(endpoint_pair, score)| (endpoint_pair.public_endpoint.port(), *score)).collect()
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