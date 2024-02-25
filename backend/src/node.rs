use std::{net::{SocketAddrV4, Ipv4Addr, SocketAddr}, fmt::Display, sync::{Arc, Mutex}, collections::HashMap, time::Duration, future, str::FromStr};

use serde::{Serialize, Deserialize};
use tokio::{fs, net::UdpSocket, sync::mpsc, time::sleep};
use uuid::Uuid;

use crate::{crypto::KeyStore, gateway::InboundGateway, http::{self, ServerContext}, message::{DiscoverPeerMessage, DpMessageKind, Heartbeat, Id, InboundMessage, IsEncrypted, Message, SeparateParts}, message_processing::{search::SearchRequestProcessor, stage::MessageStaging, stream::StreamMessageProcessor, DiscoverPeerProcessor, MessageProcessor, DPP_TTL_MILLIS, SRP_TTL_SECONDS}, peer::{self, PeerOps}, utils::TtlType};

pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: String,
    nat_kind: NatKind,
    socket: Arc<UdpSocket>,
    introducer: Option<EndpointPair>,
    initial_peers: Option<Vec<String>>
}

impl Node {
    pub async fn new(private_ip: String, private_port: String, public_ip: &str, uuid: String, introducer: Option<EndpointPair>, initial_peers: Option<Vec<String>>) -> Self {
        let socket = Arc::new(UdpSocket::bind(private_ip.clone() + ":" + &private_port).await.unwrap());
        let public_endpoint = SocketAddrV4::new(public_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        let private_endpoint = SocketAddrV4::new(private_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        println!("public: {:?}", public_endpoint);
        Self {
            endpoint_pair: EndpointPair::new(public_endpoint, private_endpoint),
            uuid,
            nat_kind: NatKind::Unknown,
            socket,
            introducer,
            initial_peers
        }
    }

    pub fn endpoint_pair(&self) -> EndpointPair { self.endpoint_pair }

    pub async fn listen(&self, is_start: bool, is_end: bool, report_trigger: Option<mpsc::Receiver<()>>) {
        let (srm_to_srp, srm_from_gateway) = mpsc::unbounded_channel();
        let (dpm_to_dpp, dpm_from_gateway) = mpsc::unbounded_channel();
        let (sm_to_smp, sm_from_gateway) = mpsc::unbounded_channel();
        let (to_staging, from_gateway) = mpsc::unbounded_channel();
        let (tx_to_smp, tx_from_http_handler) = mpsc::unbounded_channel();
    
        let key_store = Arc::new(Mutex::new(KeyStore::new()));
        let peer_ops = Arc::new(Mutex::new(PeerOps::new()));
        let peer_ops_clone = peer_ops.clone();
        let mut local_hosts = HashMap::new();
        if is_end {
            local_hosts.insert(String::from("example"), SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000));
        }
        let mut message_staging = MessageStaging::new(from_gateway, srm_to_srp.clone(), dpm_to_dpp, sm_to_smp.clone(), &key_store, self.endpoint_pair);
        let mut srp = SearchRequestProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops.clone()), TtlType::Secs(SRP_TTL_SECONDS)), srm_from_gateway, sm_to_smp.clone(), local_hosts.clone());
        let mut dpp = DiscoverPeerProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops.clone()), TtlType::Millis(DPP_TTL_MILLIS)), dpm_from_gateway);
        let mut smp = StreamMessageProcessor::new(MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, None, TtlType::Secs(SRP_TTL_SECONDS)), sm_from_gateway, local_hosts, tx_from_http_handler);
    
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
        
        let heartbeat_mp = MessageProcessor::new(self.socket.clone(), self.endpoint_pair, &key_store, Some(peer_ops), TtlType::Secs(0));
        tokio::spawn(async move {
            loop {
                if let Err(e) = heartbeat_mp.send_request(&mut Heartbeat::new(), None, true) {
                    println!("Heartbeats stopped: {}", e);
                    return;
                };
                sleep(Duration::from_secs(29)).await;
            }
        });

        if let Some(introducer) = self.introducer {
            let mut message = DiscoverPeerMessage::new(DpMessageKind::INeedSome,
                EndpointPair::default_socket(),
                Id(Uuid::new_v4().as_bytes().to_vec()),
                (peer::MAX_PEERS, peer::MAX_PEERS));
            message.add_peer(introducer);
            let inbound_message = InboundMessage::new(bincode::serialize(&message).unwrap(), IsEncrypted::False, SeparateParts::new(self.endpoint_pair.public_endpoint, message.id().to_owned()));
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

        if let Some(mut report_trigger) = report_trigger {
            let (port, uuid) = (self.endpoint_pair.public_endpoint.port(), self.uuid.clone());
            tokio::spawn(async move {
                report_trigger.recv().await;
                let node_info = NodeInfo::new(peer_ops_clone, is_start, is_end, port, uuid);
                fs::write(format!("../peer_info/{}.json", node_info.name), serde_json::to_vec(&node_info).unwrap()).await.unwrap();
            });
        }
        
        if is_start {
            println!("Tcp listening");
            let server_context = ServerContext::new(srm_to_srp, sm_to_smp, tx_to_smp);
            http::tcp_listen(SocketAddr::from(([127,0,0,1], 8080)), server_context).await;
        }
        else {
            future::pending::<()>().await;
        }
    }

    pub async fn from_node_info(value: NodeInfo) -> (Self, bool, bool) {
        let port = value.port.to_string();
        let peers = value.peers.into_iter().map(|(peer_port, _)| String::from("127.0.0.1:") + &peer_port.to_string()).collect();
        (Self::new(String::from("127.0.0.1"), port, "127.0.0.1", value.uuid, None, Some(peers)).await, value.is_start, value.is_end)
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
    peers: Vec<(u16, i32)>,
    is_start: bool,
    is_end: bool
}
impl NodeInfo {
    pub fn new(peer_ops: Arc<Mutex<PeerOps>>, is_start: bool, is_end: bool, port: u16, uuid: String) -> NodeInfo {
        let port_str = port.to_string();
        let name = if is_start { String::from("START") } else if is_end { String::from("END") + &port_str } else { port_str };
        NodeInfo {
            name,
            port,
            uuid,
            peers: peer_ops.lock().unwrap().peers_and_scores().iter().map(|(endpoint_pair, score)| (endpoint_pair.public_endpoint.port(), *score)).collect(),
            is_start,
            is_end
        }
    }
}