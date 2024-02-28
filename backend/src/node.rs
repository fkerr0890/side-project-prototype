use std::{net::{SocketAddrV4, Ipv4Addr, SocketAddr}, fmt::Display, sync::{Arc, Mutex}, collections::HashMap, time::Duration, future, str::FromStr};

use serde::{Serialize, Deserialize};
use tokio::{fs, net::UdpSocket, sync::mpsc, time::sleep};
use uuid::Uuid;

use crate::{crypto::KeyStore, http::{self, ServerContext}, message::{DiscoverPeerMessage, DpMessageKind, Heartbeat, Id, InboundMessage, IsEncrypted, Message, Peer, Sender, SeparateParts}, message_processing::{search::SearchRequestProcessor, stage::MessageStaging, stream::StreamMessageProcessor, DiscoverPeerProcessor, InboundGateway, OutboundGateway, DPP_TTL_MILLIS, HEARTBEAT_INTERVAL_SECONDS, SRP_TTL_SECONDS}, peer::{self, PeerOps}, utils::TtlType};

pub struct Node {
    nat_kind: NatKind
}

impl Node {
    pub fn new() -> Self {
        Self {
            nat_kind: NatKind::Unknown
        }
    }

    pub async fn get_socket(private_ip: String, private_port: String, public_ip: &str) -> (EndpointPair, Arc<UdpSocket>) {
        let socket = Arc::new(UdpSocket::bind(private_ip.clone() + ":" + &private_port).await.unwrap());
        let public_endpoint = SocketAddrV4::new(public_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        let private_endpoint = SocketAddrV4::new(private_ip.parse().unwrap(), socket.local_addr().unwrap().port());
        println!("public: {:?}", public_endpoint);
        (EndpointPair::new(public_endpoint, private_endpoint), socket)
    }

    pub async fn listen(&self, is_start: bool, is_end: bool, report_trigger: Option<mpsc::Receiver<()>>, introducer: Option<Peer>, uuid: String, initial_peers: Vec<(String, String)>, endpoint_pair: EndpointPair, socket: Arc<UdpSocket>) {
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
        let myself = Peer::new(endpoint_pair, uuid.clone());
        let mut message_staging = MessageStaging::new(from_gateway, srm_to_srp.clone(), dpm_to_dpp, sm_to_smp.clone(), OutboundGateway::new(socket.clone(), myself.clone(), &key_store, Some(peer_ops.clone()), TtlType::Secs(0)));
        let mut srp = SearchRequestProcessor::new(OutboundGateway::new(socket.clone(), myself.clone(), &key_store, Some(peer_ops.clone()), TtlType::Secs(SRP_TTL_SECONDS)), srm_from_gateway, sm_to_smp.clone(), local_hosts.clone());
        let mut dpp = DiscoverPeerProcessor::new(OutboundGateway::new(socket.clone(), myself.clone(), &key_store, Some(peer_ops.clone()), TtlType::Millis(DPP_TTL_MILLIS)), dpm_from_gateway);
        let mut smp = StreamMessageProcessor::new(OutboundGateway::new(socket.clone(), myself.clone(), &key_store, None, TtlType::Secs(SRP_TTL_SECONDS)), sm_from_gateway, local_hosts, tx_from_http_handler);
    
        for _ in 0..225 {
            let mut inbound_gateway = InboundGateway::new(&socket, to_staging.clone());
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
        
        let heartbeat_gateway = OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops), TtlType::Secs(0));
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
                if let Err(e) = heartbeat_gateway.send_request(&mut Heartbeat::new(), None) {
                    println!("Heartbeats stopped: {}", e);
                    return;
                };
            }
        });

        if let Some(introducer) = introducer {
            let mut message = DiscoverPeerMessage::new(DpMessageKind::INeedSome,
                None,
                Id(Uuid::new_v4().as_bytes().to_vec()),
                (peer::MAX_PEERS, peer::MAX_PEERS));
            message.add_peer(introducer);
            let inbound_message = InboundMessage::new(bincode::serialize(&message).unwrap(), IsEncrypted::False, SeparateParts::new(Sender::new(endpoint_pair.private_endpoint, uuid.clone()), message.id().to_owned()));
            socket.send_to(&bincode::serialize(&inbound_message).unwrap(), endpoint_pair.private_endpoint).await.unwrap();
        }
        else {
            println!("No introducer");
        }

        for (peer, uuid) in initial_peers {
            let public_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let private_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let peer = Peer::new(EndpointPair::new(public_endpoint, private_endpoint), uuid);
            peer_ops_clone.lock().unwrap().add_initial_peer(peer);
        }

        if let Some(mut report_trigger) = report_trigger {
            let (port, uuid) = (endpoint_pair.public_endpoint.port(), uuid.clone());
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

    pub fn read_node_info(value: NodeInfo) -> (u16, String, Vec<(String, String)>, bool, bool) {
        let port = value.port;
        let peers = value.peers.into_iter().map(|(peer_port, _, uuid)| (String::from("127.0.0.1:") + &peer_port.to_string(), uuid)).collect();
        (port, value.uuid, peers, value.is_start, value.is_end)
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Copy, Eq, PartialEq, Debug)]
pub struct EndpointPair {
    pub public_endpoint: SocketAddrV4,
    pub private_endpoint: SocketAddrV4
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
    peers: Vec<(u16, i32, String)>,
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
            peers: peer_ops.lock().unwrap().peers_and_scores().into_iter().map(|(endpoint_pair, score, uuid)| (endpoint_pair.public_endpoint.port(), score, uuid)).collect(),
            is_start,
            is_end
        }
    }
}