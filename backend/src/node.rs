use std::{collections::HashMap, fmt::Display, future, net::{Ipv4Addr, SocketAddr, SocketAddrV4}, str::FromStr, sync::{Arc, Mutex}, time::Duration};

use serde::{Serialize, Deserialize};
use tokio::{fs, net::UdpSocket, sync::mpsc, time::sleep};
use tracing::error;
use uuid::Uuid;

use crate::{crypto::KeyStore, http::{self, ServerContext}, message::{DiscoverPeerMessage, DistributionMessage, DpMessageKind, Heartbeat, InboundMessage, IsEncrypted, Message, NumId, Peer, Sender, SeparateParts}, message_processing::{distribute::DistributionHandler, search::SearchRequestProcessor, stage::MessageStaging, stream::StreamMessageProcessor, BreadcrumbService, DiscoverPeerProcessor, InboundGateway, OutboundGateway, DISTRIBUTION_TTL_SECONDS, DPP_TTL_MILLIS, HEARTBEAT_INTERVAL_SECONDS, SRP_TTL_SECONDS}, option_early_return, peer::{self, PeerOps}, utils::TtlType};

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

    pub async fn listen(&self, is_start: bool, is_end: bool, report_trigger: Option<mpsc::Receiver<()>>, introducer: Option<Peer>, id: NumId, initial_peers: Vec<(String, NumId)>, endpoint_pair: EndpointPair, socket: Arc<UdpSocket>) {
        let (srm_to_srp, srm_from_gateway) = mpsc::unbounded_channel();
        let (dpm_to_dpp, dpm_from_gateway) = mpsc::unbounded_channel();
        let (sm_to_smp, sm_from_gateway) = mpsc::unbounded_channel();
        let (to_staging, from_gateway) = mpsc::unbounded_channel();
        let (tx_to_smp, tx_from_http_handler) = mpsc::unbounded_channel();
        let (srm_to_srp2, srm_from_staging) = mpsc::unbounded_channel();
        let (sm_to_smp2, sm_from_staging) = mpsc::unbounded_channel();
        let (tx_to_smp2, tx_from_dp) = mpsc::unbounded_channel();
        let (dm_to_dh, dm_from_staging) = mpsc::unbounded_channel();
    
        let key_store = Arc::new(Mutex::new(KeyStore::new()));
        let peer_ops = Arc::new(Mutex::new(PeerOps::new()));
        let peer_ops_clone = peer_ops.clone();
        let mut local_hosts = HashMap::new();
        if is_end {
            local_hosts.insert(String::from("example"), SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000));
        }

        let myself = Peer::new(endpoint_pair, id);
        let bs = BreadcrumbService::new(TtlType::Secs(DISTRIBUTION_TTL_SECONDS));
        let search_ttl = TtlType::Secs(SRP_TTL_SECONDS);
        let discover_ttl = TtlType::Millis(DPP_TTL_MILLIS);
        let mut message_staging = MessageStaging::new(from_gateway, srm_to_srp.clone(), dpm_to_dpp, sm_to_smp.clone(), srm_to_srp2.clone(), sm_to_smp2.clone(), dm_to_dh.clone(), OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops.clone())));
        let local_hosts_clone = local_hosts.clone();
        let mut srp = SearchRequestProcessor::new(OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops.clone())), bs.clone(search_ttl), srm_from_gateway, sm_to_smp.clone(), move |m| local_hosts_clone.contains_key(m.host_name()));
        let mut dpp = DiscoverPeerProcessor::new(OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops.clone())), bs.clone(discover_ttl), dpm_from_gateway);
        let mut smp = StreamMessageProcessor::new(OutboundGateway::new(socket.clone(), myself, &key_store, None), sm_from_gateway, local_hosts.clone(), tx_from_http_handler);
        let mut dsrp = SearchRequestProcessor::new(OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops.clone())), bs.clone(search_ttl), srm_from_staging, sm_to_smp2.clone(), |m| m.origin().unwrap().endpoint_pair.private_endpoint != m.dest() && m.origin().unwrap().endpoint_pair.public_endpoint != m.dest());
        let mut dsmp = StreamMessageProcessor::new(OutboundGateway::new(socket.clone(), myself, &key_store, None), sm_from_staging, local_hosts, tx_from_dp);
        let mut distribution_handler = DistributionHandler::new(dm_from_staging, srm_to_srp2, sm_to_smp2, tx_to_smp2, OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops.clone())), bs);
    
        for _ in 0..225 {
            let mut inbound_gateway = InboundGateway::new(&socket, to_staging.clone());
            tokio::spawn(async move {
                loop {
                    inbound_gateway.receive().await;
                }
            });
        }

        tokio::spawn(async move {
            loop {
                option_early_return!(message_staging.receive().await, error!("Staging channel closed"));
            }
        });
    
        tokio::spawn(async move {
            loop {
                option_early_return!(srp.receive().await, error!("SRP channel closed"));
            }
        });
    
        tokio::spawn(async move {
            loop {
                option_early_return!(dpp.receive().await, error!("DPP channel closed"));
            }
        });

        tokio::spawn(async move {
            loop {
                option_early_return!(smp.receive().await, error!("SMP channel closed"));
            }
        });
        
        let heartbeat_gateway = OutboundGateway::new(socket.clone(), myself, &key_store, Some(peer_ops));
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
                heartbeat_gateway.send_request(&mut Heartbeat::new(), None);
            }
        });

        tokio::spawn(async move {
            loop {
                option_early_return!(dsrp.receive().await, error!("DSRP channel closed"));
            }
        });

        tokio::spawn(async move {
            loop {
                option_early_return!(dsmp.receive().await, error!("DSMP channel closed"));
            }
        });

        tokio::spawn(async move {
            loop {
                option_early_return!(distribution_handler.receive().await, error!("Distribution channel closed"));
            }
        });

        if let Some(introducer) = introducer {
            let mut message = DiscoverPeerMessage::new(DpMessageKind::INeedSome,
                None,
                NumId(Uuid::new_v4().as_u128()),
                (peer::MAX_PEERS, peer::MAX_PEERS));
            message.add_peer(introducer);
            let inbound_message = InboundMessage::new(bincode::serialize(&message).unwrap(), IsEncrypted::False, SeparateParts::new(Sender::new(endpoint_pair.private_endpoint, id), message.id()));
            socket.send_to(&bincode::serialize(&inbound_message).unwrap(), endpoint_pair.private_endpoint).await.unwrap();
        }
        else {
            println!("No introducer");
        }

        for (peer, id) in initial_peers {
            let public_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let private_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let peer = Peer::new(EndpointPair::new(public_endpoint, private_endpoint), id);
            peer_ops_clone.lock().unwrap().add_initial_peer(peer);
        }

        if let Some(mut report_trigger) = report_trigger {
            let (port, id) = (endpoint_pair.public_endpoint.port(), id);
            tokio::spawn(async move {
                report_trigger.recv().await;
                let node_info = NodeInfo::new(peer_ops_clone, is_start, is_end, port, id.0);
                fs::write(format!("../peer_info/{}.json", node_info.name), serde_json::to_vec(&node_info).unwrap()).await.unwrap();
                // if is_start {
                //     println!("Starting distribution");
                //     let dmessage = DistributionMessage::new(NumId(Uuid::new_v4().as_u128()), 2, String::from("Apple Cover Letter.pdf"));
                //     dm_to_dh.send(dmessage).unwrap();
                // }
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

    pub fn read_node_info(value: NodeInfo) -> (u16, NumId, Vec<(String, NumId)>, bool, bool) {
        let port = value.port;
        let peers = value.peers.into_iter().map(|(peer_port, _, id)| (String::from("127.0.0.1:") + &peer_port.to_string(), NumId(id))).collect();
        (port, NumId(value.id), peers, value.is_start, value.is_end)
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
    id: u128,
    peers: Vec<(u16, i32, u128)>,
    is_start: bool,
    is_end: bool
}
impl NodeInfo {
    pub fn new(peer_ops: Arc<Mutex<PeerOps>>, is_start: bool, is_end: bool, port: u16, id: u128) -> NodeInfo {
        let port_str = port.to_string();
        let name = if is_start { String::from("START") } else if is_end { String::from("END") + &port_str } else { port_str };
        NodeInfo {
            name,
            port,
            id,
            peers: peer_ops.lock().unwrap().peers_and_scores().into_iter().map(|(endpoint_pair, score, id)| (endpoint_pair.public_endpoint.port(), score, id.0)).collect(),
            is_start,
            is_end
        }
    }
}