use std::{fmt::Display, future, net::{Ipv4Addr, SocketAddr, SocketAddrV4}, str::FromStr, sync::Arc};
use rustc_hash::FxHashMap;

use serde::{Serialize, Deserialize};
use tokio::{fs, net::UdpSocket, sync::mpsc};
use tracing::{debug, error};

use crate::{http::{self, SerdeHttpResponse, ServerContext}, lock, message::{Message, NumId, Peer}, message_processing::{stage::{ClientApiRequest, MessageStaging}, DiscoverPeerProcessor, InboundGateway, OutboundGateway}, option_early_return, peer};

pub struct Node {
    nat_kind: NatKind
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
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

    pub async fn listen(&mut self,
        is_start: bool,
        is_end: bool,
        report_trigger: Option<mpsc::Receiver<()>>,
        introducer: Option<Peer>, id: NumId,
        initial_peers: Vec<(String, NumId)>,
        endpoint_pair: EndpointPair,
        socket: Arc<UdpSocket>,
        server_context: ServerContext,
        http_handler_rx: mpsc::UnboundedReceiver<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>,
        client_api_tx: mpsc::UnboundedSender<ClientApiRequest>,
        client_api_rx: mpsc::UnboundedReceiver<ClientApiRequest>
    ) {
        let (to_staging, from_gateway) = mpsc::unbounded_channel();
    
        let mut local_hosts = FxHashMap::default();
        if is_end {
            local_hosts.insert(String::from("example"), SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000));
        }

        let myself = Peer::new(endpoint_pair, id);

        let mut peers = Vec::new();
        for (peer, id) in initial_peers {
            let public_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let private_endpoint = SocketAddrV4::from_str(&peer).unwrap();
            let peer = Peer::new(EndpointPair::new(public_endpoint, private_endpoint), id);
            peers.push(peer);
        }

        let mut message_staging = MessageStaging::new(from_gateway, OutboundGateway::new(socket.clone(), to_staging.clone(), myself), DiscoverPeerProcessor::new(), client_api_tx, client_api_rx, http_handler_rx, local_hosts, peers);

        if let Some(introducer) = introducer {
            let message = Message::new_discover_peer_request(myself, introducer, peer::MAX_PEERS);
            message_staging.client_api_tx().send(ClientApiRequest::Message(message)).unwrap();
        }
        else {
            debug!("No introducer");
        }

        let peer_ops = message_staging.peer_ops().clone();
        if let Some(mut report_trigger) = report_trigger {
            let (port, id) = (endpoint_pair.public_endpoint.port(), id);
            tokio::spawn(async move {
                report_trigger.recv().await;
                let node_info = NodeInfo::new(lock!(peer_ops).peers_and_scores(), is_start, is_end, port, id.0);
                fs::write(format!("../peer_info/{}.json", node_info.name), serde_json::to_vec(&node_info).unwrap()).await.unwrap();
            });
        }

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

        if is_start {
            http::tcp_listen(SocketAddr::from(([127,0,0,1], 8080)), server_context).await;
        } else {
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
    pub fn new(peers_and_scores: Vec<(EndpointPair, i32, NumId)>, is_start: bool, is_end: bool, port: u16, id: u128) -> NodeInfo {
        let port_str = port.to_string();
        let name = if is_start { String::from("START") } else if is_end { String::from("END") + &port_str } else { port_str };
        NodeInfo {
            name,
            port,
            id,
            peers: peers_and_scores.into_iter().map(|(endpoint_pair, score, id)| (endpoint_pair.public_endpoint.port(), score, id.0)).collect(),
            is_start,
            is_end
        }
    }
}