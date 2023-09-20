use std::{net::{SocketAddrV4, Ipv4Addr}, fmt::Display};

use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, fs};
use uuid::Uuid;

use crate::{message::{BaseMessage, FullMessage, MessageDirection, MessageKind, Message}, gateway, peer::{Peer, peer_ops}};

pub struct Node {
    pub endpoint_pair: EndpointPair,
    uuid: Uuid,
    found_by: Vec<Peer>,
    nat_kind: NatKind,
    ingress: mpsc::UnboundedReceiver<FullMessage>,
    egress: mpsc::UnboundedSender<FullMessage>
}

impl Node {
    pub fn new(endpoint_pair: EndpointPair, uuid: Uuid, ingress: mpsc::UnboundedReceiver<FullMessage>, egress: mpsc::UnboundedSender<FullMessage>) -> Self {
        Self {
            endpoint_pair,
            uuid,
            found_by: Vec::new(),
            nat_kind: NatKind::Unknown, 
            ingress,
            egress
        }
    }

    pub async fn send_search_response(&self, requester: &FullMessage, requested_filename: &str) {
        gateway::log_debug("Checking for resource");
        if !check_for_resource(requested_filename).await {
            gateway::log_debug("Resource not found");
            peer_ops::send_search_request(requested_filename.to_owned(), self.endpoint_pair, *requester.origin());
            return;
        }
        if *requester.base_message().sender() == EndpointPair::default() {
            gateway::log_debug("Resource available locally, bypassing network");
            gateway::notify_resource_available(requested_filename.to_owned());
            return;
        }
        let full_path = format!("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\{}", requested_filename);
        let payload = MessageKind::SearchResponse(String::from(requested_filename), fs::read(full_path).await.unwrap());
        gateway::log_debug("Sending search response");
        let message = FullMessage::new(BaseMessage::new(*requester.base_message().sender(), self.endpoint_pair), self.endpoint_pair, MessageDirection::Response, payload.clone(), 0, 0);
        self.egress.send(message).unwrap();
    }

    pub async fn receive(&mut self) {
        let message = self.ingress.recv().await.unwrap();
        match message.payload() {
            MessageKind::SearchRequest(filename) => self.send_search_response(&message, filename).await,
            _ => { gateway::log_debug("message wasn't a search request"); } 
        }
    }

    pub fn to_node_info(&self) -> NodeInfo {
        NodeInfo {
            endpoint_pair: self.endpoint_pair,
            uuid: self.uuid.to_string(),
            nat_kind: self.nat_kind,
            max_peers: peer_ops::MAX_PEERS
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}
impl Eq for Node {}

// pub struct RendevousNode(Node);

#[derive(Hash, Clone, Serialize, Deserialize, Copy)]
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

    pub fn default() -> Self {
        Self {
            public_endpoint: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0),
            private_endpoint: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)
        }
    }
}

impl PartialEq for EndpointPair {
    fn eq(&self, other: &Self) -> bool {
        self.public_endpoint == other.public_endpoint
            && self.private_endpoint == other.private_endpoint
    }
}
impl Eq for EndpointPair {}
impl Display for EndpointPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Public: {}, Private: {}", self.private_endpoint, self.public_endpoint)
    }
}

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected
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
    pub endpoint_pair: EndpointPair,
    uuid: String,
    nat_kind: NatKind,
    max_peers: usize
}

async fn check_for_resource(requested_filename: &str) -> bool {
    let mut filenames = fs::read_dir("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\").await.unwrap();
    while let Ok(Some(filename)) = filenames.next_entry().await {
        if filename.file_name().to_str().unwrap() == requested_filename {
            return true;
        }
    }
    false
}
