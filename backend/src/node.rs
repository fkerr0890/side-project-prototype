use std::{net::{SocketAddrV4, Ipv4Addr}, fmt::Display, collections::HashMap};

use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, fs};
use uuid::Uuid;

use crate::{message::{BaseMessage, FullMessage, MessageDirection, MessageKind, Message}, gateway, peer::peer_ops};

pub const SEARCH_MAX_HOP_COUNT: usize = 5;
pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: Uuid,
    breadcrumbs: HashMap<String, SocketAddrV4>,
    nat_kind: NatKind,
    ingress: mpsc::UnboundedReceiver<FullMessage>,
    egress: mpsc::UnboundedSender<FullMessage>
}

impl Node {
    pub fn new(endpoint_pair: EndpointPair, uuid: Uuid, ingress: mpsc::UnboundedReceiver<FullMessage>, egress: mpsc::UnboundedSender<FullMessage>) -> Self {
        Self {
            endpoint_pair,
            uuid,
            breadcrumbs: HashMap::new(),
            nat_kind: NatKind::Unknown, 
            ingress,
            egress
        }
    }

    async fn send_search_response(&mut self, mut search_request: FullMessage, requested_filename: &str) {
        if self.breadcrumbs.contains_key(search_request.hash()) {
            gateway::log_debug("Already visited this node, not propagating message");
            return;
        }
        else {
            self.breadcrumbs.insert(search_request.hash().to_owned(), *search_request.base_message().sender());
        }
        gateway::log_debug("Checking for resource");
        if !gateway::check_for_resource(requested_filename).await {
            gateway::log_debug("Resource not found");
            search_request.set_origin_if_unset(self.endpoint_pair.public_endpoint);
            self.try_send(search_request, |message| peer_ops::send_search_request(message));
            return;
        }
        self.return_search_response(search_request).await;
    }

    async fn return_search_response(&mut self, message: FullMessage) {
        if let Some(dest) = self.breadcrumbs.get(message.hash()) {
            let search_response = match message.payload() {
                MessageKind::SearchResponse(..) => message.replace_dest_and_timestamp(*dest),
                MessageKind::SearchRequest(filename) => {
                    let full_path = format!("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\{}", filename);
                    let payload = MessageKind::SearchResponse(filename.to_owned(), fs::read(full_path).await.unwrap());
                    FullMessage::new(BaseMessage::new(*dest, self.endpoint_pair.public_endpoint), *message.origin(), MessageDirection::Response, payload, 0, SEARCH_MAX_HOP_COUNT, Some(message.hash().to_owned()))
                }
                _ => panic!("return_search_response() improperly called")
            };
            gateway::log_debug(&format!("Returning search response to {}", dest));
            self.try_send(search_response, |message| self.egress.send(message).unwrap());
            self.breadcrumbs.remove(message.hash());
        }
        else {
            gateway::log_debug("No breadcrumb found for search response");
        }
    }

    fn try_send(&self, message: FullMessage, f: impl Fn(FullMessage) -> ()) {
        if let Some(mut result) = message.try_increment_hop_count() {
            result.set_sender(self.endpoint_pair.public_endpoint);
            f(result);
        }
        else {
            gateway::log_debug("Max hop count reached");
        }
    }

    pub async fn receive(&mut self) {
        let message = self.ingress.recv().await.unwrap();
        match message.payload() {
            MessageKind::SearchRequest(filename) => self.send_search_response(message.clone(), filename).await,
            MessageKind::SearchResponse(..) => self.return_search_response(message).await,
            _ => gateway::log_debug("message wasn't a search request") 
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

#[derive(Hash, Clone, Serialize, Deserialize, Copy, Eq, PartialEq)]
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
