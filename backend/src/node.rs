use std::{net::{SocketAddrV4, Ipv4Addr}, fmt::Display, collections::HashMap, hash::Hash};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{message::{Message, MessageKind, MessageExt}, gateway, peer::peer_ops, http::{SerdeHttpResponse, self}};

pub const SEARCH_MAX_HOP_COUNT: u8 = 5;
pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: Uuid,
    breadcrumbs: HashMap<String, SocketAddrV4>,
    response_staging: HashMap<String, HashMap<usize, Message>>,
    nat_kind: NatKind,
    ingress: mpsc::UnboundedReceiver<Message>,
    egress: mpsc::UnboundedSender<Message>,
    local_hosts: HashMap<String, SocketAddrV4>
}

impl Node {
    pub fn new(endpoint_pair: EndpointPair, uuid: Uuid, ingress: mpsc::UnboundedReceiver<Message>, egress: mpsc::UnboundedSender<Message>, local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            endpoint_pair,
            uuid,
            breadcrumbs: HashMap::new(),
            response_staging: HashMap::new(),
            nat_kind: NatKind::Unknown, 
            ingress,
            egress,
            local_hosts
        }
    }

    pub async fn send_search_response(&mut self, mut search_request_parts: Vec<Message>, origin: SocketAddrV4) {
        let hash = search_request_parts[0].uuid().to_owned();
        if self.breadcrumbs.contains_key(&hash) {
            gateway::log_debug("Already visited this node, not propagating message");
            return;
        }
        else {
            self.breadcrumbs.insert(hash.to_owned(), *search_request_parts[0].sender());
        }
        gateway::log_debug("Checking for resource");
        if let Some(socket) = self.local_hosts.get(search_request_parts[0].message_ext().kind().host_name()) {
            // gateway::log_debug(&format!("Hash at hairpin {hash}"));
            let search_request = if search_request_parts.len() > 1 { gateway::reassemble_resource(search_request_parts) } else { search_request_parts.pop().unwrap() };
            let search_request = bincode::deserialize(&search_request.message_ext().payload()).unwrap();
            let response = http::make_request(search_request, String::from("http://") + &socket.to_string()).await;
            let dest = self.get_dest_or_panic(&hash);
            self.return_search_responses(self.construct_search_response(response, &hash, dest, origin).await).await;
            return;
        }
        gateway::log_debug("Resource not found");
        if search_request_parts.len() == 1 && bincode::serialized_size(&search_request_parts[0]).unwrap() > 1024 {            
            peer_ops::send_search_request(search_request_parts.pop().unwrap().chunked(), self.endpoint_pair.public_endpoint);
        }
        else {
            peer_ops::send_search_request(search_request_parts, self.endpoint_pair.public_endpoint);
        }
    }

    async fn return_search_responses(&mut self, search_responses: Vec<Message>) {
        let hash = search_responses[0].uuid().to_owned();
        let dest = self.breadcrumbs.get(&hash).unwrap();
        gateway::log_debug(&format!("Returning search responses to {dest}"));
        for response in search_responses {
            self.egress.send(response).unwrap();
        }
        self.breadcrumbs.remove(&hash);
    }
    
    async fn construct_search_response(&self, response: SerdeHttpResponse, hash: &str, dest: SocketAddrV4, origin: SocketAddrV4) -> Vec<Message> {
        Message::initial_http_response(dest, self.endpoint_pair.public_endpoint, origin, hash.to_owned(), response).chunked()
    }

    pub async fn receive(&mut self) {
        let message = self.ingress.recv().await.unwrap();
        if let Some(messages) = self.collect_messages(message) {
            match messages[0] {
                Message { message_ext: Some(MessageExt { kind: MessageKind::HttpRequest(..), origin, ..}), .. } => self.send_search_response(messages, origin).await,
                Message { message_ext: Some(MessageExt { kind: MessageKind::HttpResponse, .. }), .. } => {
                    
                },
                _ => gateway::log_debug("not supported") 
            }
        }
    }

    fn collect_messages(&mut self, message: Message) -> Option<Vec<Message>> {
        let (index, num_chunks) = *message.message_ext().position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            let dest = self.get_dest_or_panic(message.uuid());
            Some(vec![message.replace_dest_and_timestamp(dest)])
        }
        else {
            let hash = message.uuid().to_owned();
            let messages = self.response_staging.entry(hash.clone()).or_insert(HashMap::with_capacity(num_chunks));
            // gateway::log_debug(&format!("Collecting all search responses, {} of {}", index + 1, num_chunks));
            messages.insert(index, message);
            if messages.len() < num_chunks {
                return None;
            }
            gateway::log_debug("Collected all messages");
            let dest = self.get_dest_or_panic(&hash);
            let messages: Vec<Message> = self.response_staging.remove(&hash).unwrap().into_values().map(|message| message.replace_dest_and_timestamp(dest)).collect();
            Some(messages)
        }
    }

    fn get_dest_or_panic(&self, hash: &str) -> SocketAddrV4 {
        if let Some(dest) = self.breadcrumbs.get(hash) {
            *dest
        }
        else {
            panic!("No breadcrumb found");
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
