use std::{net::{SocketAddrV4, Ipv4Addr}, fmt::Display, collections::HashMap, hash::Hash};

use rand::{seq::SliceRandom, rngs::SmallRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, fs};
use uuid::Uuid;

use crate::{message::{Message, MessageDirection, MessageKind, MessageExt, self}, gateway, peer::peer_ops};

pub const SEARCH_MAX_HOP_COUNT: u8 = 5;
pub struct Node {
    endpoint_pair: EndpointPair,
    uuid: Uuid,
    breadcrumbs: HashMap<String, SocketAddrV4>,
    response_staging: HashMap<String, HashMap<usize, Message>>,
    nat_kind: NatKind,
    ingress: mpsc::UnboundedReceiver<Message>,
    egress: mpsc::UnboundedSender<Vec<Message>>
}

impl Node {
    pub fn new(endpoint_pair: EndpointPair, uuid: Uuid, ingress: mpsc::UnboundedReceiver<Message>, egress: mpsc::UnboundedSender<Vec<Message>>) -> Self {
        Self {
            endpoint_pair,
            uuid,
            breadcrumbs: HashMap::new(),
            response_staging: HashMap::new(),
            nat_kind: NatKind::Unknown, 
            ingress,
            egress
        }
    }

    pub async fn send_search_response(&mut self, mut search_request: Message, requested_filename: &str) {
        let hash = search_request.uuid();
        if self.breadcrumbs.contains_key(hash) {
            gateway::log_debug("Already visited this node, not propagating message");
            return;
        }
        else {
            self.breadcrumbs.insert(hash.to_owned(), *search_request.sender());
        }
        gateway::log_debug("Checking for resource");
        if !gateway::check_for_resource(requested_filename).await {
            gateway::log_debug("Resource not found");
            search_request.set_origin_if_unset(self.endpoint_pair.public_endpoint);
            self.try_send(search_request, |message| peer_ops::send_search_request(message));
            return;
        }
        // gateway::log_debug(&format!("Hash at hairpin {hash}"));
        let dest = self.get_dest_or_panic(hash);
        self.return_search_responses(self.construct_search_response(search_request.payload_inner().0.unwrap(), hash, dest, *search_request.message_ext().origin()).await).await;
    }

    async fn return_search_responses(&mut self, search_responses: Vec<Message>) {
        let hash = search_responses[0].uuid().to_owned();
        let dest = self.breadcrumbs.get(&hash).unwrap();
        gateway::log_debug(&format!("Returning search responses to {dest}"));
        self.egress.send(search_responses).unwrap();
        self.breadcrumbs.remove(&hash);
    }
    
    async fn construct_search_response(&self, filename: &str, hash: &str, dest: SocketAddrV4, origin: SocketAddrV4) -> Vec<Message> {
        let full_path = format!("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\{filename}");
        let file_bytes = fs::read(full_path).await.unwrap();
        let empty_message = Message::new(
            dest,
            self.endpoint_pair.public_endpoint,
            Some(MessageExt::new(origin,
            MessageDirection::Response,
            MessageKind::SearchResponse(filename.to_owned(), Vec::with_capacity(0)),
            SEARCH_MAX_HOP_COUNT,
            MessageExt::no_position())),
            Some(hash.to_owned()));
        let chunks = file_bytes.chunks(1024 - empty_message.size());
        let num_chunks = chunks.len();
        let mut messages: Vec<Message> = chunks
            .enumerate()
            .map(|(i, chunk)| empty_message.clone().set_position((i, num_chunks)).set_contents(chunk.to_vec()))
            .collect();
        messages.shuffle(&mut SmallRng::from_entropy());
        messages
    }

    fn try_send(&self, message: Message, f: impl Fn(Message) -> ()) {
        if let Some(mut result) = message.try_decrement_hop_count() {
            result.set_sender(self.endpoint_pair.public_endpoint);
            f(result);
        }
        else {
            gateway::log_debug("Max hop count reached");
        }
    }

    pub async fn receive(&mut self) {
        let message = self.ingress.recv().await.unwrap();
        match message.message_ext().payload() {
            MessageKind::SearchRequest(filename) => self.send_search_response(message.clone(), filename).await,
            MessageKind::SearchResponse(..) => {
                let (index, num_chunks) = *message.message_ext().position();
                if num_chunks == 1 {
                    // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
                    let dest = self.get_dest_or_panic(message.uuid());
                    self.return_search_responses(vec![message.replace_dest_and_timestamp(dest)]).await
                }
                else {
                    let hash = message.uuid().to_owned();
                    let search_responses = self.response_staging.entry(hash.clone()).or_insert(HashMap::with_capacity(num_chunks));
                    // gateway::log_debug(&format!("Collecting all search responses, {} of {}", index + 1, num_chunks));
                    search_responses.insert(index, message);
                    if search_responses.len() < num_chunks {
                        return;
                    }
                    gateway::log_debug("Collected all search responses");
                    let dest = self.get_dest_or_panic(&hash);
                    let search_responses = self.response_staging.remove(&hash).unwrap().into_values().map(|message| message.replace_dest_and_timestamp(dest)).collect();
                    self.return_search_responses(search_responses).await;
                }
            },
            _ => gateway::log_debug("not supported") 
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
