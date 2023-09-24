use chrono::{Utc, SecondsFormat};
use ring::digest::{Context, SHA256};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4};

pub trait Message {
    fn base_message(&self) -> &BaseMessage;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BaseMessage {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
}

impl BaseMessage {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4) -> Self {
        Self {
            dest,
            sender,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
        }
    }

    pub fn dest(&self) -> &SocketAddrV4 { return &self.dest }
    pub fn sender(&self) -> &SocketAddrV4 { return &self.sender }
}

#[derive(Serialize, Deserialize)]
pub struct Heartbeat(pub BaseMessage);

impl Message for Heartbeat {
    fn base_message(&self) -> &BaseMessage { &self.0 }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FullMessage {
    base_message: BaseMessage,
    origin: SocketAddrV4,
    message_direction: MessageDirection,
    payload: MessageKind,
    hop_count: usize,
    max_hop_count: usize,
    hash: String
}
impl FullMessage {
    pub fn new(base_message: BaseMessage, origin: SocketAddrV4, message_direction: MessageDirection, payload: MessageKind, hop_count: usize, max_hop_count: usize) -> Self {
        let hash = Self::hash_for_message(&origin, &message_direction, &payload);
        Self {
            base_message,
            origin,
            message_direction,
            payload,
            hop_count,
            max_hop_count,
            hash
        }
    }
    
    pub fn payload(&self) -> &MessageKind { &self.payload }
    pub fn hash(&self) -> &String { &self.hash }
    pub fn origin(&self) -> &SocketAddrV4 { &self.origin }
    pub fn direction(&self) -> &MessageDirection { &self.message_direction }

    pub fn try_increment_hop_count(mut self) -> Option<Self> {
        if self.hop_count < self.max_hop_count {
            self.hop_count += 1;
            return Some(self);
        }
        None
    }

    pub fn replace_sender(&mut self, sender: SocketAddrV4) {
        self.base_message.sender = sender;
    }

    pub fn replace_dest_and_timestamp(&self, dest: SocketAddrV4) -> Self {
        let mut result = self.clone();
        result.base_message.dest = dest;
        result.base_message.timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        result
    }
    
    pub fn hash_for_message(origin: &SocketAddrV4, message_direction: &MessageDirection, payload: &MessageKind) -> String {
        let mut context = Context::new(&SHA256);
        context.update(origin.to_string().as_bytes());
        context.update(serde_json::to_string(&message_direction).unwrap().as_bytes());
        context.update(serde_json::to_string(&payload).unwrap().as_bytes());
        let digest = context.finish();
        str::from_utf8(digest.as_ref()).unwrap().to_owned()
    }
}
impl Message for FullMessage {
    fn base_message(&self) -> &BaseMessage { return &self.base_message }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum MessageDirection {
    Request,
    Response,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageKind {
    DiscoverPeerRequest,
    DiscoverPeerResponse(String),
    SearchRequest(String),
    SearchResponse(String, Vec<u8>),
    ResourceAvailable(String)
}
impl MessageKind {
    pub fn inner(&self) -> (&str, &[u8]) {
        match self {
            MessageKind::SearchResponse(filename, contents) => (filename, contents),
            _ => panic!("Message contents not available")
        }
    }
}