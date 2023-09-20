use crate::node::EndpointPair;
use chrono::{Utc, SecondsFormat};
use serde::{Serialize, Deserialize};

pub trait Message {
    fn base_message(&self) -> &BaseMessage;
}

#[derive(Serialize, Deserialize)]
pub struct BaseMessage {
    dest: EndpointPair,
    sender: EndpointPair,
    timestamp: String,
}

impl BaseMessage {
    pub fn new(dest: EndpointPair, sender: EndpointPair) -> Self {
        Self {
            dest,
            sender,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
        }
    }

    pub fn dest(&self) -> &EndpointPair { return &self.dest }
    pub fn sender(&self) -> &EndpointPair { return &self.sender }
}

#[derive(Serialize, Deserialize)]
pub struct Heartbeat(pub BaseMessage);

impl Message for Heartbeat {
    fn base_message(&self) -> &BaseMessage { return &self.0 }
}

#[derive(Serialize, Deserialize)]
pub struct FullMessage {
    base_message: BaseMessage,
    origin: EndpointPair,
    message_direction: MessageDirection,
    payload: MessageKind,
    hop_count: usize,
    max_hop_count: usize,
}
impl FullMessage {
    pub fn new(base_message: BaseMessage, origin: EndpointPair, message_direction: MessageDirection, payload: MessageKind, hop_count: usize, max_hop_count: usize) -> Self {
        Self {
            base_message,
            origin,
            message_direction,
            payload,
            hop_count,
            max_hop_count,
        }
    }
    
    pub fn payload(&self) -> &MessageKind { return &self.payload }
}
impl FullMessage {
    pub fn origin(&self) -> &EndpointPair { return &self.origin }
}
impl Message for FullMessage {
    fn base_message(&self) -> &BaseMessage { return &self.base_message }
}

#[derive(Serialize, Deserialize)]
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