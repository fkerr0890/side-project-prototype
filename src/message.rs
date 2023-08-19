use crate::node::EndpointPair;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

pub trait Message {
    fn dest(&self) -> &EndpointPair;
    fn sender(&self) -> &EndpointPair;
    fn timestamp(&self) -> &String;
}

#[derive(Serialize, Deserialize)]
pub struct BaseMessage {
    pub dest: EndpointPair,
    pub sender: EndpointPair,
    pub timestamp: String
}

impl BaseMessage {
    pub fn new(dest: EndpointPair, sender: EndpointPair, timestamp: String) -> Self { Self { dest, sender, timestamp } }
}
impl Message for BaseMessage {
    fn dest(&self) -> &EndpointPair {
        return &self.dest;
    }

    fn sender(&self) -> &EndpointPair {
        return &self.sender;
    }

    fn timestamp(&self) -> &String {
        return &self.timestamp;
    }
}

#[derive(Serialize, Deserialize)]
pub struct FullMessage<T> {
    pub base_message: BaseMessage,
    pub origin: EndpointPair,
    pub payload: MessageDirection<T>,
    pub hop_count: usize,
    pub max_hop_count: usize,
}
impl<T> FullMessage<T> {
    pub fn new(base_message: BaseMessage, origin: EndpointPair, payload: MessageDirection<T>, hop_count: usize, max_hop_count: usize) -> Self {
        Self {
            base_message,
            origin,
            payload,
            hop_count,
            max_hop_count,
        }
    }
}
impl<T> Message for FullMessage<T> {
    fn dest(&self) -> &EndpointPair {
        return &self.base_message.dest;
    }

    fn sender(&self) -> &EndpointPair {
        return &self.base_message.sender;
    }

    fn timestamp(&self) -> &String {
        return &self.base_message.timestamp;
    }
}

#[derive(Serialize, Deserialize)]
pub enum MessageDirection<T> {
    Request(MessageKind<T>),
    Response(MessageKind<T>),
}

#[derive(Serialize, Deserialize)]
pub enum MessageKind<T> {
    DiscoverPeer(T),
    Search(T)
}

pub struct DiscoverPeerRequest;

pub struct DiscoverPeerResponse {
    uuid: Uuid,
}
impl DiscoverPeerResponse {
    pub fn new(uuid: Uuid) -> Self { Self { uuid } }
}

pub struct SearchRequest {
    uuid: Uuid
}
impl SearchRequest {
    pub fn new(uuid: Uuid) -> Self { Self { uuid } }
}

pub struct SearchResponse {
    uuid: Uuid,
    body: String
}

impl SearchResponse {
    pub fn new(uuid: Uuid, body: String) -> Self { Self { uuid, body } }
}