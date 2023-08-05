use crate::node::EndpointPair;
use uuid::Uuid;

pub struct Message<T> {
    pub dest: EndpointPair,
    pub sender: EndpointPair,
    pub origin: EndpointPair,
    pub payload: MessageDirection<T>,
    pub hop_count: usize,
    pub max_hop_count: usize,
    pub timestamp: String
}
impl<T> Message<T> {
    pub fn new(dest: EndpointPair, sender: EndpointPair, origin: EndpointPair, payload: MessageDirection<T>, hop_count: usize, max_hop_count: usize, timestamp: String) -> Self {
        Self {
            dest,
            sender,
            origin,
            payload,
            hop_count,
            max_hop_count,
            timestamp 
        }
    }
}

pub struct Heartbeat {
    pub dest: EndpointPair,
    pub sender: EndpointPair,
    pub timestamp: String
}

impl Heartbeat {
    pub fn new(dest: EndpointPair, sender: EndpointPair, timestamp: String) -> Self { Self { dest, sender, timestamp } }
}

pub enum MessageDirection<T> {
    Request(MessageKind<T>),
    Response(MessageKind<T>),
}

pub enum MessageKind<T> {
    DiscoverPeer(T),
    Search(T)
}

pub struct DiscoverPeerRequest;

pub struct DiscoverPeerResponse {
    endpoint_pair: EndpointPair,
}
impl DiscoverPeerResponse {
    fn new(endpoint_pair: EndpointPair) -> Self {
        Self { 
            endpoint_pair
        }
    }
}

pub struct SearchRequest {
    uuid: Uuid
}
impl SearchRequest {
    fn new(uuid: Uuid) -> Self {
        Self {
            uuid
        }
    }
}

pub struct SearchResponse {
    uuid: Uuid,
    body: String
}
impl SearchResponse {
    fn new(uuid: Uuid, body: String) -> Self {
        Self {
            uuid,
            body
        }
    }
}