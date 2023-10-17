use chrono::{Utc, SecondsFormat, DateTime, Duration};
use rand::{seq::SliceRandom, rngs::SmallRng, SeedableRng};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4, mem};
use uuid::Uuid;

use crate::{node::{EndpointPair, SEARCH_TIMEOUT}, http::{SerdeHttpRequest, SerdeHttpResponse}};

pub trait Message<T> {
    fn dest(&self) -> SocketAddrV4;
    fn id(&self) -> &String;
    fn is_heartbeat(&self) -> bool { false }
    fn replace_dest_and_timestamp(self, dest: SocketAddrV4) -> T;
    fn check_expiry(self) -> Result<T, ()>;
    fn set_sender(self, sender: SocketAddrV4) -> Self;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String
}

impl Heartbeat {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, timestamp: String) -> Self { Self { dest, sender, timestamp, uuid: Uuid::new_v4().simple().to_string() } }
}

impl Message<Self> for Heartbeat {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &String { &self.uuid }
    fn is_heartbeat(&self) -> bool { true }
    fn replace_dest_and_timestamp(self, _dest: SocketAddrV4) -> Self { self }
    fn check_expiry(self) -> Result<Self, ()> { Ok(self) }
    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: MessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    query: String,
    origin: SocketAddrV4,
    payload: Vec<u8>,
    expiry: String,
    position: (usize, usize)
}
impl SearchMessage {
    const NO_POSITION: (usize, usize) = (0, 1);

    fn new(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, query: String, kind: MessageKind, payload: Vec<u8>) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(datetime),
            query,
            kind,
            origin,
            payload,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT)),
            position: Self::NO_POSITION
        }
    }

    pub fn initial_http_request(query: String, request: SerdeHttpRequest) -> Self {
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), EndpointPair::default_socket(),
            query, MessageKind::Request, bincode::serialize(&request).unwrap())
    }

    pub fn initial_http_response(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, query: String, response: SerdeHttpResponse) -> Self {
        Self::new(dest, sender, origin, query, MessageKind::Response, bincode::serialize(&response).unwrap())
    }

    pub fn chunked(self) -> Vec<Self> {
        let (payload, empty_message) = self.extract_payload();
        let chunks = payload.chunks(1024 - (bincode::serialized_size(&empty_message).unwrap() as usize));
        let num_chunks = chunks.len();
        let mut messages: Vec<Self> = chunks
            .enumerate()
            .map(|(i, chunk)| empty_message.clone().set_position((i, num_chunks)).set_payload(chunk.to_vec()))
            .collect();
        messages.shuffle(&mut SmallRng::from_entropy());
        messages
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn into_payload(self) -> Vec<u8> { self.payload }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn kind(&self) -> &MessageKind { &self.kind }
    pub fn position(&self) -> (usize, usize) { self.position }
    pub fn extract_payload(mut self) -> (Vec<u8>, Self) { return (mem::take(&mut self.payload), self) }
    
    pub fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }

    pub fn set_origin_if_unset(&mut self, origin: SocketAddrV4) {
        if self.origin == EndpointPair::default_socket() {
            self.origin = origin;
        }
    }

    pub fn set_payload(mut self, bytes: Vec<u8>) -> Self {
        self.payload = bytes;
        self
    }

    pub fn reassemble_message_payload(mut messages: Vec<Self>) -> Vec<u8> {
        messages.sort_by(|a, b| a.position.0.cmp(&b.position.0));
        let mut bytes = Vec::new();
        for message in messages {
            bytes.append(&mut message.into_payload());
        }
        bytes
    }
}

impl Message<Self> for SearchMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &String { &self.query }
    fn is_heartbeat(&self) -> bool { true }

    fn replace_dest_and_timestamp(mut self, dest: SocketAddrV4) -> Self {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
        self
    }

    fn check_expiry(self) -> Result<Self, ()> {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        if expiry >= Utc::now() {
            return Ok(self);
        }
        Err(())
    }

    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DiscoverPeerMessage {
    pub kind: MessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String,
    origin: SocketAddrV4,
    peer_list: Vec<EndpointPair>,
    hop_count: u16
}

impl DiscoverPeerMessage {
    pub fn new(kind: MessageKind, dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, uuid: String) -> Self {
        Self {
            kind,
            dest,
            sender,
            timestamp: datetime_to_timestamp(Utc::now()),
            uuid,
            origin,
            peer_list: Vec::new(),
            hop_count: 0
        }
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn kind(&self) -> &MessageKind { &self.kind }
    pub fn into_peer_list(self) -> Vec<EndpointPair> { self.peer_list }

    pub fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    pub fn add_peer(&mut self, endpoint_pair: EndpointPair) { self.peer_list.push(endpoint_pair); }

    pub fn try_decrement_hop_count(&mut self) -> bool {
        if self.hop_count > 0 {
            self.hop_count -= 1;
            true
        }
        else {
            false
        }
    }
    
    pub fn set_origin_if_unset(&mut self, origin: SocketAddrV4) {
        if self.origin == EndpointPair::default_socket() {
            self.origin = origin;
        }
    }
}

impl Message<Self> for DiscoverPeerMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &String { &self.uuid }
    fn is_heartbeat(&self) -> bool { true }

    fn replace_dest_and_timestamp(mut self, dest: SocketAddrV4) -> Self {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
        self
    }

    fn check_expiry(self) -> Result<Self, ()> { Ok(self) }
    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageKind {
    Request,
    Response
}

pub fn datetime_to_timestamp(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Micros, true)
}