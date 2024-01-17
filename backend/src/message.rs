use chrono::{Utc, SecondsFormat, DateTime, Duration};
use rand::{seq::SliceRandom, rngs::SmallRng, SeedableRng};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4, mem};
use uuid::Uuid;

use crate::{message_processing::SEARCH_TIMEOUT, http::SerdeHttpRequest};
use crate::node::EndpointPair;

const NO_POSITION: (usize, usize) = (0, 1);

pub trait Message {
    fn dest(&self) -> SocketAddrV4;
    fn id(&self) -> &str;
    fn replace_dest_and_timestamp(self, dest: SocketAddrV4) -> Self;
    fn check_expiry(&self) -> bool;
    fn set_sender(self, sender: SocketAddrV4) -> Self;
    fn position(&self) -> (usize, usize);
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

impl Message for Heartbeat {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }
    fn replace_dest_and_timestamp(self, _dest: SocketAddrV4) -> Self { self }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    fn position(&self) -> (usize, usize) { NO_POSITION }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamMessage {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    position: (usize, usize),
    pub kind: StreamMessageKind,
    payload: Vec<u8>
}
impl StreamMessage {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, kind: StreamMessageKind, payload: Vec<u8>) -> Self {
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(Utc::now()),
            position: NO_POSITION,
            kind,
            payload
        }
    }

    pub fn into_payload(self) -> Vec<u8> { self.payload }
    pub fn extract_payload(mut self) -> (Vec<u8>, Self) { return (mem::take(&mut self.payload), self) }
    pub fn sender(&self) -> SocketAddrV4 { self.sender }

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

    pub fn set_payload(mut self, bytes: Vec<u8>) -> Self {
        self.payload = bytes;
        self
    }

    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }

    pub fn reassemble_message_payload(mut messages: Vec<Self>) -> Vec<u8> {
        messages.sort_by(|a, b| a.position.0.cmp(&b.position.0));
        let mut bytes = Vec::new();
        for message in messages {
            bytes.append(&mut message.into_payload());
        }
        bytes
    }
}
impl Message for StreamMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { "" }
    fn replace_dest_and_timestamp(self, _dest: SocketAddrV4) -> Self { self }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    fn position(&self) -> (usize, usize) { self.position }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: MessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String,
    query: Vec<u8>,
    origin: SocketAddrV4,
    expiry: String,
    public_key: Option<Vec<u8>>,
    position: (usize, usize)
}
impl SearchMessage {
    fn new(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, query: String, kind: MessageKind, public_key: Option<Vec<u8>>) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(datetime),
            query: query.into_bytes(),
            uuid: Uuid::new_v4().simple().to_string(),
            kind,
            origin,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT)),
            public_key,
            position: NO_POSITION
        }
    }

    pub fn initial_http_request(query: String, request: SerdeHttpRequest) -> Self {
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), EndpointPair::default_socket(),
            query + request.uri(), MessageKind::Request, None)
    }

    pub fn initial_http_response(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, query: String, public_key: Vec<u8>) -> Self {
        Self::new(dest, sender, origin, query, MessageKind::Response, Some(public_key))
    }

    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }
    fn set_query(mut self, bytes: Vec<u8>) -> Self { self.query = bytes; self }
    pub fn extract_query(mut self) -> (Vec<u8>, Self) { return (mem::take(&mut self.query), self) }

    pub fn chunked(self) -> Vec<Self> {
        let (query, empty_message) = self.extract_query();
        let chunks = query.chunks(1024 - (bincode::serialized_size(&empty_message).unwrap() as usize));
        let num_chunks = chunks.len();
        let mut messages: Vec<Self> = chunks
            .enumerate()
            .map(|(i, chunk)| empty_message.clone().set_position((i, num_chunks)).set_query(chunk.to_vec()))
            .collect();
        messages.shuffle(&mut SmallRng::from_entropy());
        messages
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn kind(&self) -> &MessageKind { &self.kind }
    pub fn into_public_key(self) -> Option<Vec<u8>> { self.public_key }
    
    pub fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
}

impl Message for SearchMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }

    fn replace_dest_and_timestamp(mut self, dest: SocketAddrV4) -> Self {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
        self
    }

    fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        return expiry >= Utc::now()
    }

    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    fn position(&self) -> (usize, usize) { self.position }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiscoverPeerMessage {
    pub kind: DpMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String,
    origin: SocketAddrV4,
    peer_list: Vec<EndpointPair>,
    hop_count: (u16, u16)
}

impl DiscoverPeerMessage {
    pub fn new(kind: DpMessageKind, dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, uuid: String, target_peer_count: (u16, u16)) -> Self {
        Self {
            kind,
            dest,
            sender,
            timestamp: datetime_to_timestamp(Utc::now()),
            uuid,
            origin,
            peer_list: Vec::new(),
            hop_count: target_peer_count
        }
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn kind(&self) -> &DpMessageKind { &self.kind }
    pub fn peer_list(&self) -> &Vec<EndpointPair> { &self.peer_list }
    pub fn hop_count(&self) -> (u16, u16) { self.hop_count }
    pub fn get_last_peer(&mut self) -> EndpointPair { self.peer_list.pop().unwrap() }
    pub fn into_peer_list(self) -> Vec<EndpointPair> { self.peer_list }

    pub fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    pub fn add_peer(&mut self, endpoint_pair: EndpointPair) { self.peer_list.push(endpoint_pair); }
    pub fn increment_hop_count(&mut self) { self.hop_count.0 += 1; }

    pub fn try_decrement_hop_count(&mut self) -> bool {
        self.hop_count.0 -= 1;
        self.hop_count.0 != 0
    }
    
    pub fn set_origin_if_unset(mut self, origin: SocketAddrV4) -> Self {
        if self.origin == EndpointPair::default_socket() {
            self.origin = origin;
        }
        self
    }

    pub fn set_kind(mut self, kind: DpMessageKind) -> Self { self.kind = kind; self }
}

impl Message for DiscoverPeerMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }

    fn replace_dest_and_timestamp(mut self, dest: SocketAddrV4) -> Self {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
        self
    }

    fn check_expiry(&self) -> bool { false }
    fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }
    fn position(&self) -> (usize, usize) { NO_POSITION }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageKind {
    Request,
    Response
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DpMessageKind {
    Request,
    Response,
    INeedSome,
    IveGotSome
}

#[derive(Serialize, Deserialize, Clone)]
pub enum StreamMessageKind {
    KeyAgreement,
    Data
}

pub fn datetime_to_timestamp(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Micros, true)
}