use chrono::{Utc, SecondsFormat, DateTime, Duration};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4, mem};
use uuid::Uuid;

use crate::message_processing::SEARCH_TIMEOUT;
use crate::node::EndpointPair;

pub const NO_POSITION: (usize, usize) = (0, 1);

pub trait Message {
    fn dest(&self) -> SocketAddrV4;
    fn id(&self) -> &str;
    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4);
    fn check_expiry(&self) -> bool;
    fn set_sender(&mut self, sender: SocketAddrV4);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InboundMessage {
    payload: Vec<u8>,
    uuid: String,
    is_encrypted: IsEncrypted,
    position: (usize, usize)
}
impl InboundMessage {
    pub fn new(payload: Vec<u8>, uuid: String, is_encrypted: IsEncrypted, position: (usize, usize)) -> Self { Self { payload, uuid, is_encrypted, position } }
    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }
    pub fn set_payload(mut self, payload: Vec<u8>) -> Self { self.payload = payload; self }
    pub fn payload(&self) -> &Vec<u8> { &self.payload }
    pub fn payload_mut(&mut self) -> &mut Vec<u8> { &mut self.payload }
    pub fn uuid(&self) -> &str { &self.uuid }
    pub fn is_encrypted(&self) -> &IsEncrypted { &self.is_encrypted }
    pub fn position(&self) -> (usize, usize) { self.position }
    pub fn extract_payload(mut self) -> (Vec<u8>, Self) { (mem::take(&mut self.payload), self) }
    pub fn into_payload(self) -> Vec<u8> { self.payload }
    pub fn into_payload_is_encrypted(self) -> (Vec<u8>, IsEncrypted) { (self.payload, self.is_encrypted) }

    pub fn reassemble_message(mut messages: Vec<Self>) -> Self {
        messages.sort_by(|a, b| a.position.0.cmp(&b.position.0));
        let last = messages.pop().unwrap();
        let (last_bytes, mut base_message) = last.extract_payload();
        let mut bytes = messages.into_iter().map(|m| m.payload).collect::<Vec<Vec<u8>>>();
        bytes.push(last_bytes);
        base_message.payload = bytes.concat();
        base_message
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum IsEncrypted {
    True(Vec<u8>),
    False
}
impl IsEncrypted {
    pub fn nonce(self) -> Vec<u8> { if let Self::True(nonce) = self { nonce } else { panic!() } }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String
}

impl Heartbeat {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4) -> Self { Self { dest, sender, timestamp: datetime_to_timestamp(Utc::now()), uuid: Uuid::new_v4().simple().to_string() } }
}

impl Message for Heartbeat {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }
    fn replace_dest_and_timestamp(&mut self, _dest: SocketAddrV4) {}
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StreamMessage {
    pub kind: StreamMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    host_name: String,
    uuid: String,
    payload: Vec<u8>
}
impl StreamMessage {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, host_name: String, uuid: String, kind: StreamMessageKind, payload: Vec<u8>) -> Self {
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(Utc::now()),
            host_name,
            uuid,
            kind,
            payload
        }
    }

    pub fn payload(&self) -> &Vec<u8> { &self.payload }
    pub fn payload_mut(&mut self) -> &mut Vec<u8> { &mut self.payload }
    pub fn into_payload(self) -> Vec<u8> { self.payload }
    pub fn extract_payload(mut self) -> (Vec<u8>, Self) { return (mem::take(&mut self.payload), self) }
    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn host_name(&self) -> &str { &self.host_name }
    pub fn into_host_name_uuid_payload(self) -> (String, String, Vec<u8>) { (self.host_name, self.uuid, self.payload) }

    pub fn set_payload(mut self, bytes: Vec<u8>) -> Self {
        self.payload = bytes;
        self
    }
}
impl Message for StreamMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }
    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: SearchMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String,
    host_name: String,
    origin: SocketAddrV4,
    expiry: String,
}
impl SearchMessage {
    fn new(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, host_name: String, uuid: String, kind: SearchMessageKind) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(datetime),
            host_name,
            uuid,
            kind,
            origin,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT)),
        }
    }

    pub fn initial_search_request(host_name: String) -> Self {
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), EndpointPair::default_socket(),
            host_name, Uuid::new_v4().simple().to_string(), SearchMessageKind::Request)
    }

    pub fn key_response(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, uuid: String, host_name: String, public_key: Vec<u8>) -> Self {
        Self::new(dest, sender, origin, host_name, uuid, SearchMessageKind::Response(public_key))
    }

    pub fn set_origin(&mut self, origin: SocketAddrV4) { self.origin = origin; }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn kind(&self) -> &SearchMessageKind { &self.kind }
    pub fn into_uuid_host_name_public_key(self) -> (String, String, Vec<u8>) {
        let public_key = if let SearchMessageKind::Response(public_key) = self.kind { public_key } else { panic!() };
        (self.uuid, self.host_name, public_key)
    }
    pub fn host_name(&self) -> &str { &self.host_name }
    pub fn into_uuid_host_name(self) -> (String, String) { (self.uuid, self.host_name) }
}

impl Message for SearchMessage {
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &str { &self.uuid }

    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }

    fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        return expiry >= Utc::now()
    }

    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
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

    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }

    fn check_expiry(&self) -> bool { true }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SearchMessageKind {
    Request,
    Response(Vec<u8>)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DpMessageKind {
    Request,
    Response,
    INeedSome,
    IveGotSome
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StreamMessageKind {
    KeyAgreement,
    Request,
    Response
}

pub fn datetime_to_timestamp(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Micros, true)
}