use chrono::{Utc, SecondsFormat, DateTime, Duration};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::mem;
use std::{str, net::SocketAddrV4};
use uuid::Uuid;

use crate::message_processing::SEARCH_TIMEOUT;
use crate::node::EndpointPair;

pub const NO_POSITION: (usize, usize) = (0, 1);

pub trait Message {
    const ENCRYPTION_REQUIRED: bool;
    fn dest(&self) -> SocketAddrV4;
    fn id(&self) -> &Id;
    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4);
    fn check_expiry(&self) -> bool;
    fn set_sender(&mut self, sender: SocketAddrV4);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InboundMessage {
    payload: Vec<u8>,
    is_encrypted: IsEncrypted,
    separate_parts: SeparateParts
}
impl InboundMessage {
    pub fn new(payload: Vec<u8>, is_encrypted: IsEncrypted, separate_parts: SeparateParts) -> Self { Self { payload, is_encrypted, separate_parts } }
    pub fn into_parts(self) -> (Vec<u8>, IsEncrypted, SeparateParts) { (self.payload, self.is_encrypted, self.separate_parts) }
    pub fn take_is_encrypted(&mut self) -> IsEncrypted { mem::take(&mut self.is_encrypted) }
    pub fn set_is_encrypted_true(&mut self, nonce: Vec<u8>) { self.is_encrypted = IsEncrypted::True(nonce) }
    pub fn payload_mut(&mut self) -> &mut Vec<u8> { &mut self.payload }
    pub fn separate_parts(&self) -> &SeparateParts { &self.separate_parts }

    pub fn reassemble_message(mut messages: Vec<Self>) -> (Vec<u8>, HashSet<SocketAddrV4>) {
        messages.sort_by(|a, b| a.separate_parts.position.0.cmp(&b.separate_parts.position.0));
        let (bytes, senders): (Vec<Vec<u8>>, Vec<SocketAddrV4>) = messages
            .into_iter()
            .map(|m| { let parts = m.into_parts(); (parts.0, parts.2.sender) })
            .unzip();
        (bytes.concat(), HashSet::from_iter(senders.into_iter()))
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IsEncrypted {
    True(Vec<u8>),
    False
}
impl IsEncrypted {
    pub fn nonce(self) -> Vec<u8> { if let Self::True(nonce) = self { nonce } else { panic!() } }
}
impl Default for IsEncrypted {
    fn default() -> Self {
        Self::False
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: Id
}

impl Heartbeat {
    pub fn new() -> Self { Self { dest: EndpointPair::default_socket(), sender: EndpointPair::default_socket(), timestamp: String::new(), uuid: Id(Uuid::new_v4().as_bytes().to_vec()) } }
}

impl Message for Heartbeat {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.uuid }
    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StreamMessage {
    pub kind: StreamMessageKind,
    dest: SocketAddrV4,
    senders: Vec<SocketAddrV4>,
    timestamp: String,
    host_name: String,
    hash: Id,
    payload: Vec<u8>
}
impl StreamMessage {
    pub fn new(host_name: String, hash: Id, kind: StreamMessageKind, payload: Vec<u8>) -> Self {
        Self {
            dest: EndpointPair::default_socket(),
            senders: Vec::new(),
            timestamp: datetime_to_timestamp(Utc::now()),
            host_name,
            hash,
            kind,
            payload
        }
    }

    pub fn payload(&self) -> &Vec<u8> { &self.payload }
    pub fn senders(&self) -> &Vec<SocketAddrV4> { &self.senders }
    pub fn only_sender(&mut self) -> SocketAddrV4 { self.senders.pop().unwrap() }
    pub fn host_name(&self) -> &str { &self.host_name }
    pub fn into_hash_payload(self) -> (Id, Vec<u8>) { (self.hash, self.payload) }
}
impl Message for StreamMessage {
    const ENCRYPTION_REQUIRED: bool = true;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.hash }
    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.senders.push(sender); }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SeparateParts {
    sender: SocketAddrV4,
    id: Id,
    position: (usize, usize)
}

impl SeparateParts {
    pub fn new(sender: SocketAddrV4, id: Id) -> Self { Self { sender, id, position: NO_POSITION } }
    pub fn into_parts(self) -> (SocketAddrV4, Id, (usize, usize))  { (self.sender, self.id, self.position) }
    pub fn position(&self) -> (usize, usize) { self.position }
    pub fn id(&self) -> &Id { &self.id}
    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    
    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: SearchMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    hash: Id,
    host_name: String,
    origin: SocketAddrV4,
    expiry: String,
}
impl SearchMessage {
    fn new(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, host_name: String, hash: Id, kind: SearchMessageKind) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: datetime_to_timestamp(datetime),
            host_name,
            hash,
            kind,
            origin,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT)),
        }
    }

    pub fn initial_search_request(host_name: String) -> Self {
        // let hash = Id(crypto::digest_parts(vec![request.method().as_bytes(), request.uri().as_bytes(), request.body()]));
        let hash = Id(Uuid::new_v4().as_bytes().to_vec());
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), EndpointPair::default_socket(),
            host_name, hash, SearchMessageKind::Request)
    }

    pub fn key_response(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, hash: Id, host_name: String, public_key: Vec<u8>) -> Self {
        Self::new(dest, sender, origin, host_name, hash, SearchMessageKind::Response(public_key))
    }

    pub fn set_origin(&mut self, origin: SocketAddrV4) { self.origin = origin; }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn into_uuid_host_name_public_key(self) -> (Id, String, Vec<u8>) {
        let public_key = if let SearchMessageKind::Response(public_key) = self.kind { public_key } else { panic!() };
        (self.hash, self.host_name, public_key)
    }
    pub fn host_name(&self) -> &String { &self.host_name }
    pub fn into_uuid_host_name(self) -> (Id, String) { (self.hash, self.host_name) }
}

impl Message for SearchMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.hash }

    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }

    fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        return expiry <= Utc::now()
    }

    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiscoverPeerMessage {
    pub kind: DpMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: Id,
    origin: SocketAddrV4,
    peer_list: Vec<EndpointPair>,
    hop_count: (u16, u16)
}

impl DiscoverPeerMessage {
    pub fn new(kind: DpMessageKind, origin: SocketAddrV4, uuid: Id, target_peer_count: (u16, u16)) -> Self {
        Self {
            kind,
            dest: EndpointPair::default_socket(),
            sender: EndpointPair::default_socket(),
            timestamp: datetime_to_timestamp(Utc::now()),
            uuid,
            origin,
            peer_list: Vec::new(),
            hop_count: target_peer_count
        }
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> SocketAddrV4 { self.origin }
    pub fn peer_list(&self) -> &Vec<EndpointPair> { &self.peer_list }
    pub fn hop_count(&self) -> (u16, u16) { self.hop_count }
    pub fn get_last_peer(&mut self) -> EndpointPair { self.peer_list.pop().unwrap() }
    pub fn into_peer_list(self) -> Vec<EndpointPair> { self.peer_list }

    pub fn add_peer(&mut self, endpoint_pair: EndpointPair) { self.peer_list.push(endpoint_pair); }

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
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.uuid }

    fn replace_dest_and_timestamp(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
        self.timestamp = datetime_to_timestamp(Utc::now());
    }

    fn check_expiry(&self) -> bool { false }
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

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct Id(pub Vec<u8>);
impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(&self.0).into_string())
    }
}
impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Id").field(&self.to_string()).finish()
    }
}