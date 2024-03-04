use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::mem;
use std::{str, net::SocketAddrV4};
use uuid::Uuid;

use crate::message_processing::SEARCH_TIMEOUT_SECONDS;
use crate::node::EndpointPair;

pub const NO_POSITION: (usize, usize) = (0, 1);

pub trait Message {
    const ENCRYPTION_REQUIRED: bool;
    fn dest(&self) -> SocketAddrV4;
    fn id(&self) -> &Id;
    fn replace_dest(&mut self, dest: SocketAddrV4);
    fn check_expiry(&self) -> bool;
    fn set_sender(&mut self, sender: SocketAddrV4);
}

#[derive(Clone, Serialize, Deserialize, Debug)]
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

    pub fn reassemble_message(mut messages: Vec<Self>) -> (Vec<u8>, HashSet<Sender>, String) {
        let timestamp = messages.iter().map(|m| DateTime::parse_from_rfc3339(&m.separate_parts.timestamp).unwrap()).min().unwrap();
        messages.sort_by(|a, b| a.separate_parts.position.0.cmp(&b.separate_parts.position.0));
        let (bytes, senders): (Vec<Vec<u8>>, Vec<Sender>) = messages
            .into_iter()
            .map(|m| { let parts = m.into_parts(); (parts.0, parts.2.sender) })
            .unzip();
        (bytes.concat(), HashSet::from_iter(senders.into_iter()), datetime_to_timestamp(timestamp.into()))
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
pub struct SeparateParts {
    sender: Sender,
    id: Id,
    position: (usize, usize),
    timestamp: String
}

impl SeparateParts {
    pub fn new(sender: Sender, id: Id) -> Self { Self { sender, id, position: NO_POSITION, timestamp: datetime_to_timestamp(Utc::now()) } }
    pub fn into_parts(self) -> (Sender, Id, (usize, usize))  { (self.sender, self.id, self.position) }
    pub fn position(&self) -> (usize, usize) { self.position }
    pub fn id(&self) -> &Id { &self.id}
    pub fn sender(&self) -> &Sender { &self.sender }
    
    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Sender {
    socket: SocketAddrV4,
    uuid: String
}

impl Sender {
    pub fn new(socket: SocketAddrV4, uuid: String) -> Self {
        Self { socket, uuid }
    }

    pub fn socket(&self) -> SocketAddrV4 { self.socket }
    pub fn uuid(&self) -> &String { &self.uuid }
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
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}

impl Message for Heartbeat {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.uuid }
    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
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
            timestamp: String::new(),
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
    pub fn into_hash_payload_host_name_kind(self) -> (Id, Vec<u8>, String, StreamMessageKind) { (self.hash, self.payload, self.host_name, self.kind) }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}
impl Message for StreamMessage {
    const ENCRYPTION_REQUIRED: bool = true;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.hash }
    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.senders.push(sender); }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Peer {
    endpoint_pair: EndpointPair,
    uuid: String
}

impl Peer {
    pub fn new(endpoint_pair: EndpointPair, uuid: String) -> Self {
        Self { endpoint_pair, uuid }
    }

    pub fn endpoint_pair(&self) -> EndpointPair { self.endpoint_pair }
    pub fn uuid(&self) -> &String { &self.uuid }
    pub fn into_parts(self) -> (EndpointPair, String) { (self.endpoint_pair, self.uuid )}
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: SearchMessageKind,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    hash: Id,
    host_name: String,
    origin: Option<Peer>,
    expiry: String,
}
impl SearchMessage {
    fn new(dest: SocketAddrV4, sender: SocketAddrV4, origin: Option<Peer>, host_name: String, hash: Id, kind: SearchMessageKind) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: String::new(),
            host_name,
            hash,
            kind,
            origin,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT_SECONDS)),
        }
    }

    pub fn initial_search_request(host_name: String, is_resource_kind: bool, id: Option<Id>) -> Self {
        // let hash = Id(crypto::digest_parts(vec![request.method().as_bytes(), request.uri().as_bytes(), request.body()]));
        let hash = if let Some(id) = id { id } else { Id(Uuid::new_v4().as_bytes().to_vec()) };
        let kind = if is_resource_kind { SearchMessageKind::Resource(SearchMessageInnerKind::Request) } else { SearchMessageKind::Distribution(SearchMessageInnerKind::Request) };
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), None,
            host_name, hash, kind)
    }

    pub fn key_response(origin: Peer, hash: Id, host_name: String, public_key: Vec<u8>, is_resource_kind: bool) -> Self {
        let kind = if is_resource_kind { SearchMessageKind::Resource(SearchMessageInnerKind::Response(public_key)) } else { SearchMessageKind::Distribution(SearchMessageInnerKind::Response(public_key)) };
        Self::new(EndpointPair::default_socket(), EndpointPair::default_socket(), Some(origin), host_name,
            hash, kind)
    }

    pub fn set_origin(&mut self, origin: Peer) { self.origin = Some(origin); }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> Option<&Peer> { self.origin.as_ref() }
    pub fn into_uuid_host_name_public_key_origin(self) -> (Id, String, Vec<u8>, Peer) {
        let public_key = Self::public_key(self.kind);
        (self.hash, self.host_name, public_key, self.origin.unwrap())
    }
    fn public_key(kind: SearchMessageKind) -> Vec<u8> {
        match kind {
            SearchMessageKind::Resource(SearchMessageInnerKind::Response(public_key)) => public_key,
            SearchMessageKind::Distribution(SearchMessageInnerKind::Response(public_key)) => public_key,
            _ => panic!()
        }
    }
    pub fn host_name(&self) -> &String { &self.host_name }
    pub fn into_uuid_host_name_origin(self) -> (Id, String, Option<Peer>) { (self.hash, self.host_name, self.origin) }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}

impl Message for SearchMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.hash }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        expiry <= Utc::now()
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
    origin: Option<Peer>,
    peer_list: Vec<Peer>,
    hop_count: (u16, u16)
}

impl DiscoverPeerMessage {
    pub fn new(kind: DpMessageKind, origin: Option<Peer>, uuid: Id, target_peer_count: (u16, u16)) -> Self {
        Self {
            kind,
            dest: EndpointPair::default_socket(),
            sender: EndpointPair::default_socket(),
            timestamp: String::new(),
            uuid,
            origin,
            peer_list: Vec::new(),
            hop_count: target_peer_count
        }
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn origin(&self) -> Option<&Peer> { self.origin.as_ref() }
    pub fn peer_list(&self) -> &Vec<Peer> { &self.peer_list }
    pub fn hop_count(&self) -> (u16, u16) { self.hop_count }
    pub fn get_last_peer(&mut self) -> Peer { self.peer_list.pop().unwrap() }
    pub fn into_peer_list(self) -> Vec<Peer> { self.peer_list }

    pub fn add_peer(&mut self, peer: Peer) { self.peer_list.push(peer); }

    pub fn try_decrement_hop_count(&mut self) -> bool {
        self.hop_count.0 -= 1;
        self.hop_count.0 != 0
    }
    
    pub fn set_origin_if_unset(mut self, origin: Peer) -> Self {
        if let None = self.origin {
            self.origin = Some(origin);
        }
        self
    }

    pub fn set_kind(&mut self, kind: DpMessageKind) { self.kind = kind; }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}

impl Message for DiscoverPeerMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.uuid }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DistributionMessage {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    host_name: String,
    uuid: Id,
    hop_count: u16
}
impl DistributionMessage {
    pub fn new(uuid: Id, target_hop_count: u16, host_name: String) -> Self {
        Self {
            dest: EndpointPair::default_socket(),
            sender: EndpointPair::default_socket(),
            timestamp: String::new(),
            uuid,
            hop_count: target_hop_count,
            host_name
        }
    }

    pub fn sender(&self) -> SocketAddrV4 { self.sender }
    pub fn hop_count(&self) -> u16 { self.hop_count }
    pub fn host_name(&self) -> &String { &self.host_name }
    pub fn into_host_name_hop_count_uuid(self) -> (String, u16, Id) { (self.host_name, self.hop_count, self.uuid) }

    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}
impl Message for DistributionMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> &Id { &self.uuid }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SearchMessageKind {
    Resource(SearchMessageInnerKind),
    Distribution(SearchMessageInnerKind),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SearchMessageInnerKind {
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
    Resource(StreamMessageInnerKind),
    Distribution(StreamMessageInnerKind)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StreamMessageInnerKind {
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