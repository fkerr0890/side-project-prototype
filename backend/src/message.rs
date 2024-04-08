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
    fn id(&self) -> NumId;
    fn replace_dest(&mut self, dest: SocketAddrV4);
    fn check_expiry(&self) -> bool;
    fn set_sender(&mut self, sender: Sender);
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
        (bytes.concat(), HashSet::from_iter(senders), datetime_to_timestamp(timestamp.into()))
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
    id: NumId,
    position: (usize, usize),
    timestamp: String
}

impl SeparateParts {
    pub fn new(sender: Sender, id: NumId) -> Self { Self { sender, id, position: NO_POSITION, timestamp: datetime_to_timestamp(Utc::now()) } }
    pub fn into_parts(self) -> (Sender, NumId, (usize, usize))  { (self.sender, self.id, self.position) }
    pub fn position(&self) -> (usize, usize) { self.position }
    pub fn id(&self) -> NumId { self.id}
    pub fn sender(&self) -> &Sender { &self.sender }
    
    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.position = position; self }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Sender {
    pub socket: SocketAddrV4,
    pub id: NumId
}

impl Sender {
    pub fn new(socket: SocketAddrV4, id: NumId) -> Self {
        Self { socket, id }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    dest: SocketAddrV4,
    sender: Option<Sender>,
    timestamp: String,
    id: NumId
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl Heartbeat {
    pub fn new() -> Self { Self { dest: EndpointPair::default_socket(), sender: None, timestamp: String::new(), id: NumId(Uuid::new_v4().as_u128()) } }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
    pub fn sender(&self) -> Option<Sender> { self.sender }
}

impl Message for Heartbeat {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> NumId { self.id }
    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: Sender) { self.sender = Some(sender); }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StreamMessage {
    pub kind: StreamMessageKind,
    dest: SocketAddrV4,
    senders: Vec<Sender>,
    timestamp: String,
    host_name: String,
    id: NumId,
    payload: Vec<u8>
}
impl StreamMessage {
    pub fn new(host_name: String, id: NumId, kind: StreamMessageKind, payload: Vec<u8>) -> Self {
        Self {
            dest: EndpointPair::default_socket(),
            senders: Vec::new(),
            timestamp: String::new(),
            host_name,
            id,
            kind,
            payload
        }
    }

    pub fn payload(&self) -> &Vec<u8> { &self.payload }
    pub fn senders(&self) -> &Vec<Sender> { &self.senders }
    pub fn only_sender(&mut self) -> Option<Sender> { assert!(self.senders.len() <= 1); self.senders.pop() }
    pub fn host_name(&self) -> &str { &self.host_name }
    pub fn into_hash_payload(self) -> (NumId, Vec<u8>) { (self.id, self.payload) }
    pub fn into_hash_payload_host_name_kind(self) -> (NumId, Vec<u8>, String, StreamMessageKind) { (self.id, self.payload, self.host_name, self.kind) }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}
impl Message for StreamMessage {
    const ENCRYPTION_REQUIRED: bool = true;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> NumId { self.id }
    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }
    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: Sender) { self.senders.push(sender); }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct Peer {
    pub endpoint_pair: EndpointPair,
    pub id: NumId
}

impl Peer {
    pub fn new(endpoint_pair: EndpointPair, id: NumId) -> Self {
        Self { endpoint_pair, id }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMessage {
    pub kind: SearchMessageKind,
    dest: SocketAddrV4,
    sender: Option<Sender>,
    timestamp: String,
    id: NumId,
    host_name: String,
    origin: Option<Peer>,
    expiry: String,
}
impl SearchMessage {
    fn new(dest: SocketAddrV4, sender: Option<Sender>, origin: Option<Peer>, host_name: String, hash: NumId, kind: SearchMessageKind) -> Self {
        let datetime = Utc::now();
        Self {
            dest,
            sender,
            timestamp: String::new(),
            host_name,
            id: hash,
            kind,
            origin,
            expiry: datetime_to_timestamp(datetime + Duration::seconds(SEARCH_TIMEOUT_SECONDS)),
        }
    }

    pub fn initial_search_request(host_name: String, is_resource_kind: bool, id: Option<NumId>) -> Self {
        // let hash = Id(crypto::digest_parts(vec![request.method().as_bytes(), request.uri().as_bytes(), request.body()]));
        let hash = if let Some(id) = id { id } else { NumId(Uuid::new_v4().as_u128()) };
        let kind = if is_resource_kind { SearchMessageKind::Resource(SearchMessageInnerKind::Request) } else { SearchMessageKind::Distribution(SearchMessageInnerKind::Request) };
        Self::new(EndpointPair::default_socket(), None, None,
            host_name, hash, kind)
    }

    pub fn key_response(origin: Peer, id: NumId, host_name: String, public_key: Vec<u8>, is_resource_kind: bool) -> Self {
        let kind = if is_resource_kind { SearchMessageKind::Resource(SearchMessageInnerKind::Response(public_key)) } else { SearchMessageKind::Distribution(SearchMessageInnerKind::Response(public_key)) };
        Self::new(EndpointPair::default_socket(), None, Some(origin), host_name,
            id, kind)
    }

    pub fn set_origin(&mut self, origin: Peer) { self.origin = Some(origin); }

    pub fn sender(&self) -> Option<Sender> { self.sender }
    pub fn sender_option(&self) -> Option<Sender> { self.sender }
    pub fn origin(&self) -> Option<Peer> { self.origin }
    pub fn into_id_host_name_public_key_origin(self) -> (NumId, String, Vec<u8>, Peer) {
        let public_key = Self::public_key(self.kind);
        (self.id, self.host_name, public_key, self.origin.unwrap())
    }
    fn public_key(kind: SearchMessageKind) -> Vec<u8> {
        match kind {
            SearchMessageKind::Resource(SearchMessageInnerKind::Response(public_key)) => public_key,
            SearchMessageKind::Distribution(SearchMessageInnerKind::Response(public_key)) => public_key,
            _ => panic!()
        }
    }
    pub fn host_name(&self) -> &String { &self.host_name }
    pub fn into_id_host_name_origin(self) -> (NumId, String, Option<Peer>) { (self.id, self.host_name, self.origin) }
    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}

impl Message for SearchMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> NumId { self.id }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.expiry).unwrap().into();
        expiry <= Utc::now()
    }

    fn set_sender(&mut self, sender: Sender) { self.sender = Some(sender); }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiscoverPeerMessage {
    pub kind: DpMessageKind,
    dest: SocketAddrV4,
    sender: Option<Sender>,
    timestamp: String,
    id: NumId,
    origin: Option<Peer>,
    peer_list: Vec<Peer>,
    hop_count: (u16, u16)
}

impl DiscoverPeerMessage {
    pub fn new(kind: DpMessageKind, origin: Option<Peer>, id: NumId, target_peer_count: (u16, u16)) -> Self {
        Self {
            kind,
            dest: EndpointPair::default_socket(),
            sender: None,
            timestamp: String::new(),
            id,
            origin,
            peer_list: Vec::new(),
            hop_count: target_peer_count
        }
    }

    pub fn sender(&self) -> Sender { self.sender.unwrap() }
    pub fn origin(&self) -> Option<Peer> { self.origin }
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
        if self.origin.is_none() {
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
    fn id(&self) -> NumId { self.id }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: Sender) { self.sender = Some(sender); }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DistributionMessage {
    dest: SocketAddrV4,
    sender: Option<Sender>,
    timestamp: String,
    host_name: String,
    id: NumId,
    hop_count: u16
}
impl DistributionMessage {
    pub fn new(id: NumId, target_hop_count: u16, host_name: String) -> Self {
        Self {
            dest: EndpointPair::default_socket(),
            sender: None,
            timestamp: String::new(),
            id,
            hop_count: target_hop_count,
            host_name
        }
    }

    pub fn sender(&self) -> Option<Sender> { self.sender }
    pub fn hop_count(&self) -> u16 { self.hop_count }
    pub fn host_name(&self) -> &String { &self.host_name }
    pub fn into_host_name_hop_count_id_sender(self) -> (String, u16, NumId, Sender) { (self.host_name, self.hop_count, self.id, self.sender.unwrap()) }

    pub fn set_timestamp(&mut self, timestamp: String) { self.timestamp = timestamp }
}
impl Message for DistributionMessage {
    const ENCRYPTION_REQUIRED: bool = false;
    fn dest(&self) -> SocketAddrV4 { self.dest }
    fn id(&self) -> NumId { self.id }

    fn replace_dest(&mut self, dest: SocketAddrV4) {
        self.dest = dest;
    }

    fn check_expiry(&self) -> bool { false }
    fn set_sender(&mut self, sender: Sender) { self.sender = Some(sender); }
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
    KeyAgreement,
    Resource(StreamMessageInnerKind),
    Distribution(StreamMessageInnerKind)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StreamMessageInnerKind {
    Request,
    Response
}

pub fn datetime_to_timestamp(datetime: DateTime<Utc>) -> String {
    datetime.to_rfc3339_opts(SecondsFormat::Micros, true)
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Hash)]
pub struct NumId(pub u128);
impl Display for NumId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}