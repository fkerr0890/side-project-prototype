use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::{Debug, Display};
use std::net::Ipv4Addr;
use std::{net::SocketAddrV4, str};
use uuid::Uuid;

use crate::http::{SerdeHttpRequest, SerdeHttpResponse};
use crate::message_processing::stream::DistributionResponse;
use crate::message_processing::SEARCH_TIMEOUT_SECONDS;
use crate::node::EndpointPair;
use crate::option_early_return;

pub const NO_POSITION: (usize, usize) = (0, 1);

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InboundMessage {
    payload: Vec<u8>,
    separate_parts: SeparateParts,
}
impl InboundMessage {
    pub fn new(payload: Vec<u8>, separate_parts: SeparateParts) -> Self {
        Self {
            payload,
            separate_parts,
        }
    }
    pub fn into_parts(self) -> (Vec<u8>, SeparateParts) {
        (self.payload, self.separate_parts)
    }
    pub fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.payload
    }
    pub fn separate_parts(&self) -> &SeparateParts {
        &self.separate_parts
    }

    pub fn reassemble_message(mut messages: Vec<Self>) -> (Vec<u8>, FxHashSet<Sender>, String) {
        let mut bytes = Vec::new();
        let mut senders = FxHashSet::default();
        let mut timestamp = None;
        messages.sort_by(|a, b| {
            a.separate_parts
                .position
                .0
                .cmp(&b.separate_parts.position.0)
        });
        for message in messages {
            let (mut payload, separate_parts) = message.into_parts();
            bytes.append(&mut payload);
            senders.insert(separate_parts.sender);
            let datetime = DateTime::parse_from_rfc3339(&separate_parts.timestamp).unwrap();
            timestamp = Some(timestamp.map_or(datetime, |t| min(t, datetime)));
        }
        (
            bytes,
            senders,
            datetime_to_timestamp(timestamp.unwrap().into()),
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SeparateParts {
    sender: Sender,
    id: NumId,
    position: (usize, usize),
    timestamp: String,
}

impl SeparateParts {
    pub fn new(sender: Sender, id: NumId) -> Self {
        Self {
            sender,
            id,
            position: NO_POSITION,
            timestamp: datetime_to_timestamp(Utc::now()),
        }
    }
    pub fn into_parts(self) -> (Sender, NumId, (usize, usize)) {
        (self.sender, self.id, self.position)
    }
    pub fn position(&self) -> (usize, usize) {
        self.position
    }
    pub fn id(&self) -> NumId {
        self.id
    }
    pub fn sender(&self) -> &Sender {
        &self.sender
    }

    pub fn set_position(mut self, position: (usize, usize)) -> Self {
        self.position = position;
        self
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Sender {
    pub socket: SocketAddrV4,
    pub id: NumId,
}

impl Sender {
    pub fn new(socket: SocketAddrV4, id: NumId) -> Self {
        Self { socket, id }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    dest: Peer,
    senders: Vec<Sender>,
    timestamp: String,
    id: NumId,
    expiry: Option<String>,
    metadata: MetadataKind,
    direction: MessageDirection,
}

impl Message {
    pub fn new(
        dest: Peer,
        id: NumId,
        expiry: Option<String>,
        metadata: MetadataKind,
        direction: MessageDirection,
    ) -> Self {
        Self {
            dest,
            senders: Vec::new(),
            timestamp: String::new(),
            id,
            expiry,
            metadata,
            direction,
        }
    }

    pub fn new_search_request(id: NumId, metadata: SearchMetadata) -> Self {
        Self::new(
            Peer::default(),
            id,
            Some(datetime_to_timestamp(
                Utc::now() + Duration::seconds(SEARCH_TIMEOUT_SECONDS.as_secs() as i64),
            )),
            MetadataKind::Search(metadata),
            MessageDirection::Request,
        )
    }

    pub fn new_discover_peer_request(
        myself: Peer,
        introducer: Peer,
        target_num_peers: u16,
    ) -> Self {
        assert_ne!(myself.id, introducer.id);
        Self::new(
            Peer::default(),
            NumId(Uuid::new_v4().as_u128()),
            Some(datetime_to_timestamp(
                Utc::now() + Duration::seconds(SEARCH_TIMEOUT_SECONDS.as_secs() as i64),
            )),
            MetadataKind::Discover(DiscoverMetadata::new(
                myself,
                vec![introducer],
                DpMessageKind::INeedSome,
                target_num_peers,
            )),
            MessageDirection::Request,
        )
    }

    pub fn new_heartbeat(dest: Peer) -> Self {
        Self::new(
            dest,
            NumId(Uuid::new_v4().as_u128()),
            None,
            MetadataKind::Heartbeat,
            MessageDirection::Request,
        )
    }

    pub fn dest(&self) -> Peer {
        self.dest
    }
    pub fn only_sender(&self) -> Option<Sender> {
        self.senders.last().copied()
    }
    pub fn clear_senders(&mut self) {
        self.senders = Vec::with_capacity(0);
    }
    pub fn id(&self) -> NumId {
        self.id
    }
    pub fn metadata(&self) -> &MetadataKind {
        &self.metadata
    }
    pub fn metadata_mut(&mut self) -> &mut MetadataKind {
        &mut self.metadata
    }
    pub fn into_metadata(self) -> MetadataKind {
        self.metadata
    }
    pub fn direction(&self) -> MessageDirection {
        self.direction
    }
    pub fn replace_dest(&mut self, dest: Peer) {
        self.dest = dest;
    }
    pub fn set_sender(&mut self, sender: Sender) {
        self.senders.push(sender);
    }
    pub fn set_timestamp(&mut self, timestamp: String) {
        self.timestamp = timestamp
    }
    pub fn set_direction(&mut self, direction: MessageDirection) {
        self.direction = direction
    }

    pub fn check_expiry(&self) -> bool {
        let expiry: DateTime<Utc> =
            DateTime::parse_from_rfc3339(option_early_return!(&self.expiry, false))
                .unwrap()
                .into();
        expiry <= Utc::now()
    }

    pub fn to_be_chunked(&self) -> (bool, bool) {
        match self.metadata {
            MetadataKind::Search(_) => (false, false),
            MetadataKind::Stream(StreamMetadata {
                payload: StreamPayloadKind::Request(_) | StreamPayloadKind::DistributionRequest(_),
                ..
            }) => (true, true),
            MetadataKind::Stream(StreamMetadata { payload: _, .. }) => (true, false),
            MetadataKind::Discover(_) => (false, false),
            MetadataKind::Distribute(_) => (false, false),
            MetadataKind::Heartbeat => (true, false),
        }
    }
}

#[cfg(test)]
impl Message {
    pub fn set_id(&mut self, id: NumId) {
        self.id = id;
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum MessageDirection {
    Request,
    Response,
    OneHop,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageDirectionAgreement {
    Request,
    Response,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MetadataKind {
    Search(SearchMetadata),
    Stream(StreamMetadata),
    Discover(DiscoverMetadata),
    Distribute(DistributeMetadata),
    Heartbeat,
}

impl MetadataKind {
    pub fn host_name(&self) -> &String {
        match self {
            Self::Search(metadata) => &metadata.host_name,
            Self::Stream(metadata) => &metadata.host_name,
            Self::Distribute(metadata) => &metadata.host_name,
            _ => panic!(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SearchMetadata {
    pub origin: Peer,
    pub hairpin: Option<Peer>,
    pub host_name: String,
    pub kind: SearchMetadataKind,
}

impl SearchMetadata {
    pub fn new(origin: Peer, host_name: String, kind: SearchMetadataKind) -> Self {
        Self {
            origin,
            hairpin: None,
            host_name,
            kind,
        }
    }
}

#[cfg(test)]
impl SearchMetadata {
    pub fn set_kind(&mut self, kind: SearchMetadataKind) {
        self.kind = kind;
    }

    pub fn origin_mut(&mut self) -> &mut Peer {
        &mut self.origin
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SearchMetadataKind {
    Retrieval,
    Distribution,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StreamMetadata {
    pub host_name: String,
    pub payload: StreamPayloadKind,
}

impl StreamMetadata {
    pub fn new(payload: StreamPayloadKind, host_name: String) -> Self {
        Self { payload, host_name }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiscoverMetadata {
    pub kind: DpMessageKind,
    pub origin: Peer,
    pub peer_list: Vec<Peer>,
    pub hop_count: (u16, u16),
}

impl DiscoverMetadata {
    pub fn new(
        origin: Peer,
        peer_list: Vec<Peer>,
        kind: DpMessageKind,
        target_num_peers: u16,
    ) -> Self {
        Self {
            origin,
            peer_list,
            hop_count: (target_num_peers, target_num_peers),
            kind,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DistributeMetadata {
    pub hop_count: u16,
    pub host_name: String,
}

impl DistributeMetadata {
    pub fn new(hop_count: u16, host_name: String) -> Self {
        Self {
            hop_count,
            host_name,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StreamPayloadKind {
    Request(SerdeHttpRequest),
    Response(SerdeHttpResponse),
    DistributionRequest(Vec<u8>),
    DistributionResponse(DistributionResponse),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KeyAgreementMessage {
    pub public_key: Vec<u8>,
    pub peer_id: NumId,
    pub direction: MessageDirectionAgreement,
}

impl KeyAgreementMessage {
    pub fn new(public_key: Vec<u8>, peer_id: NumId, direction: MessageDirectionAgreement) -> Self {
        Self {
            public_key,
            peer_id,
            direction,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Peer {
    pub endpoint_pair: EndpointPair,
    pub id: NumId,
}

impl Peer {
    pub fn new(endpoint_pair: EndpointPair, id: NumId) -> Self {
        Self { endpoint_pair, id }
    }
}

impl Default for Peer {
    fn default() -> Self {
        Self::new(
            EndpointPair::new(
                EndpointPair::default_socket(),
                EndpointPair::default_socket(),
            ),
            NumId(0),
        )
    }
}

impl From<Sender> for Peer {
    fn from(value: Sender) -> Self {
        let endpoint_pair = if value.socket.ip().is_private()
            || *value.socket.ip() == Ipv4Addr::new(127, 0, 0, 1)
        {
            EndpointPair::new(EndpointPair::default_socket(), value.socket)
        } else {
            EndpointPair::new(value.socket, EndpointPair::default_socket())
        };
        Self::new(endpoint_pair, value.id)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DpMessageKind {
    Request,
    Response,
    INeedSome,
    IveGotSome,
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
