use chrono::{Utc, SecondsFormat};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4};
use uuid::Uuid;

use crate::node::EndpointPair;

#[derive(Serialize, Deserialize, Clone)]
pub struct MessageExt {
    origin: SocketAddrV4,
    message_direction: MessageDirection,
    payload: MessageKind,
    hop_count: u8,
    max_hop_count: u8,
    uuid: String,
    position: (usize, i32)
}

impl MessageExt {
    pub fn new(origin: SocketAddrV4, message_direction: MessageDirection, payload: MessageKind, hop_count: u8, max_hop_count: u8, optional_hash: Option<String>, position: (usize, i32)) -> Self {
        let uuid = if let Some(hash) = optional_hash { hash } else { Uuid::new_v4().simple().to_string() };
        Self {
            origin,
            message_direction,
            payload,
            hop_count,
            max_hop_count,
            uuid,
            position
        }
    }

    pub fn payload(&self) -> &MessageKind { &self.payload }
    pub fn hash(&self) -> &String { &self.uuid }
    pub fn origin(&self) -> &SocketAddrV4 { &self.origin }
    pub fn direction(&self) -> &MessageDirection { &self.message_direction }
    pub fn position(&self) -> &(usize, i32) { &self.position }
    pub fn no_position() -> (usize, i32) { (0, -1) }
    
    // pub fn hash_for_message(origin: &SocketAddrV4, message_direction: &MessageDirection, payload: &MessageKind) -> String {
    //     let mut context = Context::new(&SHA256);
    //     context.update(origin.to_string().as_bytes());
    //     context.update(serde_json::to_string(&message_direction).unwrap().as_bytes());
    //     context.update(serde_json::to_string(&payload).unwrap().as_bytes());
    //     let digest = context.finish();
    //     HEXLOWER.encode(digest.as_ref())
    // }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    message_ext: Option<MessageExt>
}
impl Message {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, message_ext: Option<MessageExt>) -> Self {
        Self {
            dest,
            sender,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            message_ext
        }
    }

    pub fn dest(&self) -> &SocketAddrV4 { &self.dest }
    pub fn sender(&self) -> &SocketAddrV4 { &self.sender }
    pub fn is_heartbeat(&self) -> bool { self.message_ext.is_none() }

    pub fn is_search_response(&self) -> bool {
        if let Some(message_ext) = &self.message_ext {
            if let MessageKind::SearchResponse(..) = message_ext.payload() {
                return true;
            }
        }
        false
    }

    pub fn message_ext(&self) -> &MessageExt {
        match self.message_ext {
            Some(ref message_ext) => message_ext,
            None => panic!("No message_ext")
        }
    }

    pub fn message_ext_mut(&mut self) -> &mut MessageExt {
        match self.message_ext {
            Some(ref mut message_ext) => message_ext,
            None => panic!("No message_ext")
        }
    }

    pub fn payload_inner(&self) -> (Option<&String>, Option<&[u8]>) {
        match &self.message_ext().payload {
            MessageKind::SearchRequest(filename) => (Some(filename), None),
            MessageKind::SearchResponse(filename, contents) => (Some(filename), Some(contents)),
            MessageKind::DiscoverPeerResponse(uuid) => (Some(uuid), None),
            _ => (None, None)
        }
    }
    
    pub fn set_sender(&mut self, sender: SocketAddrV4) { self.sender = sender; }

    pub fn set_origin_if_unset(&mut self, origin: SocketAddrV4) {
        if self.message_ext().origin == EndpointPair::default_socket() {
            self.message_ext_mut().origin = origin;
        }
    }

    pub fn replace_dest_and_timestamp(&self, dest: SocketAddrV4) -> Self {
        let mut result = self.clone();
        result.dest = dest;
        result.timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        result
    }

    pub fn try_increment_hop_count(mut self) -> Option<Self> {
        if self.message_ext().hop_count < self.message_ext().max_hop_count {
            self.message_ext_mut().hop_count += 1;
            return Some(self);
        }
        None
    }

    pub fn to_payload(self) -> MessageKind {
        if let Some(message_ext) = self.message_ext {
            message_ext.payload
        } else {
            panic!("Cannot convert to payload without message_ext")
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
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