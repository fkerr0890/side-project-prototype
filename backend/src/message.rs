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
    position: (usize, usize)
}

impl MessageExt {
    pub fn new(origin: SocketAddrV4, message_direction: MessageDirection, payload: MessageKind, hop_count: u8, position: (usize, usize)) -> Self {
        Self {
            origin,
            message_direction,
            payload,
            hop_count,
            position
        }
    }

    pub fn payload(&self) -> &MessageKind { &self.payload }
    pub fn origin(&self) -> &SocketAddrV4 { &self.origin }
    pub fn direction(&self) -> &MessageDirection { &self.message_direction }
    pub fn position(&self) -> &(usize, usize) { &self.position }
    pub fn no_position() -> (usize, usize) { (0, 0) }
    
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
    message_ext: Option<MessageExt>,
    uuid: String
}
impl Message {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, message_ext: Option<MessageExt>, optional_uuid: Option<String>) -> Self {
        let uuid = if let Some(hash) = optional_uuid { hash } else { Uuid::new_v4().simple().to_string() };
        Self {
            dest,
            sender,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            message_ext,
            uuid
        }
    }

    pub fn dest(&self) -> &SocketAddrV4 { &self.dest }
    pub fn sender(&self) -> &SocketAddrV4 { &self.sender }
    pub fn uuid(&self) -> &String { &self.uuid }

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

    pub fn replace_dest_and_timestamp(mut self, dest: SocketAddrV4) -> Self {
        self.dest = dest;
        self.timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        self
    }

    pub fn set_position(mut self, position: (usize, usize)) -> Self { self.message_ext_mut().position = position; self }
    pub fn set_contents(mut self, contents: Vec<u8>) -> Self {
        if let MessageKind::SearchResponse(_, ref mut slice) = self.message_ext_mut().payload {
            *slice = contents;
        }
        self
    }

    pub fn try_decrement_hop_count(mut self) -> Option<Self> {
        if self.message_ext().hop_count > 0 {
            self.message_ext_mut().hop_count -= 1;
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

    pub fn size(&self) -> usize {
        bincode::serialize(&self).unwrap().len()
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