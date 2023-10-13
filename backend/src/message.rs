use chrono::{Utc, SecondsFormat};
use rand::{seq::SliceRandom, rngs::SmallRng, SeedableRng};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4, mem};
use uuid::Uuid;

use crate::{node::{EndpointPair, SEARCH_MAX_HOP_COUNT}, http::{SerdeHttpRequest, SerdeHttpResponse}};

#[derive(Serialize, Deserialize, Clone)]
pub struct MessageExt {
    pub kind: MessageKind,
    pub origin: SocketAddrV4,
    pub payload: Vec<u8>,
    hop_count: u8,
    position: (usize, usize)
}

impl MessageExt {
    pub fn new(origin: SocketAddrV4, message_direction: MessageKind, payload: Vec<u8>, hop_count: u8, position: (usize, usize)) -> Self {
        Self {
            payload,
            origin,
            kind: message_direction,
            hop_count,
            position
        }
    }

    pub fn payload(&self) -> &Vec<u8> { &self.payload }
    pub fn payload_mut(&mut self) -> &mut Vec<u8> { &mut self.payload }
    pub fn into_payload(self) -> Vec<u8> { self.payload }
    pub fn origin(&self) -> &SocketAddrV4 { &self.origin }
    pub fn kind(&self) -> &MessageKind { &self.kind }
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
    pub message_ext: Option<MessageExt>,
    dest: SocketAddrV4,
    sender: SocketAddrV4,
    timestamp: String,
    uuid: String
}
impl Message {
    pub fn new(dest: SocketAddrV4, sender: SocketAddrV4, message_ext: Option<MessageExt>, optional_uuid: Option<String>) -> Self {
        let uuid = if let Some(hash) = optional_uuid { hash } else { Uuid::new_v4().simple().to_string() };
        Self {
            message_ext,
            dest,
            sender,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            uuid,
        }
    }

    pub fn initial_http_request(host_name: String, request: SerdeHttpRequest) -> Self {
        Message::new(EndpointPair::default_socket(),
            EndpointPair::default_socket(),
            Some(MessageExt::new(EndpointPair::default_socket(),
            MessageKind::HttpRequest(host_name),
            bincode::serialize(&request).unwrap(),
            SEARCH_MAX_HOP_COUNT,
            MessageExt::no_position())),
            None)
    }

    pub fn initial_http_response(dest: SocketAddrV4, sender: SocketAddrV4, origin: SocketAddrV4, hash: String, response: SerdeHttpResponse) -> Self {
        Message::new(
            dest,
            sender,
            Some(MessageExt::new(origin,
            MessageKind::HttpResponse,
            bincode::serialize(&response).unwrap(),
            SEARCH_MAX_HOP_COUNT,
            MessageExt::no_position())),
            Some(hash))
    }

    pub fn chunked(self) -> Vec<Message> {
        let (payload, empty_message) = self.extract_payload();
        let chunks = payload.chunks(1024 - (bincode::serialized_size(&empty_message).unwrap() as usize));
        let num_chunks = chunks.len();
        let mut messages: Vec<Message> = chunks
            .enumerate()
            .map(|(i, chunk)| empty_message.clone().set_position((i, num_chunks)).set_payload(chunk.to_vec()))
            .collect();
        messages.shuffle(&mut SmallRng::from_entropy());
        messages
    }

    pub fn dest(&self) -> &SocketAddrV4 { &self.dest }
    pub fn sender(&self) -> &SocketAddrV4 { &self.sender }
    pub fn uuid(&self) -> &String { &self.uuid }

    pub fn is_heartbeat(&self) -> bool { self.message_ext.is_none() }

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

    pub fn into_message_ext(self) -> MessageExt {
        if let Some(message_ext) = self.message_ext {
            message_ext
        }
        else {
            panic!("No message_ext")
        }
    }

    pub fn extract_payload(mut self) -> (Vec<u8>, Self) {
        return (mem::take(self.message_ext_mut().payload_mut()), self)
    }
    
    pub fn set_sender(mut self, sender: SocketAddrV4) -> Self { self.sender = sender; self }

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

    pub fn set_payload(mut self, bytes: Vec<u8>) -> Self {
        *self.message_ext_mut().payload_mut() = bytes;
        self
    }

    pub fn try_decrement_hop_count(mut self) -> Result<Self, ()> {
        if self.message_ext().hop_count > 0 {
            self.message_ext_mut().hop_count -= 1;
            Ok(self)
        }
        else {
            Err(())
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

#[derive(Serialize, Deserialize, Clone)]
pub enum MessageKind {
    DiscoverPeerRequest,
    DiscoverPeerResponse(String),
    HttpRequest(String),
    HttpResponse
}

impl MessageKind {
    pub fn host_name(&self) -> &str {
        if let Self::HttpRequest(host_name) = self {
            host_name
        }
        else {
            panic!()
        }
    }
}