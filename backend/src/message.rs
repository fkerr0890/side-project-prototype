use chrono::{Utc, SecondsFormat};
use serde::{Serialize, Deserialize};
use std::{str, net::SocketAddrV4};
use uuid::Uuid;

use crate::{node::{EndpointPair, SEARCH_MAX_HOP_COUNT}, http::{SerdeHttpRequest, SerdeHttpResponse}};

#[derive(Serialize, Deserialize, Clone)]
pub struct MessageExt {
    pub payload: MessageKind,
    pub origin: SocketAddrV4,
    pub message_direction: MessageDirection,
    hop_count: u8,
    position: (usize, usize)
}

impl MessageExt {
    pub fn new(origin: SocketAddrV4, message_direction: MessageDirection, payload: MessageKind, hop_count: u8, position: (usize, usize)) -> Self {
        Self {
            payload,
            origin,
            message_direction,
            hop_count,
            position
        }
    }

    pub fn payload(&self) -> &MessageKind { &self.payload }
    pub fn payload_mut(&mut self) -> &mut MessageKind { &mut self.payload }
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

    pub fn into_response(self) -> SerdeHttpResponse {
        if let MessageKind::HttpResponse(response) = self.payload {
            response
        }
        else {
            panic!("That's just WRONG")
        }
    }
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

    pub fn initial_http_request(request: SerdeHttpRequest) -> Self {
        Message::new(EndpointPair::default_socket(),
            EndpointPair::default_socket(),
            Some(MessageExt::new(EndpointPair::default_socket(),
            MessageDirection::Request,
            MessageKind::HttpRequest(String::from("example"), request),
            SEARCH_MAX_HOP_COUNT,
            MessageExt::no_position())),
            None)
    }

    pub fn dest(&self) -> &SocketAddrV4 { &self.dest }
    pub fn sender(&self) -> &SocketAddrV4 { &self.sender }
    pub fn uuid(&self) -> &String { &self.uuid }

    pub fn is_heartbeat(&self) -> bool { self.message_ext.is_none() }

    pub fn is_http_response(&self) -> bool {
        if let Some(message_ext) = &self.message_ext {
            if let MessageKind::HttpResponse{..} = message_ext.payload() {
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

    pub fn into_message_ext(self) -> MessageExt {
        if let Some(message_ext) = self.message_ext {
            message_ext
        }
        else {
            panic!("No message_ext")
        }
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

    pub fn set_body(mut self, bytes: Vec<u8>) -> Self {
        if let MessageKind::HttpResponse(SerdeHttpResponse {ref mut body, ..}) = self.message_ext_mut().payload_mut() {
            *body = bytes;
        }
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
    HttpRequest(String, SerdeHttpRequest),
    HttpResponse(SerdeHttpResponse)
}