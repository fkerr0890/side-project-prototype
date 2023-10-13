use once_cell::sync::Lazy;
use tokio::sync::{Mutex, mpsc};
use tokio::net::UdpSocket;

use std::str;
use std::sync::{Arc, OnceLock};

use crate::http::SerdeHttpResponse;
use crate::message::{Message, MessageKind};
use crate::node::EndpointPair;

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();
pub static mut SEARCH_RESPONSE_COUNT: Lazy<Mutex<i16>> = Lazy::new(|| { Mutex::new(0) });

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<Message>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>
}

impl OutboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<Message>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress,
            to_http_handler
        }
    }

    pub async fn send(&mut self) {
        let outbound_message = self.ingress.recv().await.unwrap();
        if *outbound_message.dest() == EndpointPair::default_socket() {
            log_debug("Returning resource to frontend");
            if let MessageKind::HttpResponse = outbound_message.message_ext().kind() {
                let response: SerdeHttpResponse = bincode::deserialize(&outbound_message.into_message_ext().into_payload()).unwrap();
                self.to_http_handler.send(response).unwrap();
            }
            return;
        }

        let is_heartbeat = outbound_message.is_heartbeat();
        let len = outbound_message.message_ext().position().1;
        let dest = *outbound_message.dest();
        for (i, message) in outbound_message.chunked().iter().enumerate() {
            let bytes = &bincode::serialize(&message).unwrap();
            if !is_heartbeat {
                log_debug(&format!("Sending {} of {}, bytes = {}", i + 1, len, bytes.len()));
            }
            self.socket.send_to(&bytes, dest).await.unwrap();
            // sleep(Duration::from_micros(50)).await;
        }
    }
}

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    egress: mpsc::UnboundedSender<Message>
}

impl InboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, egress: &mpsc::UnboundedSender<Message>) -> Self 
    {
        Self {
            socket: socket.clone(),
            egress: egress.clone(), 
        }
    }

    pub async fn receive(&mut self) {
        let mut buf = [0; 8192];
        match self.socket.recv_from(&mut buf).await {
            Ok((n, _addr)) => {
                // log_debug(&format!("Received {n} bytes"));
                self.handle_message(&buf[..n]).await;
            }
            Err(e) => {
                log_debug(&e.to_string());
            }
        }
    }
    
    async fn handle_message(&self, message_bytes: &[u8]) {
        match bincode::deserialize::<Message>(message_bytes) {
            Ok(message) => {
                if message.is_heartbeat() {
                    log_debug(&serde_json::to_string(&message).unwrap());
                }
                else {
                    self.egress.send(message).unwrap();
                }
            },
            Err(e) => log_debug(&format!("Error deserializing net message: {}", &e.to_string()))
        }
    }
}

pub fn reassemble_resource(mut messages: Vec<Message>) -> Message {
    let (mut bytes, mut unfinished_message) = messages.pop().unwrap().extract_payload();
    messages.sort_by(|a, b| a.message_ext().position().0.cmp(&b.message_ext().position().0));
    for message in messages {
        bytes.append(&mut message.into_message_ext().into_payload());
    }
    unfinished_message = unfinished_message.set_payload(bytes);
    unfinished_message
}

pub fn log_debug(message: &str) {
    // if *IS_NM_HOST.get().unwrap() {
    //     send_to_frontend(message);
    // }
    // else {
        println!("{}", message);
    // }
}