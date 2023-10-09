use once_cell::sync::Lazy;
use tokio::sync::{Mutex, mpsc};
use tokio::net::UdpSocket;

use std::str;
use std::sync::{Arc, OnceLock};

use crate::http::SerdeHttpResponse;
use crate::message::Message;
use crate::node::EndpointPair;

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();
pub static mut SEARCH_RESPONSE_COUNT: Lazy<Mutex<i16>> = Lazy::new(|| { Mutex::new(0) });

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<Vec<Message>>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>
}

impl OutboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<Vec<Message>>, to_frontend: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress,
            to_http_handler: to_frontend
        }
    }

    pub async fn send(&mut self) {
        let outbound_messages = self.ingress.recv().await.unwrap();
        if *outbound_messages[0].dest() == EndpointPair::default_socket() {
            log_debug("Found messages to frontend");
            if outbound_messages[0].is_http_response() {
                log_debug("Returning resource to frontend");
                let response = Self::reassemble_resource(outbound_messages);
                self.to_http_handler.send(response).unwrap();
                return;
            }
        }

        for (i, message) in outbound_messages.iter().enumerate() {
            let bytes = &bincode::serialize(&message).unwrap();
            if outbound_messages[0].is_http_response() {
                log_debug(&format!("Sending {} of {} (index = {}), bytes = {}", i + 1, outbound_messages.len(), message.message_ext().position().0, bytes.len()));
            }
            self.socket.send_to(&bytes, message.dest()).await.unwrap();
            // sleep(Duration::from_micros(50)).await;
        }
    }

    fn reassemble_resource(mut messages: Vec<Message>) -> SerdeHttpResponse {
        let mut unfinished_response: Option<SerdeHttpResponse> = None;
        let mut first = false;
        messages.sort_by(|a, b| a.message_ext().position().0.cmp(&b.message_ext().position().0));
        let mut bytes: Vec<u8> = Vec::new();
        for message in messages {
            let (status_code, version, headers, mut body) = message.into_message_ext().into_response().into_parts();
            if !first {
                unfinished_response = Some(SerdeHttpResponse::without_body(status_code, version, headers));
                first = true
            }
            bytes.append(&mut body);
        }
        let mut reponse = unfinished_response.unwrap();
        reponse.body = bytes;
        reponse
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

pub fn log_debug(message: &str) {
    // if *IS_NM_HOST.get().unwrap() {
    //     send_to_frontend(message);
    // }
    // else {
        println!("{}", message);
    // }
}