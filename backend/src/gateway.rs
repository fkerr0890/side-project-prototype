use once_cell::sync::Lazy;
use tokio::sync::{Mutex, mpsc};
use tokio::net::UdpSocket;

use std::str;
use std::sync::{Arc, OnceLock};

use crate::message::Message;

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();
pub static mut SEARCH_RESPONSE_COUNT: Lazy<Mutex<i16>> = Lazy::new(|| { Mutex::new(0) });

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<Message>
}

impl OutboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<Message>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress
        }
    }

    pub async fn send(&mut self) {
        let outbound_message = self.ingress.recv().await.unwrap();
        let dest = *outbound_message.dest();
        let bytes = &bincode::serialize(&outbound_message).unwrap();
        self.socket.send_to(bytes, dest).await.unwrap();
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