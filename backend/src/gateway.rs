use tokio::fs;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;
use tokio::io::{BufReader, stdin, AsyncReadExt};
use serde::Serialize;

use std::str;
use std::sync::{Arc, OnceLock};
use std::io::{BufWriter, stdout, Write};

use crate::message::{Message, FullMessage, MessageKind, Heartbeat, BaseMessage, MessageDirection};
use crate::node::EndpointPair;

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();

pub struct OutboundGateway<T: Message> {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<T>
}

impl<T: Message + Serialize> OutboundGateway<T> {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<T>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress
        }
    }

    pub async fn send(&mut self) {
        let outbound_message = self.ingress.recv().await.unwrap();
        log_debug("Sending message");
        self.socket.send_to(serde_json::to_string(&outbound_message).unwrap().as_bytes(), outbound_message.base_message().dest().public_endpoint).await.unwrap();
    }
}

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    egress: mpsc::UnboundedSender<FullMessage>
}

impl InboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, egress: &mpsc::UnboundedSender<FullMessage>) -> Self 
    {
        Self {
            socket: socket.clone(),
            egress: egress.clone()
        }
    }

    pub async fn receive(&self) {
        let mut buf = [0; 1024];
        match self.socket.recv_from(&mut buf).await {
            Ok((n, _addr)) => {
                self.handle_message(&buf[..n]).await;
            }
            Err(e) => {
                log_debug(&e.to_string());
            }
        }
    }
    
    async fn handle_message(&self, message_bytes: &[u8]) {
        if let Ok(message) = serde_json::from_slice::<FullMessage>(message_bytes) {
            match message.payload() {
                MessageKind::SearchRequest(_) => {
                    log_debug("Received search request");
                    self.egress.send(message).unwrap();
                },
                MessageKind::SearchResponse(filename, file_bytes) => {
                    log_debug("Received search response");
                    let path = format!("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\{}", filename);
                    fs::write(path, file_bytes).await.unwrap();
                    notify_resource_available(filename.to_owned());
                }
                _ => {}
            }
        }
        else {
            match serde_json::from_slice::<Heartbeat>(message_bytes) {
                Ok(heartbeat) => log_debug(&serde_json::to_string(&heartbeat).unwrap()),
                Err(e) => log_debug(&format!("Error deserializing net message: {}", &e.to_string()))
            }
        }
    }

    pub async fn receive_frontend_message(&self) {
        let mut reader = BufReader::new(stdin());
        let mut len_bytes = [0; 4];
        reader.read_exact(&mut len_bytes).await.unwrap();
        let len = u32::from_ne_bytes(len_bytes);
        let mut message_bytes = vec![0; len as usize];
        reader.read_exact(&mut message_bytes).await.unwrap();
        let message_contents = serde_json::from_slice::<String>(&message_bytes).unwrap();
        log_debug(&format!("Received frontend message: {}", message_contents));
        self.handle_frontend_message(&message_contents).await;
    }

    async fn handle_frontend_message(&self, message_contents: &str) {
        match serde_json::from_str::<MessageKind>(message_contents) {
            Ok(payload) => {
                match payload {
                    MessageKind::ResourceAvailable(_) => panic!(),
                    MessageKind::SearchRequest(_) => {
                        log_debug("Received search request from frontend");
                        let message = FullMessage::new(BaseMessage::new(EndpointPair::default(), EndpointPair::default()), EndpointPair::default(), MessageDirection::Request, payload, 0, 0);
                        self.egress.send(message).unwrap();
                    },
                    _ => { log_debug("Frontend message didn't match any known message kind"); }
                }
            },
            Err(e) => log_debug(&format!("Error deserializing frontend message: {}", &e.to_string()))
        }
    }
}

pub fn notify_resource_available(filename: String) {
    send_to_frontend(MessageKind::ResourceAvailable(filename));
}

pub fn send_to_frontend<T: Serialize>(contents: T) {
    let mut writer = BufWriter::new(stdout());
    let contents = serde_json::to_string(&contents).unwrap();
    let len = contents.len() as u32;
    let len_bytes = len.to_ne_bytes();
    writer.write_all(&len_bytes).unwrap();
    writer.write_all(contents.as_bytes()).unwrap();
    writer.flush().unwrap();
}

pub fn log_debug(message: &str) {
    if *IS_NM_HOST.get().unwrap() {
        send_to_frontend(message);
    }
    else {
        println!("{}", message);
    }
}