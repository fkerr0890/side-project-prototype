use tokio::fs;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;
use tokio::io::{BufReader, stdin, AsyncReadExt};
use serde::Serialize;
use tokio::time::sleep;

use std::str;
use std::sync::{Arc, OnceLock};
use std::io::{BufWriter, stdout, Write};
use std::time::Duration;

use crate::message::{Message, MessageKind, MessageDirection, MessageExt};
use crate::node::{EndpointPair, SEARCH_MAX_HOP_COUNT};

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<Vec<Message>>
}

impl OutboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<Vec<Message>>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress
        }
    }

    pub async fn send(&mut self) {
        let outbound_messages = self.ingress.recv().await.unwrap();
        if *outbound_messages[0].dest() == EndpointPair::default_socket() {
            log_debug("Found messages to frontend");
            if outbound_messages[0].is_search_response() {
                log_debug("Returning resource to frontend");
                let (filename, contents) = Self::reassemble_resource(outbound_messages);
                make_resource_available(filename, contents).await;
                return;
            }
        }

        let num_retries = if outbound_messages[0].is_search_response() { 1 } else { 0 };
        // for _ in 0..=num_retries {
            for (i, message) in outbound_messages.iter().enumerate() {
                let bytes = &bincode::serialize(&message).unwrap();
                if num_retries == 1 {
                    log_debug(&format!("Sending {} of {} (index = {}), bytes = {}", i + 1, outbound_messages.len(), message.message_ext().position().0, bytes.len()));
                }
                self.socket.send_to(&bytes, message.dest()).await.unwrap();
                // sleep(Duration::from_micros(50)).await;
            }
        // }
    }

    fn reassemble_resource(mut messages: Vec<Message>) -> (String, Vec<u8>) {
        messages.sort_by(|a, b| a.message_ext().position().0.cmp(&b.message_ext().position().0));
        let mut contents: Vec<u8> = Vec::new();
        let filename = messages[0].payload_inner().0.unwrap().to_owned();
        for message in messages {
            if let MessageKind::SearchResponse(_, mut slice) = message.to_payload() {
                contents.append(&mut slice);
            }
            else {
                panic!();
            }
        }
        (filename, contents)
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
            egress: egress.clone()
        }
    }

    pub async fn receive(&self) {
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
                        let message = Message::new(EndpointPair::default_socket(), EndpointPair::default_socket(), Some(MessageExt::new(EndpointPair::default_socket(), MessageDirection::Request, payload, SEARCH_MAX_HOP_COUNT, None, MessageExt::no_position())));
                        // log_debug(&format!("Og hash {}", message.message_ext().hash()));
                        self.egress.send(message).unwrap();
                    },
                    _ => { log_debug("Frontend message didn't match any known message kind"); }
                }
            },
            Err(e) => log_debug(&format!("Error deserializing frontend message: {}", &e.to_string()))
        }
    }
}

pub async fn make_resource_available(filename: String, contents: Vec<u8>) {
    log_debug(&format!("Making {} available", filename));
    if !check_for_resource(&filename).await{
        let path = format!("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\{filename}");
        fs::write(path, contents).await.unwrap();
    }
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

pub async fn check_for_resource(requested_filename: &str) -> bool {
    let mut filenames = fs::read_dir("C:\\Users\\fredk\\side_project\\side-project-prototype\\static_hosting_test\\").await.unwrap();
    while let Ok(Some(filename)) = filenames.next_entry().await {
        if filename.file_name().to_str().unwrap() == requested_filename {
            return true;
        }
    }
    false
}

pub fn log_debug(message: &str) {
    // if *IS_NM_HOST.get().unwrap() {
    //     send_to_frontend(message);
    // }
    // else {
    //     println!("{}", message);
    // }
}