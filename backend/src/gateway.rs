use once_cell::sync::Lazy;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::sync::mpsc;
use tokio::net::{UdpSocket, TcpListener, TcpStream};
use serde::Serialize;

use std::net::SocketAddrV4;
use std::str;
use std::sync::{Arc, OnceLock, Mutex};
use std::io::{BufWriter, stdout, Write};

use crate::message::{Message, MessageKind, MessageDirection, MessageExt};
use crate::node::{EndpointPair, SEARCH_MAX_HOP_COUNT};

pub static IS_NM_HOST: OnceLock<bool> = OnceLock::new();
pub static mut SEARCH_RESPONSE_COUNT: Lazy<Mutex<i16>> = Lazy::new(|| { Mutex::new(0) });

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<Vec<Message>>,
    to_inbound_gateway: mpsc::UnboundedSender<Vec<u8>>
}

impl OutboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<Vec<Message>>, to_frontend: mpsc::UnboundedSender<Vec<u8>>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress,
            to_inbound_gateway: to_frontend
        }
    }

    pub async fn send(&mut self) {
        let outbound_messages = self.ingress.recv().await.unwrap();
        if *outbound_messages[0].dest() == EndpointPair::default_socket() {
            log_debug("Found messages to frontend");
            if outbound_messages[0].is_http() {
                log_debug("Returning resource to frontend");
                let response = Self::reassemble_resource(outbound_messages);
                self.to_inbound_gateway.send(response).unwrap();
                return;
            }
        }

        let num_retries = if outbound_messages[0].is_http() { 1 } else { 0 };
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

    pub async fn send_request(socket: SocketAddrV4, request: &Vec<u8>) -> Vec<u8> {
        let mut stream = TcpStream::connect(socket).await.unwrap();
        stream.write_all(request).await.unwrap();
        read_to_end(&mut stream).await
    }

    fn reassemble_resource(mut messages: Vec<Message>) -> Vec<u8> {
        messages.sort_by(|a, b| a.message_ext().position().0.cmp(&b.message_ext().position().0));
        let mut contents: Vec<u8> = Vec::new();
        for message in messages {
            if let MessageKind::Http(_, mut bytes) = message.into_payload() {
                contents.append(&mut bytes);
            }
            else {
                panic!();
            }
        }
        contents
    }
}

pub struct InboundGateway {
    socket: Option<Arc<UdpSocket>>,
    egress: mpsc::UnboundedSender<Message>,
    proxy: Option<TcpListener>,
    from_outbound_gateway: Option<mpsc::UnboundedReceiver<Vec<u8>>>
}

impl InboundGateway {
    pub fn new(socket: Option<&Arc<UdpSocket>>, egress: &mpsc::UnboundedSender<Message>, proxy: Option<TcpListener>, from_outbound_gateway: Option<mpsc::UnboundedReceiver<Vec<u8>>>) -> Self 
    {
        let socket = if let Some(inner) = socket {
            Some(inner.clone())
        }
        else {
            None
        };
        Self {
            socket,
            egress: egress.clone(), 
            proxy,
            from_outbound_gateway
        }
    }

    pub async fn receive(&mut self) {
        let mut buf = [0; 8192];
        match self.socket.as_ref().unwrap().recv_from(&mut buf).await {
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

    pub async fn handle_http_request(&mut self) {
        let (mut tcp_stream, _addr) = self.proxy.as_ref().unwrap().accept().await.unwrap();
        let request = read_to_end(&mut tcp_stream).await;
        let message = Message::new(EndpointPair::default_socket(),
            EndpointPair::default_socket(),
            Some(MessageExt::new(EndpointPair::default_socket(),
            MessageDirection::Request,
            MessageKind::Http(Some(String::from("example")), request),
            SEARCH_MAX_HOP_COUNT,
            MessageExt::no_position())),
            None);
        self.egress.send(message).unwrap();
        let response = self.from_outbound_gateway.as_mut().unwrap().recv().await.unwrap();
        tcp_stream.write_all(&response).await.unwrap();
    }
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

async fn read_to_end(tcp_stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    tcp_stream.read_to_end(&mut buf).await.unwrap();
    buf
}