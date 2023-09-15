use std::sync::Arc;
use tokio::{net::UdpSocket, io};
use tokio::io::{BufWriter, stdout, AsyncWriteExt};
use serde::Serialize;

use crate::message::Message;

pub struct Gateway {
    sender: Arc<UdpSocket>,
    receiver: Arc<UdpSocket>
}

impl Gateway {
    pub async fn new(socket: UdpSocket) -> Self 
    {
        let arc_socket = Arc::new(socket);
        Self {
            sender: Arc::clone(&arc_socket),
            receiver: Arc::clone(&arc_socket)
        }
    }

    pub async fn send<T: Serialize + Message>(&self, message: &T) {
        self.sender.send_to(serde_json::to_string(&message).unwrap().as_bytes(), message.dest().public_endpoint).await.expect("Sending failed");
    }

    pub async fn receive(&self) {
        let mut buf = [0; 1024];
        match self.receiver.try_recv_from(&mut buf) {
            Ok((n, _addr)) => {
                send_to_frontend(std::str::from_utf8(&buf[..n]).unwrap()).await;
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock) => {
            }
            Err(e) => {
                send_to_frontend(&e.to_string()).await;
            }
        }
    }
}

pub async fn send_to_frontend(message_contents: &str) {
    let mut writer = BufWriter::new(stdout());
    let message = serde_json::to_string(message_contents).unwrap();
    let len = message.len() as u32;
    let len_bytes = len.to_ne_bytes();
    writer.write_all(&len_bytes).await.unwrap();
    writer.write_all(message.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
}