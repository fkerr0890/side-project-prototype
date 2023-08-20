use std::sync::Arc;
use std::io::Error;

use tokio::{net::UdpSocket, io};
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

    pub fn receive(&self) -> Option<Error> {
        let mut buf = [0; 1024];
        match self.receiver.try_recv_from(&mut buf) {
            Ok((_n, _addr)) => {
                println!("Received {}", std::str::from_utf8(&buf).unwrap().to_owned());
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock) => {
                println!("Nothing to receive");
            }
            Err(e) => {
                return Some(e);
            }
        }
        None
    }
}