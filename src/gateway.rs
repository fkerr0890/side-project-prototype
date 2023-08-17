use std::sync::Arc;
use std::io::Error;

use tokio::{net::UdpSocket, io};

use crate::message::Heartbeat;

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

    pub async fn send(&self, message: Heartbeat) {
        self.sender.send_to(format!("Hello from {} {}", message.sender.to_string(), message.timestamp).as_bytes(),
            message.dest.public_endpoint).await.expect("Sending failed");
    }

    pub fn receive(&self) -> Option<Error> {
        let mut buf = [0; 1024];
        match self.receiver.try_recv_from(&mut buf) {
            Ok((_n, addr)) => {
                println!("Recieved {} from {:?}", std::str::from_utf8(&buf).unwrap().to_owned(), addr);
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