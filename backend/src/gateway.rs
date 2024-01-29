use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;

use std::net::SocketAddr;
use std::{str, net::SocketAddrV4};
use std::sync::Arc;

use crate::{message::{Message, SearchMessage, DiscoverPeerMessage, Heartbeat, StreamMessage, InboundMessage}, message_processing::send_error_response};

pub type EmptyResult = Result<(), String>;

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_staging: mpsc::UnboundedSender<(SocketAddrV4, InboundMessage)>
}

impl InboundGateway {
    pub fn new(
        socket: &Arc<UdpSocket>,
        to_staging: mpsc::UnboundedSender<(SocketAddrV4, InboundMessage)>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_staging
        }
    }

    pub async fn receive(&mut self)  -> EmptyResult {
        let mut buf = [0; 1024];
        match  self.socket.recv_from(&mut buf).await {
            Ok((n, addr)) => self.handle_message(&buf[..n], addr),
            Err(e) => { println!("Inbound gateway: receive error {}", e.to_string()); Ok(()) }
        }
    }

    fn handle_message(&self, message_bytes: &[u8], addr: SocketAddr) -> EmptyResult {
        if let SocketAddr::V4(socket) = addr {
            if let Ok(message) = bincode::deserialize::<InboundMessage>(message_bytes) {
                self.to_staging.send((socket, message)).map_err(|e| send_error_response(e, file!(), line!()))
            }
            else {
                Err(String::from("Unable to deserialize received message to a supported type"))
            }
        }
        else {
            panic!("Not v4 oh no");
        }
    }
    
    // async fn handle_message(&self, message_bytes: &[u8]) -> EmptyResult {
    //     if let Ok(message) = bincode::deserialize::<SearchMessage>(message_bytes) {
    //         println!("Received search message, uuid: {} at {:?}", message.id(), self.socket.local_addr().unwrap());
    //         self.to_srp.send(message).map_err(|e| { e.to_string() } )
    //     }
    //     else if let Ok(message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
    //         self.to_dpp.send(message).map_err(|e| { e.to_string() } )
    //     }
    //     else if let Ok(message) = bincode::deserialize::<Heartbeat>(message_bytes) {
    //         Ok(println!("{:?}", message))
    //     }
    //     else if let Ok(message) = bincode::deserialize::<StreamMessage>(message_bytes) {
    //         println!("Received stream message, uuid: {} at {:?}", message.id(), self.socket.local_addr().unwrap());
    //         self.to_smp.send(message).map_err(|e| { e.to_string() } )
    //     }
    //     else {
    //         Err(String::from("Unable to deserialize received message to a supported type"))
    //     }
    // }
}

pub fn log_debug(message: &str) {
    // if *IS_NM_HOST.get().unwrap() {
    //     send_to_frontend(message);
    // }
    // else {
        println!("{}", message);
    // }
}