use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;

use std::str;
use std::sync::Arc;

use crate::message::{Message, SearchMessage, DiscoverPeerMessage, Heartbeat};

pub type EmptyResult = Result<(), String>; 

pub struct OutboundGateway<T> {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<T>
}

impl<T: Serialize + DeserializeOwned + Message> OutboundGateway<T> {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<T>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress
        }
    }

    pub async fn send(&mut self) -> Option<usize> {
        let outbound_message = self.ingress.recv().await?;
        let dest = outbound_message.dest();
        let bytes = &bincode::serialize(&outbound_message).unwrap();
        Some(self.socket.send_to(bytes, dest).await.unwrap_or_default())
    }
}

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>
}

impl InboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, to_srp: &mpsc::UnboundedSender<SearchMessage>, to_dpp: &mpsc::UnboundedSender<DiscoverPeerMessage>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_srp: to_srp.clone(),
            to_dpp: to_dpp.clone()
        }
    }

    pub async fn receive(&mut self)  -> EmptyResult {
        let mut buf = [0; 1024];
        match  self.socket.recv_from(&mut buf).await {
            Ok(result) => self.handle_message(&buf[..result.0]).await,
            Err(e) => { log_debug(&e.to_string()); Ok(()) }
        }
        // log_debug(&format!("Received {n} bytes"));
    }
    
    async fn handle_message(&self, message_bytes: &[u8]) -> EmptyResult {
        if let Ok(message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            Ok(log_debug(&serde_json::to_string(&message).unwrap()))
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
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