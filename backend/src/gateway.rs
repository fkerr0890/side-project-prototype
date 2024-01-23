use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;

use std::{str, fmt::Debug};
use std::sync::Arc;

use crate::message::{Message, SearchMessage, DiscoverPeerMessage, Heartbeat, StreamMessage};

pub type EmptyResult = Result<(), String>; 

pub struct OutboundGateway<T> {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<T>
}

impl<T: Serialize + DeserializeOwned + Message + Debug> OutboundGateway<T> {
    pub fn new(socket: &Arc<UdpSocket>, ingress: mpsc::UnboundedReceiver<T>) -> Self 
    {
        Self {
            socket: socket.clone(),
            ingress
        }
    }

    pub async fn send(&mut self) -> Result<usize, String> {
        let Some(outbound_message) = self.ingress.recv().await else { return Err(String::from("Gateway: failed to receive from message processing")) };
        // println!("Sending message {:?}", outbound_message);
        let dest = outbound_message.dest();
        let Ok(bytes) = &bincode::serialize(&outbound_message) else { return Ok(0) };
        self.socket.send_to(bytes, dest).await.map_err(|e| e.to_string())
    }
}

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>
}

impl InboundGateway {
    pub fn new(socket: &Arc<UdpSocket>, to_srp: &mpsc::UnboundedSender<SearchMessage>, to_dpp: &mpsc::UnboundedSender<DiscoverPeerMessage>, to_smp: &mpsc::UnboundedSender<StreamMessage>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_srp: to_srp.clone(),
            to_dpp: to_dpp.clone(),
            to_smp: to_smp.clone()
        }
    }

    pub async fn receive(&mut self)  -> EmptyResult {
        let mut buf = [0; 1024];
        match  self.socket.recv_from(&mut buf).await {
            Ok((n, _)) => self.handle_message(&buf[..n]).await,
            Err(e) => { println!("Inbound gateway: receive error {}", e.to_string()); Ok(()) }
        }
    }
    
    async fn handle_message(&self, message_bytes: &[u8]) -> EmptyResult {
        if let Ok(message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            Ok(println!("{:?}", message))
        }
        else if let Ok(message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            self.to_smp.send(message).map_err(|e| { e.to_string() } )
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