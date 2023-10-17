use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::net::UdpSocket;

use std::str;
use std::sync::Arc;

use crate::message::Message;

pub type EmptyResult = Result<(), ()>; 

pub struct OutboundGateway<T> {
    socket: Arc<UdpSocket>,
    ingress: mpsc::UnboundedReceiver<T>
}

impl<T: Serialize + DeserializeOwned + Message<T>> OutboundGateway<T> {
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

pub struct InboundGateway<T> {
    socket: Arc<UdpSocket>,
    egress: mpsc::UnboundedSender<T>
}

impl<T: Serialize + DeserializeOwned + Message<T>> InboundGateway<T> {
    pub fn new(socket: &Arc<UdpSocket>, egress: &mpsc::UnboundedSender<T>) -> Self 
    {
        Self {
            socket: socket.clone(),
            egress: egress.clone(), 
        }
    }

    pub async fn receive(&mut self)  -> EmptyResult {
        let mut buf = [0; 1024];
        let Ok(result) = self.socket.recv_from(&mut buf).await else { return Ok(()) };
        // log_debug(&format!("Received {n} bytes"));
        self.handle_message(&buf[..result.0]).await
    }
    
    async fn handle_message(&self, message_bytes: &[u8]) -> EmptyResult {
        let message = bincode::deserialize::<T>(message_bytes).unwrap();
        if message.is_heartbeat() {
            Ok(log_debug(&serde_json::to_string(&message).unwrap()))
        }
        else {
            self.egress.send(message).map_err(|_| { () } )
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