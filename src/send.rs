use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::message::Heartbeat;

pub async fn send(socket: Arc<UdpSocket>, message: Heartbeat) {
    socket.send_to(format!("Hello from {} {}", message.sender.to_string(), message.timestamp).as_bytes(),
        message.dest.public_endpoint).await.expect("Sending failed");
}