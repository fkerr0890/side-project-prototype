use serde::{Serialize, Deserialize};

use crate::node::{EndpointPair, PeerStatus};

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub struct Peer {
    pub endpoint_pair: EndpointPair,
    status: PeerStatus,
    score: i32
}

impl Peer {
    fn new(endpoint_pair: EndpointPair, score: i32) -> Self {
        Self {
            endpoint_pair,
            status: PeerStatus::Disconnected,
            score
        }
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint_pair == other.endpoint_pair
    }
}
impl Eq for Peer {}

pub mod peer_ops {
    use std::{sync::{OnceLock, Mutex}, net::SocketAddrV4};

    use chrono::Utc;
    use once_cell::sync::Lazy;
    use priority_queue::DoublePriorityQueue;
    use tokio::sync::mpsc;
    
    use crate::{node::EndpointPair, message::{Message, self}, gateway::{self, EmptyResult}};

    use super::Peer;

    static mut PEERS: Lazy<Mutex<DoublePriorityQueue<Peer, i32>>> = Lazy::new(|| { Mutex::new(DoublePriorityQueue::new()) });
    pub const MAX_PEERS: usize = 6;
    pub static EGRESS: OnceLock<mpsc::UnboundedSender<Message>> = OnceLock::new();

    pub fn add_initial_peer(endpoint_pair: EndpointPair) {
        add_peer(endpoint_pair, 0);
    }

    pub fn add_peer(endpoint_pair: EndpointPair, score: i32) {
        let mut peers = unsafe { PEERS.lock().unwrap() };
        let peer_limit_reached = peers.len() >= MAX_PEERS;
        let mut should_push = !peer_limit_reached;
        if let Some(worst_peer) = peers.peek_min() {
            should_push = should_push || worst_peer.1 > &score;
        }
        if should_push {
            if peer_limit_reached {
                peers.pop_min();
            }
            peers.push(Peer::new(endpoint_pair, score), score);
        }
    }

    pub fn send_search_request(search_request_parts: Vec<Message>, sender: SocketAddrV4) -> EmptyResult {
        let peers = unsafe { PEERS.lock().unwrap() };
        for peer in peers.iter() {
            for message in search_request_parts.clone() {
                let result = message
                    .set_sender(sender)
                    .replace_dest_and_timestamp(peer.0.endpoint_pair.public_endpoint)
                    .check_expiry();
                if let Ok(message) = result {
                    EGRESS.get().unwrap().send(message).map_err(|_| { () })?;
                }
                else {
                    gateway::log_debug("Message expired");
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub async fn send_heartbeats(sender: EndpointPair) -> EmptyResult {
        let peers = unsafe { PEERS.lock().unwrap() };
        for peer in peers.iter() {
            let heartbeat = Message::new(peer.0.endpoint_pair.public_endpoint, sender.public_endpoint, message::datetime_to_timestamp(Utc::now()), None, None);
            EGRESS.get().unwrap().send(heartbeat).map_err(|_| { () })?;
        }
        Ok(())
    }
}