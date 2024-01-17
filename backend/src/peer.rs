use serde::{Serialize, Deserialize};

use crate::node::EndpointPair;

use chrono::Utc;
use priority_queue::DoublePriorityQueue;
use tokio::sync::mpsc;

use crate::{message::{Message, self, Heartbeat}, gateway::{self, EmptyResult}};

pub const MAX_PEERS: u16 = 10;

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected
}

pub struct PeerOps {
    peers: DoublePriorityQueue<EndpointPair, i32>,
    heartbeat_tx: mpsc::UnboundedSender<Heartbeat>,
    endpoint_pair: EndpointPair
}

impl PeerOps {
    pub fn new(heartbeat_tx: mpsc::UnboundedSender<Heartbeat>, endpoint_pair: EndpointPair) -> Self { Self { peers: DoublePriorityQueue::new(), heartbeat_tx, endpoint_pair } }

    pub fn peers(&self) -> Vec<(EndpointPair, i32)> { self.peers.iter().map(|(i, score)| (*i, *score)).collect() }
    pub fn peers_len(&self) -> usize { self.peers.len() }
    pub fn heartbeat_tx(&self) -> mpsc::UnboundedSender<Heartbeat> { return self.heartbeat_tx.clone() }

    pub fn add_initial_peer(&mut self, endpoint_pair: EndpointPair) {
        self.add_peer(endpoint_pair, 0);
    }

    pub fn add_peer(&mut self, endpoint_pair: EndpointPair, score: i32) {
        let None = self.peers.change_priority(&endpoint_pair, score) else { return };
        let peer_limit_reached = self.peers.len() >= MAX_PEERS as usize;
        let mut should_push = !peer_limit_reached;
        if let Some(worst_peer) = self.peers.peek_min() {
            should_push = should_push || worst_peer.1 > &score;
        }
        if should_push {
            if peer_limit_reached {
                self.peers.pop_min();
            }
            self.peers.push(endpoint_pair, score);
        }
    }

    pub fn send_request<T: Message + Clone>(&self, search_request_parts: Vec<T>, tx: &mpsc::UnboundedSender<T>) -> EmptyResult {
        // gateway::log_debug(&format!("Peers len: {}", self.peers.len()));
        for peer in self.peers.iter() {
            for message in search_request_parts.clone() {
                let message = message
                    .set_sender(self.endpoint_pair.public_endpoint)
                    .replace_dest_and_timestamp(peer.0.public_endpoint);
                if !message.check_expiry() {
                    tx.send(message).map_err(|e| { e.to_string() })?;
                }
                else {
                    gateway::log_debug("Message expired");
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn send_heartbeats(&self) -> EmptyResult {
        for peer in self.peers.iter() {
            let heartbeat = Heartbeat::new(peer.0.public_endpoint, self.endpoint_pair.public_endpoint, message::datetime_to_timestamp(Utc::now()));
            self.heartbeat_tx.send(heartbeat).map_err(|e| { e.to_string() })?;
        }
        Ok(())
    }
}