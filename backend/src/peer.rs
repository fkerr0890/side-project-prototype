use std::fmt::Debug;

use serde::{Serialize, Deserialize};

use crate::{node::EndpointPair, message::{Heartbeat, Message}};

use priority_queue::DoublePriorityQueue;

pub const MAX_PEERS: u16 = 10;

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected
}

pub struct PeerOps {
    peers: DoublePriorityQueue<EndpointPair, i32>,
    endpoint_pair: EndpointPair
}

impl PeerOps {
    pub fn new(endpoint_pair: EndpointPair) -> Self { Self { peers: DoublePriorityQueue::new(), endpoint_pair } }

    pub fn peers(&self) -> Vec<(EndpointPair, i32)> { self.peers.iter().map(|(i, score)| (*i, *score)).collect() }
    pub fn peers_len(&self) -> usize { self.peers.len() }

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

    pub fn check_request<T: Message + Clone + Debug>(&self, request: T) -> Vec<T> {
        let mut messages = Vec::new();
        for peer in self.peers.iter() {
            let mut message = request.clone();
            message.set_sender(self.endpoint_pair.public_endpoint);
            message.replace_dest_and_timestamp(peer.0.public_endpoint);
            if message.check_expiry() {
                messages.push(message);
            }
            else {
                println!("PeerOps: Message expired: {:?}", message);
            }
        }
        messages
    }

    pub fn get_heartbeats(&self) -> Vec<Heartbeat> {
        self.peers.iter().map(|peer| Heartbeat::new(peer.0.public_endpoint, self.endpoint_pair.public_endpoint)).collect()
    }
}