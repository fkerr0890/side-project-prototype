use serde::{Serialize, Deserialize};

use crate::node::EndpointPair;

use priority_queue::DoublePriorityQueue;

pub const MAX_PEERS: u16 = 10;

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected
}

pub struct PeerOps {
    peers: DoublePriorityQueue<EndpointPair, i32>
}

impl PeerOps {
    pub fn new() -> Self { Self { peers: DoublePriorityQueue::new() } }

    pub fn peers(&self) -> Vec<EndpointPair> { self.peers.iter().map(|p| *p.0).collect() }
    pub fn peers_and_scores(&self) -> Vec<(EndpointPair, i32)> { self.peers.iter().map(|(i, score)| (*i, *score)).collect() }
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
}