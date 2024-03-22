use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::{message::{NumId, Peer}, node::EndpointPair};

use priority_queue::DoublePriorityQueue;

pub const MAX_PEERS: u16 = 10;

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected
}

pub struct PeerOps {
    peer_queue: DoublePriorityQueue<NumId, i32>,
    peer_map: HashMap<NumId, EndpointPair>
}

impl PeerOps {
    pub fn new() -> Self { Self { peer_queue: DoublePriorityQueue::new(), peer_map: HashMap::new() } }

    pub fn peers(&self) -> Vec<EndpointPair> { self.peer_map.values().copied().collect() }
    pub fn peers_and_scores(&self) -> Vec<(EndpointPair, i32, NumId)> { self.peer_queue.iter().map(|(id, score)| (*self.peer_map.get(&id).unwrap(), *score, *id)).collect() }
    pub fn peers_len(&self) -> usize { self.peer_queue.len() }

    pub fn add_initial_peer(&mut self, peer: Peer) {
        self.add_peer(peer, 0);
    }

    pub fn add_peer(&mut self, peer: Peer, score: i32) {
        self.peer_map.insert(peer.id, peer.endpoint_pair);
        let None = self.peer_queue.change_priority(&peer.id, score) else { return };
        let peer_limit_reached = self.peer_queue.len() >= MAX_PEERS as usize;
        let mut should_push = !peer_limit_reached;
        if let Some(worst_peer) = self.peer_queue.peek_min() {
            should_push = should_push || worst_peer.1 > &score;
        }
        if should_push {
            if peer_limit_reached {
                self.peer_queue.pop_min();
            }
            self.peer_queue.push(peer.id, score);
        }
    }

    pub fn has_peer(&self, id: NumId) -> bool { self.peer_map.contains_key(&id) }
}