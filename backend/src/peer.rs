use rustc_hash::FxHashSet;

use serde::{Deserialize, Serialize};

use crate::{
    message::{NumId, Peer},
    node::EndpointPair,
};

use priority_queue::DoublePriorityQueue;

pub const MAX_PEERS: u16 = 10;

#[derive(Hash, Serialize, Deserialize, Copy, Clone)]
pub enum PeerStatus {
    Disconnected,
    Connected,
}

pub struct PeerOps {
    peer_queue: DoublePriorityQueue<Peer, i32>,
    peer_ids: FxHashSet<NumId>,
}

impl Default for PeerOps {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerOps {
    pub fn new() -> Self {
        Self {
            peer_queue: DoublePriorityQueue::new(),
            peer_ids: FxHashSet::default(),
        }
    }

    pub fn peers(&self) -> Vec<Peer> {
        self.peer_queue.iter().map(|(peer, _)| *peer).collect()
    }
    pub fn peers_and_scores(&self) -> Vec<(EndpointPair, i32, NumId)> {
        self.peer_queue
            .iter()
            .map(|(peer, score)| (peer.endpoint_pair, *score, peer.id))
            .collect()
    }
    pub fn peers_len(&self) -> usize {
        self.peer_queue.len()
    }

    pub fn add_initial_peer(&mut self, peer: Peer) {
        self.add_peer(peer, 0);
    }

    pub fn add_peer(&mut self, peer: Peer, score: i32) {
        let None = self.peer_queue.change_priority(&peer, score) else {
            return;
        };
        let peer_limit_reached = self.peer_queue.len() >= MAX_PEERS as usize;
        let mut should_push = !peer_limit_reached;
        if let Some(worst_peer) = self.peer_queue.peek_min() {
            should_push = should_push || worst_peer.1 < &score;
        }
        if should_push {
            if peer_limit_reached {
                self.peer_ids.remove(&self.peer_queue.pop_min().unwrap().0.id);
            }
            self.peer_ids.insert(peer.id);
            self.peer_queue.push(peer, score);
        }
    }

    pub fn has_peer(&self, peer_id: NumId) -> bool {
        self.peer_ids.contains(&peer_id)
    }
}

#[cfg(test)]
impl PeerOps {
    pub fn get_peer_score(&self, peer: Peer) -> Option<&i32> {
        self.peer_queue.get_priority(&peer)
    }
}
