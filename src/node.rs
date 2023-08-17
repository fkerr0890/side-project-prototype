use std::io::Error;
use std::sync::Arc;
use std::{net::SocketAddrV4, fmt::Display};

use priority_queue::DoublePriorityQueue;
use tokio::net::UdpSocket;
use chrono::{Utc, SecondsFormat};

use crate::message::{Heartbeat, MessageDirection, MessageKind};
use crate::gateway::Gateway;

pub struct Node {
    pub endpoint_pair: EndpointPair,
    id: String,
    peers: DoublePriorityQueue<Peer, i32>,
    found_by: Vec<Peer>,
    nat_kind: NatKind,
    max_peers: usize,
    gateway: Gateway
}

impl Node {
    pub async fn new(endpoint_pair: EndpointPair, id: String, max_peers: usize) -> Self {
        let socket = UdpSocket::bind(endpoint_pair.private_endpoint).await.expect("Socket bind failed");
        println!("Socket: {:?}", socket);
        Self {
            endpoint_pair,
            id,
            peers: DoublePriorityQueue::new(),
            found_by: Vec::new(),
            nat_kind: NatKind::Unknown, 
            max_peers,
            gateway: Gateway::new(socket).await
        }
    }

    pub fn add_peer(&mut self, endpoint_pair: EndpointPair, score: i32) {
        let peer_limit_reached = self.peers.len() >= self.max_peers;
        let mut should_push = !peer_limit_reached;
        if let Some(worst_peer) = self.peers.peek_min() {
            should_push = should_push || worst_peer.1 > &score;
        }
        if should_push {
            self.peers.push(Peer::new(endpoint_pair), score);
        }
    }

    pub async fn send_heartbeats(&self) {
        for peer in self.peers.iter() {
            let heartbeat = Heartbeat::new(peer.0.endpoint_pair.clone(), self.endpoint_pair.clone(),
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
            self.gateway.send(heartbeat).await;
        }
    }

    pub fn receive_heartbeat(&self) -> Option<Error> {
        if let Some(error) = self.gateway.receive() {
            return Some(error);
        }
        None
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Node {}

// pub struct RendevousNode(Node);

#[derive(Hash)]
pub struct Peer {
    endpoint_pair: EndpointPair,
    status: PeerStatus
}

impl Peer {
    fn new(endpoint_pair: EndpointPair) -> Self {
        Self {
            endpoint_pair,
            status: PeerStatus::Disconnected
        }
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint_pair == other.endpoint_pair
    }
}
impl Eq for Peer {}

#[derive(Hash, Clone)]
pub struct EndpointPair {
    pub public_endpoint: SocketAddrV4,
    pub private_endpoint: SocketAddrV4,
}

impl EndpointPair {
    pub fn new(public_endpoint: SocketAddrV4, private_endpoint: SocketAddrV4) -> Self {
        Self {
            public_endpoint,
            private_endpoint
        }
    }
}

impl PartialEq for EndpointPair {
    fn eq(&self, other: &Self) -> bool {
        self.public_endpoint == other.public_endpoint
            && self.private_endpoint == other.private_endpoint
    }
}
impl Eq for EndpointPair {}
impl Display for EndpointPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Public: {}, Private: {}", self.private_endpoint, self.public_endpoint)
    }
}

#[derive(Hash)]
enum PeerStatus {
    Disconnected,
    Connected
}

enum NatKind {
    Unknown,
    Static,
    Easy,
    Hard
}
