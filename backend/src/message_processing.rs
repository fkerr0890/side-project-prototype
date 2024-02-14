use std::{collections::{HashMap, HashSet}, net::SocketAddrV4, sync::{Arc, Mutex}, time::Duration};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use ring::aead;
use serde::Serialize;
use tokio::{sync::{oneshot, mpsc}, time::sleep, net::UdpSocket};

use crate::{crypto::{Direction, KeyStore}, gateway::EmptyResult, message::{DiscoverPeerMessage, Heartbeat, InboundMessage, IsEncrypted, Message, SeparateParts}, node::EndpointPair, peer::PeerOps};

pub use self::discover::DiscoverPeerProcessor;

pub const SEARCH_TIMEOUT: i64 = 30;
pub const DPP_TTL: u64 = 200;
const SRP_TTL: u64 = 30000;

pub mod stage;
pub mod stream;
pub mod search;
pub mod discover;

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error.to_string(), file, line)
}

pub struct MessageProcessor {
    socket: Arc<UdpSocket>,
    endpoint_pair: EndpointPair,
    breadcrumbs: Arc<Mutex<HashMap<String, SocketAddrV4>>>,
    key_store: Arc<Mutex<KeyStore>>,
    peer_ops: Option<Arc<Mutex<PeerOps>>>
}

impl MessageProcessor {
    pub fn new(socket: Arc<UdpSocket>, endpoint_pair: EndpointPair, key_store: &Arc<Mutex<KeyStore>>, peer_ops: Option<Arc<Mutex<PeerOps>>>,) -> Self {
        Self {
            socket: socket.clone(),
            endpoint_pair,
            breadcrumbs: Arc::new(Mutex::new(HashMap::new())),
            key_store: key_store.clone(),
            peer_ops
        }
    } 

    fn try_add_breadcrumb(&mut self, id: &str, dest: SocketAddrV4) -> bool {
        // gateway::log_debug(&format!("Sender {}", dest));
        let mut breadcrumbs = self.breadcrumbs.lock().unwrap();
        if breadcrumbs.contains_key(id) {
            // gateway::log_debug("Already visited this node, not propagating message");
            false
        }
        else {
            breadcrumbs.insert(id.to_owned(), dest);
            true
        }
    }

    fn set_breadcrumb_ttl(&self, early_return_message: Option<DiscoverPeerMessage>, id: &str, ttl: u64) {
        let (breadcrumbs_clone, dest, sender, id) = (self.breadcrumbs.clone(), self.endpoint_pair.public_endpoint, self.endpoint_pair.public_endpoint, id.to_owned());
        let (socket, key_store) = (self.socket.clone(), self.key_store.clone());
        tokio::spawn(async move {
            sleep(Duration::from_millis(ttl)).await;
            if let Some(mut message) = early_return_message {
                Self::send_static(&socket, &key_store, dest, sender, &mut message, false, false).ok();
            }
            else {
                // gateway::log_debug("Ttl for breadcrumb expired");
                breadcrumbs_clone.lock().unwrap().remove(&id);
            }
        });
    }

    fn get_dest(&self, id: &str) -> Option<SocketAddrV4> {
        self.breadcrumbs.lock().unwrap().get(id).cloned()
    }

    pub fn send_request(&self, request: &mut(impl Message + Serialize), dests: Option<HashSet<SocketAddrV4>>, to_be_chunked: bool) -> EmptyResult {
        let (peers, to_be_encrypted) = if let Some(dests) = dests { (dests, true) } else { (self.peer_ops.as_ref().unwrap().lock().unwrap().peers().into_iter().map(|p| p.public_endpoint).collect(), false) };
        for peer in peers {
            self.send(peer, request, to_be_encrypted, to_be_chunked)?;
        }
        Ok(())
    }

    fn add_new_peers(&self, peers: Vec<EndpointPair>) {
        for peer in peers {
            self.peer_ops.as_ref().unwrap().lock().unwrap().add_peer(peer, DiscoverPeerProcessor::get_score(self.endpoint_pair.public_endpoint, peer.public_endpoint));
        }
    }

    pub fn send(&self, dest: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) -> EmptyResult {
        Self::send_static(&self.socket, &self.key_store, dest, self.endpoint_pair.public_endpoint, message, to_be_encrypted, to_be_chunked)
    }

    pub fn send_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) -> EmptyResult {
        message.replace_dest_and_timestamp(dest);
        if message.check_expiry() {
            println!("PeerOps: Message expired: {}", message.id());
            return Ok(())
        }
        let serialized = bincode::serialize(message).map_err(|e| e.to_string())?;
        let separate_parts = SeparateParts::new(sender, message.id().clone());
        for chunk in Self::chunked(key_store, dest, serialized, separate_parts, to_be_encrypted, to_be_chunked)? {
            let socket_clone = socket.clone();
            tokio::spawn(async move { if let Err(e) = socket_clone.send_to(&chunk, dest).await { println!("Message processor send error: {}", e.to_string()) } });
        }
        Ok(())
    }

    pub fn send_nat_heartbeats(&self, mut rx: oneshot::Receiver<()>, dest: SocketAddrV4) {
        let (socket, key_store, sender) = (self.socket.clone(), self.key_store.clone(), self.endpoint_pair.public_endpoint);
        tokio::spawn(async move {
            while let Err(_) = rx.try_recv() {
                let mut message = Heartbeat::new();
                MessageProcessor::send_static(&socket, &key_store, dest, sender, &mut message, false, true).ok();
                sleep(Duration::from_secs(29)).await;
            }
        });
    }

    fn chunked(key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, bytes: Vec<u8>, separate_parts: SeparateParts, to_be_encrypted: bool, to_be_chunked: bool) -> Result<Vec<Vec<u8>>, String> {
        let base_is_encrypted = if to_be_encrypted { IsEncrypted::True([0u8; aead::NONCE_LEN].to_vec()) } else { IsEncrypted::False };
        let chunk_size = if to_be_chunked { 975 - (bincode::serialized_size(&base_is_encrypted).unwrap() + bincode::serialized_size(&separate_parts).unwrap()) as usize } else { bytes.len() };
        let chunks = bytes.chunks(chunk_size);
        let num_chunks = chunks.len();
        let (mut messages, errors): (Vec<(Vec<u8>, String)>, Vec<(Vec<u8>, String)>) = chunks
            .enumerate()
            .map(|(i, chunk)| Self::generate_inbound_message_bytes(key_store.clone(), dest, chunk.to_vec(), separate_parts.clone(), (i, num_chunks), to_be_encrypted))
            .map(|r| { match r { Ok(bytes) => (bytes, String::new()), Err(e) => (Vec::new(), e) } })
            .partition(|r| r.0.len() > 0);
        if errors.len() > 0 {
            return Err(errors.into_iter().map(|e| e.1).collect::<Vec<String>>().join(", "));
        }
        messages.shuffle(&mut SmallRng::from_entropy());
        Ok(messages.into_iter().map(|o| o.0).collect())
    }

    fn generate_inbound_message_bytes(key_store: Arc<Mutex<KeyStore>>, dest: SocketAddrV4, mut chunk: Vec<u8>, separate_parts: SeparateParts, position: (usize, usize), to_be_encrypted: bool) -> Result<Vec<u8>, String> {
        let is_encrypted = if to_be_encrypted {
            let nonce = key_store.lock().unwrap().transform(dest, &mut chunk, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
            IsEncrypted::True(nonce)
        }
        else {
            IsEncrypted::False
        };
        bincode::serialize(&InboundMessage::new(chunk, is_encrypted, separate_parts.set_position(position))).map_err(|e| e.to_string())
    }
}


