use std::{collections::HashSet, net::{SocketAddr, SocketAddrV4}, sync::{Arc, Mutex}};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use ring::aead;
use serde::Serialize;
use tokio::{sync::mpsc, net::UdpSocket};

use crate::{crypto::{Direction, KeyStore}, message::{DiscoverPeerMessage, Id, InboundMessage, IsEncrypted, Message, Peer, SeparateParts}, node::EndpointPair, peer::PeerOps, utils::{TransientMap, TtlType}};

pub use self::discover::DiscoverPeerProcessor;

pub const SEARCH_TIMEOUT_SECONDS: i64 = 30;
pub const DPP_TTL_MILLIS: u64 = 250;
pub const SRP_TTL_SECONDS: u64 = 30;
pub const ACTIVE_SESSION_TTL_SECONDS: u64 = 3600;
pub const HEARTBEAT_INTERVAL_SECONDS: u64 = 10;

pub mod stage;
pub mod stream;
pub mod search;
pub mod discover;

pub type EmptyResult = Result<(), String>;

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_staging: mpsc::UnboundedSender<(SocketAddrV4, InboundMessage)>
}

impl InboundGateway {
    pub fn new(
        socket: &Arc<UdpSocket>,
        to_staging: mpsc::UnboundedSender<(SocketAddrV4, InboundMessage)>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_staging
        }
    }

    pub async fn receive(&mut self)  -> EmptyResult {
        let mut buf = [0; 1024];
        match  self.socket.recv_from(&mut buf).await {
            Ok((n, addr)) => self.handle_message(&buf[..n], addr),
            Err(e) => { println!("Inbound gateway: receive error {}", e.to_string()); Ok(()) }
        }
    }

    fn handle_message(&self, message_bytes: &[u8], addr: SocketAddr) -> EmptyResult {
        if let SocketAddr::V4(socket) = addr {
            if let Ok(message) = bincode::deserialize::<InboundMessage>(message_bytes) {
                self.to_staging.send((socket, message)).map_err(|e| send_error_response(e, file!(), line!()))
            }
            else {
                Err(String::from("Unable to deserialize received message to a supported type"))
            }
        }
        else {
            panic!("Not v4 oh no");
        }
    }
}

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error.to_string(), file, line)
}

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    myself: Peer,
    breadcrumbs: TransientMap<Id, SocketAddrV4>,
    key_store: Arc<Mutex<KeyStore>>,
    peer_ops: Option<Arc<Mutex<PeerOps>>>
}

impl OutboundGateway {
    pub fn new(socket: Arc<UdpSocket>, origin: Peer, key_store: &Arc<Mutex<KeyStore>>, peer_ops: Option<Arc<Mutex<PeerOps>>>, ttl: TtlType) -> Self {
        Self {
            socket: socket.clone(),
            myself: origin,
            breadcrumbs: TransientMap::new(ttl),
            key_store: key_store.clone(),
            peer_ops
        }
    } 

    fn try_add_breadcrumb(&mut self, early_return_message: Option<DiscoverPeerMessage>, id: &Id, dest: SocketAddrV4) -> bool {
        let endpoint_pair = self.myself.endpoint_pair();
        let (early_return_dest, sender, id) = (endpoint_pair.public_endpoint, endpoint_pair.public_endpoint, id.to_owned());
        let (socket, key_store) = (self.socket.clone(), self.key_store.clone());
        let contains_key = if let Some(mut message) = early_return_message {
            self.breadcrumbs.set_timer_with_send_action(id.clone(), move || { Self::send_static(&socket, &key_store, early_return_dest, sender, &mut message, false, false).ok(); })
        }
        else {
            self.breadcrumbs.set_timer(id.clone())
        };
        if contains_key {
            self.breadcrumbs.map().lock().unwrap().insert(id, dest);
        }
        contains_key
    }

    fn get_dest(&self, id: &Id) -> Option<SocketAddrV4> {
        self.breadcrumbs.map().lock().unwrap().get(id).cloned()
    }

    pub fn send_request(&self, request: &mut(impl Message + Serialize), dests: Option<HashSet<SocketAddrV4>>, to_be_chunked: bool) -> EmptyResult {
        let (peers, to_be_encrypted) = if let Some(dests) = dests { (dests, true) } else { (self.peer_ops.as_ref().unwrap().lock().unwrap().peers().into_iter().map(|p| p.public_endpoint).collect(), false) };
        for peer in peers {
            self.send(peer, request, to_be_encrypted, to_be_chunked)?;
        }
        Ok(())
    }

    fn add_new_peer(&self, peer: EndpointPair) {
        if peer.public_endpoint != EndpointPair::default_socket() && peer.public_endpoint != self.myself.public_endpoint {
            self.peer_ops.as_ref().unwrap().lock().unwrap().add_peer(peer, DiscoverPeerProcessor::get_score(self.myself.public_endpoint, peer.public_endpoint))
        }
    }

    pub fn send(&self, dest: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) -> EmptyResult {
        Self::send_static(&self.socket, &self.key_store, dest, self.myself.public_endpoint, message, to_be_encrypted, to_be_chunked)
    }

    pub fn send_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) -> EmptyResult {
        message.replace_dest(dest);
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


