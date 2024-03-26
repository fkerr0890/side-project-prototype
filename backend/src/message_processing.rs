use std::{net::{SocketAddr, SocketAddrV4}, sync::{Arc, Mutex}, time::Duration};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use ring::aead;
use serde::Serialize;
use tokio::{net::UdpSocket, sync::mpsc, time};
use tracing::{error, info, instrument};

use crate::{crypto::{Direction, KeyStore}, message::{DiscoverPeerMessage, NumId, InboundMessage, IsEncrypted, Message, Peer, Sender, SeparateParts}, node::EndpointPair, option_early_return, peer::PeerOps, result_early_return, utils::{TransientMap, TtlType}};

pub use self::discover::DiscoverPeerProcessor;

pub const SEARCH_TIMEOUT_SECONDS: i64 = 30;
pub const DPP_TTL_MILLIS: u64 = 250;
pub const SRP_TTL_SECONDS: u64 = 30;
pub const ACTIVE_SESSION_TTL_SECONDS: u64 = 600;
pub const HEARTBEAT_INTERVAL_SECONDS: u64 = 10;

pub mod stage;
pub mod stream;
pub mod search;
pub mod discover;
pub mod distribute;

pub type EmptyOption = Option<()>;

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

    pub async fn receive(&mut self) {
        let mut buf = [0; 1024];
        let (n, addr) = result_early_return!(self.socket.recv_from(&mut buf).await);
        self.handle_message(&buf[..n], addr);
    }

    #[instrument(level = "trace", skip(self, message_bytes))]
    fn handle_message(&self, message_bytes: &[u8], addr: SocketAddr) {
        if let SocketAddr::V4(socket) = addr {
            let message = result_early_return!(bincode::deserialize::<InboundMessage>(message_bytes));
            result_early_return!(self.to_staging.send((socket, message))); return;
        }
        panic!("Not v4 oh no");
    }
}

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error.to_string(), file, line)
}

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    myself: Peer,
    breadcrumbs: TransientMap<NumId, SocketAddrV4>,
    key_store: Arc<Mutex<KeyStore>>,
    peer_ops: Option<Arc<Mutex<PeerOps>>>
}

impl OutboundGateway {
    pub fn new(socket: Arc<UdpSocket>, myself: Peer, key_store: &Arc<Mutex<KeyStore>>, peer_ops: Option<Arc<Mutex<PeerOps>>>, ttl: TtlType) -> Self {
        Self {
            socket: socket.clone(),
            myself,
            breadcrumbs: TransientMap::new(ttl, false),
            key_store: key_store.clone(),
            peer_ops
        }
    } 

    fn try_add_breadcrumb(&mut self, early_return_message: Option<DiscoverPeerMessage>, id: NumId, dest: SocketAddrV4) -> bool {
        let endpoint_pair = self.myself.endpoint_pair;
        let (early_return_dest, myself) = (endpoint_pair.private_endpoint, self.myself);
        let (socket, breadcrumbs) = (self.socket.clone(), self.breadcrumbs.map().clone());
        let contains_key = if let Some(mut message) = early_return_message {
            self.breadcrumbs.set_timer_with_send_action(id, move || {
                Self::send_static(&socket, early_return_dest, myself, &mut message, ToBeEncrypted::False, false);
                let breadcrumbs = breadcrumbs.clone();
                tokio::spawn(async move {
                    time::sleep(Duration::from_secs(2)).await;
                    breadcrumbs.lock().unwrap().remove(&id);
                });
            })
        }
        else {
            self.breadcrumbs.set_timer(id)
        };
        if contains_key {
            self.breadcrumbs.map().lock().unwrap().insert(id, dest);
        }
        contains_key
    }

    fn get_dest(&self, id: &NumId) -> Option<SocketAddrV4> {
        self.breadcrumbs.map().lock().unwrap().get(id).cloned()
    }

    pub fn send_request(&self, request: &mut(impl Message + Serialize)) {
        let sender = self.get_dest(&request.id());
        for peer in self.peer_ops.as_ref().unwrap().lock().unwrap().peers() {
            match sender { Some(s) if s == peer.public_endpoint || s == peer.private_endpoint => continue, _ => {}}
            self.send(peer, request, false, true);
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn add_new_peer(&self, peer: Peer) {
        let peer_endpoint = peer.endpoint_pair.public_endpoint;
        self.peer_ops.as_ref().unwrap().lock().unwrap().add_peer(peer, DiscoverPeerProcessor::get_score(self.myself.endpoint_pair.public_endpoint, peer_endpoint))
    }

    pub fn send(&self, dest: EndpointPair, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) {
        let to_be_encrypted = if to_be_encrypted { ToBeEncrypted::True(self.key_store.clone()) } else { ToBeEncrypted::False };
        Self::send_private_public_static(&self.socket, dest, self.myself, message, to_be_encrypted, to_be_chunked)
    }

    pub fn send_private_public_static(socket: &Arc<UdpSocket>, dest: EndpointPair, myself: Peer, message: &mut(impl Message + Serialize), to_be_encrypted: ToBeEncrypted, to_be_chunked: bool) {
        Self::send_static(socket, dest.public_endpoint, myself, message, to_be_encrypted, to_be_chunked);
        // Self::send_static(socket, dest.private_endpoint, myself, message, to_be_encrypted, to_be_chunked);
    }

    pub fn send_individual(&self, dest: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool, to_be_chunked: bool) {
        let to_be_encrypted = if to_be_encrypted { ToBeEncrypted::True(self.key_store.clone()) } else { ToBeEncrypted::False };
        Self::send_static(&self.socket, dest, self.myself, message, to_be_encrypted, to_be_chunked);
    }

    pub fn send_static(socket: &Arc<UdpSocket>, dest: SocketAddrV4, myself: Peer, message: &mut(impl Message + Serialize), to_be_encrypted: ToBeEncrypted, to_be_chunked: bool) {
        message.replace_dest(dest);
        if message.check_expiry() {
            info!("PeerOps: Message expired: {}", message.id());
        }
        let serialized = result_early_return!(bincode::serialize(message));
        let sender = if dest.ip().is_private() { myself.endpoint_pair.private_endpoint } else { myself.endpoint_pair.public_endpoint };
        let separate_parts = SeparateParts::new(Sender::new(sender, myself.id), message.id());
        let chunks = option_early_return!(Self::chunked(dest, serialized, separate_parts, to_be_encrypted, to_be_chunked));
        for chunk in chunks {
            let socket_clone = socket.clone();
            tokio::spawn(async move { result_early_return!(socket_clone.send_to(&chunk, dest).await); });
        }
    }

    fn chunked(dest: SocketAddrV4, bytes: Vec<u8>, separate_parts: SeparateParts, to_be_encrypted: ToBeEncrypted, to_be_chunked: bool) -> Option<Vec<Vec<u8>>> {
        let (base_is_encrypted, key_store) = if let ToBeEncrypted::True(key_store) = to_be_encrypted { (IsEncrypted::True([0u8; aead::NONCE_LEN].to_vec()), Some(key_store)) } else { (IsEncrypted::False, None) };
        let chunk_size = if to_be_chunked { 975 - (bincode::serialized_size(&base_is_encrypted).unwrap() + bincode::serialized_size(&separate_parts).unwrap()) as usize } else { bytes.len() };
        let chunks = bytes.chunks(chunk_size);
        let num_chunks = chunks.len();
        let (mut messages, errors): (Vec<(Vec<u8>, String)>, Vec<(Vec<u8>, String)>) = chunks
            .enumerate()
            .map(|(i, chunk)| Self::generate_inbound_message_bytes(key_store.clone(), dest, chunk.to_vec(), separate_parts.clone(), (i, num_chunks), matches!(base_is_encrypted, IsEncrypted::True(_))))
            .map(|r| { match r { Ok(bytes) => (bytes, String::new()), Err(e) => (Vec::new(), e) } })
            .partition(|r| r.0.len() > 0);
        if errors.len() > 0 {
            error!("{}", errors.into_iter().map(|e| e.1).collect::<Vec<String>>().join(", ")); return None;
        }
        messages.shuffle(&mut SmallRng::from_entropy());
        Some(messages.into_iter().map(|o| o.0).collect())
    }

    fn generate_inbound_message_bytes(key_store: Option<Arc<Mutex<KeyStore>>>, dest: SocketAddrV4, mut chunk: Vec<u8>, separate_parts: SeparateParts, position: (usize, usize), to_be_encrypted: bool) -> Result<Vec<u8>, String> {
        let is_encrypted = if to_be_encrypted {
            let nonce = key_store.unwrap().lock().unwrap().transform(dest, &mut chunk, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
            IsEncrypted::True(nonce)
        }
        else {
            IsEncrypted::False
        };
        bincode::serialize(&InboundMessage::new(chunk, is_encrypted, separate_parts.set_position(position))).map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub enum ToBeEncrypted {
    True(Arc<Mutex<KeyStore>>),
    False
}