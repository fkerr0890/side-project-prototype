use std::{net::{SocketAddr, SocketAddrV4}, sync::{Arc, Mutex}};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use serde::Serialize;
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{error, info, instrument};

use crate::{crypto::{Direction, KeyStore}, lock, message::{DiscoverPeerMessage, InboundMessage, Message, NumId, Peer, Sender, SeparateParts}, option_early_return, peer::PeerOps, result_early_return, utils::{ArcMap, TransientCollection, TtlType}};

pub use self::discover::DiscoverPeerProcessor;

pub const SEARCH_TIMEOUT_SECONDS: i64 = 30;
pub const DPP_TTL_MILLIS: u64 = 250;
pub const SRP_TTL_SECONDS: u64 = 30;
pub const ACTIVE_SESSION_TTL_SECONDS: u64 = 600;
pub const HEARTBEAT_INTERVAL_SECONDS: u64 = 10;
pub const DISTRIBUTION_TTL_SECONDS: u64 = 43200;

pub mod stage;
pub mod stream;
pub mod search;
pub mod discover;
pub mod distribute;

pub type EmptyOption = Option<()>;

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_staging: mpsc::UnboundedSender<(SocketAddrV4, Vec<u8>)>
}

impl InboundGateway {
    pub fn new(
        socket: &Arc<UdpSocket>,
        to_staging: mpsc::UnboundedSender<(SocketAddrV4, Vec<u8>)>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_staging
        }
    }

    pub async fn receive(&mut self) {
        let mut buf = Vec::with_capacity(1024);
        let (n, addr) = result_early_return!(self.socket.recv_from(&mut buf).await);
        buf.truncate(n);
        self.handle_message(buf, addr);
    }

    // #[instrument(level = "trace", skip(self, message_bytes))]
    fn handle_message(&self, message_bytes: Vec<u8>, addr: SocketAddr) {
        if let SocketAddr::V4(socket) = addr {
            result_early_return!(self.to_staging.send((socket, message_bytes))); return;
        }
        panic!("Not v4 oh no");
    }
}

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error, file, line)
}

type MessagesErrors = (Vec<(Vec<u8>, String)>, Vec<(Vec<u8>, String)>);

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    myself: Peer,
    key_store: Arc<Mutex<KeyStore>>,
    peer_ops: Arc<Mutex<PeerOps>>
}

impl OutboundGateway {
    pub fn new(socket: Arc<UdpSocket>, myself: Peer, key_store: &Arc<Mutex<KeyStore>>, peer_ops: Arc<Mutex<PeerOps>>) -> Self {
        Self {
            socket: socket.clone(),
            myself,
            key_store: key_store.clone(),
            peer_ops
        }
    }

    pub fn send_request(&self, request: &mut(impl Message + Serialize), prev_sender: Option<Sender>) {
        for peer in lock!(self.peer_ops).peers() {
            match prev_sender { Some(s) if s.id == peer.id => continue, _ => {}}
            self.send(peer, request, false);
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn add_new_peer(&self, peer: Peer) {
        let peer_endpoint = peer.endpoint_pair.public_endpoint;
        lock!(self.peer_ops).add_peer(peer, DiscoverPeerProcessor::get_score(self.myself.endpoint_pair.public_endpoint, peer_endpoint))
    }

    pub fn send(&self, dest: Peer, message: &mut(impl Message + Serialize), to_be_chunked: bool) {
        Self::send_private_public_static(&self.socket, dest, self.myself, message, self.key_store.clone(), to_be_chunked)
    }

    pub fn send_private_public_static(socket: &Arc<UdpSocket>, dest: Peer, myself: Peer, message: &mut(impl Message + Serialize), key_store: Arc<Mutex<KeyStore>>, to_be_chunked: bool) {
        Self::send_static(socket, Sender::new(dest.endpoint_pair.public_endpoint, dest.id), myself, message, key_store, to_be_chunked);
        // Self::send_static(socket, dest.private_endpoint, myself, message, to_be_encrypted, to_be_chunked);
    }

    pub fn send_individual(&self, dest: Sender, message: &mut(impl Message + Serialize), to_be_chunked: bool) {
        Self::send_static(&self.socket, dest, self.myself, message, self.key_store.clone(), to_be_chunked);
    }

    pub fn send_static(socket: &Arc<UdpSocket>, dest: Sender, myself: Peer, message: &mut(impl Message + Serialize), key_store: Arc<Mutex<KeyStore>>, to_be_chunked: bool) {
        message.replace_dest(dest.socket);
        if message.check_expiry() {
            info!("PeerOps: Message expired: {}", message.id());
        }
        let serialized = result_early_return!(bincode::serialize(message));
        let sender = if dest.socket.ip().is_private() { myself.endpoint_pair.private_endpoint } else { myself.endpoint_pair.public_endpoint };
        let separate_parts = SeparateParts::new(Sender::new(sender, myself.id), message.id());
        let chunks = option_early_return!(Self::chunked(dest, serialized, separate_parts, key_store, to_be_chunked));
        for chunk in chunks {
            let socket_clone = socket.clone();
            tokio::spawn(async move { result_early_return!(socket_clone.send_to(&chunk, dest.socket).await); });
        }
    }

    fn chunked(dest: Sender, bytes: Vec<u8>, separate_parts: SeparateParts, key_store: Arc<Mutex<KeyStore>>, to_be_chunked: bool) -> Option<Vec<Vec<u8>>> {
        let chunk_size = if to_be_chunked { 975 - bincode::serialized_size(&separate_parts).unwrap() as usize } else { bytes.len() };
        let chunks = bytes.chunks(chunk_size);
        let num_chunks = chunks.len();
        let (mut messages, errors): MessagesErrors = chunks
            .enumerate()
            .map(|(i, chunk)| Self::generate_inbound_message_bytes(key_store.clone(), dest, chunk.to_vec(), separate_parts.clone(), (i, num_chunks)))
            .map(|r| { match r { Ok(bytes) => (bytes, String::new()), Err(e) => (Vec::new(), e) } })
            .partition(|r| !r.0.is_empty());
        if !errors.is_empty() {
            error!("{}", errors.into_iter().map(|e| e.1).collect::<Vec<String>>().join(", ")); return None;
        }
        messages.shuffle(&mut SmallRng::from_entropy());
        Some(messages.into_iter().map(|o| o.0).collect())
    }

    fn generate_inbound_message_bytes(key_store: Arc<Mutex<KeyStore>>, dest: Sender, chunk: Vec<u8>, separate_parts: SeparateParts, position: (usize, usize)) -> Result<Vec<u8>, String> {
        let mut key_store = lock!(key_store);
        key_store.reset_expiration(dest.id);
        let my_peer_id = separate_parts.sender().id.0;
        let mut bytes = bincode::serialize(&InboundMessage::new(chunk, separate_parts.set_position(position))).map_err(|e| e.to_string())?;
        let nonce = key_store.transform(dest.id, &mut bytes, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
        bytes.extend(my_peer_id.to_be_bytes());
        bytes.extend(nonce);
        Ok(bytes)
    }
}

#[derive(Clone)]
pub enum ToBeEncrypted {
    True(Arc<Mutex<KeyStore>>),
    False
}

pub struct BreadcrumbService {
    breadcrumbs: TransientCollection<ArcMap<NumId, Option<Sender>>>,
}

impl BreadcrumbService {
    pub fn new(ttl: TtlType) -> Self { Self { breadcrumbs: TransientCollection::new(ttl, false, ArcMap::new()) } }

    pub fn clone(&self, ttl: TtlType) -> Self { Self { breadcrumbs: TransientCollection::from_existing(&self.breadcrumbs, ttl) } }

    pub fn try_add_breadcrumb(&mut self, early_return_context: Option<EarlyReturnContext>, id: NumId, dest: Option<Sender>) -> bool {
        let contains_key = if let Some(mut context) = early_return_context {
            self.breadcrumbs.set_timer_with_send_action(id, move || {
                let (ref mut message, dest, myself, ref socket, ref key_store) = context;
                OutboundGateway::send_static(socket, dest, myself, message, key_store.clone(), false);
            }, "BreadcrumbService")
        }
        else {
            self.breadcrumbs.set_timer(id, "BreadcrumbService")
        };
        if contains_key {
            lock!(self.breadcrumbs.collection().map()).insert(id, dest);
        }
        contains_key
    }

    pub fn get_dest(&self, id: &NumId) -> Option<Option<Sender>> {
        lock!(self.breadcrumbs.collection().map()).get(id).copied()
    }

    pub fn remove_breadcrumb(&self, id: &NumId) {
        lock!(self.breadcrumbs.collection().map()).remove(id);
    }
}

pub type EarlyReturnContext = (
    DiscoverPeerMessage,
    Sender,
    Peer,
    Arc<UdpSocket>,
    Arc<Mutex<KeyStore>>
);