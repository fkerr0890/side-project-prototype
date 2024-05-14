use std::{collections::HashMap, net::{SocketAddr, SocketAddrV4}, sync::Arc, time::Duration};

use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{error, info};

use crate::{crypto::{Direction, KeyStore}, message::{InboundMessage, KeyAgreementMessage, Message, MessageDirectionAgreement, NumId, Peer, Sender, SeparateParts}, node::EndpointPair, result_early_return};

pub use self::discover::DiscoverPeerProcessor;

pub const SEARCH_TIMEOUT_SECONDS: Duration = Duration::from_secs(30);
pub const DPP_TTL_MILLIS: Duration = Duration::from_millis(1000);
pub const SRP_TTL_SECONDS: Duration = Duration::from_secs(30);
pub const ACTIVE_SESSION_TTL_SECONDS: Duration = Duration::from_secs(600);
pub const HEARTBEAT_INTERVAL_SECONDS: Duration = Duration::from_secs(10);
pub const DISTRIBUTION_TTL_SECONDS: Duration = Duration::from_secs(43200);

pub mod stage;
pub mod stream;
pub mod search;
pub mod discover;
pub mod distribute;

pub type EmptyOption = Option<()>;

pub struct InboundGateway {
    socket: Arc<UdpSocket>,
    to_staging: mpsc::UnboundedSender<(SocketAddrV4, (usize, [u8; 1024]))>
}

impl InboundGateway {
    pub fn new(
        socket: &Arc<UdpSocket>,
        to_staging: mpsc::UnboundedSender<(SocketAddrV4, (usize, [u8; 1024]))>) -> Self 
    {
        Self {
            socket: socket.clone(),
            to_staging
        }
    }

    pub async fn receive(&mut self) {
        let mut buf = [0u8; 1024];
        let (n, addr) = result_early_return!(self.socket.recv_from(&mut buf).await);
        self.handle_message((n, buf), addr);
    }

    // #[instrument(level = "trace", skip(self, message_bytes))]
    fn handle_message(&self, message_bytes: (usize, [u8; 1024]), addr: SocketAddr) {
        if let SocketAddr::V4(socket) = addr {
            result_early_return!(self.to_staging.send((socket, message_bytes))); return;
        }
        panic!("Not v4 oh no");
    }
}

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error, file, line)
}

pub struct OutboundGateway {
    socket: Arc<UdpSocket>,
    to_staging: mpsc::UnboundedSender<(SocketAddrV4, (usize, [u8; 1024]))>,
    myself: Peer
}

impl OutboundGateway {
    pub fn new(socket: Arc<UdpSocket>, to_staging: mpsc::UnboundedSender<(SocketAddrV4, (usize, [u8; 1024]))>, myself: Peer) -> Self {
        Self {
            socket,
            to_staging,
            myself
        }
    }

    pub async fn send(&self, message: &Message, to_be_chunked: bool, key_store: &mut KeyStore) {
        let dest = message.dest();
        // if dest.endpoint_pair.private_endpoint != EndpointPair::default_socket() {
        //     self.send_individual(Sender::new(dest.endpoint_pair.private_endpoint, dest.id), message, to_be_chunked, key_store).await;
        // }
        if dest.endpoint_pair.public_endpoint != EndpointPair::default_socket() {
            self.send_individual(Sender::new(dest.endpoint_pair.public_endpoint, dest.id), message, to_be_chunked, key_store).await;
        }
    }

    pub async fn send_individual(&self, dest: Sender, message: &Message, to_be_chunked: bool, key_store: &mut KeyStore) {
        if message.check_expiry() {
            info!("Message expired: {}", message.id());
        }
        let serialized = result_early_return!(bincode::serialize(&message));
        let sender = if dest.socket.ip().is_private() { self.myself.endpoint_pair.private_endpoint } else { self.myself.endpoint_pair.public_endpoint };
        let separate_parts = SeparateParts::new(Sender::new(sender, self.myself.id), message.id());
        let chunk_size = if to_be_chunked { 800 - bincode::serialized_size(&separate_parts).unwrap() as usize } else { serialized.len() };
        //TODO: Maybe use chunks_exact
        let chunks = serialized.chunks(chunk_size);
        let num_chunks = chunks.len();
        let chunks = chunks
            .enumerate()
            .map(|(i, chunk)| Self::generate_inbound_message_bytes(key_store, dest, chunk, separate_parts.clone(), (i, num_chunks)))
            .filter_map(|r| r.map_err(|e| error!(e)).ok());
        for chunk in chunks {
            self.transport(dest.id, dest.socket, chunk).await;
        }
    }

    pub async fn send_agreement(&self, dest: Peer, public_key: Vec<u8>, direction: MessageDirectionAgreement) {
        let serialized = result_early_return!(bincode::serialize(&KeyAgreementMessage::new(public_key, self.myself.id, direction)));
        // if dest.endpoint_pair.private_endpoint != EndpointPair::default_socket() {
        //     result_early_return!(self.socket.send_to(&serialized, dest.endpoint_pair.private_endpoint).await);
        // }
        if dest.endpoint_pair.public_endpoint != EndpointPair::default_socket() {
            self.transport(dest.id, dest.endpoint_pair.public_endpoint, serialized).await;
        }
    }

    fn generate_inbound_message_bytes(key_store: &mut KeyStore, dest: Sender, chunk: &[u8], separate_parts: SeparateParts, position: (usize, usize)) -> Result<Vec<u8>, String> {
        let my_peer_id = separate_parts.sender().id.0;
        let mut bytes = bincode::serialize(&InboundMessage::new(chunk.to_vec(), separate_parts.set_position(position))).map_err(|e| e.to_string())?;
        let nonce = key_store.transform(dest.id, &mut bytes, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
        bytes.extend(my_peer_id.to_be_bytes());
        bytes.extend(nonce);
        Ok(bytes)
    }

    async fn transport(&self, dest_id: NumId, dest: SocketAddrV4, bytes: Vec<u8>) {
        if dest_id == self.myself.id {
            self.to_staging.send((self.myself.endpoint_pair.public_endpoint, chunk_to_array(bytes))).unwrap();
        }
        else {
            result_early_return!(self.socket.send_to(&bytes, dest).await);
        }
    }
}

fn chunk_to_array(mut chunk: Vec<u8>) -> (usize, [u8; 1024]) {
    let prev_len = chunk.len();
    for _ in 0..(1024 - chunk.len()) {
        chunk.push(0);
    }
    (prev_len, chunk.try_into().unwrap())
}

pub struct BreadcrumbService {
    breadcrumbs: HashMap<NumId, Option<Sender>>,
}

impl BreadcrumbService {
    pub fn new() -> Self { Self { breadcrumbs: HashMap::new() } }

    pub fn try_add_breadcrumb(&mut self, id: NumId, dest: Option<Sender>) -> bool {
        if !self.breadcrumbs.contains_key(&id) {
            self.breadcrumbs.insert(id, dest);
            return true;
        }
        false
    }

    pub fn get_dest(&self, id: &NumId) -> Option<Option<Sender>> {
        self.breadcrumbs.get(id).copied()
    }

    pub fn remove_breadcrumb(&mut self, id: &NumId) {
        self.breadcrumbs.remove(id);
    }
}