use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddrV4, sync::{Arc, Mutex}, time::Duration};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use ring::aead;
use serde::Serialize;
use tokio::{sync::{oneshot, mpsc}, time::sleep, net::UdpSocket};

use crate::{crypto::{Direction, Error, KeyStore}, gateway::EmptyResult, http::{self, SerdeHttpResponse}, message::{DiscoverPeerMessage, DpMessageKind, Heartbeat, InboundMessage, IsEncrypted, Message, SearchMessage, SearchMessageKind, StreamMessage, StreamMessageKind, SeparateParts}, node::EndpointPair, peer::PeerOps};

pub static SEARCH_TIMEOUT: i64 = 30;
pub static TTL: u64 = 200;
const SRP_TTL: u64 = 30000;

pub fn send_error_response<T>(send_error: mpsc::error::SendError<T>, file: &str, line: u32) -> String {
    format!("{} {} {}", send_error.to_string(), file, line)
}

pub struct MessageStaging {
    from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
    to_srp: mpsc::UnboundedSender<SearchMessage>,
    to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    key_store: Arc<Mutex<KeyStore>>,
    message_staging: HashMap<String, HashMap<usize, StagedMessage>>,
    cached_messages: HashMap<String, Vec<StagedMessage>>,
    staging_ttls: HashMap<String, oneshot::Receiver<()>>,
    endpoint_pair: EndpointPair
}

impl MessageStaging {
    pub fn new(
        from_gateway: mpsc::UnboundedReceiver<(SocketAddrV4, InboundMessage)>,
        to_srp: mpsc::UnboundedSender<SearchMessage>,
        to_dpp: mpsc::UnboundedSender<DiscoverPeerMessage>,
        to_smp: mpsc::UnboundedSender<StreamMessage>,
        key_store: &Arc<Mutex<KeyStore>>,
        endpoint_pair: EndpointPair) -> Self
    {
        Self {
            from_gateway,
            to_srp,
            to_dpp,
            to_smp,
            key_store: key_store.clone(),
            message_staging: HashMap::new(),
            cached_messages: HashMap::new(),
            staging_ttls: HashMap::new(),
            endpoint_pair,
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let (peer_addr, inbound_message) = self.from_gateway.recv().await.ok_or("MessageStaging: failed to receive message from gateway")?;
        let (payload, is_encrypted, separate_parts) = inbound_message.into_parts();
        let res = self.stage_message(payload, is_encrypted, separate_parts);
        if let Some(message_parts) = res {
            return self.reassemble_message(message_parts);
        }
        Ok(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<StagedMessage>) -> EmptyResult {
        let (message_bytes, senders) = StagedMessage::reassemble_message(message_parts);
        self.deserialize_message(&message_bytes, false, senders)
    }

    fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let sender = message.only_sender();
        let (uuid, peer_public_key) = message.into_uuid_payload();
        self.key_store.lock().unwrap().agree(sender, peer_public_key).map_err(|e| e.error_response(file!(), line!()))?;
        if let Some(cached_messages) = self.cached_messages.remove(&uuid) {
            let mut res = None;
            for message in cached_messages {
                let (payload, is_encrypted, sender, position) = message.into_parts();
                res = self.stage_message(payload, is_encrypted, SeparateParts::new(sender, uuid.clone()).set_position(position));
            }
            if let Some(message_parts) = res {
                return self.reassemble_message(message_parts)
            }
        }
        Ok(())
    }

    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool, mut senders: HashSet<SocketAddrV4>) -> EmptyResult {
        if let Ok(mut message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            println!("Received search message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string());
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // println!("Received dp message, uuid: {} at {}, {:?}", message.id(), self.endpoint_pair.public_endpoint.to_string(), message.kind);
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(mut message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            if Heartbeat::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            message.set_sender(senders.drain().next().unwrap());
            Ok(println!("{:?}", message))
        }
        else if let Ok(mut message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            // if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            for sender in senders {
                message.set_sender(sender);
            }
            match message {
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => { println!("Received key agreement message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.handle_key_agreement(message) },
                _ => { println!("Received stream message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string()); self.to_smp.send(message).map_err(|e| { e.to_string() } ) }
            }
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
        }
    }

    fn stage_message(&mut self, mut payload: Vec<u8>, is_encrypted: IsEncrypted, separate_parts: SeparateParts) -> Option<Vec<StagedMessage>> {
        let (sender, uuid, position) = separate_parts.into_parts();
        if let IsEncrypted::True(nonce) = is_encrypted {
            let crypto_result = self.key_store.lock().unwrap().transform(sender, &mut payload, Direction::Decode(nonce.clone()));
            match crypto_result {
                Ok(plaintext) => { payload = plaintext },
                Err(Error::NoKey) => {
                    let cached_messages = self.cached_messages.entry(uuid).or_default();
                    cached_messages.push(StagedMessage::new(payload, IsEncrypted::True(nonce), sender, position));
                    println!("no key");
                    return None;
                },
                Err(e) => { println!("{e}"); return None }
            };
        }
        let (index, num_chunks) = position;
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![StagedMessage::new(payload, IsEncrypted::False, sender, position)])
        }
        else {
            let staged_messages_len = {
                let staged_messages= self.message_staging.entry(uuid.clone()).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, StagedMessage::new(payload, IsEncrypted::False, sender, position));
                staged_messages.len()
            };
            if staged_messages_len == 1 {
                self.set_staging_ttl(&uuid, SRP_TTL);
            }
            if !self.check_staging_ttl(&uuid) {
                self.message_staging.remove(&uuid);
                return None;
            }
            if staged_messages_len == num_chunks {
                // gateway::log_debug("Collected all messages");
                let messages: Vec<StagedMessage> = self.message_staging.remove(&uuid).unwrap().into_values().collect();
                return Some(messages);
            }
            None
        }
    }

    fn set_staging_ttl(&mut self, id: &str, ttl_secs: u64) {
        let (tx, rx) = oneshot::channel();
        self.staging_ttls.insert(id.to_owned(), rx);
        tokio::spawn(async move {
            sleep(Duration::from_millis(ttl_secs)).await;
            tx.send(()).ok()
        });
    }

    fn check_staging_ttl(&mut self, id: &str) -> bool {
        let Some(rx) = self.staging_ttls.get_mut(id) else { return false };
        if let Ok(_) = rx.try_recv() {
            // gateway::log_debug("Ttl for message staging expired");
            self.staging_ttls.remove(id);
            false
        }
        else {
            true
        }
    }
}

pub struct StagedMessage {
    payload: Vec<u8>,
    is_encrypted: IsEncrypted,
    sender: SocketAddrV4,
    position: (usize, usize)
}

impl StagedMessage {
    pub fn new(payload: Vec<u8>, is_encrypted: IsEncrypted, sender: SocketAddrV4, position: (usize, usize)) -> Self { Self { payload, is_encrypted, sender, position } }
    pub fn into_parts(self) -> (Vec<u8>, IsEncrypted, SocketAddrV4, (usize, usize)) { (self.payload, self.is_encrypted, self.sender, self.position) }

    pub fn reassemble_message(mut messages: Vec<Self>) -> (Vec<u8>, HashSet<SocketAddrV4>) {
        messages.sort_by(|a, b| a.position.0.cmp(&b.position.0));
        let (bytes, senders): (Vec<Vec<u8>>, Vec<SocketAddrV4>) = messages
            .into_iter()
            .map(|m| { let parts = m.into_parts(); (parts.0, parts.2) })
            .unzip();
        (bytes.concat(), HashSet::from_iter(senders.into_iter()))
    }
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
        for chunk in Self::chunked(key_store, dest, serialized, separate_parts, to_be_encrypted)? {
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

    fn chunked(key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, bytes: Vec<u8>, separate_parts: SeparateParts, to_be_encrypted: bool) -> Result<Vec<Vec<u8>>, String> {
        let base_is_encrypted = if to_be_encrypted { IsEncrypted::True([0u8; aead::NONCE_LEN].to_vec()) } else { IsEncrypted::False };
        let chunks = bytes.chunks(975 - (bincode::serialized_size(&base_is_encrypted).unwrap() + bincode::serialized_size(&separate_parts).unwrap()) as usize);
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

pub struct SearchRequestProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    active_sessions: HashSet<String>
}

impl SearchRequestProcessor {
    pub fn new(message_processor: MessageProcessor, from_staging: mpsc::UnboundedReceiver<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            message_processor,
            from_staging,
            to_smp,
            local_hosts,
            active_sessions: HashSet::new()
        }
    }

    pub fn handle_search_request(&mut self, mut search_request: SearchMessage) -> EmptyResult {
        if !self.message_processor.try_add_breadcrumb(search_request.id(), search_request.sender()) {
            return Ok(())
        }
        else {
            self.message_processor.set_breadcrumb_ttl(None, search_request.id(), SRP_TTL);
        }
        if self.local_hosts.contains_key(search_request.host_name()) {
            println!("Found host {} at {}, uuid: {}", search_request.host_name(), self.message_processor.endpoint_pair.public_endpoint.port(), search_request.id());
            let (dest, origin) = (search_request.sender(), search_request.origin());
            let (uuid, host_name) = search_request.into_uuid_host_name();
            return self.return_search_responses(self.construct_search_response(uuid, dest, origin, host_name))
        }
        self.message_processor.send_request(&mut search_request, None, true)
    }

    fn return_search_responses(&mut self, mut search_response: SearchMessage) -> EmptyResult {
        let Some(dest) = self.message_processor.get_dest(search_response.id()) else { return Ok(()) };
        if dest == EndpointPair::default_socket() {
            let origin = search_response.origin();
            let (uuid, host_name, peer_public_key) = search_response.into_uuid_host_name_public_key();
            let mut key_store = self.message_processor.key_store.lock().unwrap();
            let my_public_key = key_store.requester_public_key(origin);
            key_store.agree(origin, peer_public_key).unwrap();
            let mut key_agreement_message = StreamMessage::new(host_name, uuid, StreamMessageKind::KeyAgreement, my_public_key.as_ref().to_vec());
            key_agreement_message.replace_dest_and_timestamp(origin);
            self.to_smp
                .send(key_agreement_message)
                .map_err(|e| send_error_response(e, file!(), line!()))
        }
        else {
            self.message_processor.send(dest, &mut search_response, false, false)
        }
    }
    
    fn construct_search_response(&self, uuid: String, dest: SocketAddrV4, origin: SocketAddrV4, host_name: String) -> SearchMessage {
        let (tx, rx) = oneshot::channel();
        self.message_processor.send_nat_heartbeats(rx, origin);
        let public_key = self.message_processor.key_store.lock().unwrap().host_public_key(origin, tx);
        SearchMessage::key_response(dest, self.message_processor.endpoint_pair.public_endpoint, self.message_processor.endpoint_pair.public_endpoint, uuid, host_name, public_key.as_ref().to_vec())
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let mut message = self.from_staging.recv().await.ok_or("SearchRequestProcessor: failed to receive message from gateway")?;
        if message.origin() == EndpointPair::default_socket() {
            if self.active_sessions.contains(message.host_name()) {
                println!("SearchMessageProcessor: Blocked search request for {}, reason: active session exists, {:?}", message.host_name(), message);
                return Ok(());
            }
            self.active_sessions.insert(message.host_name().to_owned());
            message.set_origin(self.message_processor.endpoint_pair.public_endpoint)
        }
        match message {
            SearchMessage { kind: SearchMessageKind::Request, ..} => self.handle_search_request(message),
            SearchMessage { kind: SearchMessageKind::Response(_), .. } => self.return_search_responses(message)
        }
    }
}

pub struct DiscoverPeerProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>,
    message_staging: Arc<Mutex<HashMap<String, Option<DiscoverPeerMessage>>>>
}

impl DiscoverPeerProcessor {
    pub fn new(message_processor: MessageProcessor, from_staging: mpsc::UnboundedReceiver<DiscoverPeerMessage>) -> Self {
        Self {
            message_processor,
            from_staging,
            message_staging: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let message = self.from_staging.recv().await.ok_or("DiscoverPeerProcessor: failed to receive message from gateway")?;
        match message {
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message),
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propogate_request(message),
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message),
            DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } => self.add_new_peers(message),
        }
    }

    fn propogate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
        request = request.set_origin_if_unset(self.message_processor.endpoint_pair.public_endpoint);
        let (sender, origin) = (request.sender(), request.origin());
        if self.message_processor.try_add_breadcrumb(request.id(), request.sender()) {
            let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
                request.origin(),
                request.id().to_owned(),
            request.hop_count());
            hairpin_response.try_decrement_hop_count();
            if !request.try_decrement_hop_count() {
                // gateway::log_debug("At hairpin");
                self.return_response(hairpin_response)?;
                return Ok(())
            }
            self.message_processor.set_breadcrumb_ttl(Some(hairpin_response), request.id(), TTL);
            // gateway::log_debug("Propogating request");
            self.message_processor.send_request(&mut request, None, false)?;
        }
        let endpoint_pair = if sender != EndpointPair::default_socket() {
            EndpointPair::new(sender, sender)
        } else {
            EndpointPair::new(origin, origin)
        };        
        self.message_processor.add_new_peers(vec![endpoint_pair]);
        Ok(())
    }

    fn get_score(first: SocketAddrV4, second: SocketAddrV4) -> i32 {
        (second.port() as i32).abs_diff(first.port() as i32) as i32
    }

    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        let mut message_staging = self.message_staging.lock().unwrap();
        let entry = message_staging.get(message.id());

        let staged_peers_len = if let Some(staged_message) = entry {
            if let Some(staged_message) = staged_message {
                staged_message.peer_list().len()
            }
            else {
                return false;
            }
        }
        else {
            let message_staging_clone = self.message_staging.clone();
            let (socket, key_store, uuid, dest, sender) = (self.message_processor.socket.clone(), self.message_processor.key_store.clone(), message.id().to_owned(), self.message_processor.get_dest(message.id()).unwrap(), self.message_processor.endpoint_pair.public_endpoint);
            tokio::spawn(async move {
                sleep(Duration::from_millis(TTL*2)).await;
                Self::send_final_response_static(&socket, &key_store, dest, sender, &message_staging_clone, &uuid);
                message_staging_clone.lock().unwrap().remove(&uuid);
            });
            0
        };

        let target_num_peers = message.hop_count().1;
        let peers_len = message.peer_list().len();
        println!("hop count: {:?}, peer list len: {}", message.hop_count(), peers_len);
        if peers_len > staged_peers_len {
            message_staging.insert(message.id().to_owned(), Some(message));
        }
        peers_len == target_num_peers as usize
    }

    fn return_response(&mut self, mut response: DiscoverPeerMessage) -> EmptyResult {
        response.add_peer(self.message_processor.endpoint_pair);
        let Some(dest) = self.message_processor.get_dest(response.id()) else { return Ok(()) };
        if response.origin() == self.message_processor.endpoint_pair.public_endpoint {
            let uuid = response.id().to_owned();
            if self.stage_message(response) {
                self.send_final_response(&uuid);
            }
            Ok(())
        }
        else {
            self.message_processor.send(dest, &mut response, false, false)
        }
    }

    fn send_final_response(&self, uuid: &str) {
        let dest = self.message_processor.get_dest(uuid).unwrap();
        Self::send_final_response_static(&self.message_processor.socket, &self.message_processor.key_store, dest, self.message_processor.endpoint_pair.public_endpoint, &self.message_staging, uuid);
    }

    fn send_final_response_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message_staging: &Arc<Mutex<HashMap<String, Option<DiscoverPeerMessage>>>>, uuid: &str) {
        let staged_message = {
            let mut message_staging = message_staging.lock().unwrap();
            let Some(staged_message) = message_staging.get_mut(uuid) else { return };
            staged_message.take()
        };
        if let Some(staged_message) = staged_message {
            // gateway::log_debug("I'm back at the introducer");
            MessageProcessor::send_static(socket, key_store, dest, sender, &mut staged_message.set_kind(DpMessageKind::IveGotSome), false, false).ok();
        }
    }

    fn request_new_peers(&self, mut message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug(&format!("Got INeedSome, introducer = {}", message.peer_list()[0].public_endpoint));
        let introducer = message.get_last_peer();
        self.message_processor.send(introducer.public_endpoint, 
            &mut message
                .set_kind(DpMessageKind::Request),
            false,
            false)
    }

    fn add_new_peers(&self, message: DiscoverPeerMessage) -> EmptyResult {
        self.message_processor.add_new_peers(message.into_peer_list());
        Ok(())
    }
}

pub struct StreamMessageProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>,
    active_sessions: Arc<Mutex<HashMap<String, ActiveSessionInfo>>>
}
impl StreamMessageProcessor {
    pub fn new(message_processor: MessageProcessor,from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self {
        Self
        {
            message_processor,
            from_staging,
            local_hosts,
            to_http_handler,
            active_sessions: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.from_staging.recv().await.ok_or("StreamMessageProcessor: failed to receive message from gateway")?;
        match message {
            StreamMessage { kind: StreamMessageKind::KeyAgreement, ..} => self.handle_key_agreement(message),
            StreamMessage { kind: StreamMessageKind::Request, .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Response, ..} => self.handle_response(message)
        }
    }

    fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let dest = message.dest();
        let mut active_sessions = self.active_sessions.lock().unwrap();
        let active_session = active_sessions.get_mut(message.host_name()).unwrap();
        match active_session.get_resource_mut(message.id(), dest) {
            Ok(cached_message) => {
                self.message_processor.send(dest, &mut message, false, false)?;
                self.message_processor.send(dest, cached_message, true, true)
            }
            Err(e) => { println!("{e}"); Ok(()) }
        }
    }

    fn send_follow_ups(&self, host_name: String) {
        let (socket, key_store, sender, active_sessions) = (self.message_processor.socket.clone(), self.message_processor.key_store.clone(), self.message_processor.endpoint_pair.public_endpoint, self.active_sessions.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let active_session = active_sessions.get_mut(&host_name).unwrap();
                let dests = active_session.dests();
                let requests = active_session.requests();
                if requests.len() == 0 {
                    return;
                }
                for message in requests {
                    if let StreamMessageKind::Request = message.kind {                    
                        for dest in dests.iter() {
                            println!("Sending follow up for {} to {}", message.id(), dest);
                            MessageProcessor::send_static(&socket, &key_store, *dest, sender, message, true, true).ok();
                        }
                    }
                }
            }
        });
    }

    async fn handle_request(&mut self, mut message: StreamMessage) -> EmptyResult {
        let (dest, uuid, host_name) = (message.only_sender(), message.id().to_owned(), message.host_name().to_owned());
        if dest == EndpointPair::default_socket() {
            let mut active_sessions = self.active_sessions.lock().unwrap();
            let active_session_info = active_sessions.entry(host_name.clone()).or_default();
            let dests = active_session_info.dests();
            if dests.len() > 0 {
                self.message_processor.send_request(&mut message, Some(dests), true)?;
            }
            println!("Pushed: {}", message.id());
            active_session_info.push_resource(message)?;
            Ok(self.send_follow_ups(host_name))
        }
        else {
            self.send_response(message.payload(), dest, host_name, uuid).await
        }
    }

    async fn send_response(&self, payload: &[u8], dest: SocketAddrV4, host_name: String, uuid: String) -> EmptyResult {
        let Ok(request) = bincode::deserialize(payload) else { return Ok(()) };
        let socket = self.local_hosts.get(&host_name).unwrap();
        let response = http::make_request(request, &socket.to_string()).await;
        let Ok(response_bytes) = bincode::serialize(&response) else { return Ok(()) };
        let mut response = StreamMessage::new(
            host_name,
            uuid,
            StreamMessageKind::Response,
            response_bytes);
        self.message_processor.send(dest, &mut response, true, true)
    }

    fn handle_response(&mut self, message: StreamMessage) -> EmptyResult {
        let mut active_sessions = self.active_sessions.lock().unwrap();
        let Some(active_session_info) = active_sessions.get_mut(message.host_name()) else { return Ok(()) };
        let cached_messages = match active_session_info.pop_resource(message) { Ok(message) => message, Err(e) => { println!("{e}"); return Ok(()) } };
        for message in cached_messages {
            println!("Popped: {}", message.id());
            self.return_resource(message.payload())?;
        }
        Ok(())
    }

    fn return_resource(&self, payload: &[u8]) -> EmptyResult {
        let response = bincode::deserialize(payload).unwrap_or_else(|e| http::construct_error_response((*e).to_string(), String::from("HTTP/1.1")));
        self.to_http_handler.send(response).map_err(|e| send_error_response(e, file!(), line!()))
    }
}

struct ActiveSessionInfo {
    dests: HashSet<SocketAddrV4>,
    cached_messages: HashMap<String, StreamMessage>,
    resource_queue: VecDeque<String>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), cached_messages: HashMap::new(), resource_queue: VecDeque::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn requests(&mut self) -> Vec<&mut StreamMessage> { self.cached_messages.values_mut().collect() }
    fn get_resource_mut(&mut self, uuid: &str, dest: SocketAddrV4) -> Result<&mut StreamMessage, String> {
        let Some(cached_message) = self.cached_messages.get_mut(uuid) else {
            return Err(format!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: request expired for resource {}", dest, uuid));
        };
        self.dests.insert(dest);
        if let StreamMessageKind::Request = cached_message.kind {
            return Ok(cached_message)
        }
        Err(format!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: already received resource {}", dest, uuid))
    }
    fn push_resource(&mut self, message: StreamMessage) -> EmptyResult {
        if self.cached_messages.contains_key(message.id()) {
            return Err(String::from("ActiveSessionInfo: Attempted to insert duplicate request"));
        }
        self.resource_queue.push_back(message.id().to_owned());
        self.cached_messages.insert(message.id().to_owned(), message);
        Ok(())
    }
    fn pop_resource(&mut self, message: StreamMessage) -> Result<Vec<StreamMessage>, String> {
        let Some(cached_message) = self.cached_messages.get_mut(message.id()) else {
            return Err(format!("StreamMessageProcessor: blocked response from host {:?}, reason: unsolicited response for resource {}", message.senders(), message.id()))
        };
        *cached_message = message;
        let mut popped_resources = Vec::new();
        while let Some(uuid) = self.resource_queue.front() {
            if let StreamMessageKind::Response = self.cached_messages.get(uuid).unwrap().kind {
                popped_resources.push(self.cached_messages.remove(&self.resource_queue.pop_front().unwrap()).unwrap())
            }
            else {
                break;
            }
        }
        Ok(popped_resources)
    }
}

impl Default for ActiveSessionInfo {
    fn default() -> Self {
        Self::new()
    }
}
