use std::{net::SocketAddrV4, collections::{HashMap, HashSet, VecDeque}, time::Duration, sync::{Arc, Mutex}};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use serde::Serialize;
use tokio::{sync::{oneshot, mpsc}, time::sleep, net::UdpSocket};

use crate::{message::{SearchMessageKind, SearchMessage, Message, DiscoverPeerMessage, DpMessageKind, StreamMessage, StreamMessageKind, InboundMessage, IsEncrypted, self, Heartbeat}, gateway::EmptyResult, peer::PeerOps, http::{SerdeHttpResponse, self}, node::EndpointPair, crypto::{KeyStore, Direction, Error}};

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
    message_staging: HashMap<String, HashMap<usize, InboundMessage>>,
    cached_messages: HashMap<String, InboundMessage>,
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
        let res = self.stage_message(inbound_message);
        if let Some(message_parts) = res {
            return self.reassemble_message(message_parts, peer_addr);
        }
        Ok(())
    }

    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>, peer_addr: SocketAddrV4) -> EmptyResult {
        let mut message = InboundMessage::reassemble_message(message_parts);
        let nonce = if let IsEncrypted::True(nonce) = message.is_encrypted() {
            nonce.clone()
        }
        else {
            return self.deserialize_message(message.payload(), false)
        };
        let crypto_result = self.key_store.lock().unwrap().transform(peer_addr, message.payload_mut(), Direction::Decode(nonce));
        match crypto_result {
            Ok(_) => self.deserialize_message(message.payload(), true),
            Err(Error::NoKey) => { self.cached_messages.insert(message.uuid().to_owned(), message); Ok(()) }
            Err(e) => Err(e.error_response(file!(), line!()))
        }
    }

    fn handle_key_agreement(&mut self, message: StreamMessage) -> EmptyResult {
        let sender = message.sender();
        let (uuid, peer_public_key) = message.into_uuid_payload();
        self.key_store.lock().unwrap().agree(sender, peer_public_key).map_err(|e| e.error_response(file!(), line!()))?;
        if let Some(cached_message) = self.cached_messages.remove(&uuid) {
            let (mut cached_message, is_encrypted) = cached_message.into_payload_is_encrypted();
            let (nonce1, nonce2) = is_encrypted.nonces();
            self.key_store.lock().unwrap().transform(sender, &mut cached_message, Direction::Decode()).map_err(|e| e.error_response(file!(), line!()))?;
            self.deserialize_message(&cached_message, true)?;
        }
        Ok(())
    }

    fn deserialize_message(&mut self, message_bytes: &[u8], was_encrypted: bool) -> EmptyResult {
        if let Ok(message) = bincode::deserialize::<SearchMessage>(message_bytes) {
            println!("Received search message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string());
            if SearchMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            self.to_srp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<DiscoverPeerMessage>(message_bytes) {
            // println!("Received dp message, uuid: {} at {}, {:?}", message.id(), self.endpoint_pair.public_endpoint.to_string(), message.kind);
            if DiscoverPeerMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            self.to_dpp.send(message).map_err(|e| { e.to_string() } )
        }
        else if let Ok(message) = bincode::deserialize::<Heartbeat>(message_bytes) {
            // Ok(println!("{:?}", message))
            Ok(())
        }
        else if let Ok(message) = bincode::deserialize::<StreamMessage>(message_bytes) {
            println!("Received stream message, uuid: {} at {}", message.id(), self.endpoint_pair.public_endpoint.to_string());
            if StreamMessage::ENCRYPTION_REQUIRED && !was_encrypted { return Ok(()) }
            match message {
                StreamMessage { kind: StreamMessageKind::KeyAgreement, .. } => self.handle_key_agreement(message),
                _ => self.to_smp.send(message).map_err(|e| { e.to_string() } )
            }
        }
        else {
            Err(String::from("Unable to deserialize received message to a supported type"))
        }
    }

    fn stage_message(&mut self, message: InboundMessage) -> Option<Vec<InboundMessage>> {
        let (index, num_chunks) = message.position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![message])
        }
        else {
            let uuid = message.uuid().to_owned();
            let staged_messages_len = {
                let staged_messages= self.message_staging.entry(uuid.clone()).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, message);
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
                let messages: Vec<InboundMessage> = self.message_staging.remove(&uuid).unwrap().into_values().collect();
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
                Self::send_static(&socket, &key_store, dest, sender, &mut message, false).await.ok();
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

    pub async fn send_request(&self, request: &mut(impl Message + Serialize), dests: Option<HashSet<SocketAddrV4>>) -> EmptyResult {
        let (peers, to_be_encrypted) = if let Some(dests) = dests { (dests, true) } else { (self.peer_ops.as_ref().unwrap().lock().unwrap().peers().into_iter().map(|p| p.public_endpoint).collect(), false) };
        for peer in peers {
            self.send(peer, request, to_be_encrypted).await?;
        }
        Ok(())
    }

    fn add_new_peers(&self, peers: Vec<EndpointPair>) {
        for peer in peers {
            self.peer_ops.as_ref().unwrap().lock().unwrap().add_peer(peer, DiscoverPeerProcessor::get_score(self.endpoint_pair.public_endpoint, peer.public_endpoint));
        }
    }

    pub async fn send(&self, dest: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool) -> EmptyResult {
        Self::send_static(&self.socket, &self.key_store, dest, self.endpoint_pair.public_endpoint, message, to_be_encrypted).await
    }

    pub async fn send_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message: &mut(impl Message + Serialize), to_be_encrypted: bool) -> EmptyResult {
        message.set_sender(sender);
        message.replace_dest_and_timestamp(dest);
        if message.check_expiry() {
            println!("PeerOps: Message expired: {}", message.id());
            return Ok(())
        }
        let mut unique_parts = bincode::serialize(&message.extract_unique_parts()).map_err(|e| e.to_string())?;
        let mut serialized = bincode::serialize(message).map_err(|e| e.to_string())?;
        let is_encrypted = if to_be_encrypted {
            let nonce1 = key_store.lock().unwrap().transform(dest, &mut serialized, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
            let nonce2 = key_store.lock().unwrap().transform(dest, &mut unique_parts, Direction::Encode).map_err(|e| e.error_response(file!(), line!()))?;
            IsEncrypted::True((nonce1, nonce2))
        }
        else {
            IsEncrypted::False
        };
        for chunk in Self::chunked(serialized, unique_parts, is_encrypted, message.id())? {
            socket.send_to(&chunk, dest).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn send_rapid_heartbeats(&self, mut rx: oneshot::Receiver<()>, dest: SocketAddrV4) {
        let (socket, key_store, sender) = (self.socket.clone(), self.key_store.clone(), self.endpoint_pair.public_endpoint);
        tokio::spawn(async move {
            while let Err(_) = rx.try_recv() {
                let mut message = Heartbeat::new();
                MessageProcessor::send_static(&socket, &key_store, dest, sender, &mut message, false).await.ok();
                sleep(Duration::from_millis(500)).await;
            }
        });
    }

    fn chunked(bytes: Vec<u8>, unique_parts_bytes: Vec<u8>, is_encrypted: IsEncrypted, uuid: &str) -> Result<Vec<Vec<u8>>, String> {
        let base_message = InboundMessage::new(Vec::with_capacity(0), uuid.to_owned(), is_encrypted, message::NO_POSITION, unique_parts_bytes);
        let chunks = bytes.chunks(1024 - (bincode::serialized_size(&base_message).unwrap() as usize));
        let num_chunks = chunks.len();
        let (mut messages, errors): (Vec<(Vec<u8>, String)>, Vec<(Vec<u8>, String)>) = chunks
            .enumerate()
            .map(|(i, chunk)| bincode::serialize(&base_message.clone().set_payload(chunk.to_vec()).set_position((i, num_chunks))).map_err(|e| e.to_string()))
            .map(|r| { match r { Ok(bytes) => (bytes, String::new()), Err(e) => (Vec::new(), e) } })
            .partition(|r| r.0.len() > 0);
        if errors.len() > 0 {
            return Err(errors.into_iter().map(|e| e.1).collect::<Vec<String>>().join(", "));
        }
        messages.shuffle(&mut SmallRng::from_entropy());
        Ok(messages.into_iter().map(|o| o.0).collect())
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

    pub async fn handle_search_request(&mut self, mut search_request: SearchMessage) -> EmptyResult {
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
            return self.return_search_responses(self.construct_search_response(uuid, dest, origin, host_name)).await
        }
        self.message_processor.send_request(&mut search_request, None).await
    }

    async fn return_search_responses(&mut self, mut search_response: SearchMessage) -> EmptyResult {
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
            self.message_processor.send(dest, &mut search_response, false).await
        }
    }
    
    fn construct_search_response(&self, uuid: String, dest: SocketAddrV4, origin: SocketAddrV4, host_name: String) -> SearchMessage {
        let (tx, rx) = oneshot::channel();
        self.message_processor.send_rapid_heartbeats(rx, origin);
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
            SearchMessage { kind: SearchMessageKind::Request, ..} => self.handle_search_request(message).await,
            SearchMessage { kind: SearchMessageKind::Response(_), .. } => self.return_search_responses(message).await
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
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message).await,
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propogate_request(message).await,
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message).await,
            DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } => self.add_new_peers(message),
        }
    }

    async fn propogate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
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
                self.return_response(hairpin_response).await?;
                return Ok(())
            }
            self.message_processor.set_breadcrumb_ttl(Some(hairpin_response), request.id(), TTL);
            // gateway::log_debug("Propogating request");
            self.message_processor.send_request(&mut request, None).await?;
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
                Self::send_final_response_static(&socket, &key_store, dest, sender, &message_staging_clone, &uuid).await;
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

    async fn return_response(&mut self, mut response: DiscoverPeerMessage) -> EmptyResult {
        response.add_peer(self.message_processor.endpoint_pair);
        let Some(dest) = self.message_processor.get_dest(response.id()) else { return Ok(()) };
        if response.origin() == self.message_processor.endpoint_pair.public_endpoint {
            let uuid = response.id().to_owned();
            if self.stage_message(response) {
                self.send_final_response(&uuid).await;
            }
            Ok(())
        }
        else {
            self.message_processor.send(dest, &mut response, false).await
        }
    }

    async fn send_final_response(&self, uuid: &str) {
        let dest = self.message_processor.get_dest(uuid).unwrap();
        Self::send_final_response_static(&self.message_processor.socket, &self.message_processor.key_store, dest, self.message_processor.endpoint_pair.public_endpoint, &self.message_staging, uuid).await;
    }

    async fn send_final_response_static(socket: &Arc<UdpSocket>, key_store: &Arc<Mutex<KeyStore>>, dest: SocketAddrV4, sender: SocketAddrV4, message_staging: &Arc<Mutex<HashMap<String, Option<DiscoverPeerMessage>>>>, uuid: &str) {
        let staged_message = {
            let mut message_staging = message_staging.lock().unwrap();
            let Some(staged_message) = message_staging.get_mut(uuid) else { return };
            staged_message.take()
        };
        if let Some(staged_message) = staged_message {
            // gateway::log_debug("I'm back at the introducer");
            MessageProcessor::send_static(socket, key_store, dest, sender, &mut staged_message.set_kind(DpMessageKind::IveGotSome), false).await.ok();
        }
    }

    async fn request_new_peers(&self, mut message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug(&format!("Got INeedSome, introducer = {}", message.peer_list()[0].public_endpoint));
        let introducer = message.get_last_peer();
        self.message_processor.send(introducer.public_endpoint, 
            &mut message
                .set_kind(DpMessageKind::Request),
            false,
            ).await
    }

    fn add_new_peers(&self, message: DiscoverPeerMessage) -> EmptyResult {
        self.message_processor.add_new_peers(message.into_peer_list());
        Ok(())
    }
}

pub struct StreamMessageProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    cached_messages: HashMap<String, StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>,
    active_sessions: HashMap<String, ActiveSessionInfo>
}
impl StreamMessageProcessor {
    pub fn new(message_processor: MessageProcessor,from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self {
        Self
        {
            message_processor,
            from_staging,
            cached_messages: HashMap::new(),
            local_hosts,
            to_http_handler,
            active_sessions: HashMap::new()
        }
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.from_staging.recv().await.ok_or("StreamMessageProcessor: failed to receive message from gateway")?;
        match message {
            StreamMessage { kind: StreamMessageKind::KeyAgreement, ..} => self.handle_key_agreement(message).await,
            StreamMessage { kind: StreamMessageKind::Request, .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Response, ..} => self.handle_response(message)
        }
    }

    async fn handle_key_agreement(&mut self, mut message: StreamMessage) -> EmptyResult {
        let dest = message.dest();
        let Some(cached_message) = self.cached_messages.get_mut(message.id()) else {
            println!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: request expired for resource {}", dest, message.id());
            return Ok(())
        };
        if let StreamMessageKind::Request = cached_message.kind {
            self.message_processor.send(dest, &mut message, false).await?;
            Self::send_resource(&self.message_processor, cached_message, dest).await
        }
        else {
            Ok(println!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: already received resource {}", dest, message.id()))
        }
    }

    async fn handle_request(&mut self, mut message: StreamMessage) -> EmptyResult {
        let (dest, uuid, host_name) = (message.sender(), message.id().to_owned(), message.host_name().to_owned());
        if message.sender() == EndpointPair::default_socket() {
            let active_session_info = self.active_sessions.entry(host_name).or_default();
            if active_session_info.dests().len() > 0 {
                self.message_processor.send_request(&mut message, Some(active_session_info.dests())).await?;
            }
            else {
                self.cached_messages.insert(uuid.clone(), message.clone());
            }
            Ok(active_session_info.push_resource(uuid))
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
        self.message_processor.send(dest, &mut response, true).await
    }

    fn handle_response(&mut self, message: StreamMessage) -> EmptyResult {
        let Some(active_session_info) = self.active_sessions.get_mut(message.host_name()) else { return Ok(()) };
        if !active_session_info.resource_is_valid(message.id(), message.sender()) {
            println!("StreamMessageProcessor: blocked response from host {:?}, reason: unsolicited response for resource {}", message.sender(), message.id());
            return Ok(())
        };
        if active_session_info.pop_resource(message.id()) {
            println!("Popped: {}", message.id());
            let cached_message = if let Some(cached_message) = self.cached_messages.remove(message.id()) {
                if let StreamMessageKind::Response = cached_message.kind { cached_message } else { message }
            } 
            else {
                message
            };
            return self.return_resource(cached_message.payload())
        }
        else {
            self.cached_messages.insert( message.id().to_owned(), message);
            Ok(())
        }
    }

    async fn send_resource(message_processor: &MessageProcessor, message: &mut StreamMessage, dest: SocketAddrV4) -> EmptyResult {
        message_processor.send(dest, message, true).await
    }

    fn return_resource(&self, payload: &[u8]) -> EmptyResult {
        let response = bincode::deserialize(payload).unwrap_or_else(|e| http::construct_error_response((*e).to_string(), String::from("HTTP/1.1")));
        self.to_http_handler.send(response).map_err(|e| send_error_response(e, file!(), line!()))
    }
}

struct ActiveSessionInfo {
    dests: HashSet<SocketAddrV4>,
    resource_queue: VecDeque<String>,
    resolved_resources: HashSet<String>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), resource_queue: VecDeque::new(), resolved_resources: HashSet::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn push_resource(&mut self, uuid: String) { self.resource_queue.push_back(uuid) }
    fn pop_resource(&mut self, uuid: &String) -> bool {
        if self.is_front_resource(uuid) {
            self.resolved_resources.insert(self.resource_queue.pop_front().unwrap());
            return true;
        }
        false
    }
    fn is_front_resource(&self, uuid: &String) -> bool { self.resource_queue.front().unwrap() == uuid }
    fn resource_is_valid(&mut self, uuid: &String, sender: SocketAddrV4) -> bool {
        if self.resolved_resources.contains(uuid) || self.resource_queue.contains(uuid) {
            self.dests.insert(sender);
            return !self.resolved_resources.contains(uuid);
        }
        false        
    }
}

impl Default for ActiveSessionInfo {
    fn default() -> Self {
        Self::new()
    }
}
