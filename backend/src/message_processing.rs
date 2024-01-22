use std::{net::SocketAddrV4, collections::{HashMap, HashSet}, time::Duration, sync::{Arc, Mutex}};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{sync::{oneshot, mpsc}, time::sleep};

use crate::{message::{SearchMessageKind, SearchMessage, Message, DiscoverPeerMessage, DpMessageKind, StreamMessage, StreamMessageKind}, gateway::{self, EmptyResult}, peer::PeerOps, http::{SerdeHttpResponse, self, SerdeHttpRequest}, node::EndpointPair, crypto::{KeyStore, Direction}};

pub static SEARCH_TIMEOUT: i64 = 30;
pub static TTL: u64 = 200;
const SRP_TTL: u64 = 30000;

pub struct MessageProcessor<T> {
    endpoint_pair: EndpointPair,
    breadcrumbs: Arc<Mutex<HashMap<String, SocketAddrV4>>>,
    message_staging: HashMap<String, HashMap<usize, T>>,
    staging_ttls: HashMap<String, oneshot::Receiver<()>>,
    ingress: mpsc::UnboundedReceiver<T>,
    egress: mpsc::UnboundedSender<T>
}

impl<T: Serialize + DeserializeOwned + Message + Clone + Send> MessageProcessor<T> {
    pub fn new(endpoint_pair: EndpointPair, ingress: mpsc::UnboundedReceiver<T>, egress: mpsc::UnboundedSender<T>) -> Self {
        Self {
            endpoint_pair,
            breadcrumbs: Arc::new(Mutex::new(HashMap::new())),
            message_staging: HashMap::new(),
            staging_ttls: HashMap::new(),
            ingress,
            egress
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

    fn set_breadcrumb_ttl(&self, egress: Option<mpsc::UnboundedSender<DiscoverPeerMessage>>, early_return_message: Option<DiscoverPeerMessage>, id: &str, ttl: u64) {
        let breadcrumbs_clone = self.breadcrumbs.clone();
        // let early_return_tx = early_return_tx.clone();
        // let early_return_message = early_return_message.clone();
        let dest = self.endpoint_pair.public_endpoint;
        let id = id.to_owned();
        tokio::spawn(async move {
            sleep(Duration::from_millis(ttl)).await;
            if let Some(tx) = egress {
                if let Some(message) = early_return_message {
                    tx.send(message.replace_dest_and_timestamp(dest)).unwrap();
                }
            }
            else {
                gateway::log_debug("Ttl for breadcrumb expired");
                breadcrumbs_clone.lock().unwrap().remove(&id);
            }
        });
    }

    fn return_responses(&mut self, search_responses: Vec<T>, delete_breadcrumb: bool) -> Result<Option<Vec<T>>, String> {
        let breadcrumbs = self.breadcrumbs.lock().unwrap();
        let Some(dest) = (if delete_breadcrumb { breadcrumbs.get(search_responses[0].id()).cloned() } else { breadcrumbs.get(search_responses[0].id()).cloned() }) else { return Ok(None) };
        gateway::log_debug(&format!("Returning search responses to {dest}"));
        if dest == EndpointPair::default_socket() {
            Ok(Some(search_responses))
        }
        else {
            for response in search_responses {
                self.egress.send(response.replace_dest_and_timestamp(dest)).map_err(|e| { e.to_string() })?;
            }
            Ok(None)
        }
    }

    fn stage_message(&mut self, message: T) -> Option<Vec<T>> {
        let (index, num_chunks) = message.position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![message])
        }
        else {
            let id = message.id().to_owned();
            let staged_messages_len = {
                let staged_messages= self.message_staging.entry(id.clone()).or_insert(HashMap::with_capacity(num_chunks));
                staged_messages.insert(index, message);
                staged_messages.len()
            };
            if staged_messages_len == 1 {
                self.set_staging_ttl(&id, SRP_TTL);
            }
            if !self.check_staging_ttl(&id) {
                self.message_staging.remove(&id);
                return None;
            }
            if staged_messages_len == num_chunks {
                // gateway::log_debug("Collected all messages");
                let messages: Vec<T> = self.message_staging.remove(&id).unwrap().into_values().collect();
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

pub struct SearchRequestProcessor {
    message_processor: MessageProcessor<SearchMessage>,
    to_smp: mpsc::UnboundedSender<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    peer_ops: Arc<Mutex<PeerOps>>,
    key_store: Arc<Mutex<KeyStore>>,
    active_sessions: HashSet<String>
}

impl SearchRequestProcessor {
    pub fn new(message_processor: MessageProcessor<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, peer_ops: &Arc<Mutex<PeerOps>>, key_store: &Arc<Mutex<KeyStore>>) -> Self {
        Self {
            message_processor,
            to_smp,
            local_hosts,
            peer_ops: peer_ops.clone(),
            key_store: key_store.clone(),
            active_sessions: HashSet::new()
        }
    }

    pub async fn handle_search_request(&mut self, search_request_parts: Vec<SearchMessage>) -> EmptyResult {
        let uuid = search_request_parts[0].id();
        if !self.message_processor.try_add_breadcrumb(uuid, search_request_parts[0].sender()) {
            return Ok(())
        }
        else {
            self.message_processor.set_breadcrumb_ttl(None, None, uuid, SRP_TTL);
        }
        let host_name = search_request_parts[0].host_name();
        if self.local_hosts.contains_key(host_name) {
            println!("Found host {} at {}, uuid: {}", host_name, self.message_processor.endpoint_pair.public_endpoint.port(), uuid);
            let (dest, origin) = (search_request_parts[0].sender(), search_request_parts[0].origin());
            return self.return_search_responses(self.construct_search_response(uuid.to_owned(), dest, origin, host_name))
        }
        self.peer_ops.lock().unwrap().send_request(search_request_parts, &self.message_processor.egress)?;
        Ok(())
    }

    fn return_search_responses(&mut self, search_responses: Vec<SearchMessage>) -> EmptyResult {
        if let Some(mut search_responses) = self.message_processor.return_responses(search_responses, true)? {
            let message = search_responses.pop().unwrap();
            let origin = message.origin();
            let uuid = message.id().to_owned();
            let host_name = message.host_name().to_owned();
            let peer_public_key = message.into_public_key();
            let mut key_store = self.key_store.lock().unwrap();
            let my_public_key = key_store.requester_public_key(origin);
            key_store.agree(origin, peer_public_key).unwrap();
            self.to_smp
                .send(StreamMessage::new(origin, EndpointPair::default_socket(), host_name, uuid, StreamMessageKind::KeyAgreement, my_public_key.as_ref().to_vec()))
                .map_err(|e| { e.to_string() })?;
        }
        Ok(())
    }
    
    fn construct_search_response(&self, uuid: String, dest: SocketAddrV4, origin: SocketAddrV4, host_name: &str) -> Vec<SearchMessage> {
        let public_key = self.key_store.lock().unwrap().host_public_key(origin, self.peer_ops.lock().unwrap().heartbeat_tx(), self.message_processor.endpoint_pair.public_endpoint);
        vec![SearchMessage::key_response(dest, self.message_processor.endpoint_pair.public_endpoint, self.message_processor.endpoint_pair.public_endpoint, uuid, host_name.to_owned(), public_key.as_ref().to_vec())]
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let mut message = self.message_processor.ingress.recv().await.unwrap();
        if message.origin() == EndpointPair::default_socket() {
            if self.active_sessions.contains(message.host_name()) {
                return Ok(());
            }
            self.active_sessions.insert(message.host_name().to_owned());
            message.set_origin(self.message_processor.endpoint_pair.public_endpoint)
        }
        if let Some(messages) = self.message_processor.stage_message(message) {
            match messages[0] {
                SearchMessage { kind: SearchMessageKind::Request, ..} => return self.handle_search_request(messages).await,
                SearchMessage { kind: SearchMessageKind::Response(_), .. } => return self.return_search_responses(messages)
            };
        };
        Ok(())
    }
}

pub struct DiscoverPeerProcessor {
    message_processor: MessageProcessor<DiscoverPeerMessage>,
    message_staging: Arc<Mutex<HashMap<String, Option<DiscoverPeerMessage>>>>,
    peer_ops: Arc<Mutex<PeerOps>>
}

impl DiscoverPeerProcessor {
    pub fn new(message_processor: MessageProcessor<DiscoverPeerMessage>, peer_ops: &Arc<Mutex<PeerOps>>) -> Self {
        Self {
            message_processor,
            message_staging: Arc::new(Mutex::new(HashMap::new())),
            peer_ops: peer_ops.clone()
        }
    }

    pub async fn receive(&mut self) -> EmptyResult {
        let message = self.message_processor.ingress.recv().await.ok_or("Channel closed")?;
        match message {
            DiscoverPeerMessage { kind: DpMessageKind::INeedSome, .. } => self.request_new_peers(message),
            DiscoverPeerMessage { kind: DpMessageKind::Request, ..} => self.propogate_request(message),
            DiscoverPeerMessage { kind: DpMessageKind::Response, .. } => self.return_response(message),
            DiscoverPeerMessage { kind: DpMessageKind::IveGotSome, .. } => self.add_new_peers(message),
        }
    }

    fn propogate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
        let (sender, origin) = (request.sender(), request.origin());
        if self.message_processor.try_add_breadcrumb(request.id(), request.sender()) {
            let mut hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
                request.sender(),
                self.message_processor.endpoint_pair.public_endpoint,
                request.origin(),
                request.id().to_owned(),
            request.hop_count());
            hairpin_response.try_decrement_hop_count();
            if !request.try_decrement_hop_count() {
                // gateway::log_debug("At hairpin");
                self.return_response(hairpin_response)?;
                return Ok(())
            }
            self.message_processor.set_breadcrumb_ttl(Some(self.message_processor.egress.clone()), Some(hairpin_response), request.id(), TTL);
            // gateway::log_debug("Propogating request");
            self.peer_ops.lock().unwrap().send_request(vec![request], &self.message_processor.egress)?;
        }
        let (endpoint_pair, score) = if sender != EndpointPair::default_socket() {
            (EndpointPair::new(sender, sender), self.get_score(sender))
        } else {
            (EndpointPair::new(origin, origin), self.get_score(origin))
        };        
        self.peer_ops.lock().unwrap().add_peer(endpoint_pair, score);
        Ok(())
    }

    fn get_score(&self, other: SocketAddrV4) -> i32 {
        (other.port() as i32).abs_diff(self.message_processor.endpoint_pair.public_endpoint.port() as i32) as i32
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
            let egress = self.message_processor.egress.clone();
            let message_staging_clone = self.message_staging.clone();
            let uuid = message.id().to_owned();
            tokio::spawn(async move {
                sleep(Duration::from_millis(TTL*2)).await;
                Self::send_final_response(egress, message_staging_clone.clone(), &uuid);
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
        // if response.sender() != self.message_processor.endpoint_pair.public_endpoint {
        //     response.increment_hop_count();
        // }
        // gateway::log_debug(&format!("Peers along the way: {:?}", response.peer_list()));
        let delete_breadcrumb = response.peer_list().len() == response.hop_count().1 as usize;
        if let Some(mut responses) = self.message_processor.return_responses(vec![response], delete_breadcrumb)? {
            let to_be_staged = responses.pop().unwrap();
            let uuid = to_be_staged.id().to_owned();
            if self.stage_message(to_be_staged) {
                Self::send_final_response(self.message_processor.egress.clone(), self.message_staging.clone(), &uuid);
            }
        }
        Ok(())
    }

    fn send_final_response(egress: mpsc::UnboundedSender<DiscoverPeerMessage>, message_staging: Arc<Mutex<HashMap<String, Option<DiscoverPeerMessage>>>>, uuid: &str) {
        let mut message_staging = message_staging.lock().unwrap();
        let Some(staged_message) = message_staging.get_mut(uuid) else { return };
        let staged_message = staged_message.take();
        if let Some(staged_message) = staged_message {
            // gateway::log_debug("I'm back at the introducer");
            let dest = staged_message.origin();
            egress.send(staged_message.set_kind(DpMessageKind::IveGotSome).replace_dest_and_timestamp(dest)).ok();
        }
    }

    fn request_new_peers(&self, mut message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug(&format!("Got INeedSome, introducer = {}", message.peer_list()[0].public_endpoint));
        let introducer = message.get_last_peer();
        self.message_processor.egress.send(message
            .set_kind(DpMessageKind::Request)
            .set_origin_if_unset(self.message_processor.endpoint_pair.public_endpoint)
            .replace_dest_and_timestamp(introducer.public_endpoint))
            .map_err(|e| { e.to_string() })
    }

    fn add_new_peers(&self, message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug("Got IveGotSome");
        for peer in message.into_peer_list() {
            self.peer_ops.lock().unwrap().add_peer(peer, self.get_score(peer.public_endpoint));
        }
        Ok(())
    }
}

pub struct StreamMessageProcessor {
    message_processor: MessageProcessor<StreamMessage>,
    key_store: Arc<Mutex<KeyStore>>,
    cached_messages: HashMap<String, StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>,
    active_sessions: HashMap<String, (SocketAddrV4, HashSet<String>)>
}
impl StreamMessageProcessor {
    pub fn new(message_processor: MessageProcessor<StreamMessage>, key_store: &Arc<Mutex<KeyStore>>, local_hosts: HashMap<String, SocketAddrV4>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self {
        Self
        {
            message_processor,
            key_store: key_store.clone(),
            cached_messages: HashMap::new(),
            local_hosts,
            to_http_handler,
            active_sessions: HashMap::new()
        }
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.message_processor.ingress.recv().await.unwrap();
        match message {
            StreamMessage { kind: StreamMessageKind::KeyAgreement, ..} => self.handle_key_agreement(message),
            StreamMessage { kind: StreamMessageKind::Request, .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Response, ..} => self.handle_response(message)
        }
    }

    fn handle_key_agreement(&mut self, message: StreamMessage) -> EmptyResult {
        let sender = message.sender();
        if sender == EndpointPair::default_socket() {
            let (dest, uuid) = (message.dest(), message.id().to_owned());
            self.message_processor.egress.send(message.set_sender(self.message_processor.endpoint_pair.public_endpoint)).ok();
            let cached_message = self.cached_messages.get(&uuid).unwrap();
            self.send_cached_request(cached_message.clone(), dest)
        }
        else {
            println!("Received key from requester");
            let peer_public_key = message.into_payload();
            self.key_store.lock().unwrap().agree(sender, peer_public_key).unwrap();
            Ok(())
        }
    }

    fn send_cached_request(&self, mut cached_message: StreamMessage, dest: SocketAddrV4) -> EmptyResult {
        self.key_store.lock().unwrap().transform(dest, cached_message.payload_mut(), Direction::Encode).unwrap();
        for message in cached_message.chunked() {
            self.message_processor.egress.send(message.set_sender(self.message_processor.endpoint_pair.public_endpoint).replace_dest_and_timestamp(dest)).ok();
        }
        Ok(())
    }

    async fn handle_request(&mut self, message: StreamMessage) -> EmptyResult {
        let (dest, uuid, host_name) = (message.sender(), message.id().to_owned(), message.host_name().to_owned());
        if message.sender() == EndpointPair::default_socket() {
            if let Some((dest, _)) = self.active_sessions.get(&host_name) {
                println!("Found active session for {}", &host_name);
                return self.send_cached_request(message, *dest);
            }
            else {
                println!("StreamProcessor: cached a message: {:?}", message);
                self.cached_messages.insert(uuid, message);
            }
        }
        else if let Some(request_parts) = self.message_processor.stage_message(message) {
            let mut request_bytes = StreamMessage::reassemble_message_payload(request_parts);
            self.key_store.lock().unwrap().transform(dest, &mut request_bytes, Direction::Decode).unwrap();
            let request: SerdeHttpRequest = bincode::deserialize(&request_bytes).unwrap();
            let socket = self.local_hosts.get(&host_name).unwrap();
            let response = http::make_request(request, &socket.to_string()).await;
            let mut response_bytes = bincode::serialize(&response).unwrap();
            self.key_store.lock().unwrap().transform(dest, &mut response_bytes, Direction::Encode).unwrap();
            let response = StreamMessage::new(
                dest,
                self.message_processor.endpoint_pair.public_endpoint,
                host_name,
                uuid,
                StreamMessageKind::Response,
                response_bytes);
            for message in response.chunked() {
                self.message_processor.egress.send(message).ok();
            }
        }
        Ok(())
    }

    fn handle_response(&mut self, message: StreamMessage) -> EmptyResult {
        if let Some(response_parts) = self.message_processor.stage_message(message) {
            let host_name = response_parts[0].host_name().to_owned(); 
            let (_socket, active_session_ids) = self.active_sessions.entry(host_name).or_insert((response_parts[0].sender(), HashSet::new()));
            if active_session_ids.contains(response_parts[0].id()) {
                return Ok(());
            }            
            active_session_ids.insert(response_parts[0].id().to_owned());
            let peer_addr = response_parts[0].sender();
            let mut bytes = StreamMessage::reassemble_message_payload(response_parts);
            self.key_store.lock().unwrap().transform(peer_addr, &mut bytes, Direction::Decode).unwrap();
            let response = bincode::deserialize(&bytes).unwrap();
            self.to_http_handler.send(response).ok();
        }
        Ok(())
    }
}
