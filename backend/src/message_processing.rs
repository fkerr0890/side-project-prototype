use std::{net::SocketAddrV4, collections::HashMap, time::Duration, sync::{Arc, Mutex}};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{sync::{oneshot, mpsc}, time::sleep};

use crate::{message::{MessageKind, SearchMessage, Message, DiscoverPeerMessage, DpMessageKind}, gateway::{self, EmptyResult}, peer::PeerOps, http::{SerdeHttpResponse, self}, node::EndpointPair};

pub static SEARCH_TIMEOUT: i64 = 30;
pub static TTL: u64 = 100;

pub struct MessageProcessor<T> {
    endpoint_pair: EndpointPair,
    breadcrumbs: Arc<Mutex<HashMap<String, SocketAddrV4>>>,
    staging_ttls: HashMap<String, oneshot::Receiver<()>>,
    ingress: mpsc::UnboundedReceiver<T>,
    egress: mpsc::UnboundedSender<T>
}

impl<T: Serialize + DeserializeOwned + Message<T> + Clone + Send> MessageProcessor<T> {
    pub fn new(endpoint_pair: EndpointPair, ingress: mpsc::UnboundedReceiver<T>, egress: mpsc::UnboundedSender<T>) -> Self {
        Self {
            endpoint_pair,
            breadcrumbs: Arc::new(Mutex::new(HashMap::new())),
            staging_ttls: HashMap::new(),
            ingress,
            egress
        }
    } 

    fn try_add_breadcrumb(&mut self, id: String, dest: SocketAddrV4, egress: Option<mpsc::UnboundedSender<DiscoverPeerMessage>>, early_return_message: Option<DiscoverPeerMessage>) -> bool {
        // gateway::log_debug(&format!("Sender {}", dest));
        let mut breadcrumbs = self.breadcrumbs.lock().unwrap();
        if breadcrumbs.contains_key(&id) {
            gateway::log_debug("Already visited this node, not propagating message");
            false
        }
        else {
            breadcrumbs.insert(id.clone(), dest);
            let breadcrumbs_clone = self.breadcrumbs.clone();
            // let early_return_tx = early_return_tx.clone();
            // let early_return_message = early_return_message.clone();
            let dest = self.endpoint_pair.public_endpoint;
            tokio::spawn(async move {
                sleep(Duration::from_millis(TTL)).await;
                // gateway::log_debug("Ttl for breadcrumb expired");
                if let Some(tx) = egress {
                    if let Some(message) = early_return_message {
                        tx.send(message.replace_dest_and_timestamp(dest)).unwrap();
                    }
                }
                else {
                    breadcrumbs_clone.lock().unwrap().remove(&id);
                }
            });
            true
        }
    }

    fn return_responses(&mut self, search_responses: Vec<T>, delete_breadcrumb: bool) -> Result<Option<Vec<T>>, String> {
        let hash = search_responses[0].id().to_owned();
        let Some(dest) = self.breadcrumbs.lock().unwrap().remove(&hash) else { return Ok(None) };
        if !delete_breadcrumb {
            self.try_add_breadcrumb(hash, dest, None, None);
        }
        // gateway::log_debug(&format!("Returning search responses to {dest}"));
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

    fn set_staging_ttl(&mut self, id: &String, ttl_secs: u64) {
        let (tx, rx) = oneshot::channel();
        self.staging_ttls.insert(id.clone(), rx);
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
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>,
    local_hosts: HashMap<String, SocketAddrV4>,
    message_staging: HashMap<String, HashMap<usize, SearchMessage>>,
    peer_ops: Arc<Mutex<PeerOps>>
}

impl SearchRequestProcessor {
    pub fn new(message_processor: MessageProcessor<SearchMessage>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>, local_hosts: HashMap<String, SocketAddrV4>, peer_ops: &Arc<Mutex<PeerOps>>) -> Self {
        Self {
            message_processor,
            to_http_handler,
            local_hosts,
            message_staging: HashMap::new(),
            peer_ops: peer_ops.clone()
        }
    }

    pub async fn send_search_response(&mut self, search_request_parts: Vec<SearchMessage>) -> EmptyResult {
        let query = search_request_parts[0].id().to_owned();
        if !self.message_processor.try_add_breadcrumb(query.clone(), search_request_parts[0].sender(), None, None) {
            return Ok(())
        }
        gateway::log_debug("Checking for resource");
        if let Some(socket) = self.local_hosts.get(&query) {
            // gateway::log_debug(&format!("Hash at hairpin {hash}"));
            let (dest, origin) = (search_request_parts[0].sender(), search_request_parts[0].origin());
            let bytes = SearchMessage::reassemble_message_payload(search_request_parts);
            let search_request = bincode::deserialize(&bytes).unwrap();
            let response = http::make_request(search_request, String::from("http://") + &socket.to_string()).await;
            return self.return_search_responses(self.construct_search_response(response, &query, dest, origin));
        }
        gateway::log_debug("Resource not found");
        self.peer_ops.lock().unwrap().send_request(search_request_parts, &self.message_processor.egress)?;
        Ok(())
    }

    fn return_search_responses(&mut self, search_responses: Vec<SearchMessage>) -> EmptyResult {
        if let Some(search_responses) = self.message_processor.return_responses(search_responses, true)? {
            let payload = SearchMessage::reassemble_message_payload(search_responses);
            let response = bincode::deserialize(&payload).unwrap();
            self.to_http_handler.send(response).map_err(|e| { e.to_string() })?;
        }
        Ok(())
    }
    
    fn construct_search_response(&self, response: SerdeHttpResponse, query: &str, dest: SocketAddrV4, origin: SocketAddrV4) -> Vec<SearchMessage> {
        SearchMessage::initial_http_response(dest, self.message_processor.endpoint_pair.public_endpoint, origin, query.to_owned(), response).chunked()
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.message_processor.ingress.recv().await.unwrap();
        if let Some(messages) = self.stage_message(message) {
            match messages[0] {
                SearchMessage { kind: MessageKind::Request, ..} => return self.send_search_response(messages).await,
                SearchMessage { kind: MessageKind::Response, .. } => return self.return_search_responses(messages)
            };
        };
        Ok(())
    }

    fn stage_message(&mut self, message: SearchMessage) -> Option<Vec<SearchMessage>> {
        let (index, num_chunks) = message.position();
        if num_chunks == 1 {
            // gateway::log_debug(&format!("Hash on the way back: {}", message.message_ext().hash()));
            Some(vec![message])
        }
        else {
            let query = message.id().clone();
            let staged_messages= self.message_staging.entry(query.clone()).or_insert(HashMap::with_capacity(num_chunks));
            if staged_messages.len() == 0 {
                self.message_processor.set_staging_ttl(&query, TTL);
            }
            if !self.message_processor.check_staging_ttl(&query) {
                self.message_staging.remove(&query);
                return None;
            }
            staged_messages.insert(index, message);
            if self.message_staging.len() == num_chunks {
                gateway::log_debug("Collected all messages");
                let messages: Vec<SearchMessage> = self.message_staging.remove(&query).unwrap().into_values().collect();
                return Some(messages);
            }
            None
        }
    }
}

pub struct DiscoverPeerProcessor {
    message_processor: MessageProcessor<DiscoverPeerMessage>,
    message_staging: Arc<Mutex<HashMap<String, (Vec<DiscoverPeerMessage>, bool)>>>,
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
        let hairpin_response = DiscoverPeerMessage::new(DpMessageKind::Response,
            request.sender(),
            self.message_processor.endpoint_pair.public_endpoint,
            request.origin(),
            request.id().clone(),
        request.hop_count());
        if !request.try_decrement_hop_count() {
            gateway::log_debug("At hairpin");
            self.return_response(hairpin_response)?;
            return Ok(())
        }
        if self.message_processor.try_add_breadcrumb(request.id().clone(), request.sender(), Some(self.message_processor.egress.clone()), Some(hairpin_response)) {
            // gateway::log_debug("Propogating request");
            self.peer_ops.lock().unwrap().send_request(vec![request], &self.message_processor.egress)?
        }
        Ok(())
    }

    fn stage_message(&mut self, message: DiscoverPeerMessage) -> bool {
        let mut message_staging = self.message_staging.lock().unwrap();
        let (staged_messages, active) = message_staging.entry(message.id().clone()).or_insert((Vec::new(), true));
        if !*active {
            return false;
        }
        if staged_messages.len() == 0 {
            let egress = self.message_processor.egress.clone();
            let message_staging = self.message_staging.clone();
            let uuid = message.id().clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(TTL + 2)).await;
                Self::send_final_response(egress, message_staging, uuid);
            });
        }
        gateway::log_debug(&format!("Staging message with {} peers", message.peer_list().len()));
        let target_num_peers = message.hop_count();
        staged_messages.push(message);
        staged_messages.len() == target_num_peers as usize
    }

    fn return_response(&mut self, mut response: DiscoverPeerMessage) -> EmptyResult {
        response.add_peer(self.message_processor.endpoint_pair);
        response.increment_hop_count();
        // gateway::log_debug(&format!("Peers along the way: {:?}", response.peer_list()));
        if let Some(mut responses) = self.message_processor.return_responses(vec![response], false)? {
            let mut to_be_staged = responses.pop().unwrap();
            to_be_staged.try_decrement_hop_count();
            let uuid = to_be_staged.id().clone();
            if self.stage_message(to_be_staged) {
                Self::send_final_response(self.message_processor.egress.clone(), self.message_staging.clone(), uuid);
            }
        }
        Ok(())
    }

    fn send_final_response(egress: mpsc::UnboundedSender<DiscoverPeerMessage>, message_staging: Arc<Mutex<HashMap<String, (Vec<DiscoverPeerMessage>, bool)>>>, uuid: String) {
        let mut message_staging = message_staging.lock().unwrap();
        let Some((responses, active)) = message_staging.get_mut(&uuid) else { return };
        if !*active {
            return;
        }
        gateway::log_debug("I'm back at the introducer");
        *active = false;
        // let mut responses = std::mem::replace(value.0, Vec::with_capacity(0));
        responses.sort_by_key(|response| response.peer_list().len());
        let Some(best_result) = responses.pop() else { return };
        let dest = best_result.origin();
        egress.send(best_result.set_kind(DpMessageKind::IveGotSome).replace_dest_and_timestamp(dest)).ok();
    }

    fn request_new_peers(&self, mut message: DiscoverPeerMessage) -> EmptyResult {
        gateway::log_debug(&format!("Got INeedSome, introducer = {}", message.peer_list()[0].public_endpoint));
        let introducer = message.get_last_peer();
        self.message_processor.egress.send(message
            .set_kind(DpMessageKind::Request)
            .set_origin_if_unset(self.message_processor.endpoint_pair.public_endpoint)
            .replace_dest_and_timestamp(introducer.public_endpoint))
            .map_err(|e| { e.to_string() })
    }

    fn add_new_peers(&self, message: DiscoverPeerMessage) -> EmptyResult {
        // gateway::log_debug("Got IveGotSome");
        println!("My endpoint: {}, peers: {:?}", message.origin(), message.peer_list());
        for peer in message.into_peer_list() {
            self.peer_ops.lock().unwrap().add_peer(peer, 0);
        }
        Ok(())
    }
}