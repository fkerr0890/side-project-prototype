use std::{net::{SocketAddrV4, Ipv4Addr}, fmt::Display, collections::HashMap, hash::Hash, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{sync::{mpsc::{self, UnboundedSender}, oneshot}, time::sleep};

use crate::{message::{MessageKind, SearchMessage, Message, DiscoverPeerMessage}, gateway::{self, EmptyResult}, peer::peer_ops, http::{SerdeHttpResponse, self}};

pub static SEARCH_TIMEOUT: i64 = 30;

pub struct MessageProcessor<T> {
    endpoint_pair: EndpointPair,
    breadcrumbs: HashMap<String, (SocketAddrV4, oneshot::Receiver<()>)>,
    staging_ttls: HashMap<String, oneshot::Receiver<()>>,
    ingress: mpsc::UnboundedReceiver<T>,
    egress: mpsc::UnboundedSender<T>
}

impl<T: Serialize + DeserializeOwned + Message<T>> MessageProcessor<T> {
    pub fn new(endpoint_pair: EndpointPair, ingress: mpsc::UnboundedReceiver<T>, egress: mpsc::UnboundedSender<T>) -> Self {
        Self {
            endpoint_pair,
            breadcrumbs: HashMap::new(),
            staging_ttls: HashMap::new(),
            ingress,
            egress
        }
    } 

    fn try_add_breadcrumb(&mut self, id: String, dest: SocketAddrV4) -> bool {
        if self.breadcrumbs.contains_key(&id) {
            gateway::log_debug("Already visited this node, not propagating message");
            false
        }
        else {
            let (tx, rx) = oneshot::channel();
            self.breadcrumbs.insert(id, (dest, rx));
            tokio::spawn(async move {
                sleep(Duration::from_secs(30)).await;
                tx.send(()).ok()
            });
            true
        }
    }

    fn return_responses(&mut self, search_responses: Vec<T>) -> Result<Option<Vec<T>>, ()> {
        let hash = search_responses[0].id().to_owned();
        let Some(dest) = self.get_breadcrumb_ttl(&hash) else { return Ok(None) };
        gateway::log_debug(&format!("Returning search responses to {dest}"));
        if dest == EndpointPair::default_socket() {
            return Ok(Some(search_responses));
        }
        else {
            for response in search_responses {
                self.egress.send(response.replace_dest_and_timestamp(dest)).map_err(|_| { () })?;
            }
        }
        self.breadcrumbs.remove(&hash);
        Ok(None)
    }

    fn get_breadcrumb_ttl(&mut self, query: &str) -> Option<SocketAddrV4> {
        let Some((dest, rx)) = self.breadcrumbs.get_mut(query) else { return None };
        if let Err(_) = rx.try_recv() {
            Some(*dest)
        }
        else {
            gateway::log_debug("Ttl for breadcrumb expired");
            self.breadcrumbs.remove(query);
            None
        }
    }

    fn set_staging_ttl(&mut self, id: &String, ttl_secs: u64) {
        let (tx, rx) = oneshot::channel();
        self.staging_ttls.insert(id.clone(), rx);
        tokio::spawn(async move {
            sleep(Duration::from_secs(ttl_secs)).await;
            tx.send(()).ok()
        });
    }

    fn check_staging_ttl(&mut self, id: &str) -> bool {
        let Some(rx) = self.staging_ttls.get_mut(id) else { return false };
        if let Ok(_) = rx.try_recv() {
            gateway::log_debug("Ttl for message staging expired");
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
    message_staging: HashMap<String, HashMap<usize, SearchMessage>>
}

impl SearchRequestProcessor {
    pub fn new(message_processor: MessageProcessor<SearchMessage>, to_http_handler: UnboundedSender<SerdeHttpResponse>, local_hosts: HashMap<String, SocketAddrV4>) -> Self {
        Self {
            message_processor,
            to_http_handler,
            local_hosts,
            message_staging: HashMap::new()
        }
    }

    pub async fn send_search_response(&mut self, search_request_parts: Vec<SearchMessage>) -> EmptyResult {
        let query = search_request_parts[0].id().to_owned();
        if !self.message_processor.try_add_breadcrumb(query.clone(), search_request_parts[0].sender()) {
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
        peer_ops::send_request(search_request_parts, self.message_processor.endpoint_pair.public_endpoint, &self.message_processor.egress)?;
        Ok(())
    }

    fn return_search_responses(&mut self, search_responses: Vec<SearchMessage>) -> EmptyResult {
        if let Some(search_responses) = self.message_processor.return_responses(search_responses)? {
            let payload = SearchMessage::reassemble_message_payload(search_responses);
            let response = bincode::deserialize(&payload).unwrap();
            self.to_http_handler.send(response).map_err(|_| { () })?;
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
            let staged_messages= self.message_staging.entry(message.id().clone()).or_insert(HashMap::with_capacity(num_chunks));
            if staged_messages.len() == 0 {
                self.message_processor.set_staging_ttl(&query, 30);
            }
            else if !self.message_processor.check_staging_ttl(&query) {
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

    pub fn to_node_info(&self) -> NodeInfo {
        NodeInfo {
            endpoint_pair: self.message_processor.endpoint_pair,
            uuid: String::from("yah fuck ya"),
            nat_kind: NatKind::Unknown,
            max_peers: peer_ops::MAX_PEERS
        }
    }
}

pub struct DiscoverPeerProcessor {
    message_processor: MessageProcessor<DiscoverPeerMessage>
}

impl DiscoverPeerProcessor {
    pub fn new(message_processor: MessageProcessor<DiscoverPeerMessage>) -> Self { Self { message_processor } }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.message_processor.ingress.recv().await.unwrap();
        match message {
            DiscoverPeerMessage { kind: MessageKind::Request, ..} => return self.propogate_request(message),
            DiscoverPeerMessage { kind: MessageKind::Response, .. } => return self.return_response(message),
        };
    }

    fn propogate_request(&mut self, mut request: DiscoverPeerMessage) -> EmptyResult {
        if !request.try_decrement_hop_count() {
            let response = DiscoverPeerMessage::new(MessageKind::Response,
                request.sender(),
                self.message_processor.endpoint_pair.public_endpoint,
                request.origin(),
                request.id().clone());
            request.add_peer(self.message_processor.endpoint_pair);
            self.message_processor.return_responses(vec![response])?;
            return Ok(())
        }
        if self.message_processor.try_add_breadcrumb(request.id().clone(), request.sender()) {
            peer_ops::send_request(vec![request], self.message_processor.endpoint_pair.public_endpoint, &self.message_processor.egress)?
        }
        Ok(())
    }

    fn return_response(&mut self, mut response: DiscoverPeerMessage) -> EmptyResult {
        response.add_peer(self.message_processor.endpoint_pair);
        if let Some(mut responses) = self.message_processor.return_responses(vec![response])? {
            for peer in responses.pop().unwrap().into_peer_list() {
                peer_ops::add_peer(peer, 0);
            }
        }
        Ok(())
    }
}

// pub struct RendevousNode(Node);

#[derive(Hash, Clone, Serialize, Deserialize, Copy, Eq, PartialEq)]
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

    pub fn default_socket() -> SocketAddrV4 { SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0) }
}

impl Display for EndpointPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Public: {}, Private: {}", self.private_endpoint, self.public_endpoint)
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
enum NatKind {
    Unknown,
    Static,
    Easy,
    Hard
}

#[derive(Serialize, Deserialize)]
pub struct NodeInfo {
    pub endpoint_pair: EndpointPair,
    uuid: String,
    nat_kind: NatKind,
    max_peers: usize
}
