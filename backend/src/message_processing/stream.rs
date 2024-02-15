use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddrV4, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use crate::{gateway::EmptyResult, http::{self, SerdeHttpResponse}, message::{Message, StreamMessage, StreamMessageKind}, node::EndpointPair, utils::TransientMap};
use super::{send_error_response, MessageProcessor, ACTIVE_SESSION_TTL};

pub struct StreamMessageProcessor {
    message_processor: MessageProcessor,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>,
    active_sessions: TransientMap<String, ActiveSessionInfo>
}
impl StreamMessageProcessor {
    pub fn new(message_processor: MessageProcessor,from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> Self {
        Self
        {
            message_processor,
            from_staging,
            local_hosts,
            to_http_handler,
            active_sessions: TransientMap::new(ACTIVE_SESSION_TTL)
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
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let active_session = active_sessions.get_mut(message.host_name()).unwrap();
        match active_session.send_cached_message(message.id(), dest, |cached_message| self.message_processor.send(dest, cached_message, true, true)) {
            Ok(_) => {
                self.message_processor.send(dest, &mut message, false, false)
            }
            Err(e) => { println!("{e}"); Ok(()) }
        }
    }

    fn send_follow_ups(&self, host_name: String) {
        let (socket, key_store, sender, active_sessions) = (self.message_processor.socket.clone(), self.message_processor.key_store.clone(), self.message_processor.endpoint_pair.public_endpoint, self.active_sessions.map().clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let active_session = active_sessions.get_mut(&host_name).unwrap();
                active_session.send_cached_messages(|requests, dests| {
                    for message in requests {
                        if let StreamMessageKind::Request = message.kind {                    
                            for dest in dests.iter() {
                                println!("Sending follow up for {} to {}", message.id(), dest);
                                MessageProcessor::send_static(&socket, &key_store, *dest, sender, message, true, true).ok();
                            }
                        }
                    }
                });
            }
        });
    }

    async fn handle_request(&mut self, mut message: StreamMessage) -> EmptyResult {
        let (dest, uuid, host_name) = (message.only_sender(), message.id().to_owned(), message.host_name().to_owned());
        if dest == EndpointPair::default_socket() {
            self.active_sessions.set_timer(host_name.clone(), String::from("StreamActiveSessions"));
            let mut active_sessions = self.active_sessions.map().lock().unwrap();
            let start_loop = !active_sessions.contains_key(&host_name);
            let active_session_info = active_sessions.entry(host_name.clone()).or_default();
            let dests = active_session_info.dests();
            if dests.len() > 0 {
                self.message_processor.send_request(&mut message, Some(dests), true)?;
            }
            println!("Pushed: {}", message.id());
            active_session_info.push_resource(message)?;
            if start_loop {
                self.send_follow_ups(host_name);
            }
            Ok(())
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
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
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
    cached_messages: TransientMap<String, StreamMessage>,
    resource_queue: VecDeque<String>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), cached_messages: TransientMap::new(30), resource_queue: VecDeque::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn send_cached_messages(&self, action: impl Fn(Vec<&mut StreamMessage>, &HashSet<SocketAddrV4>)) {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        action(cached_messages.values_mut().collect::<Vec<&mut StreamMessage>>(), &self.dests);
    }
    fn send_cached_message(&mut self, uuid: &str, dest: SocketAddrV4, action: impl Fn(&mut StreamMessage) -> EmptyResult) -> EmptyResult {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let Some(cached_message) = cached_messages.get_mut(uuid) else {
            return Err(format!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: request expired for resource {}", dest, uuid));
        };
        self.dests.insert(dest);
        if let StreamMessageKind::Request = cached_message.kind {
            return action(cached_message)
        }
        Err(format!("StreamMessageProcessor: blocked key agreement/initial request from client to host {:?}, reason: already received resource {}", dest, uuid))
    }
    fn push_resource(&mut self, message: StreamMessage) -> EmptyResult {
        self.cached_messages.set_timer(message.id().to_owned(), String::from("ActiveSessionsCache"));
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        if cached_messages.contains_key(message.id()) {
            return Err(String::from("ActiveSessionInfo: Attempted to insert duplicate request"));
        }
        self.resource_queue.push_back(message.id().to_owned());
        cached_messages.insert(message.id().to_owned(), message);
        Ok(())
    }
    fn pop_resource(&mut self, message: StreamMessage) -> Result<Vec<StreamMessage>, String> {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let Some(cached_message) = cached_messages.get_mut(message.id()) else {
            return Err(format!("StreamMessageProcessor: blocked response from host {:?}, reason: unsolicited response for resource {}", message.senders(), message.id()))
        };
        *cached_message = message;
        let mut popped_resources = Vec::new();
        while let Some(uuid) = self.resource_queue.front() {
            if let StreamMessageKind::Response = cached_messages.get(uuid).unwrap().kind {
                popped_resources.push(cached_messages.remove(&self.resource_queue.pop_front().unwrap()).unwrap())
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