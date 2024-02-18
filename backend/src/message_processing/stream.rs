use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddrV4, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use crate::{gateway::EmptyResult, http::{self, SerdeHttpResponse}, message::{Id, Message, StreamMessage, StreamMessageKind}, node::EndpointPair, utils::TransientMap};
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

    fn handle_key_agreement(&mut self, message: StreamMessage) -> EmptyResult {
        let dest = message.dest();
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let active_session = active_sessions.get_mut(message.host_name()).unwrap();
        match active_session.initial_request(dest, message, |message, cached_message, dest| {
            self.message_processor.send(dest, message, false, false)?;
            self.message_processor.send(dest, cached_message, true, true)
        }) {
            Ok(_) => Ok(()),
            Err(e) => { println!("{}", e); Ok(()) }
        }
    }

    fn start_follow_ups(&self, host_name: String, uuid: Id) {
        let (socket, key_store, sender, active_sessions) = (self.message_processor.socket.clone(), self.message_processor.key_store.clone(), self.message_processor.endpoint_pair.public_endpoint, self.active_sessions.map().clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let Some(active_session) = active_sessions.get_mut(&host_name) else { return };
                if !active_session.follow_up(&uuid, |request, dests| {                  
                    for dest in dests.iter() {
                        println!("Sending follow up for {} to {}", request.id(), dest);
                        MessageProcessor::send_static(&socket, &key_store, *dest, sender, request, true, true).ok();
                    }
                }) {
                    return
                }
            }
        });
    }

    async fn handle_request(&mut self, mut message: StreamMessage) -> EmptyResult {
        let (dest, uuid, host_name) = (message.only_sender(), message.id().to_owned(), message.host_name().to_owned());
        if dest == EndpointPair::default_socket() {
            self.active_sessions.set_timer(host_name.clone());
            let mut active_sessions = self.active_sessions.map().lock().unwrap();
            let active_session_info = active_sessions.entry(host_name.clone()).or_default();
            let dests = active_session_info.dests();
            if dests.len() > 0 {
                self.message_processor.send_request(&mut message, Some(dests), true)?;
            }
            println!("Pushed: {}", message.id());
            active_session_info.push_resource(message)?;
            Ok(self.start_follow_ups(host_name, uuid))
        }
        else {
            self.send_response(message.payload(), dest, host_name, uuid).await
        }
    }

    async fn send_response(&self, payload: &[u8], dest: SocketAddrV4, host_name: String, uuid: Id) -> EmptyResult {
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
    cached_messages: TransientMap<Id, StreamMessage>,
    resource_queue: VecDeque<Id>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), cached_messages: TransientMap::new(30), resource_queue: VecDeque::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn handle_cached_message<'a>(uuid: &Id, cached_message: Option<&'a mut StreamMessage>) -> Result<&'a mut StreamMessage, String> {
        let Some(cached_message) = cached_message else {
            return Err(format!("StreamMessageProcessor: prevented request from client, reason: request expired for resource {}", uuid));
        };
        if let StreamMessageKind::Request = cached_message.kind {
            return Ok(cached_message)
        }
        Err(format!("StreamMessageProcessor: prevented request from client, reason: already received resource {}", uuid))
    }
    fn initial_request(&mut self, dest: SocketAddrV4, mut key_agreement: StreamMessage, action: impl Fn(&mut StreamMessage, &mut StreamMessage, SocketAddrV4) -> EmptyResult) -> EmptyResult {
        self.dests.insert(dest);
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = cached_messages.get_mut(key_agreement.id());
        let cached_message = Self::handle_cached_message(key_agreement.id(), cached_message)?;
        action(&mut key_agreement, cached_message, dest)
    }
    fn follow_up(&mut self, uuid: &Id, action: impl Fn(&mut StreamMessage, &HashSet<SocketAddrV4>)) -> bool {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = cached_messages.get_mut(uuid);
        let cached_message = match Self::handle_cached_message(uuid, cached_message) { Ok(cached_message) => cached_message, Err(e) => { println!("{}", e); return false; } };
        action(cached_message, &self.dests);
        true
    }
    fn push_resource(&mut self, message: StreamMessage) -> EmptyResult {
        self.cached_messages.set_timer(message.id().to_owned());
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