use std::{collections::{HashMap, HashSet, VecDeque}, net::{Ipv4Addr, SocketAddrV4}, time::Duration};
use tokio::{fs, sync::mpsc, time::sleep};
use crate::{http::{self, SerdeHttpResponse}, message::{Id, Message, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair, utils::{TransientMap, TtlType}};
use super::{EmptyResult, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS, SRP_TTL_SECONDS};

pub struct StreamMessageProcessor
{
    outbound_gateway: OutboundGateway,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<SerdeHttpResponse>>,
    active_sessions: TransientMap<String, ActiveSessionInfo>,
    mode: StreamMode
}
impl StreamMessageProcessor
{
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<SerdeHttpResponse>>, mode: StreamMode) -> Self {
        Self
        {
            outbound_gateway,
            from_staging,
            local_hosts,
            from_http_handler,
            active_sessions: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS)),
            mode
        }
    }

    pub async fn receive(&mut self) -> EmptyResult  {
        let message = self.from_staging.recv().await.ok_or("StreamMessageProcessor: failed to receive message from gateway")?;
        match message {
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) | StreamMessageKind::Distribution(StreamMessageInnerKind::KeyAgreement), ..} => self.handle_key_agreement(message),
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request), .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Response) | StreamMessageKind::Distribution(StreamMessageInnerKind::Response), ..} => self.return_resource(message)
        }
    }

    fn handle_key_agreement(&mut self, message: StreamMessage) -> EmptyResult {
        let dest = message.dest();
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let active_session = active_sessions.get_mut(message.host_name()).unwrap();
        match active_session.initial_request(dest, message, |message, cached_message, dest| {
            self.outbound_gateway.send_individual(dest, message, false, false)?;
            self.outbound_gateway.send_individual(dest, cached_message, true, true)
        }) {
            Ok(_) => Ok(()),
            Err(e) => { println!("{}", e); Ok(()) }
        }
    }

    fn start_follow_ups(&self, host_name: String, uuid: Id) {
        let (socket, key_store, active_sessions, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.active_sessions.map().clone(), self.outbound_gateway.myself.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let Some(active_session) = active_sessions.get_mut(&host_name) else { return };
                if !active_session.follow_up(&uuid, |request, dests| {                  
                    for dest in dests.iter() {
                        println!("Sending follow up for {} to {}", request.id(), dest);
                        OutboundGateway::send_static(&socket, &key_store, *dest, &myself, request, true, true).ok();
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
            let set_timer = if dests.len() > 0 { self.outbound_gateway.send_request(&mut message, Some(dests))?; true } else { false };
            println!("Pushed: {}", message.id());
            let Ok(to_http_handler) = self.from_http_handler.try_recv() else { panic!("Oh fuck nah") };
            active_session_info.push_resource(message, set_timer, to_http_handler)?;
            Ok(self.start_follow_ups(host_name, uuid))
        }
        else {
            self.send_response(message.payload(), dest, host_name, uuid).await
        }
    }

    async fn send_response(&mut self, payload: &[u8], dest: SocketAddrV4, host_name: String, uuid: Id) -> EmptyResult {
        let response = match self.mode {
            StreamMode::Http => self.http_response_action(payload, host_name, uuid).await,
            StreamMode::Distribution => self.distribution_respose_action(payload, host_name, uuid).await
        };
        if let Some(mut response) = response {
            self.outbound_gateway.send_individual(dest, &mut response, true, true)?;
        }
        Ok(())
    }

    fn return_resource(&self, message: StreamMessage) -> EmptyResult {
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let Some(active_session) = active_sessions.get_mut(message.host_name()) else { return Ok(()) };
        if let Err(e) = active_session.pop_resource(message) {
            println!("{}", e);
        }
        Ok(())
    }

    async fn http_response_action(&self, payload: &[u8], host_name: String, uuid: Id) -> Option<StreamMessage> {
        let Ok(request) = bincode::deserialize(payload) else { return None };
        let socket = self.local_hosts.get(&host_name).unwrap();
        let response = http::make_request(request, &socket.to_string()).await;
        let Ok(response_bytes) = bincode::serialize(&response) else { return None };
        Some(StreamMessage::new(
            host_name,
            uuid,
            StreamMessageKind::Resource(StreamMessageInnerKind::Response),
            response_bytes))
    }
    
    async fn distribution_respose_action(&mut self, payload: &[u8], host_name: String, uuid: Id) -> Option<StreamMessage> {
        let result = if fs::write("C:/Users/fredk/Downloads/p2p-dump.tar.gz", payload).await.is_ok() { 1u8 } else { 0u8 };
        self.local_hosts.insert(host_name.clone(), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000));
        Some(StreamMessage::new(host_name, uuid, StreamMessageKind::Distribution(StreamMessageInnerKind::Response), vec![result]))
    }
}

#[derive(Debug)]
struct ActiveSessionInfo {
    dests: HashSet<SocketAddrV4>,
    cached_messages: TransientMap<Id, (StreamMessage, mpsc::UnboundedSender<SerdeHttpResponse>)>,
    resource_queue: VecDeque<Id>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), cached_messages: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS)), resource_queue: VecDeque::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn handle_cached_message<'a>(uuid: &Id, cached_message: Option<&'a mut (StreamMessage, mpsc::UnboundedSender<SerdeHttpResponse>)>) -> Result<&'a mut StreamMessage, String> {
        let Some(cached_message) = cached_message else {
            return Err(format!("StreamMessageProcessor: prevented request from client, reason: request expired for resource {}", uuid));
        };
        if let StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request) = cached_message.0.kind {
            return Ok(&mut cached_message.0)
        }
        Err(format!("StreamMessageProcessor: prevented request from client, reason: already received resource {}", uuid))
    }
    fn initial_request(&mut self, dest: SocketAddrV4, mut key_agreement: StreamMessage, action: impl Fn(&mut StreamMessage, &mut StreamMessage, SocketAddrV4) -> EmptyResult) -> EmptyResult {
        self.dests.insert(dest);
        self.cached_messages.set_timer(key_agreement.id().to_owned());
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = Self::handle_cached_message(key_agreement.id(), cached_messages.get_mut(key_agreement.id()))?;
        action(&mut key_agreement, cached_message, dest)
    }
    fn follow_up(&mut self, hash: &Id, action: impl Fn(&mut StreamMessage, &HashSet<SocketAddrV4>)) -> bool {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = match Self::handle_cached_message(hash, cached_messages.get_mut(hash)) { Ok(cached_message) => cached_message, Err(e) => { println!("{}", e); return false; } };
        action(cached_message, &self.dests);
        true
    }
    fn push_resource(&mut self, message: StreamMessage, set_timer: bool, to_http_handler: mpsc::UnboundedSender<SerdeHttpResponse>) -> EmptyResult {
        if set_timer {
            self.cached_messages.set_timer(message.id().to_owned());
        }
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        if cached_messages.contains_key(message.id()) {
            return Err(String::from("ActiveSessionInfo: Attempted to insert duplicate request"));
        }
        self.resource_queue.push_back(message.id().to_owned());
        cached_messages.insert(message.id().to_owned(), (message, to_http_handler));
        Ok(())
    }
    fn pop_resource(&mut self, message: StreamMessage) -> EmptyResult {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let Some((cached_message, _)) = cached_messages.get_mut(message.id()) else {
            return Err(format!("StreamMessageProcessor: blocked response from host {:?}, reason: unsolicited response for resource {}", message.senders(), message.id()))
        };
        *cached_message = message;
        let mut result = Ok(());
        while let Some(uuid) = self.resource_queue.front() {
            if let StreamMessageKind::Resource(StreamMessageInnerKind::Response) | StreamMessageKind::Distribution(StreamMessageInnerKind::Response) = cached_messages.get(uuid).unwrap().0.kind {
                let (cached_message, tx) = cached_messages.remove(&self.resource_queue.pop_front().unwrap()).unwrap();
                println!("Popped: {}", cached_message.id());
                let response = bincode::deserialize(cached_message.payload()).unwrap_or_else(|e| http::construct_error_response((*e).to_string(), String::from("HTTP/1.1")));
                result = tx.send(response).map_err(|e| format!("Failed to return response to http handler {:?} at {} {}", e, file!(), line!()));
            }
            else {
                break;
            }
        }
        result
    }
}

impl Default for ActiveSessionInfo {
    fn default() -> Self {
        Self::new()
    }
}

pub enum StreamMode {
    Http,
    Distribution
}