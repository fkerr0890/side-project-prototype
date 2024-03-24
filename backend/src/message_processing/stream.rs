use std::{collections::{HashMap, HashSet, VecDeque}, net::{Ipv4Addr, SocketAddrV4}, time::Duration};
use tokio::{fs, sync::mpsc, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{http::{self, SerdeHttpResponse}, message::{NumId, Message, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, node::EndpointPair, option_early_return, result_early_return, utils::{TransientMap, TtlType}};
use super::{EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS, SRP_TTL_SECONDS};

pub struct StreamMessageProcessor
{
    outbound_gateway: OutboundGateway,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    local_hosts: HashMap<String, SocketAddrV4>,
    from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>,
    active_sessions: TransientMap<String, ActiveSessionInfo>,
    dmessage_staging: DMessageStaging
}
impl StreamMessageProcessor
{
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>) -> Self {
        Self
        {
            outbound_gateway,
            from_staging,
            local_hosts,
            from_http_handler,
            active_sessions: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true),
            dmessage_staging: DMessageStaging::new()
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let message = self.from_staging.recv().await?;
        match message {
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) | StreamMessageKind::Distribution(StreamMessageInnerKind::KeyAgreement), ..} => self.handle_key_agreement(message),
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request), .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Response) | StreamMessageKind::Distribution(StreamMessageInnerKind::Response), ..} => self.return_resource(message)
        }
        Some(())
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.host_name = message.host_name()))]
    fn handle_key_agreement(&mut self, message: StreamMessage) {
        let dest = message.dest();
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let active_session = active_sessions.get_mut(message.host_name()).unwrap();
        active_session.initial_request(dest, message, |message, cached_message, dest| {
            self.outbound_gateway.send_individual(dest, message, false, false);
            self.outbound_gateway.send_individual(dest, cached_message, true, true);
        });
    }

    #[instrument(level = "trace", skip(self))]
    fn start_follow_ups(&self, host_name: String, id: NumId) {
        let (socket, key_store, active_sessions, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.active_sessions.map().clone(), self.outbound_gateway.myself);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let Some(active_session) = active_sessions.get_mut(&host_name) else { return };
                if !active_session.follow_up(id, |request, dests| {                  
                    for dest in dests.iter() {
                        debug!(id = %request.id(), %dest, "Sending follow up");
                        OutboundGateway::send_static(&socket, &key_store, *dest, myself, request, true, true);
                    }
                }) {
                    return
                }
            }
        });
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.id = %message.id()))]
    async fn handle_request(&mut self, mut message: StreamMessage) {
        let dest = message.only_sender();
        if dest == EndpointPair::default_socket() {
            let (id, host_name) = (message.id(), message.host_name().to_owned());
            self.active_sessions.set_timer(host_name.clone());
            let mut active_sessions = self.active_sessions.map().lock().unwrap();
            let active_session_info = active_sessions.entry(host_name.clone()).or_default();
            let dests = active_session_info.dests();
            let set_timer = if dests.len() > 0 {
                {
                    let mut key_store = self.outbound_gateway.key_store.lock().unwrap();
                    for dest in dests.iter() {
                        key_store.reset_expiration(*dest);
                    }
                }
                self.outbound_gateway.send_request(&mut message, Some(dests)); true
            } else {
                false
            };
            trace!(id = %message.id(), "Pushed");
            let Ok(to_http_handler) = self.from_http_handler.try_recv() else { panic!("Oh fuck nah") };
            active_session_info.push_resource(message, set_timer, to_http_handler);
            self.start_follow_ups(host_name, id);
        }
        else {
            let (id, payload, host_name, kind) = message.into_hash_payload_host_name_kind();
            self.send_response(payload, dest, host_name, id, kind).await;
        }
    }

    #[instrument(level = "trace", skip(self, payload))]
    async fn send_response(&mut self, payload: Vec<u8>, dest: SocketAddrV4, host_name: String, id: NumId, kind: StreamMessageKind) {
        let response = match kind {
            StreamMessageKind::Resource(_) => self.http_response_action(&payload, host_name, id).await,
            StreamMessageKind::Distribution(_) => self.distribution_response_action(payload, host_name, id).await
        };
        if let Some(mut response) = response {
            self.outbound_gateway.send_individual(dest, &mut response, true, true);
        }
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.id = %message.id()))]
    fn return_resource(&self, message: StreamMessage) {
        let mut active_sessions = self.active_sessions.map().lock().unwrap();
        let active_session = option_early_return!(active_sessions.get_mut(message.host_name()));
        active_session.pop_resource(message);
    }

    async fn http_response_action(&self, payload: &[u8], host_name: String, id: NumId) -> Option<StreamMessage> {
        let request = result_early_return!(bincode::deserialize(payload), None);
        let socket = self.local_hosts.get(&host_name).unwrap();
        let response = http::make_request(request, &socket.to_string()).await;
        let response_bytes = result_early_return!(bincode::serialize(&response), None);
        Some(StreamMessage::new(
            host_name,
            id,
            StreamMessageKind::Resource(StreamMessageInnerKind::Response),
            response_bytes))
    }
    
    async fn distribution_response_action(&mut self, payload: Vec<u8>, host_name: String, id: NumId) -> Option<StreamMessage> {
        let result = self.dmessage_staging.stage_message(payload, host_name.clone()).await;
        if result.len() > 0 && result[0] == 1 {
            self.local_hosts.insert(host_name.clone(), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3000));
        }
        Some(StreamMessage::new(host_name, id, StreamMessageKind::Distribution(StreamMessageInnerKind::Response), result))
    }
}

#[derive(Debug)]
struct ActiveSessionInfo {
    dests: HashSet<SocketAddrV4>,
    cached_messages: TransientMap<NumId, (StreamMessage, mpsc::UnboundedSender<StreamResponseType>)>,
    //TODO: Make transient
    resource_queue: VecDeque<NumId>
}

impl ActiveSessionInfo {
    fn new() -> Self { Self { dests: HashSet::new(), cached_messages: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS), false), resource_queue: VecDeque::new() } }
    fn dests(&self) -> HashSet<SocketAddrV4> { self.dests.clone() }
    fn handle_cached_message<'a>(id: NumId, cached_message: Option<&'a mut (StreamMessage, mpsc::UnboundedSender<StreamResponseType>)>) -> Option<&'a mut StreamMessage> {
        let Some(cached_message) = cached_message else {
            debug!(%id, "Prevented request from client, reason: request expired for resource"); return None;
        };
        if let StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request) = cached_message.0.kind {
            return Some(&mut cached_message.0);
        }
        debug!(%id, "Prevented request from client, reason: already received resource"); None
    }
    fn initial_request(&mut self, dest: SocketAddrV4, mut key_agreement: StreamMessage, action: impl Fn(&mut StreamMessage, &mut StreamMessage, SocketAddrV4)) {
        if self.dests.contains(&dest) {
            return;
        }
        self.dests.insert(dest);
        self.cached_messages.set_timer(key_agreement.id());
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = option_early_return!(Self::handle_cached_message(key_agreement.id(), cached_messages.get_mut(&key_agreement.id())));
        action(&mut key_agreement, cached_message, dest)
    }
    fn follow_up(&mut self, id: NumId, action: impl Fn(&mut StreamMessage, &HashSet<SocketAddrV4>)) -> bool {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = match Self::handle_cached_message(id, cached_messages.get_mut(&id)) { Some(cached_message) => cached_message, None => { return false; } };
        action(cached_message, &self.dests);
        true
    }
    fn push_resource(&mut self, message: StreamMessage, set_timer: bool, to_http_handler: mpsc::UnboundedSender<StreamResponseType>) {
        if set_timer {
            self.cached_messages.set_timer(message.id());
        }
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        if cached_messages.contains_key(&message.id()) {
            return warn!(id = %message.id(), "ActiveSessionInfo: Attempted to insert duplicate request");
        }
        self.resource_queue.push_back(message.id());
        cached_messages.insert(message.id(), (message, to_http_handler));
    }
    fn pop_resource(&mut self, message: StreamMessage) {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let Some((cached_message, _)) = cached_messages.get_mut(&message.id()) else {
            return debug!(senders = ?message.senders(), id = %message.id(), "Blocked response from host, reason: unsolicited response for resource")
        };
        *cached_message = message;
        while let Some(id) = self.resource_queue.front() {
            let kind = &cached_messages.get(id).unwrap().0.kind;
            if let StreamMessageKind::Resource(StreamMessageInnerKind::Response) = kind {
                let (cached_message, tx) = cached_messages.remove(&self.resource_queue.pop_front().unwrap()).unwrap();
                trace!(id = %cached_message.id(), "Popped");
                let response = bincode::deserialize(cached_message.payload()).unwrap_or_else(|e| http::construct_error_response((*e).to_string(), String::from("HTTP/1.1")));
                result_early_return!(tx.send(StreamResponseType::Http(response)));
            }
            else if let StreamMessageKind::Distribution(StreamMessageInnerKind::Response) = kind {
                let (cached_message, tx) = cached_messages.remove(&self.resource_queue.pop_front().unwrap()).unwrap();
                let (id, payload) = cached_message.into_hash_payload();
                let message = if payload.len() == 1 { StreamResponseType::Distribution(NumId(payload[0] as u128)) } else { StreamResponseType::Distribution(id) };
                result_early_return!(tx.send(message));
            }
            else {
                break;
            }
        }
    }
}

impl Default for ActiveSessionInfo {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DMessageStaging {
    message_staging: TransientMap<String, Vec<Vec<u8>>>
}

impl DMessageStaging {
    pub fn new() -> Self {
        Self { message_staging: TransientMap::new(TtlType::Secs(1800), false) }
    }

    pub async fn stage_message(&self, payload: Vec<u8>, host_name: String) -> Vec<u8> {
        if payload.len() > 0 {
            self.message_staging.map().lock().unwrap().entry(host_name).or_default().push(payload);
            Vec::with_capacity(0)
        }
        else {
            let Some(contents) = ({ let mut message_staging = self.message_staging.map().lock().unwrap(); message_staging.remove(&host_name) }) else { return vec![0u8] };
            if fs::write("C:/Users/fredk/Downloads/p2p-dump.tar.gz", &contents.concat()).await.is_ok() { vec![1u8] } else { vec![0u8] }
        }
    }
}

pub enum StreamResponseType {
    Http(SerdeHttpResponse),
    Distribution(NumId)
}
impl StreamResponseType {
    pub fn unwrap_http(self) -> SerdeHttpResponse { if let Self::Http(response) = self { response } else { panic!() }}
    pub fn unwrap_distribution(self) -> NumId { if let Self::Distribution(response) = self { response } else { panic!() }}
}