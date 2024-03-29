use std::{collections::{HashMap, HashSet, VecDeque}, net::{Ipv4Addr, SocketAddrV4}, sync::{Arc, Mutex}, time::Duration};
use tokio::{fs, sync::mpsc::{self, UnboundedSender}, time::sleep};
use tracing::{debug, error, instrument, trace, warn};
use crate::{http::{self, SerdeHttpResponse}, message::{Heartbeat, Message, NumId, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, message_processing::ToBeEncrypted, node::EndpointPair, option_early_return, result_early_return, utils::{TransientMap, TransientSet, TtlType}};
use super::{EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, SRP_TTL_SECONDS};

pub struct StreamMessageProcessor
{
    session_manager: SessionManager,
    from_staging: mpsc::UnboundedReceiver<StreamMessage>,
    from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>,
    local_hosts: HashMap<String, SocketAddrV4>,
    dmessage_staging: DMessageStaging
}
impl StreamMessageProcessor
{
    pub fn new(outbound_gateway: OutboundGateway, from_staging: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>) -> Self {
        Self
        {
            session_manager: SessionManager::new(outbound_gateway),
            from_staging,
            from_http_handler,
            local_hosts,
            dmessage_staging: DMessageStaging::new()
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let message = self.from_staging.recv().await?;
        match message {
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::KeyAgreement) | StreamMessageKind::Distribution(StreamMessageInnerKind::KeyAgreement), ..} => self.handle_key_agreement(message),
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request), .. } => self.handle_request(message).await,
            StreamMessage { kind: StreamMessageKind::Resource(StreamMessageInnerKind::Response) | StreamMessageKind::Distribution(StreamMessageInnerKind::Response), ..} => self.session_manager.return_resource(message)
        }
        Some(())
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.host_name = message.host_name()))]
    fn handle_key_agreement(&mut self, message: StreamMessage) {
        self.session_manager.initial_request(message)
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.id = %message.id()))]
    async fn handle_request(&mut self, mut message: StreamMessage) {
        let host_name = message.host_name().to_owned();
        let sender = message.only_sender();
        if sender == EndpointPair::default_socket() {
            let Ok(to_http_handler) = self.from_http_handler.try_recv() else { panic!("Oh fuck nah") };
            self.session_manager.session_logic(IsHost::False((to_http_handler, message)), host_name).await;
        }
        else {
            self.session_manager.session_logic(IsHost::True(sender), host_name).await;
            let (id, payload, host_name, kind) = message.into_hash_payload_host_name_kind();
            self.send_response(payload, sender, host_name, id, kind).await;
        }
    }

    #[instrument(level = "trace", skip(self, payload))]
    async fn send_response(&mut self, payload: Vec<u8>, dest: SocketAddrV4, host_name: String, id: NumId, kind: StreamMessageKind) {
        let response = match kind {
            StreamMessageKind::Resource(_) => self.http_response_action(&payload, host_name, id).await,
            StreamMessageKind::Distribution(_) => self.distribution_response_action(payload, host_name, id).await
        };
        if let Some(mut response) = response {
            self.session_manager.outbound_gateway.send_individual(dest, &mut response, true, true);
        }
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

enum IsHost {
    True(SocketAddrV4),
    False((mpsc::UnboundedSender<StreamResponseType>, StreamMessage))
}

struct SessionManager {
    active_sessions_client: TransientMap<String, ActiveSessionInfo>,
    active_sessions_host: TransientMap<String, ActiveDests>,
    outbound_gateway: OutboundGateway
}

impl SessionManager {
    fn new(outbound_gateway: OutboundGateway) -> Self {
        Self {
            active_sessions_client: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true),
            active_sessions_host: TransientMap::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true),
            outbound_gateway
        }
    }

    async fn session_logic(&mut self, is_host: IsHost, host_name: String) {
        match is_host {
            IsHost::True(dest) => self.renew_dests_host(host_name, dest),
            IsHost::False((to_http_handler, message)) => self.renew_dests_client(host_name, message, to_http_handler)
        };
    }

    fn renew_dests_host(&mut self, host_name: String, dest: SocketAddrV4) {
        let new_entry = self.active_sessions_host.set_timer(host_name.clone());
        let mut active_sessions_host = self.active_sessions_host.map().lock().unwrap();
        let active_dests = active_sessions_host.entry(host_name).or_default();
        self.keep_peer_conns_alive(active_dests.dests.set().clone(), new_entry);
        active_dests.dests.insert(dest);
        let mut key_store = self.outbound_gateway.key_store.lock().unwrap();
        key_store.reset_expiration(dest);
    }

    fn renew_dests_client(&mut self, host_name: String, mut message: StreamMessage, to_http_handler: UnboundedSender<StreamResponseType>) {
        let new_entry = self.active_sessions_client.set_timer(host_name.clone());
        let mut active_sessions_client = self.active_sessions_client.map().lock().unwrap();
        let active_session_info = active_sessions_client.entry(host_name.clone()).or_default();
        self.keep_peer_conns_alive(active_session_info.active_dests.dests.set().clone(), new_entry);
        let dests = active_session_info.active_dests.dests_cloned();
        let set_cached_message_timer = dests.len() > 0;
        for dest in dests {
            self.outbound_gateway.send_individual(dest, &mut message, true, true);
            active_session_info.active_dests.dests.insert(dest);
        }
        let id = message.id();
        trace!(%id, "Pushed");
        if active_session_info.push_resource(message, set_cached_message_timer, to_http_handler) && set_cached_message_timer {
            self.start_follow_ups(host_name, id);
        }
    }
    
    #[instrument(level = "trace", skip_all)]
    fn keep_peer_conns_alive(&self, dests: Arc<Mutex<HashSet<SocketAddrV4>>>, begin: bool) {
        if !begin {
            return;
        }
        let (socket, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.myself);
        tokio::spawn(async move {
            loop {
                //TODO: Don't send to dests that are already peers
                for peer in dests.lock().unwrap().iter() {
                    OutboundGateway::send_static(&socket, *peer, myself, &mut Heartbeat::new(), ToBeEncrypted::False, true);
                }
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
            }
        });
    }

    #[instrument(level = "trace", skip(self))]
    fn start_follow_ups(&self, host_name: String, id: NumId) {
        let (socket, key_store, active_sessions, myself) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.active_sessions_client.map().clone(), self.outbound_gateway.myself);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut active_sessions = active_sessions.lock().unwrap();
                let Some(active_session) = active_sessions.get_mut(&host_name) else { return };
                if !active_session.follow_up(id, |request, dests| {                  
                    for dest in dests.iter() {
                        debug!(id = %request.id(), %dest, "Sending follow up");
                        OutboundGateway::send_static(&socket, *dest, myself, request, ToBeEncrypted::True(key_store.clone()), true);
                    }
                }) {
                    return
                }
            }
        });
    }

    fn initial_request(&self, key_agreement: StreamMessage) {
        let mut active_sessions = self.active_sessions_client.map().lock().unwrap();
        let active_session = active_sessions.get_mut(key_agreement.host_name()).unwrap();
        let (host_name, id) = (key_agreement.host_name().to_owned(), key_agreement.id());
        if active_session.initial_request(key_agreement, |message, cached_message, dest| {
            self.outbound_gateway.send_individual(dest, message, false, false);
            self.outbound_gateway.send_individual(dest, cached_message, true, true);
        }) {
            self.start_follow_ups(host_name, id);
        }
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.id = %message.id()))]
    fn return_resource(&self, message: StreamMessage) {
        let mut active_sessions = self.active_sessions_client.map().lock().unwrap();
        let active_session = option_early_return!(active_sessions.get_mut(message.host_name()));
        active_session.pop_resource(message);
    }
}

struct ActiveDests {
    dests: TransientSet<SocketAddrV4>
}

impl ActiveDests {
    fn new() -> Self {
        Self { dests: TransientSet::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true) }
    }

    fn dests_cloned(&self) -> HashSet<SocketAddrV4> { self.dests.set().lock().unwrap().clone() }
}

impl Default for ActiveDests {
    fn default() -> Self {
        Self::new()
    }
}

struct ActiveSessionInfo {
    active_dests: ActiveDests,
    cached_messages: TransientMap<NumId, (StreamMessage, mpsc::UnboundedSender<StreamResponseType>)>,
    //TODO: Make transient
    resource_queue: VecDeque<NumId>
}

impl ActiveSessionInfo {
    fn new() -> Self {
        Self {
            active_dests: ActiveDests::new(),
            cached_messages: TransientMap::new(TtlType::Secs(SRP_TTL_SECONDS), false),
            resource_queue: VecDeque::new()
        }
    }

    fn handle_cached_message<'a>(id: NumId, cached_message: Option<&'a mut (StreamMessage, mpsc::UnboundedSender<StreamResponseType>)>) -> Option<&'a mut StreamMessage> {
        let Some(cached_message) = cached_message else {
            debug!(%id, "Prevented request from client, reason: request expired for resource"); return None;
        };
        if let StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request) = cached_message.0.kind {
            return Some(&mut cached_message.0);
        }
        debug!(%id, "Prevented request from client, reason: already received resource"); None
    }

    fn initial_request(&mut self, mut key_agreement: StreamMessage, action: impl Fn(&mut StreamMessage, &mut StreamMessage, SocketAddrV4)) -> bool {
        let dest = key_agreement.dest();
        if self.active_dests.dests.set().lock().unwrap().contains(&dest) {
            return false;
        }
        self.active_dests.dests.insert(dest);
        self.cached_messages.set_timer(key_agreement.id());
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = option_early_return!(Self::handle_cached_message(key_agreement.id(), cached_messages.get_mut(&key_agreement.id())), false);
        action(&mut key_agreement, cached_message, dest);
        true
    }

    fn follow_up(&mut self, id: NumId, action: impl Fn(&mut StreamMessage, &HashSet<SocketAddrV4>)) -> bool {
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        let cached_message = match Self::handle_cached_message(id, cached_messages.get_mut(&id)) { Some(cached_message) => cached_message, None => { return false; } };
        action(cached_message, &self.active_dests.dests.set().lock().unwrap());
        true
    }

    fn push_resource(&mut self, message: StreamMessage, set_timer: bool, to_http_handler: mpsc::UnboundedSender<StreamResponseType>) -> bool {
        if set_timer {
            self.cached_messages.set_timer(message.id());
        }
        let mut cached_messages = self.cached_messages.map().lock().unwrap();
        if cached_messages.contains_key(&message.id()) {
            warn!(id = %message.id(), "ActiveSessionInfo: Attempted to insert duplicate request");
            return false;
        }
        self.resource_queue.push_back(message.id());
        cached_messages.insert(message.id(), (message, to_http_handler));
        true
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

struct DMessageStaging {
    message_staging: TransientMap<String, Vec<Vec<u8>>>
}

impl DMessageStaging {
    fn new() -> Self {
        Self { message_staging: TransientMap::new(TtlType::Secs(1800), false) }
    }

    async fn stage_message(&self, payload: Vec<u8>, host_name: String) -> Vec<u8> {
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