use std::{collections::{HashMap, HashSet}, net::{Ipv4Addr, SocketAddrV4}, sync::{Arc, Mutex}, time::Duration};
use tokio::{fs, net::UdpSocket, sync::mpsc::{self, UnboundedSender}, time::sleep};
use tracing::{debug, instrument, trace, warn};
use crate::{crypto::KeyStore, http::{self, SerdeHttpResponse}, lock, message::{Heartbeat, Message, NumId, Peer, Sender, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, message_processing::ToBeEncrypted, option_early_return, result_early_return, utils::{ArcCollection, ArcDeque, ArcMap, ArcSet, TransientCollection, TtlType}};
use super::{EmptyOption, OutboundGateway, ACTIVE_SESSION_TTL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, SRP_TTL_SECONDS};

pub struct StreamMessageProcessor
{
    session_manager: SessionManager,
    sm_from: mpsc::UnboundedReceiver<StreamMessage>,
    from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>,
    local_hosts: HashMap<String, SocketAddrV4>,
    dmessage_staging: DMessageStaging
}
impl StreamMessageProcessor
{
    pub fn new(outbound_gateway: OutboundGateway, sm_from: mpsc::UnboundedReceiver<StreamMessage>, local_hosts: HashMap<String, SocketAddrV4>, from_http_handler: mpsc::UnboundedReceiver<mpsc::UnboundedSender<StreamResponseType>>) -> Self {
        Self
        {
            session_manager: SessionManager::new(outbound_gateway),
            sm_from,
            from_http_handler,
            local_hosts,
            dmessage_staging: DMessageStaging::new()
        }
    }

    pub async fn receive(&mut self) -> EmptyOption {
        let message = self.sm_from.recv().await?;
        match message {
            StreamMessage { kind: StreamMessageKind::KeyAgreement, ..} => self.handle_key_agreement(message),
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
        if let Some(sender) = sender {
            self.session_manager.session_logic(IsHost::True(sender), host_name).await;
            let (id, payload, host_name, kind) = message.into_hash_payload_host_name_kind();
            self.send_response(payload, sender.socket, host_name, id, kind).await;
        }
        else {
            let Ok(to_http_handler) = self.from_http_handler.try_recv() else { panic!("Oh fuck nah") };
            self.session_manager.session_logic(IsHost::False((to_http_handler, message)), host_name).await;
        }
    }

    #[instrument(level = "trace", skip(self, payload))]
    async fn send_response(&mut self, payload: Vec<u8>, dest: SocketAddrV4, host_name: String, id: NumId, kind: StreamMessageKind) {
        let response = match kind {
            StreamMessageKind::Resource(_) => self.http_response_action(&payload, host_name, id).await,
            StreamMessageKind::Distribution(_) => self.distribution_response_action(payload, host_name, id).await,
            _ => unimplemented!()
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
    True(Sender),
    False((mpsc::UnboundedSender<StreamResponseType>, StreamMessage))
}

struct SessionManager {
    active_sessions_client: TransientCollection<ArcMap<String, ActiveSessionInfo>>,
    active_sessions_host: TransientCollection<ArcMap<String, ActiveDests>>,
    outbound_gateway: OutboundGateway
}

impl SessionManager {
    fn new(outbound_gateway: OutboundGateway) -> Self {
        Self {
            active_sessions_client: TransientCollection::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true, ArcMap::new()),
            active_sessions_host: TransientCollection::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true, ArcMap::new()),
            outbound_gateway
        }
    }

    async fn session_logic(&mut self, is_host: IsHost, host_name: String) {
        match is_host {
            IsHost::True(dest) => self.renew_dests_host(host_name, dest),
            IsHost::False((to_http_handler, message)) => self.renew_dests_client(host_name, message, to_http_handler)
        };
    }

    fn renew_dests_host(&mut self, host_name: String, dest: Sender) {
        let new_entry = self.active_sessions_host.set_timer(host_name.clone(), "Stream:ActiveSessionsHostRenew");
        let mut active_sessions_host = lock!(self.active_sessions_host.collection().map());
        let active_dests = active_sessions_host.entry(host_name).or_default();
        self.keep_peer_conns_alive(active_dests.dests.collection().set().clone(), new_entry);
        active_dests.dests.insert(dest, "Stream:ActiveDestsHostRenew");
        let mut key_store = lock!(self.outbound_gateway.key_store);
        key_store.reset_expiration(dest.socket);
    }

    fn renew_dests_client(&mut self, host_name: String, mut message: StreamMessage, to_http_handler: UnboundedSender<StreamResponseType>) {
        let new_entry = self.active_sessions_client.set_timer(host_name.clone(), "Stream:ActiveSessionsClientRenew");
        let mut active_sessions_client = lock!(self.active_sessions_client.collection().map());
        let active_session_info = active_sessions_client.entry(host_name.clone()).or_default();
        self.keep_peer_conns_alive(active_session_info.active_dests.dests.collection().set().clone(), new_entry);
        let dests = active_session_info.active_dests.dests_cloned();
        let set_cached_message_timer = dests.len() > 0;
        for dest in dests {
            self.outbound_gateway.send_individual(dest.socket, &mut message, true, true);
            active_session_info.active_dests.dests.insert(dest, "Stream:ActiveDestsClientRenew");
        }
        let id = message.id();
        trace!(%id, "Pushed");
        active_session_info.push_resource(message, if set_cached_message_timer { Some((self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.outbound_gateway.myself)) } else { None }, to_http_handler);
    }
    
    #[instrument(level = "trace", skip_all)]
    fn keep_peer_conns_alive(&self, dests: Arc<Mutex<HashSet<Sender>>>, begin: bool) {
        if !begin {
            return;
        }
        let (socket, myself, peer_ops) = (self.outbound_gateway.socket.clone(), self.outbound_gateway.myself, self.outbound_gateway.peer_ops.clone());
        tokio::spawn(async move {
            loop {
                {
                    let dests = lock!(dests);
                    let mut dests_filtered = dests.iter().filter(|peer|peer.id != myself.id && !lock!(peer_ops).has_peer(peer.id));
                    if (&mut dests_filtered).peekable().peek().is_none() { return };
                    for peer in dests_filtered {
                        OutboundGateway::send_static(&socket, peer.socket, myself, &mut Heartbeat::new(), ToBeEncrypted::False, true);
                    }
                }
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS)).await;
            }
        });
    }

    fn initial_request(&self, key_agreement: StreamMessage) {
        let mut active_sessions = lock!(self.active_sessions_client.collection().map());
        let active_session = active_sessions.get_mut(key_agreement.host_name()).unwrap();
        active_session.initial_request(key_agreement, |message, cached_message, dest| {
            self.outbound_gateway.send_individual(dest, message, false, false);
            self.outbound_gateway.send_individual(dest, cached_message, true, true);
        }, (self.outbound_gateway.socket.clone(), self.outbound_gateway.key_store.clone(), self.outbound_gateway.myself));
    }

    #[instrument(level = "trace", skip_all, fields(message.senders = ?message.senders(), message.id = %message.id()))]
    fn return_resource(&self, message: StreamMessage) {
        let mut active_sessions = lock!(self.active_sessions_client.collection().map());
        let active_session = option_early_return!(active_sessions.get_mut(message.host_name()));
        active_session.pop_resource(message);
    }
}

struct ActiveDests {
    dests: TransientCollection<ArcSet<Sender>>
}

impl ActiveDests {
    fn new() -> Self {
        Self { dests: TransientCollection::new(TtlType::Secs(ACTIVE_SESSION_TTL_SECONDS), true, ArcSet::new()) }
    }

    fn dests_cloned(&self) -> HashSet<Sender> { lock!(self.dests.collection().set()).clone() }
}

impl Default for ActiveDests {
    fn default() -> Self {
        Self::new()
    }
}

type FollowUpComponents = (Arc<UdpSocket>, Arc<Mutex<KeyStore>>, Peer);

struct ActiveSessionInfo {
    active_dests: ActiveDests,
    cached_messages: TransientCollection<ArcMap<NumId, (StreamMessage, Option<mpsc::UnboundedSender<StreamResponseType>>)>>,
    resource_queue: TransientCollection<ArcDeque<NumId>>
}

impl ActiveSessionInfo {
    fn new() -> Self {
        Self {
            active_dests: ActiveDests::new(),
            cached_messages: TransientCollection::new(TtlType::Secs(SRP_TTL_SECONDS), false, ArcMap::new()),
            resource_queue: TransientCollection::new(TtlType::Secs(SRP_TTL_SECONDS), false, ArcDeque::new())
        }
    }

    fn handle_cached_message<'a>(id: NumId, cached_message: Option<&'a mut (StreamMessage, Option<mpsc::UnboundedSender<StreamResponseType>>)>) -> Option<&'a mut StreamMessage> {
        let Some(cached_message) = cached_message else {
            debug!(%id, "Prevented request from client, reason: request expired for resource"); return None;
        };
        if let StreamMessageKind::KeyAgreement | StreamMessageKind::Resource(StreamMessageInnerKind::Request) | StreamMessageKind::Distribution(StreamMessageInnerKind::Request) = cached_message.0.kind {
            return Some(&mut cached_message.0);
        }
        debug!(%id, "Prevented request from client, reason: already received resource"); None
    }

    fn initial_request(&mut self, mut key_agreement: StreamMessage, action: impl Fn(&mut StreamMessage, &mut StreamMessage, SocketAddrV4), follow_up_components: FollowUpComponents) {
        let dest = key_agreement.only_sender().unwrap();
        if self.active_dests.dests.contains_key(&dest) {
            return;
        }
        self.active_dests.dests.insert(dest, "Stream:ActiveSessionInfo:ActiveDests");
        let id = key_agreement.id();
        let cached_key_agreement_id = NumId(id.0 + 1);
        self.cached_messages.set_timer(cached_key_agreement_id, "Stream:ActiveSessionInfo:CachedMessagesKeyAgreement");
        self.resource_queue.set_timer_with_override(id, "Stream:ActiveSessionInfo:ResourceQueue");
        self.cached_messages.set_timer_with_override(id, "Stream:ActiveSessionInfo:CachedMessages");
        let mut cached_messages = lock!(self.cached_messages.collection().map());
        {
            let cached_message = option_early_return!(Self::handle_cached_message(id, cached_messages.get_mut(&id)));
            action(&mut key_agreement, cached_message, dest.socket);
        }
        cached_messages.insert(cached_key_agreement_id, (key_agreement, None));
        self.start_follow_ups(cached_key_agreement_id, follow_up_components.clone());
        self.start_follow_ups(id, follow_up_components);
    }

    #[instrument(level = "trace", skip_all, fields(id))]
    fn start_follow_ups(&self, id: NumId, follow_up_components: FollowUpComponents) {
        let (dests, cached_messages) = (self.active_dests.dests.collection().clone(), self.cached_messages.collection().map().clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(2)).await;
                let mut cached_messages = lock!(cached_messages);
                let cached_message = match Self::handle_cached_message(id, cached_messages.get_mut(&id)) { Some(cached_message) => cached_message, None => { return } };
                for dest in lock!(dests.set()).iter() {
                    debug!(%id, ?dest, "Sending follow up");
                    let (ref socket, ref key_store, myself) = follow_up_components;
                    OutboundGateway::send_static(socket, dest.socket, myself, cached_message, ToBeEncrypted::True(key_store.clone()), true);
                }
            }
        });
    }

    fn push_resource(&mut self, message: StreamMessage, set_timer: Option<FollowUpComponents>, to_http_handler: mpsc::UnboundedSender<StreamResponseType>) {
        if let Some(follow_up_components) = set_timer {
            self.cached_messages.set_timer(message.id(), "Stream:ActiveSessionInfo:CachedMessages");
            self.start_follow_ups(message.id(), follow_up_components);
        }
        let mut cached_messages = lock!(self.cached_messages.collection().map());
        if cached_messages.contains_key(&message.id()) {
            warn!(id = %message.id(), "ActiveSessionInfo: Attempted to insert duplicate request");
            return;
        }
        self.resource_queue.collection_mut().push(message.id());
        cached_messages.insert(message.id(), (message, Some(to_http_handler)));
    }

    fn pop_resource(&mut self, message: StreamMessage) {
        let mut cached_messages = lock!(self.cached_messages.collection().map());
        let Some((cached_message, _)) = cached_messages.get_mut(&message.id()) else {
            return debug!(senders = ?message.senders(), id = %message.id(), "Blocked response from host, reason: unsolicited response for resource")
        };
        *cached_message = message;
        while let Some(id) = self.resource_queue.collection().front() {
            let kind = &cached_messages.get(&id).unwrap().0.kind;
            if let StreamMessageKind::Resource(StreamMessageInnerKind::Response) = kind {
                let (cached_message, tx) = cached_messages.remove(&self.resource_queue.pop(&id).unwrap()).unwrap();
                cached_messages.remove(&NumId(id.0 + 1));
                trace!(id = %cached_message.id(), "Popped");
                let response = bincode::deserialize(cached_message.payload()).unwrap_or_else(|e| http::construct_error_response((*e).to_string(), String::from("HTTP/1.1")));
                result_early_return!(tx.unwrap().send(StreamResponseType::Http(response)));
            }
            else if let StreamMessageKind::Distribution(StreamMessageInnerKind::Response) = kind {
                let (cached_message, tx) = cached_messages.remove(&self.resource_queue.pop(&id).unwrap()).unwrap();
                let (id, payload) = cached_message.into_hash_payload();
                let message = if payload.len() == 1 { StreamResponseType::Distribution(NumId(payload[0] as u128)) } else { StreamResponseType::Distribution(id) };
                result_early_return!(tx.unwrap().send(message));
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
    message_staging: TransientCollection<ArcMap<String, Vec<Vec<u8>>>>
}

impl DMessageStaging {
    fn new() -> Self {
        Self { message_staging: TransientCollection::new(TtlType::Secs(1800), false, ArcMap::new()) }
    }

    async fn stage_message(&mut self, payload: Vec<u8>, host_name: String) -> Vec<u8> {
        if payload.len() > 0 {
            lock!(self.message_staging.collection().map()).entry(host_name).or_default().push(payload);
            Vec::with_capacity(0)
        }
        else {
            let Some(contents) = self.message_staging.pop(&host_name) else { return vec![0u8] };
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