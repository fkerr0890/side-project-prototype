use crate::{
    crypto::{Direction, KeyStore},
    event::{TimeboundAction, TimeboundEventManager},
    http::SerdeHttpResponse,
    lock,
    message::{
        DiscoverMetadata, DistributeMetadata, DpMessageKind, InboundMessage, KeyAgreementMessage,
        Message, MessageDirection, MessageDirectionAgreement, MetadataKind, NumId, Peer,
        SearchMetadata, SearchMetadataKind, Sender, StreamMetadata, StreamPayloadKind,
    },
    message_processing::{DISTRIBUTION_SEARCH_TTL_SECONDS, HEARTBEAT_INTERVAL_SECONDS},
    option_early_return,
    peer::PeerOps,
    result_early_return,
};
use ring::aead;
use rustc_hash::{FxHashMap, FxHashSet};
use std::{
    fmt::Debug,
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{select, sync::mpsc};
use tracing::{debug, info, instrument, trace, warn};

use super::{
    search,
    stream::{DistributionResponse, StreamSessionManager},
    BreadcrumbService, CipherReceiver, DiscoverPeerProcessor, EmptyOption, OutboundGateway,
    Payload, DISTRIBUTION_TTL_SECONDS, DPP_TTL_MILLIS, SRP_TTL_SECONDS,
};

type CachedOutboundMessages = Vec<(Message, Vec<Peer>)>;
type SendCheckedInput<T> = (T, Message, bool, Option<NumId>);

pub struct MessageStaging {
    from_inbound_gateway: CipherReceiver,
    message_staging: FxHashMap<NumId, FxHashMap<usize, InboundMessage>>,
    cached_outbound_messages: FxHashMap<NumId, CachedOutboundMessages>,
    unconfirmed_peers: FxHashMap<NumId, Peer>,
    outbound_gateway: OutboundGateway,
    key_store: KeyStore,
    peer_ops: Arc<Mutex<PeerOps>>,
    breadcrumb_service: BreadcrumbService,
    discover_peer_processor: DiscoverPeerProcessor,
    stream_session_manager: StreamSessionManager,
    client_api_tx: mpsc::UnboundedSender<ClientApiRequest>,
    client_api_rx: mpsc::UnboundedReceiver<ClientApiRequest>,
    to_http_handlers: FxHashMap<NumId, mpsc::UnboundedSender<SerdeHttpResponse>>,
    from_http_handlers:
        mpsc::UnboundedReceiver<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>,
    cached_stream_messages: FxHashMap<NumId, Message>,
    event_manager: TimeboundEventManager,
}

impl MessageStaging {
    pub fn new(
        from_inbound_gateway: CipherReceiver,
        outbound_gateway: OutboundGateway,
        discover_peer_processor: DiscoverPeerProcessor,
        client_api_tx: mpsc::UnboundedSender<ClientApiRequest>,
        client_api_rx: mpsc::UnboundedReceiver<ClientApiRequest>,
        from_http_handlers: mpsc::UnboundedReceiver<(
            Message,
            mpsc::UnboundedSender<SerdeHttpResponse>,
        )>,
        local_hosts: FxHashMap<String, SocketAddrV4>,
        intial_peers: Vec<Peer>,
    ) -> Self {
        let mut peer_ops = PeerOps::new();
        for peer in intial_peers {
            peer_ops.add_initial_peer(peer)
        }
        let mut ret = Self {
            from_inbound_gateway,
            message_staging: FxHashMap::default(),
            cached_outbound_messages: FxHashMap::default(),
            unconfirmed_peers: FxHashMap::default(),
            outbound_gateway,
            key_store: KeyStore::new(),
            peer_ops: Arc::new(Mutex::new(peer_ops)),
            breadcrumb_service: BreadcrumbService::new(),
            client_api_tx,
            client_api_rx,
            discover_peer_processor,
            stream_session_manager: StreamSessionManager::new(local_hosts),
            to_http_handlers: FxHashMap::default(),
            from_http_handlers,
            cached_stream_messages: FxHashMap::default(),
            event_manager: TimeboundEventManager::new(Duration::from_millis(500)),
        };
        ret.event_manager
            .put_event(TimeboundAction::SendHeartbeats, HEARTBEAT_INTERVAL_SECONDS);
        ret
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    pub async fn receive(&mut self) -> EmptyOption {
        let send_checked_input = select! {
            message = self.from_inbound_gateway.recv() => return Some(self.process_inbound_message(message?).await),
            message = self.client_api_rx.recv() => return Some(self.process_client_request(message?).await),
            message = self.from_http_handlers.recv() => self.initial_retrieval_request(message?).await,
            id = self.stream_session_manager.follow_up_rx().recv() => self.process_follow_up(id?).await,
            action = self.event_manager.tick() => return Some(self.process_timebound_action(action).await)
        };
        self.pre_send_checked(send_checked_input).await;
        Some(())
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn pre_send_checked_multiple<T: IntoIterator<Item = Peer> + Debug>(
        &mut self,
        inputs: Vec<SendCheckedInput<T>>,
    ) {
        for input in inputs {
            self.pre_send_checked(Some(input)).await;
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn pre_send_checked<T: IntoIterator<Item = Peer> + Debug>(
        &mut self,
        input: Option<SendCheckedInput<T>>,
    ) {
        let (dests, message, to_be_chunked, cache_id) = option_early_return!(input);
        let message = option_early_return!(self.send_checked(dests, message, to_be_chunked).await);
        if let Some(id) = cache_id {
            self.insert_cached_stream_message(id, message);
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn process_inbound_message(&mut self, message: (SocketAddrV4, Payload)) {
        let (sender_addr, message_bytes) = message;
        if let Ok(message) = bincode::deserialize::<KeyAgreementMessage>(&message_bytes.1) {
            let result = self.handle_key_agreement(message, sender_addr);
            return self.post_handle_key_agreement(result).await;
        }
        if let Some(message) = self.stage_message(message_bytes, sender_addr) {
            self.send_outbound_message(message).await;
        };
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn stage_message(
        &mut self,
        message_bytes: Payload,
        sender_addr: SocketAddrV4,
    ) -> Option<Message> {
        let (length, message_bytes) = message_bytes;
        if length < aead::NONCE_LEN + 16 {
            return None;
        }
        let (message_bytes, _tail) = message_bytes.split_at(length);
        let (message_bytes, suffix) = message_bytes.split_at(length - aead::NONCE_LEN - 16);
        let (peer_id_bytes, nonce) = suffix.split_at(suffix.len() - aead::NONCE_LEN);
        let peer_id = NumId(u128::from_be_bytes(peer_id_bytes.try_into().unwrap()));
        let mut message_bytes = message_bytes.to_vec();
        message_bytes = result_early_return!(
            self.key_store
                .transform(peer_id, &mut message_bytes, Direction::Decode(nonce)),
            None
        );
        let inbound_message: InboundMessage =
            result_early_return!(bincode::deserialize(&message_bytes), None);
        if inbound_message.separate_parts().sender().socket != sender_addr {
            warn!(sender = %inbound_message.separate_parts().sender().socket, actual_sender = %sender_addr, "Sender doesn't match actual sender");
            return None;
        }
        if let Some(peer) = self
            .unconfirmed_peers
            .remove(&inbound_message.separate_parts().sender().id)
        {
            // println!("Confirmed peer {}", inbound_message.separate_parts().sender().id());
            self.add_new_peer(peer);
        }
        let (index, num_chunks) = inbound_message.separate_parts().position();
        if num_chunks == 1 {
            return Some(self.reassemble_message(vec![inbound_message]));
        }
        let id = inbound_message.separate_parts().id();
        let staged_messages_len = {
            if !self.message_staging.contains_key(&id) {
                self.event_manager
                    .put_event(TimeboundAction::RemoveStagedMessage(id), SRP_TTL_SECONDS);
            }
            let staged_messages = self
                .message_staging
                .entry(id)
                .or_insert(FxHashMap::default());
            staged_messages.insert(index, inbound_message);
            staged_messages.len()
        };
        if staged_messages_len == num_chunks {
            let messages = self.message_staging.remove(&id)?;
            let messages: Vec<InboundMessage> = messages.into_values().collect();
            return Some(self.reassemble_message(messages));
        }
        None
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn reassemble_message(&mut self, message_parts: Vec<InboundMessage>) -> Message {
        let (message_bytes, senders, timestamp) = InboundMessage::reassemble_message(message_parts);
        let mut message = bincode::deserialize::<Message>(&message_bytes).unwrap();
        for sender in senders {
            message.set_sender(sender);
        }
        message.set_timestamp(timestamp);
        // if let MetadataKind::Heartbeat = message.metadata() {
        //     info!(?message, "Heartbeat");
        // }
        message
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn process_client_request(&mut self, request: ClientApiRequest) {
        match request {
            ClientApiRequest::Message(mut message) => {
                let id = message.id();
                match message.metadata_mut() {
                    MetadataKind::Discover(metadata) => {
                        if let (true, introducer) = self.client_discover_logic(metadata, id) {
                            self.send_checked(vec![introducer.unwrap()], message, false)
                                .await;
                            return;
                        }
                    }
                    MetadataKind::Distribute(metadata) => {
                        return self
                            .initial_distribution_request(id, metadata.clone())
                            .await
                    }
                    _ => unimplemented!(),
                }
            }
            ClientApiRequest::ClearActiveSessions => {
                self.stream_session_manager.clear_all_sources_sinks();
                self.key_store.clear();
            }
            ClientApiRequest::AddHost(host_name) => {
                self.stream_session_manager.add_local_host(
                    String::from(host_name),
                    SocketAddrV4::new("127.0.0.1".parse().unwrap(), 3000),
                );
            }
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn client_discover_logic(
        &mut self,
        metadata: &mut DiscoverMetadata,
        id: NumId,
    ) -> (bool, Option<Peer>) {
        match metadata.kind {
            DpMessageKind::INeedSome => {
                let introducer = metadata.peer_list.pop().unwrap();
                self.try_add_breadcrumb(id, None, Some(DPP_TTL_MILLIS));
                metadata.kind = DpMessageKind::Request;
                (true, Some(introducer))
            }
            _ => (false, None),
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn process_follow_up(&mut self, id: NumId) -> Option<SendCheckedInput<FxHashSet<Peer>>> {
        let message = self.cached_stream_messages.remove(&id)?;
        match message.metadata() {
            MetadataKind::Stream(StreamMetadata {
                payload: StreamPayloadKind::Request(_),
                host_name,
            }) => Some((
                self.stream_session_manager
                    .get_destinations_source_retrieval(host_name),
                message,
                true,
                Some(id),
            )),
            MetadataKind::Stream(StreamMetadata {
                payload: StreamPayloadKind::DistributionRequest(_),
                host_name,
            }) => Some((
                self.stream_session_manager
                    .get_destinations_source_distribution(host_name),
                message,
                true,
                Some(id),
            )),
            _ => panic!(),
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn process_timebound_action(&mut self, actions: Option<Vec<TimeboundAction>>) {
        let actions = option_early_return!(actions);
        trace!(?actions, "process_timebound_action");
        for action in actions {
            match action {
                TimeboundAction::LockDestsDistribution(host_name, id) => {
                    // info!(myself = %self.outbound_gateway.myself.id, sinks = ?self.stream_session_manager.get_all_destinations_source_distribution());
                    self.stream_session_manager
                        .finalize_all_resources_distribution(&host_name);
                    self.stream_session_manager
                        .lock_dests_distribution(&host_name);
                    self.send_distribution_request(id, host_name).await;
                }
                TimeboundAction::SendHeartbeats => {
                    let send_checked_input = Some(self.send_heartbeats());
                    self.pre_send_checked(send_checked_input).await;
                }
                TimeboundAction::RemoveCachedStreamMessage(id) => {
                    self.cached_stream_messages.remove(&id);
                }
                TimeboundAction::RemoveStagedMessage(id) => {
                    self.message_staging.remove(&id);
                }
                TimeboundAction::RemoveHttpHandlerTx(id) => {
                    self.to_http_handlers.remove(&id);
                }
                TimeboundAction::RemoveCachedOutboundMessages(peer_id) => {
                    self.cached_outbound_messages.remove(&peer_id);
                }
                TimeboundAction::RemoveBreadcrumb(id) => {
                    self.breadcrumb_service.remove_breadcrumb(&id)
                }
                TimeboundAction::RemovePrivateKey(peer_id) => {
                    self.key_store.remove_private_key(&peer_id)
                }
                TimeboundAction::RemoveSymmetricKey(peer_id) => {
                    self.key_store.remove_symmetric_key(&peer_id)
                }
                TimeboundAction::RemoveUnconfirmedPeer(peer_id) => {
                    self.unconfirmed_peers.remove(&peer_id);
                }
                TimeboundAction::SendEarlyReturnMessage(message) => {
                    self.send_response(message).await
                }
                TimeboundAction::FinalizeDiscover(id) => {
                    let metadata = self.discover_peer_processor.new_peers(&id);
                    for peer in metadata.peer_list {
                        self.add_new_peer(peer);
                    }
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn insert_cached_stream_message(&mut self, id: NumId, message: Message) {
        assert!(!self.cached_stream_messages.contains_key(&id));
        self.event_manager.put_event(
            TimeboundAction::RemoveCachedStreamMessage(id),
            SRP_TTL_SECONDS,
        );
        self.cached_stream_messages.insert(id, message);
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn send_heartbeats(&mut self) -> SendCheckedInput<FxHashSet<Peer>> {
        let mut peers: FxHashSet<Peer> =
            FxHashSet::from_iter(lock!(self.peer_ops).peers().into_iter());
        peers.extend(
            self.stream_session_manager
                .get_all_destinations_source_retrieval()
                .into_iter(),
        );
        peers.extend(
            self.stream_session_manager
                .get_all_destinations_source_distribution()
                .into_iter(),
        );
        peers.extend(
            self.stream_session_manager
                .get_all_destinations_sink()
                .into_iter(),
        );
        peers.extend(self.unconfirmed_peers.values());
        // info!(?peers, myself = ?self.outbound_gateway.myself);
        self.event_manager
            .put_event(TimeboundAction::SendHeartbeats, HEARTBEAT_INTERVAL_SECONDS);
        (peers, Message::new_heartbeat(Peer::default()), true, None)
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn send_outbound_message(&mut self, mut message: Message) {
        let new_direction = self.get_direction(&mut message);
        match new_direction {
            PropagationDirection::Forward => {
                self.send_request(message).await;
            }
            PropagationDirection::Reverse => self.send_response(message).await,
            PropagationDirection::Final => self.execute_final_action(message).await,
            PropagationDirection::Stop => {
                trace!("Stopped")
            }
        };
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn send_request(&mut self, mut message: Message) -> Option<Message> {
        let prev_sender = message.only_sender();
        message.set_direction(MessageDirection::Request);
        let dests = lock!(self.peer_ops)
            .peers()
            .into_iter()
            .filter(|p| prev_sender.is_none() || prev_sender.unwrap().id != p.id);
        message = self.send_checked(dests, message, false).await?;
        Some(message)
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn send_response(&mut self, mut message: Message) {
        let dest = option_early_return!(
            self.breadcrumb_service.get_dest(&message.id()),
            warn!("Fuck")
        );
        let mut dests = if let Some(dest) = dest {
            vec![Peer::from(dest)]
        } else {
            return self.execute_final_action(message).await;
        };
        match message.metadata() {
            MetadataKind::Search(metadata) => dests.push(metadata.origin),
            MetadataKind::Discover(metadata) => dests.push(metadata.origin),
            _ => {}
        };
        message.set_direction(MessageDirection::Response);
        self.send_checked(dests, message, false).await;
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn send_checked(
        &mut self,
        dests: impl IntoIterator<Item = Peer> + Debug,
        mut message: Message,
        to_be_chunked: bool,
    ) -> Option<Message> {
        let mut remaining_dests = dests.into_iter();
        while let Some(dest) = remaining_dests.next() {
            message.replace_dest(dest);
            message.clear_senders();
            if !self.check_key_agreement(&message).await {
                let mut remaining_dests: Vec<Peer> = remaining_dests.collect();
                remaining_dests.push(dest);
                self.insert_cached_outbound_message(message, remaining_dests);
                return None;
            }
            self.outbound_gateway
                .send(&message, to_be_chunked, &mut self.key_store)
                .await;
        }
        Some(message)
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn check_key_agreement(&mut self, message: &Message) -> bool {
        if self.key_store.agreement_exists(&message.dest().id) {
            return true;
        }
        let public_key = option_early_return!(
            self.key_store
                .public_key(message.dest().id, &mut self.event_manager),
            false
        );
        self.outbound_gateway
            .send_agreement(
                message.dest(),
                public_key,
                MessageDirectionAgreement::Request,
            )
            .await;
        false
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn insert_cached_outbound_message(&mut self, message: Message, remaining_dests: Vec<Peer>) {
        let peer_id = message.dest().id;
        if !self.cached_outbound_messages.contains_key(&peer_id) {
            self.event_manager.put_event(
                TimeboundAction::RemoveCachedOutboundMessages(peer_id),
                SRP_TTL_SECONDS,
            )
        }
        let outbound_messages = self.cached_outbound_messages.entry(peer_id).or_default();
        outbound_messages.push((message, remaining_dests));
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn initial_retrieval_request(
        &mut self,
        from_http_handler: (Message, mpsc::UnboundedSender<SerdeHttpResponse>),
    ) -> Option<SendCheckedInput<FxHashSet<Peer>>> {
        let (message, tx) = from_http_handler;
        let (id, host_name) = (message.id(), message.metadata().host_name().clone());
        assert!(!self.to_http_handlers.contains_key(&id));
        self.event_manager
            .put_event(TimeboundAction::RemoveHttpHandlerTx(id), SRP_TTL_SECONDS);
        self.to_http_handlers.insert(id, tx);
        if self
            .stream_session_manager
            .source_active_retrieval(&host_name)
        {
            self.stream_session_manager
                .push_resource_retrieval(&host_name, id);
            Some((
                self.stream_session_manager
                    .get_destinations_source_retrieval(&host_name),
                message,
                true,
                Some(id),
            ))
        } else {
            self.stream_session_manager
                .new_source_retrieval(host_name.clone());
            self.stream_session_manager
                .push_resource_retrieval(&host_name, id);
            self.insert_cached_stream_message(id, message);
            let search_message = Message::new_search_request(
                id,
                SearchMetadata::new(
                    self.outbound_gateway.myself,
                    host_name,
                    SearchMetadataKind::Retrieval,
                ),
            );
            self.send_outbound_message(search_message).await;
            None
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn initial_distribution_request(&mut self, id: NumId, metadata: DistributeMetadata) {
        if !self
            .stream_session_manager
            .host_installed(&metadata.host_name)
        {
            warn!(myself = %self.outbound_gateway.myself.id, host_name = metadata.host_name, "Blocked distribution request, reason: I do not host this app");
            return;
        }
        if self
            .stream_session_manager
            .source_active_distribution(&metadata.host_name)
        {
            return;
        }
        info!(myself = ?self.outbound_gateway.myself, "new source at");
        self.stream_session_manager
            .new_source_distribution(metadata.host_name.clone(), id, metadata.hop_count)
            .await;
        let file = option_early_return!(self.stream_session_manager.file_mut(&metadata.host_name));
        let (new_id, chunk) = result_early_return!(
            file.next_chunk_and_id(NumId(option_early_return!(u128::checked_sub(id.0, 1))))
                .await
        );
        assert_eq!(new_id, id);
        self.stream_session_manager
            .push_resource_distribution(&metadata.host_name, id);
        let initial_message = Message::new(
            Peer::default(),
            id,
            None,
            MetadataKind::Stream(StreamMetadata::new(
                StreamPayloadKind::DistributionRequest(chunk),
                metadata.host_name.clone(),
            )),
            MessageDirection::OneHop,
        );
        self.insert_cached_stream_message(id, initial_message);
        let search_message = Message::new_search_request(
            id,
            SearchMetadata::new(
                self.outbound_gateway.myself,
                metadata.host_name.clone(),
                SearchMetadataKind::Distribution,
            ),
        );
        self.breadcrumb_service.remove_breadcrumb(&id);
        self.try_add_breadcrumb(id, None, Some(DISTRIBUTION_TTL_SECONDS));
        self.send_request(search_message).await;
        self.event_manager.put_event(
            TimeboundAction::LockDestsDistribution(metadata.host_name, id),
            DISTRIBUTION_SEARCH_TTL_SECONDS,
        );
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn send_distribution_request(&mut self, received_id: NumId, host_name: String) {
        self.cached_stream_messages.remove(&received_id);
        self.stream_session_manager
            .set_dests_remaining_distribution(&host_name);
        let dests = self
            .stream_session_manager
            .get_destinations_source_distribution(&host_name);
        if dests.is_empty() {
            return;
        }
        let file = option_early_return!(self.stream_session_manager.file_mut(&host_name));
        let (new_id, chunk) = result_early_return!(file.next_chunk_and_id(received_id).await);
        self.stream_session_manager
            .push_resource_distribution(&host_name, new_id);
        let message = Message::new(
            Peer::default(),
            new_id,
            None,
            MetadataKind::Stream(StreamMetadata::new(
                StreamPayloadKind::DistributionRequest(chunk),
                host_name,
            )),
            MessageDirection::OneHop,
        );
        let message = option_early_return!(self.send_checked(dests, message, true).await);
        self.insert_cached_stream_message(new_id, message);
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn get_direction(&mut self, message: &mut Message) -> PropagationDirection {
        if let MessageDirection::Response = message.direction() {
            return PropagationDirection::Reverse;
        }
        let id = message.id();
        let (origin, ttl, direction) = match message.metadata_mut() {
            MetadataKind::Search(metadata) => search::logic(
                id,
                self.outbound_gateway.myself,
                metadata,
                self.stream_session_manager.local_hosts(),
            ),
            MetadataKind::Discover(metadata) => {
                if self
                    .discover_peer_processor
                    .continue_propagating(metadata, self.outbound_gateway.myself)
                {
                    let origin = metadata.origin;
                    self.event_manager.put_event(
                        TimeboundAction::SendEarlyReturnMessage(message.clone()),
                        DPP_TTL_MILLIS / 2,
                    );
                    (origin, Some(DPP_TTL_MILLIS), PropagationDirection::Forward)
                } else {
                    metadata.kind = DpMessageKind::Response;
                    (
                        metadata.origin,
                        Some(DPP_TTL_MILLIS),
                        PropagationDirection::Reverse,
                    )
                }
            }
            _ => return PropagationDirection::Final,
        };

        if !self.try_add_breadcrumb(message.id(), message.only_sender(), ttl) {
            return PropagationDirection::Stop;
        }

        self.send_nat_heartbeats(origin);
        direction
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn try_add_breadcrumb(
        &mut self,
        id: NumId,
        dest: Option<Sender>,
        ttl: Option<Duration>,
    ) -> bool {
        if !self.breadcrumb_service.try_add_breadcrumb(id, dest) {
            return false;
        }
        self.event_manager.put_event(
            TimeboundAction::RemoveBreadcrumb(id),
            ttl.unwrap_or(SRP_TTL_SECONDS),
        );
        true
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn execute_final_action(&mut self, message: Message) {
        let (sender, id) = (message.only_sender(), message.id());
        let sender = if let Some(sender) = sender {
            Peer::from(sender)
        } else {
            self.outbound_gateway.myself
        };
        match message.into_metadata() {
            MetadataKind::Search(metadata) => self.final_action_search(metadata, id).await,
            MetadataKind::Discover(metadata) => self.final_action_discover(metadata, id),
            MetadataKind::Stream(metadata) => self.final_action_stream(metadata, sender, id).await,
            MetadataKind::Distribute(metadata) => {
                self.initial_distribution_request(id, metadata).await
            }
            _ => {}
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn final_action_search(&mut self, metadata: SearchMetadata, id: NumId) {
        let SearchMetadata {
            origin: _,
            hairpin,
            host_name,
            kind,
        } = metadata;
        let hairpin = hairpin.unwrap();
        match kind {
            SearchMetadataKind::Retrieval => {
                if !self
                    .stream_session_manager
                    .add_destination_source_retrieval(&host_name, hairpin)
                {
                    return;
                }
            }
            SearchMetadataKind::Distribution => {
                if !self
                    .stream_session_manager
                    .add_destination_source_distribution(&host_name, hairpin)
                {
                    return;
                }
            }
        };
        let send_checked_input = self.send_initial_stream_message(id, hairpin);
        self.pre_send_checked(send_checked_input).await;
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn final_action_discover(&mut self, metadata: DiscoverMetadata, id: NumId) {
        if metadata.peer_list.len() == metadata.hop_count.1 as usize {
            for peer in metadata.peer_list {
                self.add_new_peer(peer);
            }
            return;
        }
        let peer_len_curr_max = self.discover_peer_processor.peer_len_curr_max(&id);
        if peer_len_curr_max == 0 {
            self.event_manager
                .put_event(TimeboundAction::FinalizeDiscover(id), DPP_TTL_MILLIS);
        }
        self.discover_peer_processor
            .stage_message(id, metadata, peer_len_curr_max);
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn final_action_stream(&mut self, metadata: StreamMetadata, sender: Peer, id: NumId) {
        let response = match metadata.payload {
            StreamPayloadKind::Request(payload) => {
                if !self
                    .stream_session_manager
                    .sink_active_retrieval(&metadata.host_name)
                {
                    self.stream_session_manager
                        .new_sink_retrieval(metadata.host_name.clone())
                }
                self.stream_session_manager
                    .add_destination_sink(&metadata.host_name, sender);
                option_early_return!(
                    self.stream_session_manager
                        .retrieval_response_action(payload, metadata.host_name, id)
                        .await
                )
            }
            StreamPayloadKind::Response(payload) => {
                self.stream_session_manager
                    .finalize_resource_retrieval(&metadata.host_name, &id);
                self.cached_stream_messages.remove(&id);
                return option_early_return!(self.to_http_handlers.remove(&id))
                    .send(payload)
                    .unwrap();
            }
            StreamPayloadKind::DistributionRequest(bytes) => {
                if !self
                    .stream_session_manager
                    .sink_active_distribution(&metadata.host_name)
                {
                    self.stream_session_manager
                        .new_sink_distribution(metadata.host_name.clone());
                }
                option_early_return!(
                    self.stream_session_manager
                        .distribution_response_action(bytes, metadata.host_name, id)
                        .await
                )
            }
            StreamPayloadKind::DistributionResponse(response) => {
                return self
                    .handle_distribution_response(response, metadata.host_name, id, sender.id)
                    .await
            }
        };
        self.send_checked(vec![sender], response, true).await;
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn handle_distribution_response(
        &mut self,
        response: DistributionResponse,
        host_name: String,
        id: NumId,
        sender_id: NumId,
    ) {
        self.stream_session_manager
            .remove_dest_remaining_distribution(&host_name, &sender_id);
        if !self
            .stream_session_manager
            .dests_locked_distribution(&host_name)
            || !self
                .stream_session_manager
                .finalize_resource_distribution(&host_name, &id)
        {
            return;
        }
        match response {
            DistributionResponse::Continue => self.send_distribution_request(id, host_name).await,
            DistributionResponse::InstallError => panic!(),
            DistributionResponse::InstallOk => {
                self.cached_stream_messages.remove(&id);
                let (mut hop_count, distribution_id) = option_early_return!(self
                    .stream_session_manager
                    .curr_hop_count_and_distribution_id(&host_name));
                hop_count -= 1;
                if hop_count == 0 {
                    info!(myself = %self.outbound_gateway.myself.id, host_name, "Distribution finished");
                    return;
                }
                let distribution_message = Message::new(
                    Peer::default(),
                    distribution_id,
                    None,
                    MetadataKind::Distribute(DistributeMetadata::new(hop_count, host_name)),
                    MessageDirection::Request,
                );
                info!(myself = %self.outbound_gateway.myself.id, %distribution_id, "Next distribution");
                self.send_request(distribution_message).await;
            }
        }
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn send_initial_stream_message(
        &mut self,
        id: NumId,
        sender: Peer,
    ) -> Option<SendCheckedInput<Vec<Peer>>> {
        let Some(request) = self.cached_stream_messages.remove(&id) else {
            debug!("Cached stream retrieval message unavailable, checking for pending outbound message");
            let outbound_messages = self.cached_outbound_messages.get_mut(&sender.id)?;
            let cached_message = outbound_messages.iter_mut().find(|m| m.0.id() == id)?;
            cached_message.1.push(sender);
            return None;
        };
        Some((vec![sender], request, true, Some(id)))
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn send_nat_heartbeats(&mut self, peer: Peer) {
        if peer.id == self.outbound_gateway.myself.id
            || self.unconfirmed_peers.contains_key(&peer.id)
            || lock!(self.peer_ops).has_peer(peer.id)
        {
            return;
        }
        // println!("Start sending nat heartbeats to peer {:?} at {:?}", peer, self.outbound_gateway.myself);
        self.event_manager.put_event(
            TimeboundAction::RemoveUnconfirmedPeer(peer.id),
            SRP_TTL_SECONDS,
        );
        self.unconfirmed_peers.insert(peer.id, peer);
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn add_new_peer(&mut self, peer: Peer) {
        let peer_endpoint = peer.endpoint_pair.public_endpoint;
        let mut peers = lock!(self.peer_ops);
        peers.add_peer(
            peer,
            DiscoverPeerProcessor::get_score(
                self.outbound_gateway.myself.endpoint_pair.public_endpoint,
                peer_endpoint,
            ),
        );
        info!(peers = ?peers.peers().into_iter().map(|p| p.id).collect::<Vec<NumId>>());
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    fn handle_key_agreement(
        &mut self,
        message: KeyAgreementMessage,
        sender: SocketAddrV4,
    ) -> HandleKeyAgreementResult {
        let KeyAgreementMessage {
            public_key: public_key_peer,
            peer_id,
            direction,
        } = message;
        let public_key = option_early_return!(
            self.key_store.public_key(peer_id, &mut self.event_manager),
            HandleKeyAgreementResult::SymmetricKeyExists
        );
        result_early_return!(
            self.key_store
                .agree(peer_id, public_key_peer, &mut self.event_manager),
            HandleKeyAgreementResult::AgreementError
        );
        // TODO: Simplify
        let cached_messages = match direction {
            MessageDirectionAgreement::Request if peer_id == self.outbound_gateway.myself.id => {
                self.cached_outbound_messages.remove(&peer_id)
            }
            MessageDirectionAgreement::Request => {
                return HandleKeyAgreementResult::SendResponse(
                    Peer::from(Sender::new(sender, peer_id)),
                    public_key,
                )
            }
            MessageDirectionAgreement::Response => self.cached_outbound_messages.remove(&peer_id),
        };
        let mut send_checked_inputs = Vec::new();
        if let Some(cached_messages) = cached_messages {
            trace!("Sending cached outbound messages");
            for (message, remaining_dests) in cached_messages {
                let (to_be_chunked, is_stream_request) = message.to_be_chunked();
                let id = message.id();
                send_checked_inputs.push((
                    remaining_dests,
                    message,
                    to_be_chunked,
                    if is_stream_request { Some(id) } else { None },
                ));
            }
        }
        HandleKeyAgreementResult::SendCachedOutboundMessages(send_checked_inputs)
    }

    #[instrument(level = "trace", skip(self), fields(myself = ?self.outbound_gateway.myself))]
    async fn post_handle_key_agreement(&mut self, result: HandleKeyAgreementResult) {
        match result {
            HandleKeyAgreementResult::SendResponse(dest, public_key) => {
                self.outbound_gateway
                    .send_agreement(dest, public_key, MessageDirectionAgreement::Response)
                    .await;
            }
            HandleKeyAgreementResult::SendCachedOutboundMessages(messages) => {
                self.pre_send_checked_multiple(messages).await
            }
            _ => {}
        }
    }

    pub fn client_api_tx(&self) -> &mpsc::UnboundedSender<ClientApiRequest> {
        &self.client_api_tx
    }

    pub fn peer_ops(&self) -> &Arc<Mutex<PeerOps>> {
        &self.peer_ops
    }
}

#[derive(Debug)]
pub enum PropagationDirection {
    Forward,
    Reverse,
    Stop,
    Final,
}

#[derive(Debug)]
pub enum ClientApiRequest {
    ClearActiveSessions,
    AddHost(String),
    Message(Message),
}

#[derive(Debug)]
pub enum HandleKeyAgreementResult {
    SendResponse(Peer, Vec<u8>),
    SendCachedOutboundMessages(Vec<SendCheckedInput<Vec<Peer>>>),
    AgreementError,
    SymmetricKeyExists,
}

#[cfg(test)]
impl MessageStaging {
    pub fn session_manager(&mut self) -> &mut StreamSessionManager {
        &mut self.stream_session_manager
    }

    pub fn event_manager(&self) -> &TimeboundEventManager {
        &self.event_manager
    }

    pub fn key_store(&mut self) -> &mut KeyStore {
        &mut self.key_store
    }

    pub fn handle_key_agreement_pub(
        &mut self,
        message: KeyAgreementMessage,
        sender: SocketAddrV4,
    ) -> HandleKeyAgreementResult {
        self.handle_key_agreement(message, sender)
    }

    pub fn send_heartbeats_pub(&mut self) -> SendCheckedInput<FxHashSet<Peer>> {
        self.send_heartbeats()
    }

    pub async fn initial_retrieval_request_pub(
        &mut self,
        from_http_handler: (Message, mpsc::UnboundedSender<SerdeHttpResponse>),
    ) -> Option<SendCheckedInput<FxHashSet<Peer>>> {
        self.initial_retrieval_request(from_http_handler).await
    }

    pub fn get_direction_pub(&mut self, message: &mut Message) -> PropagationDirection {
        self.get_direction(message)
    }
}
